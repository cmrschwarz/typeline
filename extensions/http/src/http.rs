use std::{
    collections::HashMap, io::Read, iter, net::ToSocketAddrs, time::Duration,
};

use mio::{net::TcpStream, Events, Interest, Poll, Token};
use scr_core::{
    job_session::JobData,
    liveness_analysis::OpOutputIdx,
    operators::{
        errors::OperatorApplicationError,
        operator::{Operator, OperatorBase},
        transform::{
            basic_transform_update, BasicUpdateData, Transform, TransformData,
            TransformId, TransformState,
        },
    },
    record_data::{
        field::FieldId,
        iter_hall::IterId,
        iters::{BoundedIter, DestructuredFieldDataRef, Iter},
        push_interface::PushInterface,
        ref_iter::AutoDerefIter,
        stream_value::{StreamValue, StreamValueData, StreamValueId},
        typed::TypedValue,
    },
    smallbox,
    utils::identity_hasher::BuildIdentityHasher,
};

#[derive(Default)]
pub struct OpHttpRequest {}

#[allow(unused)] // TOOD
pub struct Request {
    stream: TcpStream,
    stream_value: Option<StreamValueId>,
}

pub struct TfHttpRequest {
    iter_id: IterId,
    pending_requests: Vec<Request>,
    poll: Poll,
    events: Events,
    stream_buffer_size: usize,
}

impl Operator for OpHttpRequest {
    fn default_name(
        &self,
    ) -> scr_core::operators::operator::DefaultOperatorName {
        "http-get".into()
    }

    fn output_count(&self, _op_base: &OperatorBase) -> usize {
        1
    }

    fn has_dynamic_outputs(&self, _op_base: &OperatorBase) -> bool {
        false
    }

    fn update_variable_liveness(
        &self,
        _ld: &mut scr_core::liveness_analysis::LivenessData,
        _bb_id: scr_core::liveness_analysis::BasicBlockId,
        _bb_offset: u32,
        access_flags: &mut scr_core::liveness_analysis::AccessFlags,
    ) {
        access_flags.non_stringified_input_access = false;
        access_flags.may_dup_or_drop = false;
    }

    fn build_transform<'a>(
        &'a self,
        sess: &mut JobData,
        _op_base: &OperatorBase,
        tf_state: &mut TransformState,
        _prebound_outputs: &HashMap<OpOutputIdx, FieldId, BuildIdentityHasher>,
    ) -> TransformData<'a> {
        let tf = TfHttpRequest {
            pending_requests: Vec::new(),
            poll: Poll::new().unwrap(),
            events: Events::with_capacity(64),
            iter_id: sess.field_mgr.claim_iter(tf_state.input_field),
            stream_buffer_size: sess
                .get_transform_chain_from_tf_state(tf_state)
                .settings
                .stream_buffer_size,
        };
        TransformData::Custom(smallbox!(tf))
    }
}

impl TfHttpRequest {
    fn register_steam(
        &mut self,
        url: &str,
        bud: &mut BasicUpdateData,
    ) -> Result<StreamValueId, std::io::Error> {
        let addr = url.to_socket_addrs()?.next().unwrap();
        let mut stream = TcpStream::connect(addr).unwrap();
        // Register the stream with `Poll`
        self.poll
            .registry()
            .register(
                &mut stream,
                Token(self.pending_requests.len()),
                Interest::READABLE,
            )
            .unwrap();
        let stream_value = bud.sv_mgr.stream_values.claim_with_value(
            StreamValue::new(StreamValueData::Bytes(Vec::new()), false, false),
        );
        self.pending_requests.push(Request {
            stream,
            stream_value: Some(stream_value),
        });
        Ok(stream_value)
    }

    fn basic_update<'a>(
        &mut self,
        mut bud: BasicUpdateData,
        iter: &mut AutoDerefIter<
            'a,
            BoundedIter<'a, Iter<'a, DestructuredFieldDataRef<'a>>>,
        >,
    ) -> (usize, bool) {
        let mut of = bud.field_mgr.fields[bud.output_field_id].borrow_mut();
        let mut inserter = of.iter_hall.varying_type_inserter();
        while let Some((v, rl, _)) = iter.next_value(bud.match_set_mgr) {
            // we properly support fetching from the same url mutliple times,
            // but we don't bother making that fast
            for _ in 0..rl {
                match v {
                    TypedValue::TextInline(txt) => {
                        match self.register_steam(txt, &mut bud) {
                            Ok(sv_id) => inserter
                                .push_stream_value_id(sv_id, 1, true, false),
                            Err(e) => inserter.push_error(
                                OperatorApplicationError::new_s(
                                    format!("HTTP GET request failed: {e}"),
                                    bud.tf_mgr.transforms[bud.tf_id]
                                        .op_id
                                        .unwrap(),
                                ),
                                1,
                                true,
                                false,
                            ),
                        }
                    }
                    TypedValue::BytesBuffer(_)
                    | TypedValue::BytesInline(_) => todo!(),
                    _ => inserter.push_error(
                        OperatorApplicationError::new_s(
                            format!(
                                "unsupported datatype for http-get url: {}",
                                v.repr()
                            ),
                            bud.tf_mgr.transforms[bud.tf_id].op_id.unwrap(),
                        ),
                        1,
                        true,
                        false,
                    ),
                }
            }
        }
        (0, false)
    }
}

impl Transform for TfHttpRequest {
    fn display_name(
        &self,
    ) -> scr_core::operators::transform::DefaultTransformName {
        "http-get".into()
    }

    fn update(&mut self, jd: &mut JobData, tf_id: TransformId) {
        basic_transform_update(jd, tf_id, [], self.iter_id, |bud, iter| {
            self.basic_update(bud, iter)
        })
    }
    fn stream_producer_update(
        &mut self,
        jd: &mut JobData,
        tf_id: TransformId,
    ) {
        self.poll
            .poll(&mut self.events, Some(Duration::from_micros(1)))
            .unwrap(); // TODO: handle errors?

        for event in &self.events {
            let Token(req_id) = event.token();
            debug_assert!(
                event.is_read_closed()
                    || event.is_readable()
                    || event.is_error(),
            );
            let req = &mut self.pending_requests[req_id];
            let sv_id = req.stream_value.unwrap();
            let sv = &mut jd.sv_mgr.stream_values[sv_id];
            let StreamValueData::Bytes(buf) = &mut sv.data else {
                unreachable!()
            };
            if sv.bytes_are_chunk {
                buf.clear();
            }
            let buf_len_before = buf.len();
            buf.extend(iter::repeat(0).take(self.stream_buffer_size));
            match req.stream.read(&mut buf[buf_len_before..]) {
                Err(e) => {
                    sv.data = StreamValueData::Error(
                        OperatorApplicationError::new_s(
                            format!("IO Error in HTTP GET Request: {e}"),
                            jd.tf_mgr.transforms[tf_id].op_id.unwrap(),
                        ),
                    );
                }
                Ok(bytes_read) => {
                    buf.truncate(buf_len_before + bytes_read);
                }
            }
            jd.sv_mgr.inform_stream_value_subscribers(sv_id);
        }
    }
}
