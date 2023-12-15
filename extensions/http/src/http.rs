use std::{collections::HashMap, net::ToSocketAddrs, time::Duration};

use mio::{net::TcpStream, Events, Interest, Poll, Token};
use scr_core::{
    job_session::JobData,
    liveness_analysis::OpOutputIdx,
    operators::{
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
        };
        TransformData::Custom(smallbox!(tf))
    }
}

impl TfHttpRequest {
    fn register_steam(
        &mut self,
        url: &str,
        bud: &mut BasicUpdateData,
    ) -> Result<(), std::io::Error> {
        let addr = url.to_socket_addrs()?.next().unwrap();
        let mut stream = TcpStream::connect(addr).unwrap();
        // Register the stream with `Poll`
        self.poll
            .registry()
            .register(
                &mut stream,
                Token(self.pending_requests.len()),
                Interest::READABLE | Interest::WRITABLE,
            )
            .unwrap();
        let stream_value = bud.sv_mgr.stream_values.claim_with_value(
            StreamValue::new(StreamValueData::Bytes(Vec::new()), false, false),
        );
        self.pending_requests.push(Request {
            stream,
            stream_value: Some(stream_value),
        });
        Ok(())
    }

    fn basic_update<'a>(
        &mut self,
        mut bud: BasicUpdateData,
        iter: &mut AutoDerefIter<
            'a,
            BoundedIter<'a, Iter<'a, DestructuredFieldDataRef<'a>>>,
        >,
    ) -> (usize, bool) {
        // Wait for the socket to become ready. This has to happens in a loop to
        // handle spurious wakeups.
        self.poll
            .poll(&mut self.events, Some(Duration::from_micros(1)))
            .unwrap(); // TODO: handle errors?

        for event in &self.events {
            if event.token() == Token(0) && event.is_writable() {
                // The socket connected (probably, it could still be a spurious
                // wakeup)
                return (0, false);
            }
        }

        while let Some((v, rl, _)) = iter.next_value(bud.match_set_mgr) {
            // we properly support fetching from the same url mutliple times,
            // but we don't bother making that fast
            for _ in 0..rl {
                match v {
                    TypedValue::TextInline(txt) => {
                        self.register_steam(txt, &mut bud).unwrap();
                    }
                    TypedValue::BytesBuffer(_)
                    | TypedValue::BytesInline(_) => todo!(),
                    _ => unimplemented!(),
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
        _jd: &mut JobData,
        _tf_id: TransformId,
    ) {
    }
    fn handle_stream_value_update(
        &mut self,
        _sess: &mut JobData,
        _tf_id: TransformId,
        _sv_id: StreamValueId,
        _custom: usize,
    ) {
    }
}
