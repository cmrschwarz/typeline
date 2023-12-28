use std::{
    collections::HashMap,
    io::{Read, Write},
    net::ToSocketAddrs,
    sync::Arc,
    time::Duration,
};

use mio::{event::Event, net::TcpStream, Events, Interest, Poll, Token};
use pki_types::InvalidDnsNameError;
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
        push_interface::PushInterface,
        stream_value::{StreamValue, StreamValueData, StreamValueId},
        typed::TypedValue,
    },
    smallbox,
    utils::{identity_hasher::BuildIdentityHasher, universe::CountedUniverse},
};
use thiserror::Error;

use crate::tls_client::{make_config, TlsSettings};

#[derive(Debug, Error)]
pub enum HttpRequestError {
    #[error(transparent)]
    Tls(#[from] rustls::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Dns(#[from] InvalidDnsNameError),
}

#[derive(Default)]
pub struct OpHttpRequest {}

pub struct Connection {
    socket: TcpStream,
    tls_conn: Option<rustls::ClientConnection>,
    stream_value: Option<StreamValueId>,
    interest: mio::Interest,
    header_parsed_until: u32,
    header_lines_count: u32,
    header_parsed: bool,
}

impl Connection {
    fn reregister(&mut self, registry: &mio::Registry, token: mio::Token) {
        let Some(conn) = &self.tls_conn else {
            return;
        };
        let new_interest = match (conn.wants_read(), conn.wants_write()) {
            (true, true) => mio::Interest::READABLE | mio::Interest::WRITABLE,
            (false, true) => mio::Interest::WRITABLE,
            _ => mio::Interest::READABLE, // interest can't be empty
        };

        if new_interest != self.interest {
            self.interest = new_interest;
            registry
                .reregister(&mut self.socket, token, new_interest)
                .unwrap();
        }
    }
}

pub struct TfHttpRequest {
    iter_id: IterId,
    running_connections: CountedUniverse<usize, Connection>,
    tls_config: Arc<rustls::ClientConfig>,
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
            running_connections: CountedUniverse::default(),
            poll: Poll::new().unwrap(),
            events: Events::with_capacity(64),
            iter_id: sess.field_mgr.claim_iter(tf_state.input_field),
            stream_buffer_size: sess
                .get_transform_chain_from_tf_state(tf_state)
                .settings
                .stream_buffer_size,
            tls_config: make_config(&TlsSettings::default()), // TODO
        };
        TransformData::Custom(smallbox!(tf))
    }
}

impl TfHttpRequest {
    fn register_steam(
        &mut self,
        url: &str,
        bud: &mut BasicUpdateData,
    ) -> Result<StreamValueId, HttpRequestError> {
        let port = 443; // TODO
        let hostname = url;
        let sock_addr = (hostname, port).to_socket_addrs()?.next().unwrap();
        let mut stream = TcpStream::connect(sock_addr)?;

        let server_name =
            pki_types::ServerName::try_from(hostname)?.to_owned();

        let mut tls_conn = rustls::ClientConnection::new(
            self.tls_config.clone(),
            server_name,
        )?;

        let httpreq = format!(
            "GET / HTTP/1.1\r\nHost: {hostname}\r\nConnection: \
                               close\r\nAccept-Encoding: identity\r\n\r\n",
        );
        tls_conn.writer().write_all(httpreq.as_bytes())?;

        let token = self.running_connections.peek_claim_id();

        let interest = Interest::READABLE | Interest::WRITABLE;

        self.poll
            .registry()
            .register(&mut stream, Token(token), interest)
            .unwrap();

        let stream_value = bud.sv_mgr.stream_values.claim_with_value(
            StreamValue::new(StreamValueData::Bytes(Vec::new()), false, false),
        );

        self.running_connections.claim_with_value(Connection {
            socket: stream,
            stream_value: Some(stream_value),
            tls_conn: Some(tls_conn),
            interest,
            header_parsed: false,
            header_lines_count: 0,
            header_parsed_until: 0,
        });

        Ok(stream_value)
    }

    fn basic_update(&mut self, mut bud: BasicUpdateData) -> (usize, bool) {
        let mut of = bud.field_mgr.fields[bud.output_field_id].borrow_mut();
        let mut inserter = of.iter_hall.varying_type_inserter();
        while let Some((v, rl, _)) = bud.iter.next_value(bud.match_set_mgr) {
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
        if !self.running_connections.is_empty() {
            bud.tf_mgr.make_stream_producer(bud.tf_id);
        }
        (bud.batch_size, bud.ps.input_done)
    }
}

fn process_tls(
    event: &Event,
    c: &mut Connection,
    tgt: &mut Vec<u8>,
    buffer_size: usize,
) -> Result<bool, HttpRequestError> {
    let tls_conn = c.tls_conn.as_mut().unwrap();

    let mut eof = event.is_read_closed();

    if !eof && event.is_readable() {
        match tls_conn.read_tls(&mut c.socket) {
            Err(e) => {
                if e.kind() != std::io::ErrorKind::WouldBlock {
                    return Err(e.into());
                }
            }
            Ok(0) => {
                // if we are ready, but there's no data it means eof
                eof = true;
            }
            Ok(_) => (),
        }
        let state = tls_conn.process_new_packets()?;
        eof |= state.peer_has_closed();
        std::io::copy(
            &mut tls_conn
                .reader()
                .take(buffer_size.min(state.plaintext_bytes_to_read()) as u64),
            tgt,
        )?;
    }
    if event.is_writable() {
        tls_conn.write_tls(&mut c.socket)?;
    }

    Ok(eof)
}

fn header_completed(req: &mut Connection, buf: &Vec<u8>) -> bool {
    let mut parsed_until = req.header_parsed_until as usize;
    let mut end_reached = false;
    while let Some(i) = memchr::memchr(b'\n', &buf[parsed_until..]) {
        let sequence =
            &buf[parsed_until + i + 1..(parsed_until + i + 3).min(buf.len())];
        parsed_until += i + 1;

        if sequence == b"\r\n" {
            parsed_until += 2;
            end_reached = true;
            break;
        }
        if &sequence[0..2.min(sequence.len())] == b"\n" {
            parsed_until += 1;
            end_reached = true;
            break;
        }
        req.header_lines_count += 1;
    }
    req.header_parsed_until = parsed_until as u32;
    end_reached
}

impl Transform for TfHttpRequest {
    fn display_name(
        &self,
    ) -> scr_core::operators::transform::DefaultTransformName {
        "http-get".into()
    }

    fn update(&mut self, jd: &mut JobData, tf_id: TransformId) {
        basic_transform_update(jd, tf_id, [], self.iter_id, |bud| {
            self.basic_update(bud)
        })
    }
    fn stream_producer_update(
        &mut self,
        jd: &mut JobData,
        tf_id: TransformId,
    ) {
        if let Err(e) = self
            .poll
            .poll(&mut self.events, Some(Duration::from_secs(0)))
        {
            for pe in self.running_connections.iter_mut() {
                let _ = self.poll.registry().deregister(&mut pe.socket);
                let _ = pe.socket.shutdown(std::net::Shutdown::Both);
                let sv_id = pe.stream_value.unwrap();
                let sv = &mut jd.sv_mgr.stream_values[sv_id];
                sv.data =
                    StreamValueData::Error(OperatorApplicationError::new_s(
                        format!("IO Error in HTTP GET Request: {e}"),
                        jd.tf_mgr.transforms[tf_id].op_id.unwrap(),
                    ));
                sv.done = true;
                jd.sv_mgr.inform_stream_value_subscribers(sv_id);
                jd.sv_mgr.drop_field_value_subscription(sv_id, None);
            }
            self.running_connections.clear();
            return;
        };

        for event in &self.events {
            let Token(token) = event.token();
            let req = &mut self.running_connections[token];
            let sv_id = req.stream_value.unwrap();
            let sv = &mut jd.sv_mgr.stream_values[sv_id];

            let StreamValueData::Bytes(buf) = &mut sv.data else {
                unreachable!()
            };
            if req.header_parsed && sv.bytes_are_chunk {
                buf.clear();
            }
            let buf_len_before = buf.len();
            let mut update = false;
            match process_tls(event, req, buf, self.stream_buffer_size) {
                Err(e) => {
                    sv.data = StreamValueData::Error(
                        OperatorApplicationError::new_s(
                            format!("IO Error in HTTP GET Request: {e}"),
                            jd.tf_mgr.transforms[tf_id].op_id.unwrap(),
                        ),
                    );
                    sv.done = true;
                }
                Ok(eof) => {
                    update = buf_len_before < buf.len() || eof;
                    if update && !req.header_parsed {
                        if header_completed(req, buf) {
                            // TODO: proper parsing
                            buf.drain(0..req.header_parsed_until as usize);
                            req.header_parsed = true;
                        } else {
                            update = false;
                        }
                    }
                    if eof {
                        sv.done = true;
                    }
                }
            }
            let done = sv.done;
            if done || update {
                jd.sv_mgr.inform_stream_value_subscribers(sv_id);
            }
            if done {
                let _ = self.poll.registry().deregister(&mut req.socket);
                self.running_connections.release(token);
                jd.sv_mgr.drop_field_value_subscription(sv_id, None);
            } else {
                req.reregister(self.poll.registry(), mio::Token(token));
            }
        }
        if !self.running_connections.is_empty() {
            jd.tf_mgr.make_stream_producer(tf_id);
        }
    }
}
