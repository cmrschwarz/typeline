use std::{
    collections::HashMap,
    io::{Read, Write},
    net::{SocketAddr, ToSocketAddrs},
    sync::Arc,
    time::Duration,
};

use mio::{event::Event, net::TcpStream, Events, Interest, Poll, Token};
use pki_types::InvalidDnsNameError;
use rustls::ClientConfig;
use scr_core::{
    job::JobData,
    liveness_analysis::OpOutputIdx,
    operators::{
        errors::OperatorApplicationError,
        operator::{Operator, OperatorBase, OperatorData},
        transform::{
            basic_transform_update, BasicUpdateData, Transform, TransformData,
            TransformId, TransformState,
        },
    },
    record_data::{
        field::FieldId,
        field_value_ref::FieldValueRef,
        iter_hall::{IterId, IterKind},
        push_interface::PushInterface,
        stream_value::{StreamValue, StreamValueData, StreamValueId},
    },
    smallbox,
    utils::{identity_hasher::BuildIdentityHasher, universe::CountedUniverse},
};
use std::io::ErrorKind as IoErrorKind;
use thiserror::Error;
use url::{ParseError, Url};

use crate::tls_client::{make_config, TlsSettings};

#[derive(Debug, Error)]
pub enum HttpRequestError {
    #[error(transparent)]
    Tls(#[from] rustls::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Dns(#[from] InvalidDnsNameError),
    #[error(transparent)]
    Url(#[from] ParseError),
    #[error("{0}")]
    Other(String),
}

pub struct OpHttpRequest {
    client_config: Arc<ClientConfig>,
}

pub struct Connection {
    hostname: String,
    socket: TcpStream,
    tls_conn: Option<rustls::ClientConnection>,
    stream_value: Option<StreamValueId>,
    header_parsed_until: u32,
    header_lines_count: u32,
    header_parsed: bool,
    request_data: Box<[u8]>,
    request_offset: usize,
    response_size: usize,
    expected_response_size: Option<usize>,
    remaining_socket_addresses: Vec<SocketAddr>,
}

impl Connection {
    fn reregister(&mut self, registry: &mio::Registry, token: mio::Token) {
        let interest = if let Some(c) = &self.tls_conn {
            let mut interest = mio::Interest::READABLE;
            if !self.request_done() || c.is_handshaking() || c.wants_write() {
                interest |= mio::Interest::WRITABLE;
            }
            interest
        } else {
            let mut interest = mio::Interest::READABLE;
            if !self.request_done() {
                interest |= mio::Interest::WRITABLE;
            }
            interest
        };
        registry
            .reregister(&mut self.socket, token, interest)
            .unwrap();
    }
    fn request_done(&self) -> bool {
        self.request_offset == self.request_data.len()
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
        jd: &mut JobData,
        _op_base: &OperatorBase,
        tf_state: &mut TransformState,
        _prebound_outputs: &HashMap<OpOutputIdx, FieldId, BuildIdentityHasher>,
    ) -> TransformData<'a> {
        let tf = TfHttpRequest {
            running_connections: CountedUniverse::default(),
            poll: Poll::new().unwrap(),
            events: Events::with_capacity(64),
            iter_id: jd.field_mgr.claim_iter(
                tf_state.input_field,
                IterKind::Transform(jd.tf_mgr.transforms.peek_claim_id()),
            ),
            stream_buffer_size: jd
                .get_transform_chain_from_tf_state(tf_state)
                .settings
                .stream_buffer_size,
            tls_config: self.client_config.clone(),
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
        let mut url_parsed = match Url::parse(url) {
            Ok(u) => u,
            Err(e) => match e {
                ParseError::RelativeUrlWithoutBase => {
                    if url.trim().starts_with('/') {
                        return Err(e.into());
                    }
                    let mut with_scheme = String::from("http://");
                    with_scheme.push_str(url);
                    Url::parse(&with_scheme).map_err(|_| e)?
                }
                e => return Err(e.into()),
            },
        };
        let mut https = false;

        match url_parsed.scheme() {
            "https" => {
                https = true;
            }
            "" | "http" => (),
            other => {
                let mut with_scheme = String::from("http://");
                with_scheme.push_str(url);
                // Last ditch effort to catch cases like 'localhost:8080'
                // where 'localhost' will be interpreted as a scheme at first.
                let Ok(url_parsed_with_scheme) = Url::parse(&with_scheme)
                else {
                    return Err(HttpRequestError::Other(format!(
                        "unsupported url scheme '{other}'"
                    )));
                };
                url_parsed = url_parsed_with_scheme;
            }
        }
        let Some(hostname) = url_parsed.host_str() else {
            return Err(ParseError::EmptyHost.into());
        };
        let port = url_parsed.port().unwrap_or(if https { 443 } else { 80 });

        let location = url_parsed.path();

        let mut socket_addresses = (hostname, port)
            .to_socket_addrs()?
            // We pop these, but we want to keep the 'intended' order.
            // Usually IPv6 is reported first by this etc...
            .rev()
            .collect::<Vec<_>>();

        let Some(first_addr) = socket_addresses.pop() else {
            return Err(HttpRequestError::Other(format!(
                "failed to resolve hostname '{hostname}'"
            )));
        };
        let token = self.running_connections.peek_claim_id();
        let (socket, tls_conn) =
            self.setup_connection(first_addr, https, hostname, token)?;

        let stream_value = bud.sv_mgr.stream_values.claim_with_value(
            StreamValue::from_data_unfinished(
                StreamValueData::Bytes(Vec::new()),
                false,
            ),
        );

        // Accept-Encoding: identity\r\n\
        let httpreq = format!(
            "GET {location} HTTP/1.1\r\n\
            Host: {hostname}\r\n\
            Connection: close\r\n\
            \r\n\
            ",
        )
        .into_bytes()
        .into_boxed_slice();

        self.running_connections.claim_with_value(Connection {
            hostname: hostname.to_owned(),
            socket,
            stream_value: Some(stream_value),
            tls_conn,
            header_parsed: false,
            header_lines_count: 0,
            header_parsed_until: 0,
            request_data: httpreq,
            request_offset: 0,
            expected_response_size: None,
            response_size: 0,
            remaining_socket_addresses: socket_addresses,
        });

        Ok(stream_value)
    }

    fn setup_connection(
        &self,
        address: SocketAddr,
        https: bool,
        hostname: &str,
        token: usize,
    ) -> Result<(TcpStream, Option<rustls::ClientConnection>), HttpRequestError>
    {
        let mut socket = TcpStream::connect(address)?;
        let mut tls_conn = None;
        if https {
            let server_name =
                pki_types::ServerName::try_from(hostname)?.to_owned();
            let tls = rustls::ClientConnection::new(
                self.tls_config.clone(),
                server_name,
            )?;
            tls_conn = Some(tls)
        }
        let interest = Interest::READABLE | Interest::WRITABLE;
        self.poll
            .registry()
            .register(&mut socket, Token(token), interest)
            .unwrap();
        Ok((socket, tls_conn))
    }

    fn basic_update(&mut self, mut bud: BasicUpdateData) -> (usize, bool) {
        let mut of = bud.field_mgr.fields[bud.output_field_id].borrow_mut();
        let mut inserter = of.iter_hall.varying_type_inserter();
        while let Some((v, rl, _)) = bud.iter.next_value(bud.match_set_mgr) {
            // we properly support fetching from the same url mutliple times,
            // but we don't bother making that fast
            for _ in 0..rl {
                match v {
                    FieldValueRef::Text(txt) => {
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
                    FieldValueRef::Bytes(_) => todo!(),
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

fn process_tls_event(
    event: &Event,
    c: &mut Connection,
    tgt: &mut Vec<u8>,
    buffer_size: usize,
) -> Result<bool, HttpRequestError> {
    let request_done = c.request_done();
    let tls_conn = c.tls_conn.as_mut().unwrap();
    let mut eof = event.is_read_closed();

    if event.is_readable() {
        let read = tls_conn.read_tls(&mut c.socket)?;
        if read == 0 {
            eof = true;
        }
        let state = tls_conn.process_new_packets()?;

        let bytes_to_read = state.plaintext_bytes_to_read();
        if bytes_to_read > 0 {
            tls_conn
                .reader()
                .take(buffer_size.min(bytes_to_read) as u64)
                .read_to_end(tgt)?;
        }
        eof |= state.peer_has_closed();
    }
    if event.is_writable() {
        if !request_done {
            c.request_offset += tls_conn
                .writer()
                .write(&c.request_data[c.request_offset..])?;
        }
        if tls_conn.wants_write() {
            tls_conn.write_tls(&mut c.socket)?;
        }
    }

    Ok(eof)
}

fn process_event(
    event: &Event,
    c: &mut Connection,
    tgt: &mut Vec<u8>,
    buffer_size: usize,
) -> Result<bool, HttpRequestError> {
    if c.tls_conn.is_some() {
        return process_tls_event(event, c, tgt, buffer_size);
    }

    if !c.request_done() && event.is_writable() {
        c.request_offset +=
            c.socket.write(&c.request_data[c.request_offset..])?;

        if c.request_done() {
            c.socket.flush()?;
        }
    }

    let mut eof = event.is_read_closed();
    if event.is_readable() {
        match std::io::copy(&mut c.socket, tgt) {
            Ok(0) => eof = true,
            Ok(_) => (),
            Err(e) if e.kind() == IoErrorKind::WouldBlock => (),
            Err(e) => return Err(e.into()),
        }
    }
    Ok(eof)
}

lazy_static::lazy_static! {
    static ref CONTENT_LENGTH_REGEX: regex::bytes::Regex = regex::bytes::RegexBuilder::new(
        r"^\s*Content-Length\s*:\s*([0-9]+)\s*\r?$"
    ).case_insensitive(true).build().unwrap();
}

fn header_completed(req: &mut Connection, buf: &[u8]) -> bool {
    let mut parsed_until = req.header_parsed_until as usize;
    let mut end_reached = false;
    while let Some(i) = memchr::memchr(b'\n', &buf[parsed_until..]) {
        if i == 0 && req.header_lines_count > 0 {
            parsed_until += i + 1;
            end_reached = true;
            break;
        }
        let sequence =
            &buf[parsed_until + i + 1..(parsed_until + i + 3).min(buf.len())];
        if sequence == b"\r\n" {
            parsed_until += i + 1 + 2;
            end_reached = true;
            break;
        }
        if &sequence[0..2.min(sequence.len())] == b"\n" {
            parsed_until += i + 1 + 1;
            end_reached = true;
            break;
        }
        let line = &buf[parsed_until..parsed_until + i];
        // PERF: don't allocate
        if let Some(cl_match) = CONTENT_LENGTH_REGEX.captures(line) {
            let len_seq = cl_match.get(1).unwrap().as_bytes();
            if let Ok(len_str) = std::str::from_utf8(len_seq) {
                if let Ok(len) = len_str.parse::<usize>() {
                    req.expected_response_size = Some(len);
                }
            }
        }
        parsed_until += i + 1;
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
        let op_id = jd.tf_mgr.transforms[tf_id].op_id.unwrap();
        if let Err(e) = self
            .poll
            .poll(&mut self.events, Some(Duration::from_millis(1)))
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
            let mut req = &mut self.running_connections[token];
            let sv_id = req.stream_value.unwrap();
            let sv = &mut jd.sv_mgr.stream_values[sv_id];

            let StreamValueData::Bytes(buf) = &mut sv.data else {
                unreachable!()
            };
            if req.header_parsed && !sv.is_buffered {
                buf.clear();
            }
            let buf_len_before = buf.len();
            let mut update = false;
            match process_event(event, req, buf, self.stream_buffer_size) {
                Err(HttpRequestError::Io(e))
                    if e.kind() == std::io::ErrorKind::ConnectionRefused
                        && !req.remaining_socket_addresses.is_empty() =>
                {
                    let addr = req.remaining_socket_addresses.pop().unwrap();
                    let imm_req = &self.running_connections[token];
                    let https = imm_req.tls_conn.is_some();
                    let hostname = &imm_req.hostname;
                    let result =
                        self.setup_connection(addr, https, hostname, token);
                    req = &mut self.running_connections[token];
                    match result {
                        Ok((socket, tls_conn)) => {
                            req.tls_conn = tls_conn;
                            req.socket = socket;
                        }
                        Err(e) => {
                            sv.data = StreamValueData::Error(
                                // ENHANCE: include number of addresses tried
                                OperatorApplicationError::new_s(
                                    format!("HTTP GET request failed: {e}"),
                                    op_id,
                                ),
                            );
                            sv.done = true;
                        }
                    }
                }
                Err(e) => {
                    sv.data = StreamValueData::Error(
                        OperatorApplicationError::new_s(
                            format!("IO Error in HTTP GET Request: {e}"),
                            op_id,
                        ),
                    );
                    sv.done = true;
                }
                Ok(eof) => {
                    let len_read = buf.len() - buf_len_before;
                    req.response_size += len_read;
                    update = len_read > 0 || eof;
                    if update && !req.header_parsed {
                        if header_completed(req, buf) {
                            // TODO: proper parsing
                            buf.drain(0..req.header_parsed_until as usize);
                            req.header_parsed = true;
                            if let Some(len) = &mut req.expected_response_size
                            {
                                *len += req.header_parsed_until as usize;
                            }
                        } else {
                            update = false;
                        }
                    }
                    if eof {
                        if !req.header_parsed {
                            sv.data = StreamValueData::Error(
                                OperatorApplicationError::new(
                                    if req.response_size == 0 {
                                        "HTTP GET got no response"
                                    } else {
                                        "HTTP GET got invalid response"
                                    },
                                    jd.tf_mgr.transforms[tf_id].op_id.unwrap(),
                                ),
                            );
                        }
                        sv.done = true;
                    }
                }
            }
            if let Some(len) = req.expected_response_size {
                if let StreamValueData::Bytes(buf) = &mut sv.data {
                    if req.header_parsed && req.response_size >= len {
                        if let Some(tls) = &mut req.tls_conn {
                            tls.send_close_notify();
                        } else {
                            let _ignored =
                                req.socket.shutdown(std::net::Shutdown::Both);
                        }
                        if req.response_size > len {
                            let handled = req.response_size - buf.len();
                            buf.truncate(len - handled);
                        };
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

#[allow(non_snake_case)]
pub fn create_op_GET_with_opts(
    tls_settings: TlsSettings,
) -> Result<OperatorData, rustls::Error> {
    Ok(OperatorData::Custom(smallbox![OpHttpRequest {
        client_config: make_config(tls_settings)?
    }]))
}

#[allow(non_snake_case)]
pub fn create_op_GET() -> OperatorData {
    create_op_GET_with_opts(TlsSettings::default()).unwrap()
}
