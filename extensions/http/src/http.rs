use std::{
    collections::{hash_map::Entry, HashMap},
    io::{Read, Write},
    net::{SocketAddr, ToSocketAddrs},
    sync::Arc,
    time::Duration,
};

use mio::{event::Event, net::TcpStream, Events, Interest, Poll, Token};
use once_cell::sync::Lazy;
use pki_types::InvalidDnsNameError;
use rustls::ClientConfig;
use scr_core::{
    context::SessionData,
    job::{Job, JobData, TransformManager},
    liveness_analysis::{
        AccessFlags, BasicBlockId, LivenessData, OpOutputIdx,
        OperatorCallEffect,
    },
    operators::{
        errors::OperatorApplicationError,
        operator::{
            OffsetInChain, Operator, OperatorData, OperatorId,
            PreboundOutputsMap, TransformInstatiation,
        },
        transform::{Transform, TransformData, TransformId, TransformState},
        utils::basic_transform_update::{
            basic_transform_update, BasicUpdateData,
        },
    },
    options::chain_settings::SettingStreamBufferSize,
    record_data::{
        field_data::{FieldData, RunLength},
        field_value_ref::FieldValueRef,
        iter_hall::IterId,
        push_interface::PushInterface,
        stream_value::{
            StreamValue, StreamValueBufferMode, StreamValueData,
            StreamValueDataType, StreamValueId, StreamValueManager,
        },
        varying_type_inserter::VaryingTypeInserter,
    },
    smallbox,
    utils::universe::CountedUniverse,
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
    socket_established: bool,
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
    #[allow(unused)] // TODO: do we ever need this?
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
    // TODO: lru
    dns_cache: HashMap<(String, u16), Vec<SocketAddr>>,
    running_connections: CountedUniverse<usize, Connection>,
    tls_config: Arc<rustls::ClientConfig>,
    poll: Poll,
    events: Events,
    stream_buffer_size: usize,
}

impl Operator for OpHttpRequest {
    fn default_name(&self) -> scr_core::operators::operator::OperatorName {
        "http-get".into()
    }

    fn output_count(&self, _sess: &SessionData, _op_id: OperatorId) -> usize {
        1
    }

    fn has_dynamic_outputs(
        &self,
        _sess: &SessionData,
        _op_id: OperatorId,
    ) -> bool {
        false
    }

    fn update_variable_liveness(
        &self,
        _sess: &SessionData,
        _ld: &mut LivenessData,
        access_flags: &mut AccessFlags,
        _op_offset_after_last_write: OffsetInChain,
        _op_id: OperatorId,
        _bb_id: BasicBlockId,
        _input_field: OpOutputIdx,
        _output_offset: usize,
    ) -> Option<(OpOutputIdx, OperatorCallEffect)> {
        access_flags.non_stringified_input_access = false;
        access_flags.may_dup_or_drop = false;
        None
    }

    fn build_transforms<'a>(
        &'a self,
        job: &mut Job,
        tf_state: &mut TransformState,
        _op_id: OperatorId,
        _prebound_outputs: &PreboundOutputsMap,
    ) -> TransformInstatiation<'a> {
        let tf = TfHttpRequest {
            running_connections: CountedUniverse::default(),
            poll: Poll::new().unwrap(),
            dns_cache: HashMap::new(),
            events: Events::with_capacity(64),
            iter_id: job.job_data.claim_iter_for_tf_state(tf_state),
            tls_config: self.client_config.clone(),
            stream_buffer_size: job
                .job_data
                .get_setting_from_tf_state::<SettingStreamBufferSize>(
                    tf_state,
                ),
        };
        TransformInstatiation::Single(TransformData::Custom(smallbox!(tf)))
    }
}

enum EventResult {
    Spurious,
    Update,
    Done,
    TryNextIp,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Proto {
    Http,
    Https,
}

impl TfHttpRequest {
    fn lookup_socket_addresses(
        &mut self,
        url: &str,
    ) -> Result<(Url, Vec<SocketAddr>, Proto), HttpRequestError> {
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

        let socket_addresses =
            match self.dns_cache.entry((hostname.to_owned(), port)) {
                Entry::Occupied(e) => e.get().clone(),
                // PERF: this is synchronuos, and slow
                Entry::Vacant(e) => e
                    .insert(
                        (hostname, port)
                            .to_socket_addrs()?
                            // We pop these, but we want to keep the 'intended'
                            // order. Usually IPv6 is
                            // reported first by this etc...
                            .rev()
                            .collect::<Vec<_>>(),
                    )
                    .clone(),
            };

        let proto = if https { Proto::Https } else { Proto::Http };

        Ok((url_parsed, socket_addresses, proto))
    }

    fn register_stream(
        &mut self,
        url: &str,
        tf_mgr: &TransformManager,
        tf_id: TransformId,
        sv_mgr: &mut StreamValueManager,
        rl: RunLength,
        inserter: &mut VaryingTypeInserter<&'_ mut FieldData>,
    ) {
        fn fail(
            inserter: &mut VaryingTypeInserter<&'_ mut FieldData>,
            tf_mgr: &TransformManager,
            tf_id: TransformId,
            e: HttpRequestError,
            rl: RunLength,
        ) {
            inserter.push_error(
                OperatorApplicationError::new_s(
                    format!("HTTP GET request failed: {e}"),
                    tf_mgr.transforms[tf_id].op_id.unwrap(),
                ),
                rl as usize,
                true,
                false,
            );
        }

        let (url, mut socket_addresses, proto) =
            match self.lookup_socket_addresses(url) {
                Ok(v) => v,
                Err(e) => {
                    fail(inserter, tf_mgr, tf_id, e, rl);
                    return;
                }
            };

        let hostname = url.host_str().unwrap_or("");
        let location = url.path();

        let Some(first_addr) = socket_addresses.pop() else {
            fail(
                inserter,
                tf_mgr,
                tf_id,
                HttpRequestError::Other(format!(
                    "failed to resolve hostname '{hostname}'",
                )),
                rl,
            );
            return;
        };

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

        for _ in 0..rl {
            let token = self.running_connections.peek_claim_id();
            let (socket, tls_conn) = match self.setup_connection(
                first_addr,
                proto == Proto::Https,
                hostname,
                token,
            ) {
                Ok(v) => v,
                Err(e) => {
                    fail(inserter, tf_mgr, tf_id, e, 1);
                    continue;
                }
            };

            let stream_value_id =
                sv_mgr.claim_stream_value(StreamValue::from_data(
                    Some(StreamValueDataType::Bytes),
                    StreamValueData::Bytes {
                        data: Arc::new(Vec::new()),
                        range: 0..0,
                    },
                    StreamValueBufferMode::Stream,
                    false,
                ));

            self.running_connections.claim_with_value(Connection {
                hostname: hostname.to_owned(),
                socket_established: false,
                socket,
                stream_value: Some(stream_value_id),
                tls_conn,
                header_parsed: false,
                header_lines_count: 0,
                header_parsed_until: 0,
                request_data: httpreq.clone(),
                request_offset: 0,
                expected_response_size: None,
                response_size: 0,
                remaining_socket_addresses: socket_addresses.clone(),
            });
            inserter.push_stream_value_id(stream_value_id, 1, true, false);
        }
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

    fn basic_update(&mut self, bud: BasicUpdateData) -> (usize, bool) {
        let mut of = bud.field_mgr.fields[bud.output_field_id].borrow_mut();
        let mut inserter = of.iter_hall.varying_type_inserter();
        let mut bs_rem = bud.batch_size;
        while let Some((v, rl, _)) =
            bud.iter.next_value(bud.match_set_mgr, bs_rem)
        {
            // we properly support fetching from the same url mutliple times,
            // but we don't bother making that fast
            match v {
                FieldValueRef::Text(txt) => {
                    self.register_stream(
                        txt,
                        bud.tf_mgr,
                        bud.tf_id,
                        bud.sv_mgr,
                        rl,
                        &mut inserter,
                    );
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
                    rl as usize,
                    true,
                    false,
                ),
            }
            bs_rem -= rl as usize;
        }
        if !self.running_connections.is_empty() {
            bud.tf_mgr.make_stream_producer(bud.tf_id);
        }
        (bud.batch_size, bud.ps.input_done)
    }

    fn handle_event(
        req: &mut Connection,
        op_id: OperatorId,
        event: &Event,
        buf: &mut Vec<u8>,
        buffer_size: usize,
    ) -> Result<EventResult, Arc<OperatorApplicationError>> {
        let buf_len_before = buf.len();
        let mut result = EventResult::Spurious;
        match process_event(event, req, buf, buffer_size) {
            Err(e) => {
                if !req.socket_established
                    && !req.remaining_socket_addresses.is_empty()
                {
                    return Ok(EventResult::TryNextIp);
                }

                return Err(Arc::new(OperatorApplicationError::new_s(
                    format!("IO Error in HTTP GET Request: {e}"),
                    op_id,
                )));
            }
            Ok(eof) => {
                req.socket_established = true;
                let len_read = buf.len() - buf_len_before;
                req.response_size += len_read;
                let update = len_read > 0 || eof;
                if update && !req.header_parsed && header_completed(req, buf) {
                    // TODO: proper parsing
                    buf.drain(0..req.header_parsed_until as usize);
                    req.header_parsed = true;
                    if let Some(len) = &mut req.expected_response_size {
                        *len += req.header_parsed_until as usize;
                    }
                    result = EventResult::Update;
                }
                if eof {
                    if !req.header_parsed {
                        return Err(Arc::new(OperatorApplicationError::new(
                            if req.response_size == 0 {
                                "HTTP GET got no response"
                            } else {
                                "HTTP GET got invalid response"
                            },
                            op_id,
                        )));
                    }
                    result = EventResult::Done;
                }
            }
        }
        if let Some(len) = req.expected_response_size {
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
        Ok(result)
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
    let mut eof = event.is_read_closed() && event.is_write_closed();

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
        let data = &c.request_data[c.request_offset..];
        c.request_offset += c.socket.write(data)?;

        if c.request_done() {
            c.socket.flush()?;
        }
    }

    let mut eof = event.is_read_closed() && event.is_write_closed();
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

pub static CONTENT_LENGTH_REGEX: Lazy<regex::bytes::Regex> = Lazy::new(|| {
    regex::bytes::RegexBuilder::new(
        r"^\s*Content-Length\s*:\s*([0-9]+)\s*\r?$",
    )
    .case_insensitive(true)
    .build()
    .unwrap()
});

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

impl Transform<'_> for TfHttpRequest {
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
                sv.set_error(Arc::new(OperatorApplicationError::new_s(
                    format!("IO Error in HTTP GET Request: {e}"),
                    jd.tf_mgr.transforms[tf_id].op_id.unwrap(),
                )));
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

            let sbs = self.stream_buffer_size;
            let mut inserter = sv.data_inserter(sv_id, sbs, req.header_parsed);
            let res = inserter.with_bytes_buffer(|buf| {
                Self::handle_event(req, op_id, event, buf, sbs)
            });
            drop(inserter);
            match res {
                Err(e) => {
                    sv.set_error(e);
                    jd.sv_mgr.inform_stream_value_subscribers(sv_id);
                    jd.sv_mgr.drop_field_value_subscription(sv_id, None);
                    self.running_connections.release(token);
                    continue;
                }
                Ok(EventResult::TryNextIp) => {
                    let addr = req.remaining_socket_addresses.pop().unwrap();
                    let imm_req = &self.running_connections[token];
                    let https = imm_req.tls_conn.is_some();
                    let hostname = &imm_req.hostname;
                    let result =
                        self.setup_connection(addr, https, hostname, token);
                    let req = &mut self.running_connections[token];
                    match result {
                        Ok((socket, tls_conn)) => {
                            req.tls_conn = tls_conn;
                            req.socket = socket;
                        }
                        Err(e) => {
                            sv.set_error(Arc::new(
                                // ENHANCE: include number of addresses tried
                                OperatorApplicationError::new_s(
                                    format!("HTTP GET request failed: {e}"),
                                    op_id,
                                ),
                            ));
                        }
                    }
                }
                Ok(EventResult::Spurious) => {
                    // req.reregister(self.poll.registry(), mio::Token(token));
                }
                Ok(EventResult::Update) => {
                    jd.sv_mgr.inform_stream_value_subscribers(sv_id);
                    // req.reregister(self.poll.registry(), mio::Token(token));
                }
                Ok(EventResult::Done) => {
                    sv.done = true;
                    jd.sv_mgr.inform_stream_value_subscribers(sv_id);
                    let _ = self.poll.registry().deregister(&mut req.socket);
                    self.running_connections.release(token);
                    jd.sv_mgr.drop_field_value_subscription(sv_id, None);
                }
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
