use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{collections::VecDeque, pin::Pin};

use bytes::BytesMut;

use super::*;

type BoxFut<T> = Pin<Box<dyn std::future::Future<Output = T> + Send>>;

mod invocation {
    // panics if dropped without being fulfilled
    pub struct Expected {
        fulfilled: bool,
        method_name: &'static str,
    }

    impl Expected {
        pub fn new(method_name: &'static str) -> Self {
            Self {
                fulfilled: false,
                method_name,
            }
        }

        pub fn fulfill(&mut self) {
            if self.fulfilled {
                panic!("unexpected invocation of {}", self.method_name);
            }

            self.fulfilled = true;
        }
    }

    impl Drop for Expected {
        fn drop(&mut self) {
            if !self.fulfilled && !std::thread::panicking() {
                panic!("unfulfilled invocation of {}", self.method_name);
            }
        }
    }
}

#[derive(Clone)]
pub struct DropFlag(Arc<AtomicBool>);

impl DropFlag {
    pub fn new() -> Self {
        DropFlag(Arc::new(AtomicBool::new(false)))
    }

    pub fn is_dropped(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

pub struct DropWatcher {
    flag: DropFlag,
}

impl DropWatcher {
    pub fn new(flag: DropFlag) -> Self {
        DropWatcher { flag }
    }
}

impl Drop for DropWatcher {
    fn drop(&mut self) {
        self.flag.0.store(true, Ordering::Release);
    }
}

enum ReceiveGreetingStep {
    Drop(DropFlag),
    ClientGreeting(msg::ClientGreeting),
    Socks5Error(Socks5Error),
}

enum MethodSelectionStep {
    Drop(DropFlag),
    ServerMethod(u8),
}

enum RequestStep {
    ClientRequest(msg::ClientRequest),
    Socks5Error(Socks5Error),
}

enum ReplyStep {
    Drop(DropFlag),
    ServerBoundAddr(SocketAddr),
    Error(u8),
}

enum StreamingStep {
    Streaming(
        BytesMut,
        Box<dyn AsyncRead + Unpin + Send + 'static>,
        Box<dyn AsyncWrite + Unpin + Send + 'static>,
    ),
    IoError(std::io::Error),
}

enum Step {
    ReceiveGreeting(ReceiveGreetingStep),
    MethodSelection(MethodSelectionStep),
    Request(RequestStep),
    Reply(ReplyStep),
    Streaming(StreamingStep),
}

fn test_eq<T: PartialEq + std::fmt::Debug + std::fmt::Display>(
    expected: T,
    actual: T,
    name: &'static str,
) {
    assert_eq!(
        expected, actual,
        "unexpected {}, expected: {}, actual: {}",
        name, expected, actual
    );
}

fn build_init(steps: &mut VecDeque<Step>) -> Init {
    let receive_greeting_message_step = match steps.pop_front().unwrap() {
        Step::ReceiveGreeting(step) => step,
        _ => panic!("unexpected step"),
    };

    match receive_greeting_message_step {
        ReceiveGreetingStep::Drop(flag) => {
            let watcher = DropWatcher::new(flag);
            Init {
                receive_greeting_message_func: Box::new(move || {
                    drop(watcher);
                    Box::pin(async move { panic!("unexpected invocation") })
                }),
            }
        }
        ReceiveGreetingStep::ClientGreeting(msg) => {
            let mut invocation_expectation = invocation::Expected::new("receive greeting message");

            let result = Ok((msg, build_greeted(steps)));

            Init {
                receive_greeting_message_func: Box::new(move || {
                    invocation_expectation.fulfill();
                    Box::pin(std::future::ready(result))
                }),
            }
        }
        ReceiveGreetingStep::Socks5Error(err) => Init {
            receive_greeting_message_func: Box::new(move || Box::pin(std::future::ready(Err(err)))),
        },
    }
}

fn build_greeted(steps: &mut VecDeque<Step>) -> Greeted {
    let method_selection_step = match steps.pop_front().unwrap() {
        Step::MethodSelection(step) => step,
        _ => panic!("unexpected step"),
    };

    match method_selection_step {
        MethodSelectionStep::Drop(flag) => {
            let watcher = DropWatcher::new(flag);
            Greeted {
                send_method_selection_message_func: Box::new(|_| {
                    drop(watcher);
                    Box::pin(async move { panic!("unexpected invocation") })
                }),
            }
        }
        MethodSelectionStep::ServerMethod(expected_method) => {
            let request_step = match steps.pop_front().unwrap() {
                Step::Request(step) => step,
                _ => panic!("unexpected step"),
            };

            let result = match request_step {
                RequestStep::ClientRequest(req_msg) => Ok((req_msg, build_requested(steps))),
                RequestStep::Socks5Error(err) => Err(err),
            };

            let mut invocation_expectation =
                invocation::Expected::new("send method selection message");

            Greeted {
                send_method_selection_message_func: Box::new(move |method| {
                    invocation_expectation.fulfill();
                    test_eq(expected_method, method, "method");
                    Box::pin(std::future::ready(result))
                }),
            }
        }
    }
}

fn build_requested(steps: &mut VecDeque<Step>) -> Requested {
    let reply_step = match steps.pop_front().unwrap() {
        Step::Reply(step) => step,
        _ => panic!("unexpected step"),
    };

    match reply_step {
        ReplyStep::Drop(flag) => {
            let watcher = DropWatcher::new(flag);
            let watcher = Arc::new(watcher);
            let watcher_clone = watcher.clone();
            Requested {
                reply_func: Box::new(|_| {
                    Box::pin({
                        async move {
                            drop(watcher);
                            panic!("unexpected invocation")
                        }
                    })
                }),

                reply_error_func: Box::new(|_| {
                    Box::pin(async move {
                        drop(watcher_clone);
                        panic!("unexpected invocation")
                    })
                }),
            }
        }
        ReplyStep::ServerBoundAddr(expected_addr) => {
            let streaming_step = match steps.pop_front().unwrap() {
                Step::Streaming(step) => step,
                _ => panic!("unexpected step"),
            };

            let result = match streaming_step {
                StreamingStep::Streaming(read_buf, read_stream, write_stream) => {
                    Ok((BufDecoder::from_parts(read_buf, read_stream), write_stream))
                }
                StreamingStep::IoError(err) => Err(err),
            };

            let mut invocation_expectation = invocation::Expected::new("reply");

            Requested {
                reply_func: Box::new(move |addr| {
                    invocation_expectation.fulfill();
                    test_eq(expected_addr, addr, "addr");
                    Box::pin(std::future::ready(result))
                }),
                reply_error_func: Box::new(|_| {
                    Box::pin(async move { panic!("unexpected invocation") })
                }),
            }
        }
        ReplyStep::Error(expected_err) => {
            let mut invocation_expectation = invocation::Expected::new("reply_error");

            Requested {
                reply_func: Box::new(move |_| {
                    Box::pin(async move { panic!("unexpected invocation") })
                }),
                reply_error_func: Box::new(move |err| {
                    invocation_expectation.fulfill();
                    test_eq(expected_err, err, "reply error");
                    Box::pin(std::future::ready(Ok(())))
                }),
            }
        }
    }
}

pub fn script() -> InitBuilder {
    InitBuilder::new()
}

pub struct InitBuilder(VecDeque<Step>);

impl InitBuilder {
    pub fn new() -> Self {
        Self(VecDeque::new())
    }

    pub fn provide_greeting_message(
        mut self,
        greeting: msg::ClientGreeting,
    ) -> MethodSelectionBuilder {
        self.0
            .push_back(Step::ReceiveGreeting(ReceiveGreetingStep::ClientGreeting(
                greeting,
            )));
        MethodSelectionBuilder(self.0)
    }

    pub fn provide_socks5_error(mut self, err: Socks5Error) -> Init {
        self.0
            .push_back(Step::ReceiveGreeting(ReceiveGreetingStep::Socks5Error(err)));

        build_init(&mut self.0)
    }

    // expected to be dropped without invocation to receive_greeting_message
    pub fn to_be_dropped(mut self, flag: DropFlag) -> Init {
        self.0
            .push_back(Step::ReceiveGreeting(ReceiveGreetingStep::Drop(flag)));
        build_init(&mut self.0)
    }
}

pub struct MethodSelectionBuilder(VecDeque<Step>);

impl MethodSelectionBuilder {
    pub fn expect_method_selection(mut self, method: u8) -> GreetedBuilder {
        self.0
            .push_back(Step::MethodSelection(MethodSelectionStep::ServerMethod(
                method,
            )));
        GreetedBuilder(self.0)
    }

    // expected to be dropped without invocation to send_method_selection_message
    pub fn to_be_dropped(mut self, flag: DropFlag) -> Init {
        self.0
            .push_back(Step::MethodSelection(MethodSelectionStep::Drop(flag)));

        build_init(&mut self.0)
    }
}

pub struct GreetedBuilder(VecDeque<Step>);

impl GreetedBuilder {
    pub fn provide_request_message(mut self, req_msg: msg::ClientRequest) -> BoundBuilder {
        self.0
            .push_back(Step::Request(RequestStep::ClientRequest(req_msg)));
        BoundBuilder(self.0)
    }

    pub fn provide_socks5_error(mut self, err: Socks5Error) -> Init {
        self.0
            .push_back(Step::Request(RequestStep::Socks5Error(err)));

        build_init(&mut self.0)
    }
}

pub struct BoundBuilder(VecDeque<Step>);

impl BoundBuilder {
    pub fn expect_reply(mut self, bound_addr: SocketAddr) -> ReplyBuilder {
        self.0
            .push_back(Step::Reply(ReplyStep::ServerBoundAddr(bound_addr)));

        ReplyBuilder(self.0)
    }

    pub fn expect_reply_error(mut self, err_code: u8) -> Init {
        self.0.push_back(Step::Reply(ReplyStep::Error(err_code)));

        build_init(&mut self.0)
    }

    pub fn to_be_dropped(mut self, flag: DropFlag) -> Init {
        self.0.push_back(Step::Reply(ReplyStep::Drop(flag)));

        build_init(&mut self.0)
    }
}

pub struct ReplyBuilder(VecDeque<Step>);

impl ReplyBuilder {
    pub fn provide_stream<
        ReadStream: AsyncRead + Unpin + Send + 'static,
        WriteStream: AsyncWrite + Unpin + Send + 'static,
    >(
        mut self,
        read_buf: BytesMut,
        read_stream: ReadStream,
        write_stream: WriteStream,
    ) -> Init {
        self.0.push_back(Step::Streaming(StreamingStep::Streaming(
            read_buf,
            Box::new(read_stream),
            Box::new(write_stream),
        )));

        build_init(&mut self.0)
    }

    pub fn provide_io_error(mut self, err: std::io::Error) -> Init {
        self.0
            .push_back(Step::Streaming(StreamingStep::IoError(err)));

        build_init(&mut self.0)
    }
}

pub struct Init {
    pub(super) receive_greeting_message_func:
        Box<dyn FnOnce() -> BoxFut<Result<(msg::ClientGreeting, Greeted), Socks5Error>> + Send>,
}

impl crate::socks5::server_agent::Init for Init {
    fn receive_greeting_message(
        self,
    ) -> impl futures::Future<
        Output = Result<
            (msg::ClientGreeting, impl socks5::server_agent::Greeted),
            socks5::Socks5Error,
        >,
    > + std::marker::Send {
        (self.receive_greeting_message_func)()
    }
}

pub struct Greeted {
    pub(super) send_method_selection_message_func:
        Box<dyn FnOnce(u8) -> BoxFut<Result<(msg::ClientRequest, Requested), Socks5Error>> + Send>,
}

impl crate::socks5::server_agent::Greeted for Greeted {
    fn send_method_selection_message(
        self,
        method: u8,
    ) -> impl futures::Future<
        Output = Result<
            (msg::ClientRequest, impl socks5::server_agent::Requested),
            socks5::Socks5Error,
        >,
    > + std::marker::Send {
        (self.send_method_selection_message_func)(method)
    }
}

pub struct Requested {
    pub(super) reply_func: Box<
        dyn FnOnce(
                SocketAddr,
            ) -> BoxFut<
                Result<
                    (
                        BufDecoder<Box<dyn AsyncRead + Send + Unpin + 'static>>,
                        Box<dyn AsyncWrite + Send + Unpin + 'static>,
                    ),
                    std::io::Error,
                >,
            > + Send,
    >,

    pub(super) reply_error_func: Box<dyn FnOnce(u8) -> BoxFut<Result<(), std::io::Error>> + Send>,
}

impl crate::socks5::server_agent::Requested for Requested {
    fn reply(
        self,
        bound_addr: SocketAddr,
    ) -> impl Future<
        Output = Result<
            (
                BufDecoder<impl AsyncRead + Unpin + Send + 'static>,
                impl AsyncWrite + Unpin + Send + 'static,
            ),
            std::io::Error,
        >,
    > + Send {
        (self.reply_func)(bound_addr)
    }

    fn reply_error(self, err_code: u8) -> impl Future<Output = Result<(), std::io::Error>> + Send {
        (self.reply_error_func)(err_code)
    }
}

mod tests {
    use super::*;

    #[test]
    fn init_drop() {
        let flag = DropFlag::new();
        let agent = script().to_be_dropped(flag.clone());

        drop(agent);
        assert!(flag.is_dropped());
    }

    #[tokio::test]
    async fn init_panic_on_invocation_if_expecting_drop() {
        let agent = script().to_be_dropped(DropFlag::new());

        let task_result = tokio::spawn(async move { agent.receive_greeting_message().await }).await;

        let err = match task_result {
            Err(err) => err,
            _ => panic!("unexpected result"),
        };

        assert!(err.is_panic());
    }

    #[tokio::test]
    async fn init_error() {
        let agent = script().provide_socks5_error(Socks5Error::Io(std::io::Error::new(
            std::io::ErrorKind::Other,
            "test",
        )));

        let result = agent.receive_greeting_message().await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn greeted_drop() {
        let flag = DropFlag::new();
        let agent = script()
            .provide_greeting_message(msg::ClientGreeting {
                ver: 5,
                methods: BytesMut::from([0].as_ref()),
            })
            .to_be_dropped(flag.clone());

        let (_msg, agent) = agent.receive_greeting_message().await.unwrap();

        drop(agent);
        assert!(flag.is_dropped());
    }

    #[tokio::test]
    async fn greeted_panic_on_invocation_if_expecting_drop() {
        let agent = script()
            .provide_greeting_message(msg::ClientGreeting {
                ver: 5,
                methods: BytesMut::from([0].as_ref()),
            })
            .to_be_dropped(DropFlag::new());

        let agent = agent.receive_greeting_message().await.unwrap().1;

        let task_result =
            tokio::spawn(async move { agent.send_method_selection_message(0).await }).await;

        let err = match task_result {
            Err(err) => err,
            _ => panic!("unexpected result"),
        };

        assert!(err.is_panic());
    }

    #[tokio::test]
    async fn greeted_unexpected_method() {
        let agent = script()
            .provide_greeting_message(msg::ClientGreeting {
                ver: 5,
                methods: BytesMut::from([0].as_ref()),
            })
            .expect_method_selection(0)
            .provide_socks5_error(Socks5Error::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "test",
            )));

        let (_msg, agent) = agent.receive_greeting_message().await.unwrap();

        let task_result = tokio::spawn(async move { agent.send_method_selection_message(1) }).await;
        let err = match task_result {
            Err(err) => err,
            _ => panic!("unexpected result"),
        };
        assert!(err.is_panic());
    }

    #[tokio::test]
    async fn greeted_method() {
        let agent = script()
            .provide_greeting_message(msg::ClientGreeting {
                ver: 5,
                methods: BytesMut::from([0].as_ref()),
            })
            .expect_method_selection(0)
            .provide_socks5_error(Socks5Error::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "test",
            )));

        let (_msg, agent) = agent.receive_greeting_message().await.unwrap();

        let result = agent.send_method_selection_message(0).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn request_drop() {
        let flag = DropFlag::new();
        let agent = script()
            .provide_greeting_message(msg::ClientGreeting {
                ver: 5,
                methods: BytesMut::from([0].as_ref()),
            })
            .expect_method_selection(0)
            .provide_request_message(msg::ClientRequest {
                ver: 5,
                cmd: 1,
                rsv: 0,
                addr: ReadRequestAddr::Ipv4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
                port: 7878,
            })
            .to_be_dropped(flag.clone());

        let (_greeting, agent) = agent.receive_greeting_message().await.unwrap();
        let (_req, agent) = agent.send_method_selection_message(0).await.unwrap();

        drop(agent);
        assert!(flag.is_dropped());
    }

    #[tokio::test]
    async fn request_panic_on_invocation_if_expecting_drop_1() {
        let agent = script()
            .provide_greeting_message(msg::ClientGreeting {
                ver: 5,
                methods: BytesMut::from([0].as_ref()),
            })
            .expect_method_selection(0)
            .provide_request_message(msg::ClientRequest {
                ver: 5,
                cmd: 1,
                rsv: 0,
                addr: ReadRequestAddr::Ipv4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
                port: 7878,
            })
            .to_be_dropped(DropFlag::new());

        let (_greeting, agent) = agent.receive_greeting_message().await.unwrap();
        let (_req, agent) = agent.send_method_selection_message(0).await.unwrap();

        let task_result = tokio::spawn(async move {
            agent
                .reply(SocketAddr::new(
                    std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
                    80,
                ))
                .await
        })
        .await;

        let err = match task_result {
            Err(err) => err,
            _ => panic!("unexpected result"),
        };

        assert!(err.is_panic());
    }

    #[tokio::test]
    async fn request_panic_on_invocation_if_expecting_drop_2() {
        let agent = script()
            .provide_greeting_message(msg::ClientGreeting {
                ver: 5,
                methods: BytesMut::from([0].as_ref()),
            })
            .expect_method_selection(0)
            .provide_request_message(msg::ClientRequest {
                ver: 5,
                cmd: 1,
                rsv: 0,
                addr: ReadRequestAddr::Ipv4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
                port: 7878,
            })
            .to_be_dropped(DropFlag::new());

        let (_greeting, agent) = agent.receive_greeting_message().await.unwrap();
        let (_req, agent) = agent.send_method_selection_message(0).await.unwrap();

        let task_result = tokio::spawn(async move { agent.reply_error(1).await }).await;

        let err = match task_result {
            Err(err) => err,
            _ => panic!("unexpected result"),
        };

        assert!(err.is_panic());
    }

    #[tokio::test]
    async fn request_reply() {
        let agent = script()
            .provide_greeting_message(msg::ClientGreeting {
                ver: 5,
                methods: BytesMut::from([0].as_ref()),
            })
            .expect_method_selection(0)
            .provide_request_message(msg::ClientRequest {
                ver: 5,
                cmd: 1,
                rsv: 0,
                addr: ReadRequestAddr::Ipv4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
                port: 7878,
            })
            .expect_reply(SocketAddr::new(
                std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
                80,
            ))
            .provide_io_error(std::io::Error::new(std::io::ErrorKind::Other, "test"));

        let (_greeting, agent) = agent.receive_greeting_message().await.unwrap();
        let (_req, agent) = agent.send_method_selection_message(0).await.unwrap();

        let result = agent
            .reply(SocketAddr::new(
                std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
                80,
            ))
            .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn requexted_unexpected_bound_addr() {
        let agent = script()
            .provide_greeting_message(msg::ClientGreeting {
                ver: 5,
                methods: BytesMut::from([0].as_ref()),
            })
            .expect_method_selection(0)
            .provide_request_message(msg::ClientRequest {
                ver: 5,
                cmd: 1,
                rsv: 0,
                addr: ReadRequestAddr::Ipv4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
                port: 7878,
            })
            .expect_reply(SocketAddr::new(
                std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
                80,
            ))
            .provide_io_error(std::io::Error::new(std::io::ErrorKind::Other, "test"));

        let (_greeting, agent) = agent.receive_greeting_message().await.unwrap();
        let (_req, agent) = agent.send_method_selection_message(0).await.unwrap();

        let result = tokio::spawn(async move {
            agent
                .reply(SocketAddr::new(
                    std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 2)),
                    80,
                ))
                .await
        })
        .await;

        let err = match result {
            Err(err) => err,
            _ => panic!("unexpected result"),
        };

        assert!(err.is_panic());
    }
}
