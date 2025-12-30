#![feature(trait_alias)]
#![feature(assert_matches)]

pub mod connect_target;
pub mod future_ext;
pub mod handover;
pub mod sink_ext;
pub mod task_scope;
mod message_dispatch;

#[macro_use]
pub mod decode;
pub mod encrypt;

pub mod protocol;
pub mod session;
pub mod socks5;

pub mod client;
pub mod server;

pub mod cli_parameters {
    use clap::Parser;
    use clap::ValueEnum;
    use tracing::Subscriber;
    use tracing_subscriber::fmt::format::FmtSpan;
    use tracing_subscriber::prelude::*;

    #[derive(Parser, Debug)]
    pub struct LogParameter {
        #[arg(long = "log-format", value_enum, default_value_t = LogFormat::Json)]
        pub format: LogFormat,
        #[arg(long = "no-color", default_value_t = false)]
        pub no_color: bool,
    }

    impl LogParameter {
        pub fn setup_subscriber(&self) {
            let layer = tracing_subscriber::fmt::layer()
                // .with_writer(std::io::stderr)
                .with_span_events(FmtSpan::NEW | FmtSpan::CLOSE)
                .with_ansi(!self.no_color)
                .with_format(self.format);

            let subscriber = tracing_subscriber::registry().with(layer);

            subscriber.init();
        }
    }

    #[derive(Clone, Copy, Debug, ValueEnum)]
    pub enum LogFormat {
        Full,
        Compact,
        Json,
        Pretty,
    }

    pub trait LogFormatExt {
        type S: Subscriber;
        fn with_format(
            self,
            format: LogFormat,
        ) -> Box<dyn tracing_subscriber::Layer<Self::S> + Send + Sync>;
    }

    impl<S, N, L, T, W> LogFormatExt
        for tracing_subscriber::fmt::Layer<S, N, tracing_subscriber::fmt::format::Format<L, T>, W>
    where
        S: Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
        N: Sync + Send + for<'writer> tracing_subscriber::fmt::FormatFields<'writer> + 'static,
        L: Sync + Send,
        T: Sync + Send + tracing_subscriber::fmt::time::FormatTime,
        tracing_subscriber::fmt::format::Format<L, T>:
            tracing_subscriber::fmt::FormatEvent<S, N> + 'static,
        W: Sync + Send + for<'writer> tracing_subscriber::fmt::MakeWriter<'writer> + 'static,
    {
        type S = S;

        // because this method erases the type of the returned format layer
        // (from struct `tracing_subscriber::fmt::Layer` to trait object of `tracing_subscriber::Layer`)
        // call this method last before composing the layer into a subscriber
        // otherwise you won't be able to use other builder methods afterwards
        // that depends on the layer being a format layer, like `with_writer`.
        //
        // this is because different builder methods for formating returns different types
        // which rust does not allow to bind to the same name.
        // What do I do if there is another set of options that branches type?
        // IDK. This is what it is for now.
        fn with_format(
            self,
            format: LogFormat,
        ) -> Box<dyn tracing_subscriber::Layer<S> + Send + Sync> {
            match format {
                LogFormat::Full => Box::new(self),
                LogFormat::Compact => Box::new(self.compact()),
                LogFormat::Json => Box::new(self.json()),
                LogFormat::Pretty => Box::new(self.pretty()),
            }
        }
    }
}

pub mod prelude {
    pub use crate::connect_target::ConnectTarget;
    pub use crate::decode::*;
    pub use crate::encrypt::{EncryptedRead, EncryptedWrite};
    pub use crate::future_ext::FutureExt as _;
    pub use crate::handover::ChannelExt as _;
    pub use crate::sink_ext::SinkExt as _;
    pub use crate::socks5::server_agent::{Greeted as _, Init as _, Requested as _};
    pub use crate::task_scope;
    pub use crate::{decode, protocol, session, socks5};
    pub use protocol::{ClientId, Key, Nonce};
}
