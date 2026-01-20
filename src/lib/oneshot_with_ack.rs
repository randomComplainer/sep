use tokio::sync::oneshot;

pub struct Acker(oneshot::Sender<()>);

impl Acker {
    pub fn ack(self) {
        let _ = self.0.send(());
    }
}

struct WithAcker<Msg> {
    msg: Msg,
    acker: Acker,
}

pub enum SendError<Msg> {
    Rejected(Msg),
    Lost,
}

pub type RecvError = oneshot::error::RecvError;

pub struct RawSender<Msg>(oneshot::Sender<WithAcker<Msg>>)
where
    Msg: Unpin + Send + 'static;

impl<Msg> RawSender<Msg>
where
    Msg: Unpin + Send + 'static,
{
    fn new(sender: oneshot::Sender<WithAcker<Msg>>) -> Self {
        Self(sender)
    }
}

pub trait Sender
where
    Self: Sized + Unpin + Send + 'static,
{
    type Msg;
    fn send(self, msg: Self::Msg) -> impl Future<Output = Result<(), SendError<Self::Msg>>> + Send;
}

impl<Msg> Sender for RawSender<Msg>
where
    Msg: Unpin + Send + 'static,
{
    type Msg = Msg;
    async fn send(self, msg: Self::Msg) -> Result<(), SendError<Self::Msg>> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.0
            .send(WithAcker {
                msg,
                acker: Acker(ack_tx),
            })
            .map_err(|e| SendError::Rejected(e.msg))?;

        ack_rx.await.map_err(|_| SendError::Lost)
    }
}

pub struct RawReceiver<Msg>(oneshot::Receiver<WithAcker<Msg>>);

impl<Msg> Future for RawReceiver<Msg>
where
    Msg: Unpin + Send + 'static,
{
    type Output = Result<(Msg, Acker), RecvError>;

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        unsafe {
            self.map_unchecked_mut(|x| &mut x.0)
                .poll(cx)
                .map(|x| x.map(|x| (x.msg, x.acker)))
        }
    }
}

pub fn channel<Msg>() -> (RawSender<Msg>, RawReceiver<Msg>)
where
    Msg: Unpin + Send + 'static,
{
    let (sender, receiver) = oneshot::channel();
    (RawSender::new(sender), RawReceiver(receiver))
}
