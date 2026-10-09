//! WebSocket 有界消息通道与原连接文本透传；空闲读取与响应正文消费互斥

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use gateway_core::engine::response_control::{
    ResponseControlTransport, ResponseControlUnavailable,
};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio_tungstenite::tungstenite::{self, Message};

pub(super) const WEBSOCKET_SEND_TIMEOUT: Duration = Duration::from_secs(5 * 60);

pub(super) enum PumpCommand {
    Send {
        message: Message,
        ack: oneshot::Sender<Result<(), tungstenite::Error>>,
    },
}

pub(super) type SharedMessages = Arc<Mutex<mpsc::Receiver<Result<Message, tungstenite::Error>>>>;

pub(super) struct PumpControl {
    pub(super) tx_command: mpsc::Sender<PumpCommand>,
    pub(super) rx_message: SharedMessages,
}

pub(super) async fn send_message(
    commands: &mpsc::Sender<PumpCommand>,
    message: Message,
) -> Result<(), tungstenite::Error> {
    let (ack, rx_ack) = oneshot::channel();
    commands
        .send(PumpCommand::Send { message, ack })
        .await
        .map_err(|_| tungstenite::Error::ConnectionClosed)?;
    rx_ack
        .await
        .unwrap_or(Err(tungstenite::Error::ConnectionClosed))
}

#[async_trait]
impl ResponseControlTransport for PumpControl {
    async fn send(&self, payload: &str) -> Result<(), ResponseControlUnavailable> {
        tokio::time::timeout(
            WEBSOCKET_SEND_TIMEOUT,
            send_message(&self.tx_command, Message::Text(payload.to_owned().into())),
        )
        .await
        .map_err(|_| ResponseControlUnavailable)?
        .map_err(|_| ResponseControlUnavailable)
    }

    async fn receive(&self) -> Result<String, ResponseControlUnavailable> {
        match self.rx_message.lock().await.recv().await {
            Some(Ok(Message::Text(text))) => Ok(text.to_string()),
            _ => Err(ResponseControlUnavailable),
        }
    }
}
