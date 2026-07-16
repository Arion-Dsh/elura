use std::net::SocketAddr;

use async_trait::async_trait;
use elura_core::Result;
use elura_gateway::transport::{GatewayTransport, GatewayTransportListener};

struct CustomTransport {
    listen: SocketAddr,
}

struct CustomListener {
    listen: SocketAddr,
}

#[async_trait]
impl GatewayTransportListener for CustomListener {
    type Io = tokio::io::DuplexStream;

    async fn accept(&mut self) -> Result<(SocketAddr, Self::Io)> {
        let (client, io) = tokio::io::duplex(1024);
        drop(client);
        Ok((self.listen, io))
    }
}

#[async_trait]
impl GatewayTransport for CustomTransport {
    type Listener = CustomListener;

    fn name(&self) -> &'static str {
        "custom"
    }

    fn listen(&self) -> SocketAddr {
        self.listen
    }

    async fn bind(&self) -> Result<Self::Listener> {
        Ok(CustomListener {
            listen: self.listen,
        })
    }
}

#[test]
fn transport_can_be_replaced_using_only_public_api() {
    fn assert_transport<T: GatewayTransport>() {}
    assert_transport::<CustomTransport>();
}
