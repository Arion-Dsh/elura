use std::io;
use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use elura::core::protocol::{Frame, FrameCodec, FrameKind, ROUTE_AUTHENTICATE};
use elura::prelude::*;
use elura_spot_demo::{Arena, DEMO_TICKET_KEY, Move, MoveRequest, ROUTE_MOVE, Snapshot};
use futures_util::{SinkExt, StreamExt};
use prost::Message;
use tokio::net::TcpStream;
use tokio::time::timeout;
use tokio_util::codec::Framed;

#[tokio::test]
async fn two_tcp_clients_share_one_authoritative_snapshot() {
    let gateway_port = unused_loopback_port();
    let admin_port = unused_loopback_port();
    let gateway_address = SocketAddr::from((Ipv4Addr::LOCALHOST, gateway_port));

    let mut gateway = GatewayConfig::default();
    gateway.ticket.key = DEMO_TICKET_KEY.to_owned();
    let mut tcp = TcpConfig::default();
    tcp.listen = gateway_address;
    let arena = Arc::new(Mutex::new(Arena::default()));
    let server = Monolith::new(gateway, WorldConfig::default())
        .transport(TcpTransport::new(tcp).unwrap())
        .route(Move, move |context: WorldContext, request| {
            let arena = arena.clone();
            async move {
                Ok(arena
                    .lock()
                    .map_err(|_| elura::Error::Internal("arena lock poisoned".into()))?
                    .apply_move(context.identity.user_id, request))
            }
        })
        .build()
        .unwrap();

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let server_task = tokio::spawn(server.serve(
        AdminServerConfig::loopback(admin_port, "tiny-arena-test", "test"),
        shutdown_rx,
    ));

    let mut player_one = connect_player(gateway_address, 1).await.unwrap();
    let first = move_player(&mut player_one, 2, 1, 0).await.unwrap();
    assert_eq!(first.players.len(), 1);

    let mut player_two = connect_player(gateway_address, 2).await.unwrap();
    move_player(&mut player_two, 2, 0, -1).await.unwrap();
    let shared = move_player(&mut player_one, 3, 0, 0).await.unwrap();
    assert_eq!(
        shared
            .players
            .iter()
            .map(|player| player.id)
            .collect::<Vec<_>>(),
        vec![1, 2]
    );

    drop(player_one);
    drop(player_two);
    shutdown_tx.send(true).unwrap();
    timeout(Duration::from_secs(5), server_task)
        .await
        .expect("server shutdown timed out")
        .expect("server task panicked")
        .expect("server returned an error");
}

fn unused_loopback_port() -> u16 {
    TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn connect_player(
    address: SocketAddr,
    player_id: i64,
) -> io::Result<Framed<TcpStream, FrameCodec>> {
    let stream = connect_when_ready(address).await?;
    let mut framed = Framed::new(stream, FrameCodec::default());
    let tickets = TicketService::new(
        DEMO_TICKET_KEY,
        "game-login",
        "game-gateway",
        Duration::from_secs(60),
        Duration::from_secs(30 * 60),
    )
    .map_err(io::Error::other)?;
    let ticket = tickets
        .issue_login(Identity {
            account_id: player_id,
            user_id: player_id,
            region_id: 1,
            realm_id: 1,
            generation: 1,
        })
        .map_err(io::Error::other)?;
    let payload = serde_json::to_vec(&serde_json::json!({ "ticket": ticket }))?;
    let response = exchange(
        &mut framed,
        Frame::request(ROUTE_AUTHENTICATE, 1, payload).map_err(io::Error::other)?,
    )
    .await?;
    assert_eq!(response.kind, FrameKind::Response);
    Ok(framed)
}

async fn connect_when_ready(address: SocketAddr) -> io::Result<TcpStream> {
    let mut last_error = None;
    for _ in 0..50 {
        match TcpStream::connect(address).await {
            Ok(stream) => return Ok(stream),
            Err(error) => last_error = Some(error),
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Err(last_error.unwrap_or_else(|| io::Error::other("server did not start")))
}

async fn move_player(
    framed: &mut Framed<TcpStream, FrameCodec>,
    request_id: u64,
    dx: i32,
    dy: i32,
) -> io::Result<Snapshot> {
    let payload = MoveRequest { dx, dy }.encode_to_vec();
    let response = exchange(
        framed,
        Frame::request(ROUTE_MOVE, request_id, Bytes::from(payload)).map_err(io::Error::other)?,
    )
    .await?;
    assert_eq!(response.kind, FrameKind::Response);
    Snapshot::decode(response.payload).map_err(io::Error::other)
}

async fn exchange(framed: &mut Framed<TcpStream, FrameCodec>, request: Frame) -> io::Result<Frame> {
    framed.send(request).await?;
    timeout(Duration::from_secs(2), framed.next())
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "response timeout"))?
        .ok_or_else(|| io::Error::new(io::ErrorKind::UnexpectedEof, "server closed"))?
}
