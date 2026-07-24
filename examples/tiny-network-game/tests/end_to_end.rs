use std::io;
use std::net::{Ipv4Addr, SocketAddr, TcpListener};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use elura::prelude::*;
use elura_client::EluraClient;
use elura_spot_demo::{Arena, DEMO_TICKET_KEY, Move, MoveRequest, ROUTE_MOVE, Snapshot};
use tokio::time::timeout;

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

    let player_one = connect_player(gateway_address, 1).await.unwrap();
    let first = move_player(&player_one, 1, 0).await.unwrap();
    assert_eq!(first.players.len(), 1);

    let player_two = connect_player(gateway_address, 2).await.unwrap();
    move_player(&player_two, 0, -1).await.unwrap();
    let shared = move_player(&player_one, 0, 0).await.unwrap();
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

async fn connect_player(address: SocketAddr, player_id: i64) -> io::Result<EluraClient> {
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
    connect_when_ready(address, ticket).await
}

async fn connect_when_ready(address: SocketAddr, ticket: String) -> io::Result<EluraClient> {
    let mut last_error = None;
    for _ in 0..50 {
        match EluraClient::connect(address.to_string(), ticket.clone()).await {
            Ok(client) => return Ok(client),
            Err(error) => last_error = Some(error),
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Err(last_error
        .map(io::Error::other)
        .unwrap_or_else(|| io::Error::other("server did not start")))
}

async fn move_player(client: &EluraClient, dx: i32, dy: i32) -> io::Result<Snapshot> {
    client
        .request_protobuf(ROUTE_MOVE, &MoveRequest { dx, dy })
        .await
        .map_err(io::Error::other)
}
