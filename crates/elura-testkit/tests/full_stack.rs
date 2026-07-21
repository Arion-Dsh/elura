use elura_core::Result;
use elura_testkit::{
    FullStackBuilder, FullStackLoadConfig, TcpTestTransport, WebSocketTestTransport, test_identity,
};
use elura_world::Route;
use prost::Message;

#[derive(Clone, PartialEq, Message)]
struct Echo {
    #[prost(string, tag = "1")]
    value: String,
}

struct EchoRoute;

impl Route for EchoRoute {
    const ID: u32 = 100;
    const NAME: &'static str = "test.echo";

    type Request = Echo;
    type Response = Echo;
}

fn stack() -> Result<FullStackBuilder> {
    Ok(
        FullStackBuilder::loopback()?.route(EchoRoute, |_context, mut request| async move {
            request.value.make_ascii_uppercase();
            Ok(request)
        }),
    )
}

#[tokio::test]
async fn tcp_calls_cross_gateway_and_world_protocol_boundaries() {
    async fn run() -> Result<()> {
        let harness = stack()?.start(TcpTestTransport::loopback()?).await?;
        let client = harness.client(test_identity(1)).await?;
        let response = client
            .call(
                EchoRoute,
                Echo {
                    value: "hello".into(),
                },
            )
            .await?;
        assert_eq!(response.value, "HELLO");

        let report = harness
            .load_scenario(
                FullStackLoadConfig::new(2, 3),
                |worker| test_identity(worker as i64 + 10),
                |client, worker, iteration| async move {
                    assert_eq!(client.identity().user_id, worker as i64 + 10);
                    let response = client
                        .call(
                            EchoRoute,
                            Echo {
                                value: format!("{worker}:{iteration}"),
                            },
                        )
                        .await?;
                    assert_eq!(
                        response.value,
                        format!("{worker}:{iteration}").to_uppercase()
                    );
                    Ok(())
                },
            )
            .await?;
        assert_eq!(report.transport, "tcp");
        assert_eq!(report.attempted, 6);
        assert_eq!(report.succeeded, 6);
        assert!(report.is_success());
        assert!(report.operation_latency.p99 > std::time::Duration::ZERO);
        drop(client);
        harness.shutdown().await?;
        Ok(())
    }
    run().await.unwrap();
}

#[tokio::test]
async fn websocket_uses_the_same_transport_neutral_business_client() {
    async fn run() -> Result<()> {
        let harness = stack()?.start(WebSocketTestTransport::loopback()?).await?;
        let client = harness.client(test_identity(2)).await?;
        let response = client
            .call(
                EchoRoute,
                Echo {
                    value: "websocket".into(),
                },
            )
            .await?;
        assert_eq!(response.value, "WEBSOCKET");
        assert_eq!(harness.transport_name(), "websocket");

        let report = harness
            .load_scenario(
                FullStackLoadConfig::new(1, 2),
                |_| test_identity(20),
                |client, _, iteration| async move {
                    let response = client
                        .call(
                            EchoRoute,
                            Echo {
                                value: iteration.to_string(),
                            },
                        )
                        .await?;
                    assert_eq!(response.value, iteration.to_string());
                    Ok(())
                },
            )
            .await?;
        assert_eq!(report.transport, "websocket");
        assert!(report.is_success());
        drop(client);
        harness.shutdown().await?;
        Ok(())
    }
    run().await.unwrap();
}
