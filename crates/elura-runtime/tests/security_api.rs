use elura_runtime::security::{BoxedServiceStream, InternalToken, ServiceStream};

fn assert_service_stream<T: ServiceStream>() {}

#[test]
fn trusted_service_primitives_use_the_public_security_module() {
    assert_service_stream::<tokio::io::DuplexStream>();
    let (_client, server) = tokio::io::duplex(64);
    let _stream: BoxedServiceStream = Box::new(server);

    let token = InternalToken::new("0123456789abcdef0123456789abcdef").unwrap();
    assert!(token.authorizes(token.expose()));
}

#[test]
fn launch_configs_have_stable_constructors() {
    use elura_runtime::launch::ServerTlsFilesConfig;
    use elura_runtime::observability::AdminServerConfig;

    let listen = "127.0.0.1:17001".parse().unwrap();
    let _ = AdminServerConfig::new(listen, "gateway", "gateway-1");
    let _ = ServerTlsFilesConfig::new("server.crt", "server.key");
}
