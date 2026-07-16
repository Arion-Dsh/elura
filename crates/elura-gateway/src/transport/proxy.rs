use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;
use std::time::Duration;

use elura_core::{Error, Result};
use ipnet::IpNet;
use tokio::io::{AsyncRead, AsyncReadExt};

const V1_PREFIX: &[u8] = b"PROXY ";
const V2_SIGNATURE: &[u8; 12] = b"\r\n\r\n\0\r\nQUIT\n";

#[derive(Debug, Clone, Default)]
pub struct TrustedProxies {
    networks: Arc<[IpNet]>,
}

impl TrustedProxies {
    pub fn new(networks: impl IntoIterator<Item = IpNet>) -> Self {
        Self {
            networks: networks.into_iter().collect::<Vec<_>>().into(),
        }
    }

    pub fn parse<'a>(networks: impl IntoIterator<Item = &'a str>) -> Result<Self> {
        networks
            .into_iter()
            .map(|value| {
                value.parse::<IpNet>().map_err(|_| {
                    Error::InvalidConfig(format!("invalid trusted proxy CIDR {value}"))
                })
            })
            .collect::<Result<Vec<_>>>()
            .map(Self::new)
    }

    pub fn is_empty(&self) -> bool {
        self.networks.is_empty()
    }

    pub fn contains(&self, address: IpAddr) -> bool {
        let address = canonical_ip(address);
        self.networks
            .iter()
            .any(|network| network.contains(&address))
    }

    pub fn forwarded_address(&self, peer: SocketAddr, forwarded_for: &str) -> SocketAddr {
        if !self.contains(peer.ip()) || forwarded_for.trim().is_empty() {
            return peer;
        }
        let addresses = forwarded_for
            .split(',')
            .map(|value| value.trim().parse::<IpAddr>().map(canonical_ip))
            .collect::<std::result::Result<Vec<_>, _>>();
        let Ok(addresses) = addresses else {
            return peer;
        };
        let mut selected = canonical_ip(peer.ip());
        for address in addresses.into_iter().rev() {
            selected = address;
            if !self.contains(address) {
                break;
            }
        }
        SocketAddr::new(selected, peer.port())
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ProxyProtocolConfig {
    pub trusted_proxies: TrustedProxies,
    pub header_timeout: Duration,
    pub max_header_bytes: usize,
}

impl ProxyProtocolConfig {
    pub fn new(trusted_proxies: TrustedProxies) -> Result<Self> {
        let config = Self {
            trusted_proxies,
            header_timeout: Duration::from_secs(5),
            max_header_bytes: 1024,
        };
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<()> {
        if self.trusted_proxies.is_empty()
            || self.header_timeout.is_zero()
            || self.max_header_bytes < 16
            || self.max_header_bytes > u16::MAX as usize + 16
        {
            return Err(Error::InvalidConfig(
                "PROXY protocol requires trusted proxies, timeout, and a bounded header".into(),
            ));
        }
        Ok(())
    }
}

pub(crate) async fn proxy_client_address<S>(
    stream: &mut S,
    peer: SocketAddr,
    config: &ProxyProtocolConfig,
) -> Result<SocketAddr>
where
    S: AsyncRead + Unpin,
{
    config.validate()?;
    if !config.trusted_proxies.contains(peer.ip()) {
        return Err(Error::Authentication);
    }
    tokio::time::timeout(
        config.header_timeout,
        read_proxy_header(stream, config.max_header_bytes),
    )
    .await
    .map_err(|_| Error::Timeout)?
}

async fn read_proxy_header<S>(stream: &mut S, maximum: usize) -> Result<SocketAddr>
where
    S: AsyncRead + Unpin,
{
    let mut prefix = [0_u8; 12];
    stream.read_exact(&mut prefix).await?;
    if &prefix == V2_SIGNATURE {
        read_v2(stream, maximum).await
    } else if prefix.starts_with(V1_PREFIX) {
        read_v1(stream, prefix.to_vec(), maximum).await
    } else {
        Err(Error::InvalidFrame(
            "missing or invalid PROXY protocol header".into(),
        ))
    }
}

async fn read_v1<S>(stream: &mut S, mut header: Vec<u8>, maximum: usize) -> Result<SocketAddr>
where
    S: AsyncRead + Unpin,
{
    while !header.ends_with(b"\r\n") {
        if header.len() >= maximum {
            return Err(Error::InvalidFrame("PROXY v1 header is too large".into()));
        }
        header.push(stream.read_u8().await?);
    }
    let value = std::str::from_utf8(&header)
        .map_err(|_| Error::InvalidFrame("PROXY v1 header is not ASCII".into()))?;
    let fields = value
        .trim_end_matches("\r\n")
        .split_ascii_whitespace()
        .collect::<Vec<_>>();
    if fields.len() != 6 || fields[0] != "PROXY" {
        return Err(Error::InvalidFrame("invalid PROXY v1 fields".into()));
    }
    let source_ip = fields[2]
        .parse::<IpAddr>()
        .map_err(|_| Error::InvalidFrame("invalid PROXY v1 source address".into()))?;
    let destination_ip = fields[3]
        .parse::<IpAddr>()
        .map_err(|_| Error::InvalidFrame("invalid PROXY v1 destination address".into()))?;
    let family_matches = matches!(
        (fields[1], source_ip, destination_ip),
        ("TCP4", IpAddr::V4(_), IpAddr::V4(_)) | ("TCP6", IpAddr::V6(_), IpAddr::V6(_))
    );
    if !family_matches {
        return Err(Error::InvalidFrame(
            "invalid PROXY v1 address family".into(),
        ));
    }
    let source_port = parse_port(fields[4])?;
    parse_port(fields[5])?;
    Ok(SocketAddr::new(canonical_ip(source_ip), source_port))
}

async fn read_v2<S>(stream: &mut S, maximum: usize) -> Result<SocketAddr>
where
    S: AsyncRead + Unpin,
{
    let version_command = stream.read_u8().await?;
    let family_protocol = stream.read_u8().await?;
    let length = stream.read_u16().await? as usize;
    if version_command != 0x21 || length + 16 > maximum {
        return Err(Error::InvalidFrame(
            "invalid PROXY v2 version, command, or length".into(),
        ));
    }
    let mut payload = vec![0_u8; length];
    stream.read_exact(&mut payload).await?;
    match family_protocol {
        0x11 if payload.len() >= 12 => {
            let source = Ipv4Addr::new(payload[0], payload[1], payload[2], payload[3]);
            let port = u16::from_be_bytes([payload[8], payload[9]]);
            if port == 0 {
                return Err(Error::InvalidFrame("invalid PROXY v2 source port".into()));
            }
            Ok(SocketAddr::new(IpAddr::V4(source), port))
        }
        0x21 if payload.len() >= 36 => {
            let source = Ipv6Addr::from(
                <[u8; 16]>::try_from(&payload[..16])
                    .map_err(|_| Error::InvalidFrame("invalid PROXY v2 IPv6 source".into()))?,
            );
            let port = u16::from_be_bytes([payload[32], payload[33]]);
            if port == 0 {
                return Err(Error::InvalidFrame("invalid PROXY v2 source port".into()));
            }
            Ok(SocketAddr::new(canonical_ip(IpAddr::V6(source)), port))
        }
        _ => Err(Error::InvalidFrame(
            "PROXY v2 requires TCP over IPv4 or IPv6".into(),
        )),
    }
}

fn parse_port(value: &str) -> Result<u16> {
    value
        .parse::<u16>()
        .ok()
        .filter(|port| *port != 0)
        .ok_or_else(|| Error::InvalidFrame("invalid PROXY source or destination port".into()))
}

fn canonical_ip(address: IpAddr) -> IpAddr {
    match address {
        IpAddr::V6(address) => address
            .to_ipv4_mapped()
            .map_or(IpAddr::V6(address), IpAddr::V4),
        address => address,
    }
}

#[cfg(test)]
mod tests {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    use super::*;

    fn config() -> ProxyProtocolConfig {
        ProxyProtocolConfig::new(TrustedProxies::parse(["10.0.0.0/8"]).unwrap()).unwrap()
    }

    #[test]
    fn forwarded_chain_requires_a_trusted_peer() {
        let trusted = TrustedProxies::parse(["10.0.0.0/8"]).unwrap();
        let proxy = "10.0.0.2:443".parse().unwrap();
        assert_eq!(
            trusted.forwarded_address(proxy, "192.0.2.9, 10.0.0.1").ip(),
            "192.0.2.9".parse::<IpAddr>().unwrap()
        );
        let direct = "203.0.113.5:443".parse().unwrap();
        assert_eq!(
            trusted.forwarded_address(direct, "192.0.2.9").ip(),
            direct.ip()
        );
        assert_eq!(
            trusted.forwarded_address(proxy, "bad, 10.0.0.1").ip(),
            proxy.ip()
        );
    }

    #[test]
    fn proxy_protocol_requires_an_explicit_trust_boundary() {
        assert!(ProxyProtocolConfig::new(TrustedProxies::default()).is_err());
        assert!(TrustedProxies::parse(["not-a-network"]).is_err());
    }

    #[tokio::test]
    async fn rejects_an_untrusted_transport_peer_before_reading() {
        let (_client, mut server) = tokio::io::duplex(64);
        let error =
            proxy_client_address(&mut server, "203.0.113.5:5000".parse().unwrap(), &config())
                .await
                .unwrap_err();
        assert!(matches!(error, Error::Authentication));
    }

    #[tokio::test]
    async fn v1_preserves_application_payload() {
        let (mut client, mut server) = tokio::io::duplex(256);
        client
            .write_all(b"PROXY TCP4 192.0.2.1 198.51.100.1 1234 443\r\nhello")
            .await
            .unwrap();
        let address =
            proxy_client_address(&mut server, "10.1.2.3:5000".parse().unwrap(), &config())
                .await
                .unwrap();
        assert_eq!(address, "192.0.2.1:1234".parse().unwrap());
        let mut payload = [0_u8; 5];
        server.read_exact(&mut payload).await.unwrap();
        assert_eq!(&payload, b"hello");
    }

    #[tokio::test]
    async fn v2_reads_ipv4_tcp_source() {
        let (mut client, mut server) = tokio::io::duplex(256);
        let mut header = V2_SIGNATURE.to_vec();
        header.extend([0x21, 0x11, 0, 12]);
        header.extend([192, 0, 2, 2, 198, 51, 100, 2]);
        header.extend(2345_u16.to_be_bytes());
        header.extend(443_u16.to_be_bytes());
        client.write_all(&header).await.unwrap();
        let address =
            proxy_client_address(&mut server, "10.1.2.3:5000".parse().unwrap(), &config())
                .await
                .unwrap();
        assert_eq!(address, "192.0.2.2:2345".parse().unwrap());
    }
}
