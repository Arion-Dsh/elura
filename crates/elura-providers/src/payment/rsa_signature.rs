use pem_rfc7468::decode_vec;
use ring::rsa;

use crate::{ProviderError, ProviderResult};

pub(super) fn parse_private_key(pem: &str, provider: &str) -> ProviderResult<rsa::KeyPair> {
    let (label, der) = decode_vec(pem.trim().as_bytes())
        .map_err(|_| ProviderError::Config(format!("invalid {provider} RSA private key PEM")))?;
    let key = match label {
        "PRIVATE KEY" => rsa::KeyPair::from_pkcs8(&der),
        "RSA PRIVATE KEY" => rsa::KeyPair::from_der(&der),
        _ => {
            return Err(ProviderError::Config(format!(
                "unsupported {provider} RSA private key type {label:?}"
            )));
        }
    };
    key.map_err(|_| ProviderError::Config(format!("invalid {provider} RSA private key")))
}

pub(super) fn parse_public_key(pem: &str, provider: &str) -> ProviderResult<Vec<u8>> {
    let (label, der) = decode_vec(pem.trim().as_bytes())
        .map_err(|_| ProviderError::Config(format!("invalid {provider} RSA public key PEM")))?;
    let key = match label {
        "RSA PUBLIC KEY" => der,
        "PUBLIC KEY" => subject_public_key(&der)?.to_vec(),
        "CERTIFICATE" => subject_public_key(certificate_spki(&der)?)?.to_vec(),
        _ => {
            return Err(ProviderError::Config(format!(
                "unsupported {provider} RSA public key type {label:?}"
            )));
        }
    };
    validate_public_key(&key)?;
    Ok(key)
}

fn subject_public_key(spki: &[u8]) -> ProviderResult<&[u8]> {
    let (tag, start, end) = tlv(spki, 0)?;
    if tag != 0x30 || end != spki.len() {
        return Err(ProviderError::Config("invalid RSA SPKI".into()));
    }
    let (algorithm_tag, _, algorithm_end) = tlv(spki, start)?;
    let (key_tag, key_start, key_end) = tlv(spki, algorithm_end)?;
    if algorithm_tag != 0x30 || key_tag != 0x03 || key_end != end || spki.get(key_start) != Some(&0)
    {
        return Err(ProviderError::Config("invalid RSA SPKI".into()));
    }
    spki.get(key_start + 1..key_end)
        .ok_or_else(|| ProviderError::Config("invalid RSA SPKI key".into()))
}

fn validate_public_key(key: &[u8]) -> ProviderResult<()> {
    let (tag, start, end) = tlv(key, 0)?;
    let (modulus_tag, modulus_start, modulus_end) = tlv(key, start)?;
    let (exponent_tag, exponent_start, exponent_end) = tlv(key, modulus_end)?;
    let modulus = key
        .get(modulus_start..modulus_end)
        .unwrap_or_default()
        .strip_prefix(&[0])
        .unwrap_or_else(|| key.get(modulus_start..modulus_end).unwrap_or_default());
    let exponent = key.get(exponent_start..exponent_end).unwrap_or_default();
    if tag != 0x30
        || modulus_tag != 0x02
        || exponent_tag != 0x02
        || exponent_end != end
        || end != key.len()
        || !(256..=1024).contains(&modulus.len())
        || exponent.is_empty()
        || exponent.len() > 5
        || exponent.last().is_none_or(|byte| byte & 1 == 0)
    {
        return Err(ProviderError::Config("invalid RSA public key".into()));
    }
    Ok(())
}

fn certificate_spki(certificate: &[u8]) -> ProviderResult<&[u8]> {
    let (_, outer_start, outer_end) = tlv(certificate, 0)?;
    let (_, tbs_start, tbs_end) = tlv(certificate, outer_start)?;
    let mut position = tbs_start;
    if certificate.get(position) == Some(&0xa0) {
        position = tlv(certificate, position)?.2;
    }
    for _ in 0..5 {
        position = tlv(certificate, position)?.2;
    }
    let start = position;
    let (tag, _, end) = tlv(certificate, position)?;
    if tag != 0x30 || end > tbs_end || outer_end > certificate.len() {
        return Err(ProviderError::Config("invalid certificate SPKI".into()));
    }
    Ok(&certificate[start..end])
}

fn tlv(data: &[u8], position: usize) -> ProviderResult<(u8, usize, usize)> {
    let tag = *data
        .get(position)
        .ok_or_else(|| ProviderError::Config("invalid DER".into()))?;
    let first = *data
        .get(position + 1)
        .ok_or_else(|| ProviderError::Config("invalid DER".into()))?;
    let (length, header) = if first & 0x80 == 0 {
        (usize::from(first), 2)
    } else {
        let count = usize::from(first & 0x7f);
        if count == 0 || count > 4 {
            return Err(ProviderError::Config("invalid DER length".into()));
        }
        let mut length = 0usize;
        for byte in data
            .get(position + 2..position + 2 + count)
            .ok_or_else(|| ProviderError::Config("invalid DER length".into()))?
        {
            length = length
                .checked_mul(256)
                .and_then(|value| value.checked_add(usize::from(*byte)))
                .ok_or_else(|| ProviderError::Config("DER length overflow".into()))?;
        }
        (length, 2 + count)
    };
    let start = position + header;
    let end = start
        .checked_add(length)
        .filter(|end| *end <= data.len())
        .ok_or_else(|| ProviderError::Config("invalid DER range".into()))?;
    Ok((tag, start, end))
}
