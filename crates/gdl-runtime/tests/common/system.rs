//! Asking a segment process's system listener, as a probe does.

use std::net::SocketAddr;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// Send `request` as it is and return the whole response.
pub async fn send(address: SocketAddr, request: &[u8]) -> String {
    let exchange = async {
        let mut stream = tokio::net::TcpStream::connect(address).await?;
        stream.write_all(request).await?;
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await?;
        Ok::<_, std::io::Error>(response)
    };
    let response = tokio::time::timeout(Duration::from_secs(5), exchange)
        .await
        .expect("the listener answered in time")
        .expect("the exchange");
    String::from_utf8(response).expect("text")
}

/// `GET path`: the status code and the body.
pub async fn get(address: SocketAddr, path: &str) -> (u16, String) {
    let response = send(
        address,
        format!("GET {path} HTTP/1.1\r\nHost: probe\r\n\r\n").as_bytes(),
    )
    .await;
    let code = response
        .split(' ')
        .nth(1)
        .and_then(|code| code.parse().ok())
        .expect("a status code");
    let body = response.split("\r\n\r\n").nth(1).unwrap_or("").to_string();
    (code, body)
}
