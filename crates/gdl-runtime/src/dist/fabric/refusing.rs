//! An instance whose address refuses every connection, for tests.

use std::net::SocketAddr;

use socket2::{Domain, Socket, Type};
use velo::transports::tcp::{TcpTransport, TcpTransportBuilder};
use velo::{InstanceId, PeerInfo, Transport};

/// An instance whose address refuses every connection while this lives,
/// as the address of an instance that stopped does.
///
/// Tests use this instead of stopping a node. The port a stopped node
/// frees is not reserved: another node (another test, or another copy of
/// the test binary) may bind it at once, and the stopped node's own
/// listener may still be open when its shutdown returns on a busy host.
/// Either one accepts the connection and never answers velo's handshake, so
/// the send waits out velo's handshake timeout instead of failing as
/// unsent.
///
/// The port is held by a socket bound without `SO_REUSEADDR` that never
/// listens. A connect to it is refused, and no other socket can bind it.
pub(crate) struct Refusing {
    id: InstanceId,
    /// Never started: it only holds the socket, and gives the address in
    /// velo's own encoding.
    transport: TcpTransport,
    addr: SocketAddr,
}

impl Refusing {
    pub(crate) fn new() -> Self {
        let socket = Socket::new(Domain::IPV4, Type::STREAM, None).expect("a socket");
        socket
            .bind(&SocketAddr::from(([127, 0, 0, 1], 0)).into())
            .expect("a port");
        let held: std::net::TcpListener = socket.into();
        let addr = held.local_addr().expect("its address");
        let transport = TcpTransportBuilder::new()
            .from_listener(held)
            .expect("the held socket")
            .build()
            .expect("a transport");
        Self {
            id: InstanceId::new_v4(),
            transport,
            addr,
        }
    }

    /// The instance, as a roster or a node is told of it.
    pub(crate) fn peer(&self) -> PeerInfo {
        PeerInfo::new(self.id, self.transport.address())
    }

    fn addr(&self) -> SocketAddr {
        self.addr
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Nothing can answer at a refusing instance's address: a connect is
    /// refused, and no listener can take its port. A stopped node's address
    /// fails the second check.
    #[test]
    fn nothing_can_answer_at_a_refusing_address() {
        let refusing = Refusing::new();
        let error = std::net::TcpStream::connect(refusing.addr()).expect_err("a connection");
        assert_eq!(
            error.kind(),
            std::io::ErrorKind::ConnectionRefused,
            "{error}"
        );
        let taken = std::net::TcpListener::bind(refusing.addr());
        assert!(taken.is_err(), "a listener took the port");
    }
}
