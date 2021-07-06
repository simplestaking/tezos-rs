use std::str::FromStr;
use std::fmt::{self, Display};
use std::hash::{Hash, Hasher};
use std::net::{SocketAddr, IpAddr, Ipv4Addr, AddrParseError};

pub type Port = u16;

#[derive(Debug, Hash, Eq, PartialEq, Clone, Copy)]
// #[derive(Debug, Clone, Copy)]
#[repr(transparent)]
pub struct PeerAddress(SocketAddr);

impl PeerAddress {
    pub fn new(ip: IpAddr, port: Port) -> Self {
        Self(SocketAddr::new(ip, port))
    }

    pub fn ip(&self) -> IpAddr {
        self.0.ip()
    }

    pub fn port(&self) -> Port {
        self.0.port()
    }

    pub fn set_port(&mut self, new_port: Port) {
        self.0.set_port(new_port)
    }

    /// For testing purposes.
    ///
    /// Get unique ipv4 address based on index.
    pub fn ipv4_from_index(index: u64) -> Self {
        Self(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(
                ((index / 256 / 256 / 256) % 256) as u8,
                ((index / 256 / 256) % 256) as u8,
                ((index / 256) % 256) as u8,
                (index % 256) as u8,
            )),
            12345,
        ))
    }

    /// For testing purposes.
    ///
    /// turn ipv4 to unique index.
    pub fn to_index(&self) -> u64 {
        match self.0 {
            SocketAddr::V4(addr) => {
                let [a, b, c, d] = addr.ip().octets();
                let (a, b, c, d) = (a as u64, b as u64, c as u64, d as u64);
                a * 256 * 256 * 256 + b * 256 * 256 + c * 256 + d
            }
            SocketAddr::V6(_) => unimplemented!(),
        }
    }
}

impl Display for PeerAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

// impl Hash for PeerAddress {
//     /// Hash only by ip address, so we don't have more than 1 peer per IP.
//     fn hash<H: Hasher>(&self, state: &mut H) {
//         self.0.ip().hash(state)
//     }
// }

// impl PartialEq for PeerAddress {
//     /// Compare only by ip address, so we don't have more than 1 peer per IP.
//     fn eq(&self, other: &Self) -> bool {
//         self.0.ip().eq(&other.ip())
//     }
// }

// impl Eq for PeerAddress {}

impl FromStr for PeerAddress {
    type Err = AddrParseError;
    fn from_str(s: &str) -> Result<Self, AddrParseError> {
        Ok(Self(s.parse()?))
    }
}

impl From<SocketAddr> for PeerAddress {
    fn from(addr: SocketAddr) -> Self {
        Self(addr)
    }
}

impl From<&SocketAddr> for PeerAddress {
    fn from(addr: &SocketAddr) -> Self {
        Self(*addr)
    }
}

impl From<PeerAddress> for SocketAddr {
    fn from(addr: PeerAddress) -> Self {
        addr.0
    }
}

impl From<&PeerAddress> for SocketAddr {
    fn from(addr: &PeerAddress) -> Self {
        addr.0
    }
}

#[derive(Debug, Hash, Eq, PartialEq, Clone, Copy)]
#[repr(transparent)]
pub struct PeerListenerAddress(SocketAddr);

impl PeerListenerAddress {
    pub fn new(ip: IpAddr, peer_listener_port: Port) -> Self {
        Self(SocketAddr::new(ip, peer_listener_port))
    }
}

impl Display for PeerListenerAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for PeerListenerAddress {
    type Err = AddrParseError;
    fn from_str(s: &str) -> Result<Self, AddrParseError> {
        Ok(Self(s.parse()?))
    }
}

impl From<SocketAddr> for PeerListenerAddress {
    fn from(addr: SocketAddr) -> Self {
        Self(addr)
    }
}

impl From<&SocketAddr> for PeerListenerAddress {
    fn from(addr: &SocketAddr) -> Self {
        Self(*addr)
    }
}

impl From<PeerListenerAddress> for SocketAddr {
    fn from(addr: PeerListenerAddress) -> Self {
        addr.0
    }
}

impl From<&PeerListenerAddress> for SocketAddr {
    fn from(addr: &PeerListenerAddress) -> Self {
        addr.0
    }
}

impl From<PeerListenerAddress> for PeerAddress {
    fn from(addr: PeerListenerAddress) -> Self {
        addr.0.into()
    }
}

impl From<&PeerListenerAddress> for PeerAddress {
    fn from(addr: &PeerListenerAddress) -> Self {
        addr.0.into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_peer_address_from_index_equals_to_index() {
        assert_eq!(255, PeerAddress::ipv4_from_index(255).to_index());
        assert_eq!(255 + 600, PeerAddress::ipv4_from_index(255 + 600).to_index());
        assert_eq!(255 * 255 + 100, PeerAddress::ipv4_from_index(255 * 255 + 100).to_index());
        assert_eq!(255 * 255 * 255 + 100, PeerAddress::ipv4_from_index(255 * 255 * 255 + 100).to_index());
        assert_eq!(255 * 255 * 255 * 255 + 100, PeerAddress::ipv4_from_index(255 * 255 * 255 * 255 + 100).to_index());
    }
}
