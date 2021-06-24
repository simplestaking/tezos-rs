use std::time::{Instant, Duration};
use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};

use crypto::hash::CryptoboxPublicKeyHash;

use crate::{PeerAddress, Port};

#[derive(Debug, Clone)]
pub struct BlacklistedPeer {
    pub since: Instant,
    pub port: Option<Port>,
}

impl BlacklistedPeer {
    pub fn is_expired(&self, at: Instant, expiary_duration: Duration) -> bool {
        at.duration_since(self.since) >= expiary_duration
    }
}

#[derive(Debug, Clone)]
pub struct BlacklistedPeers {
    ips: HashMap<IpAddr, BlacklistedPeer>,
}

impl BlacklistedPeers {
    pub fn new() -> Self {
        Self {
            ips: HashMap::new(),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.ips.len()
    }

    pub fn insert_ip(&mut self, ip: IpAddr, peer: BlacklistedPeer) {
        self.ips.insert(ip, peer);
    }

    pub fn is_address_blacklisted(&self, addr: &PeerAddress) -> bool {
        self.ips.contains_key(&addr.ip())
    }

    pub fn is_identity_blacklisted(&self, identity: CryptoboxPublicKeyHash) -> bool {
        // TODO
        false
    }

    pub(crate) fn take_expired_blacklisted_peers(
        &mut self,
        at: Instant,
        expiary_duration: Duration,
    ) -> Vec<(IpAddr, Option<Port>)>
    {
        let addrs = self.ips.iter()
            .filter(|(_, x)| x.is_expired(at, expiary_duration))
            .map(|(ip, peer)| (*ip, peer.port))
            .collect::<Vec<_>>();

        for addr in addrs.iter() {
            self.ips.remove(&addr.0);
        }

        addrs
    }
}
