use crypto::nonce::Nonce;
use rand::{Rng, prelude::IteratorRandom};

use crate::peer_address::{PeerAddress, PeerListenerAddress};

pub trait Effects {
    fn get_nonce(&mut self, peer: &PeerAddress) -> Nonce;

    fn choose_peers_to_connect_to(
        &mut self,
        potential_peers: std::collections::hash_set::Iter<PeerListenerAddress>,
        potential_peers_len: usize,
        choice_len: usize,
    ) -> Vec<PeerListenerAddress>;
}

#[derive(Debug, Default)]
pub struct DefaultEffects;

impl Effects for DefaultEffects {
    fn get_nonce(&mut self, peer: &PeerAddress) -> Nonce {
        Nonce::random()
    }

    fn choose_peers_to_connect_to(
        &mut self,
        potential_peers: std::collections::hash_set::Iter<PeerListenerAddress>,
        potential_peers_len: usize,
        choice_len: usize,
    ) -> Vec<PeerListenerAddress>
    {
        if choice_len >= potential_peers_len {
            potential_peers.cloned().collect()
        } else {
            let mut rng = rand::thread_rng();
            potential_peers.cloned().choose_multiple(&mut rng, choice_len)
        }
    }
}
