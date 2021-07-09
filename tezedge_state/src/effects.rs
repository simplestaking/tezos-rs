use std::collections::HashSet;
use rand::{Rng, prelude::IteratorRandom};

use crypto::nonce::Nonce;

use crate::peer_address::{PeerAddress, PeerListenerAddress};

pub trait Effects {
    fn get_nonce(&mut self, peer: &PeerAddress) -> Nonce;

    fn choose_peers_to_connect_to(
        &mut self,
        potential_peers: &HashSet<PeerListenerAddress>,
        choice_len: usize,
    ) -> Vec<PeerListenerAddress>;

    fn choose_potential_peers_for_nack(
        &mut self,
        potential_peers: &HashSet<PeerListenerAddress>,
    ) -> Vec<PeerListenerAddress>;
}

#[derive(Debug, Default, Clone)]
pub struct DefaultEffects;

impl Effects for DefaultEffects {
    fn get_nonce(&mut self, peer: &PeerAddress) -> Nonce {
        Nonce::random()
    }

    fn choose_peers_to_connect_to(
        &mut self,
        potential_peers: &HashSet<PeerListenerAddress>,
        choice_len: usize,
    ) -> Vec<PeerListenerAddress>
    {
        if choice_len == 0 {
            return vec![];
        }

        if choice_len >= potential_peers.len() {
            potential_peers.iter().cloned().collect()
        } else {
            let mut rng = rand::thread_rng();
            potential_peers.iter().cloned().choose_multiple(&mut rng, choice_len)
        }
    }

    fn choose_potential_peers_for_nack(
        &mut self,
        potential_peers: &HashSet<PeerListenerAddress>,
    ) -> Vec<PeerListenerAddress>
    {
        let mut rng = rand::thread_rng();
        let len = rng.gen_range(1, 80.min(potential_peers.len()));
        if len >= potential_peers.len() {
            potential_peers.iter().cloned().collect()
        } else {
            potential_peers.iter().cloned().choose_multiple(&mut rng, len)
        }
    }
}
