use crypto::nonce::Nonce;

use crate::PeerAddress;

pub trait Effects {
    fn get_nonce(&mut self, peer: &PeerAddress) -> Nonce;
}

#[derive(Debug, Default)]
pub struct DefaultEffects;

impl Effects for DefaultEffects {
    fn get_nonce(&mut self, peer: &PeerAddress) -> Nonce {
        Nonce::random()
    }
}
