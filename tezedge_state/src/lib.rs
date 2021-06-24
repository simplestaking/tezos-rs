mod peer_address;
pub use peer_address::{PeerAddress, Port};

mod peer_crypto;
pub use peer_crypto::PeerCrypto;

pub mod proposals;
pub mod acceptors;
pub mod chunking;

mod shell_compatibility_version;
pub use shell_compatibility_version::*;

mod state;
pub use state::*;

mod tezedge_state_wrapper;
pub use tezedge_state_wrapper::*;

pub mod proposer;

pub mod sample_tezedge_state;

#[derive(Debug, Eq, PartialEq)]
pub enum InvalidProposalError {
    ProposalOutdated,
}
