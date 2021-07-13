use crate::Proposal;

pub trait Acceptor<P: Proposal> {
    fn accept(&mut self, proposal: P);
}
