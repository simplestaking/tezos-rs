use std::time::Instant;

pub trait Proposal {
    fn time(&self) -> Instant;
}
