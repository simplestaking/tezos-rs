use std::time::Instant;
use std::fmt::Debug;

#[derive(Debug)]
pub enum InvalidProposalError {
    ProposalOutdated,
}

#[derive(Debug)]
pub enum AcceptorError<E: Debug> {
    InvalidProposal(InvalidProposalError),
    Custom(E),
}

impl<E: Debug> From<InvalidProposalError> for AcceptorError<E> {
    fn from(error: InvalidProposalError) -> Self {
        AcceptorError::InvalidProposal(error)
    }
}

pub trait Proposal {
    fn time(&self) -> Instant;
}

pub trait NewestTimeSeen {
    fn newest_time_seen(&self) -> Instant;
    fn newest_time_seen_mut(&mut self) -> &mut Instant;
}

pub trait Acceptor<P: Proposal>: NewestTimeSeen {
    type Error: Debug;

    fn accept(&mut self, proposal: P) -> Result<(), AcceptorError<Self::Error>>;

    fn check_and_update_time(&mut self, proposal: &P) -> Result<(), InvalidProposalError> {
        let mut time = self.newest_time_seen_mut();
        if proposal.time() >= *time {
            *time = proposal.time();
            Ok(())
        } else {
            Err(InvalidProposalError::ProposalOutdated)
        }
    }

    fn validate_proposal(&mut self, proposal: &P) -> Result<(), InvalidProposalError> {
        self.check_and_update_time(proposal)?;

        Ok(())
    }
}

pub trait React {
    fn react(&mut self) {
    }
}

pub trait GetRequests {
    type Request: Debug;

    fn get_requests(&self) -> Vec<Self::Request>;
}
