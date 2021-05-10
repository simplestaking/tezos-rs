use std::time::Instant;

pub enum InvalidProposalError {
    ProposalOutdated,
}

pub enum AcceptorError<E> {
    InvalidProposal(InvalidProposalError),
    Custom(E),
}

impl<E> From<InvalidProposalError> for AcceptorError<E> {
    fn from(error: InvalidProposalError) -> Self {
        AcceptorError::InvalidProposal(error)
    }
}

impl<E> From<E> for AcceptorError<E> {
    fn from(error: E) -> Self {
        AcceptorError::Custom(error)
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
    type Error;

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
