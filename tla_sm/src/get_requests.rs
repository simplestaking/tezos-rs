use std::fmt::Debug;

pub trait GetRequests {
    type Request: Debug;

    fn get_requests(&self, buf: &mut Vec<Self::Request>) -> usize;
}
