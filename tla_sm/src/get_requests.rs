use std::fmt::Debug;

pub trait GetRequests {
    type Request: Debug;

    fn get_requests(&self) -> Vec<Self::Request>;
}
