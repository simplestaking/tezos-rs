// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

// TODO - NEWERRORS: revise

use anyhow::Context;
use thiserror::Error;
//use failure::{Backtrace, Context};
//use anyhow::Error;
use std::fmt::{self, Display};

/// Error produced by a [BinaryReader].
#[derive(Debug, Error)]
#[error(transparent)]
pub struct EncodingError<T: Display + std::fmt::Debug + Send + Sync + 'static> {
    #[from]
    inner: ErrorInfo<T>,
}

//impl<T: Display + Send + Sync + std::fmt::Debug + 'static> std::error::Error for EncodingError<T> {
//    fn source(&self) -> Option<&dyn std::error::Error> {
//        self.source.source()
//    }
//
//    //fn backtrace(&self) -> Option<&Backtrace> {
//    //    self.inner.backtrace()
//    //}
//}

//impl<T: Display + Send + Sync + 'static> Display for EncodingError<T> {
//    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//        Display::fmt(&self.inner, f)
//    }
//}

impl<T: Display + std::fmt::Debug + Send + Sync + Clone + 'static> EncodingError<T> {
    //pub fn kind(&self) -> T {
    //    self.inner.0.clone()
    //}

    pub fn location(&self) -> &String {
        &self.inner.1
    }

    pub(crate) fn field(&self, name: &String) -> ErrorInfo<T> {
        self.inner.field(name)
    }

    pub(crate) fn element_of(&self) -> ErrorInfo<T> {
        self.inner.element_of()
    }
}

impl<T: Display + std::fmt::Debug + Send + Sync + 'static> From<T> for EncodingError<T> {
    fn from(kind: T) -> Self {
        EncodingError { inner: kind.into() }
    }
}

//impl<T: Display + Send + Sync + 'static> From<ErrorInfo<T>> for EncodingError<T> {
//    fn from(info: ErrorInfo<T>) -> Self {
//        Self {
//            inner: Context::new(info),
//        }
//    }
//}
//
//impl<T: Display + Send + Sync + 'static> From<Context<ErrorInfo<T>>> for EncodingError<T> {
//    fn from(context: Context<ErrorInfo<T>>) -> Self {
//        Self { inner: context }
//    }
//}

/// Error kind with a string describing the error context
#[derive(Debug, Error)]
pub(crate) struct ErrorInfo<T: Display + std::fmt::Debug + Send + Sync + 'static>(T, String);

impl<T: Display + std::fmt::Debug + Send + Sync + Clone + 'static> ErrorInfo<T> {
    pub fn field(&self, name: &String) -> Self {
        let msg = if self.1.is_empty() {
            format!("field `{}`", name)
        } else {
            format!("{} @ field `{}`", self.1, name)
        };
        ErrorInfo(self.0.clone(), msg)
    }

    pub fn element_of(&self) -> Self {
        let msg = if self.1.is_empty() {
            format!("list element")
        } else {
            format!("{} @ list element", self.1)
        };
        ErrorInfo(self.0.clone(), msg)
    }
}

impl<T: Display + std::fmt::Debug + Send + Sync + 'static> Display for ErrorInfo<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        if self.1.is_empty() {
            write!(f, "{} @ unknown location", self.0)
        } else {
            write!(f, "{} @ {}", self.0, self.1)
        }
    }
}

impl<T: Display + std::fmt::Debug + Send + Sync + 'static> From<T> for ErrorInfo<T> {
    fn from(kind: T) -> Self {
        Self(kind, "".to_string())
    }
}
