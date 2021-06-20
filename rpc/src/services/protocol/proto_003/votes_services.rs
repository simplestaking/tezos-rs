// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use anyhow::format_err;
use crypto::hash::ContextHash;
use itertools::Itertools;

use storage::num_from_slice;
use tezos_messages::base::signature_public_key_hash::SignaturePublicKeyHash;
use tezos_messages::protocol::proto_003::votes::VoteListings;
use tezos_new_context::context_key_owned;

use crate::server::RpcServiceEnvironment;
use crate::services::protocol::VotesError;

pub fn get_votes_listings(
    env: &RpcServiceEnvironment,
    context_hash: &ContextHash,
) -> Result<Option<serde_json::Value>, VotesError> {
    // filter out the listings data
    let listings_data = if let Some(val) = env
        .tezedge_context()
        .get_key_values_by_prefix(&context_hash, context_key_owned!("data/votes/listings"))?
    {
        val
    } else {
        return Err(VotesError::ServiceError {
            reason: format_err!("No votes listings found in context"),
        });
    };

    // convert the raw context data to VoteListings
    let mut listings = Vec::with_capacity(listings_data.len());
    for (key, value) in listings_data.into_iter() {
        // get the address an the curve tag from the key (e.g. data/votes/listings/ed25519/2c/ca/28/ab/01/9ae2d8c26f4ce4924cad67a2dc6618)
        let keystr = key.join("/");
        let address = keystr.split('/').skip(4).take(6).join("");
        let curve = keystr.split('/').skip(3).take(1).join("");

        let address_decoded = SignaturePublicKeyHash::from_hex_hash_and_curve(&address, &curve)?
            .to_string_representation();
        listings.push(VoteListings::new(
            address_decoded,
            num_from_slice!(value, 0, i32),
        ));
    }

    // sort the vector in reverse ordering (as in ocaml node)
    listings.sort();
    listings.reverse();
    Ok(Some(serde_json::to_value(listings)?))
}
