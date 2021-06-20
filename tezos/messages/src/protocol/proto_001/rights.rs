// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use std::collections::HashMap;

use serde::Serialize;

use crate::base::rpc_support::{ToRpcJsonMap, UniversalValue};
use crate::base::signature_public_key_hash::SignaturePublicKeyHash;

/// Endorsing rights structure, final response look like Vec<EndorsingRight>
#[derive(Serialize, Debug, Clone)]
pub struct EndorsingRight {
    /// block level for which endorsing rights are generated
    pub level: i32,

    /// endorser contract id
    pub delegate: SignaturePublicKeyHash,

    /// list of endorsement slots
    pub slots: Vec<u16>,

    /// estimated time of endorsement, is set to None if in past relative to block_id
    pub estimated_time: Option<i64>,
}

impl EndorsingRight {
    /// Simple constructor to construct EndorsingRight
    pub fn new(
        level: i32,
        delegate: SignaturePublicKeyHash,
        slots: Vec<u16>,
        estimated_time: Option<i64>,
    ) -> Self {
        Self {
            level,
            delegate,
            slots,
            estimated_time,
        }
    }
}

impl ToRpcJsonMap for EndorsingRight {
    fn as_map(&self) -> HashMap<&'static str, UniversalValue> {
        let mut ret: HashMap<&'static str, UniversalValue> = Default::default();
        ret.insert("level", UniversalValue::num(self.level));
        ret.insert(
            "delegate",
            UniversalValue::string(self.delegate.to_string_representation()),
        );
        ret.insert("slots", UniversalValue::num_list(self.slots.iter()));
        if let Some(ts) = self.estimated_time {
            ret.insert("estimated_time", UniversalValue::timestamp_rfc3339(ts));
        }
        ret
    }
}

/// Object containing information about the baking rights
#[derive(Serialize, Debug, Clone)]
pub struct BakingRights {
    /// block level for which baking rights are generated
    pub level: i32,

    /// baker contract id
    pub delegate: SignaturePublicKeyHash,

    /// baker priority to bake block
    pub priority: u16,

    /// estimated time of baking based on baking priority, is set to None if in past relative to block_id
    pub estimated_time: Option<i64>,
}

impl BakingRights {
    /// Simple constructor to construct BakingRights
    pub fn new(
        level: i32,
        delegate: SignaturePublicKeyHash,
        priority: u16,
        estimated_time: Option<i64>,
    ) -> Self {
        Self {
            level,
            delegate,
            priority,
            estimated_time,
        }
    }
}

impl ToRpcJsonMap for BakingRights {
    fn as_map(&self) -> HashMap<&'static str, UniversalValue> {
        let mut ret: HashMap<&'static str, UniversalValue> = Default::default();
        ret.insert("level", UniversalValue::num(self.level));
        ret.insert(
            "delegate",
            UniversalValue::string(self.delegate.to_string_representation()),
        );
        ret.insert("priority", UniversalValue::num(self.priority));
        if let Some(ts) = self.estimated_time {
            ret.insert("estimated_time", UniversalValue::timestamp_rfc3339(ts));
        }
        ret
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Error;
    use assert_json_diff::assert_json_eq;

    use serde_json::json;

    use crate::base::rpc_support::ToRpcJsonMap;
    use crate::base::signature_public_key_hash::SignaturePublicKeyHash;
    use crate::protocol::proto_001::rights::{BakingRights, EndorsingRight};

    #[test]
    fn test_endorsing_right_with_tz1_to_json() -> Result<(), Error> {
        let er = EndorsingRight::new(
            296772,
            SignaturePublicKeyHash::from_b58_hash("tz1VxS7ff4YnZRs8b4mMP4WaMVpoQjuo1rjf")?,
            vec![27, 16, 14, 13, 0],
            Some(1585207011),
        );

        let json = er.as_map();
        let expected_json = json!({"level":296772,"delegate":"tz1VxS7ff4YnZRs8b4mMP4WaMVpoQjuo1rjf","slots":[27,16,14,13,0],"estimated_time":"2020-03-26T07:16:51Z"});
        assert_json_eq!(expected_json, serde_json::to_value(json)?);
        Ok(())
    }

    #[test]
    fn test_endorsing_right_with_tz3_to_json() -> Result<(), Error> {
        let er = EndorsingRight::new(
            296771,
            SignaturePublicKeyHash::from_b58_hash("tz3WXYtyDUNL91qfiCJtVUX746QpNv5i5ve5")?,
            vec![30, 1],
            Some(1585207011),
        );

        let json = er.as_map();
        let expected_json = json!({"level":296771,"delegate":"tz3WXYtyDUNL91qfiCJtVUX746QpNv5i5ve5","slots":[30,1],"estimated_time":"2020-03-26T07:16:51Z"});
        assert_json_eq!(expected_json, serde_json::to_value(json)?);
        Ok(())
    }

    #[test]
    fn test_baking_right_to_json() -> Result<(), Error> {
        let er = BakingRights::new(
            296781,
            SignaturePublicKeyHash::from_b58_hash("tz2BFE2MEHhphgcR7demCGQP2k1zG1iMj1oj")?,
            123,
            Some(1585207381),
        );

        let json = er.as_map();
        let expected_json = json!({"level":296781,"delegate":"tz2BFE2MEHhphgcR7demCGQP2k1zG1iMj1oj","priority":123,"estimated_time":"2020-03-26T07:23:01Z"});
        assert_json_eq!(expected_json, serde_json::to_value(json)?);
        Ok(())
    }
}
