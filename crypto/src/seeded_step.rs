// Copyright (c) SimpleStaking and Tezedge Contributors
// SPDX-License-Identifier: MIT

use sodiumoxide::crypto::hash::sha256;
use sodiumoxide::crypto::hash::sha256::Digest;

use crate::hash::{BlockHash, CryptoboxPublicKeyHash};

#[derive(Clone)]
pub struct Seed<'s, 'r> {
    sender_id: &'s CryptoboxPublicKeyHash,
    receiver_id: &'r CryptoboxPublicKeyHash,
}

impl<'s, 'r> Seed<'s, 'r> {
    pub fn new(
        sender_id: &'s CryptoboxPublicKeyHash,
        receiver_id: &'r CryptoboxPublicKeyHash,
    ) -> Self {
        Self {
            sender_id,
            receiver_id,
        }
    }
}

/// Step implementation according to tezos [block_locator.ml]
/// The sequence is deterministic for a given triple of sender, receiver and block hash.
pub struct Step {
    step: i32,
    counter: i32,
    seed: Vec<u8>,
}

impl Step {
    pub fn init(seed: &Seed, block_hash: &BlockHash) -> Step {
        let mut hash_state = sha256::State::new();
        hash_state.update(seed.sender_id.as_ref());
        hash_state.update(seed.receiver_id.as_ref());
        hash_state.update(block_hash.as_ref());
        let Digest(h) = hash_state.finalize();
        Step {
            step: 1,
            counter: 9,
            seed: h.to_vec(),
        }
    }

    /// Returns new sequence number
    pub fn next(&mut self) -> i32 {
        let random_gap = if self.step <= 1 {
            0
        } else {
            // (Int32.succ(Int32.div step 2l))
            let n = (self.step / 2) + 1;

            // calculate new random gap
            // (Int32.rem (TzEndian.get_int32 seed 0) n)
            // TzEndian.get_int32 == Bytes.get_int32_be
            const MAX_SIZE: usize = std::mem::size_of::<i32>();
            let mut seed_as_bytes: [u8; MAX_SIZE] = Default::default();
            seed_as_bytes.copy_from_slice(&self.seed[0..MAX_SIZE]);
            let random_gap = i32::from_be_bytes(seed_as_bytes) % n;

            // mutate new seed
            let Digest(new_seed) = sha256::hash(&self.seed);
            self.seed = new_seed.to_vec();

            // return random gap
            random_gap
        };

        // mutate inner state and return old self.step
        let old_step = if self.counter == 0 {
            // reset counter
            self.counter = 9;
            // change step
            let new_step = self.step * 2;
            std::mem::replace(&mut self.step, new_step)
        } else {
            // just decrement counter
            self.counter = self.counter - 1;
            self.step
        };

        // calculate new sequence
        old_step - random_gap
    }
}

#[cfg(test)]
mod tests {
    use std::convert::{TryFrom, TryInto};

    use crate::hash::{CryptoboxPublicKeyHash, HashType};
    use crate::seeded_step::{Seed, Step};

    #[test]
    pub fn test_step_init() -> Result<(), anyhow::Error> {
        let step = Step::init(
            &Seed::new(&generate_key_string('s'), &generate_key_string('r')),
            &"BLrJE6yTjLLuEYgyqLxDBduEuA5S1uCkiq499tCK81TLoqTbNmm".try_into()?,
        );
        assert_step_state(
            &step,
            (
                1,
                9,
                "2343509bfcea5ce59e9c241ec61ff76c4e01f8a87eae3743755cb61ec875eaf5",
            ),
        )
    }

    #[test]
    pub fn test_step_next() -> Result<(), anyhow::Error> {
        let mut step = Step::init(
            &Seed::new(&generate_key_string('s'), &generate_key_string('r')),
            &"BLrJE6yTjLLuEYgyqLxDBduEuA5S1uCkiq499tCK81TLoqTbNmm".try_into()?,
        );
        assert_step_state(
            &step,
            (
                1,
                9,
                "2343509bfcea5ce59e9c241ec61ff76c4e01f8a87eae3743755cb61ec875eaf5",
            ),
        )?;

        // this is fixed/deterministic algorithm
        assert_eq!(1, step.next());
        assert_step_state(
            &step,
            (
                1,
                8,
                "2343509bfcea5ce59e9c241ec61ff76c4e01f8a87eae3743755cb61ec875eaf5",
            ),
        )?;

        assert_eq!(1, step.next());
        assert_step_state(
            &step,
            (
                1,
                7,
                "2343509bfcea5ce59e9c241ec61ff76c4e01f8a87eae3743755cb61ec875eaf5",
            ),
        )?;

        assert_eq!(1, step.next());
        assert_step_state(
            &step,
            (
                1,
                6,
                "2343509bfcea5ce59e9c241ec61ff76c4e01f8a87eae3743755cb61ec875eaf5",
            ),
        )?;

        assert_eq!(1, step.next());
        assert_step_state(
            &step,
            (
                1,
                5,
                "2343509bfcea5ce59e9c241ec61ff76c4e01f8a87eae3743755cb61ec875eaf5",
            ),
        )?;

        assert_eq!(1, step.next());
        assert_step_state(
            &step,
            (
                1,
                4,
                "2343509bfcea5ce59e9c241ec61ff76c4e01f8a87eae3743755cb61ec875eaf5",
            ),
        )?;

        assert_eq!(1, step.next());
        assert_step_state(
            &step,
            (
                1,
                3,
                "2343509bfcea5ce59e9c241ec61ff76c4e01f8a87eae3743755cb61ec875eaf5",
            ),
        )?;

        assert_eq!(1, step.next());
        assert_step_state(
            &step,
            (
                1,
                2,
                "2343509bfcea5ce59e9c241ec61ff76c4e01f8a87eae3743755cb61ec875eaf5",
            ),
        )?;

        assert_eq!(1, step.next());
        assert_step_state(
            &step,
            (
                1,
                1,
                "2343509bfcea5ce59e9c241ec61ff76c4e01f8a87eae3743755cb61ec875eaf5",
            ),
        )?;

        assert_eq!(1, step.next());
        assert_step_state(
            &step,
            (
                1,
                0,
                "2343509bfcea5ce59e9c241ec61ff76c4e01f8a87eae3743755cb61ec875eaf5",
            ),
        )?;

        assert_eq!(1, step.next());
        assert_step_state(
            &step,
            (
                2,
                9,
                "2343509bfcea5ce59e9c241ec61ff76c4e01f8a87eae3743755cb61ec875eaf5",
            ),
        )?;

        assert_eq!(1, step.next());
        assert_step_state(
            &step,
            (
                2,
                8,
                "62f4659c52f3e66107e4d1cfe3d5b3c174828a3d285d0da526a4b58e553426cc",
            ),
        )?;

        assert_eq!(2, step.next());
        assert_step_state(
            &step,
            (
                2,
                7,
                "b8d01c6925465e67c907b29dc569b16c40e7ec95f5b3bfa0e32fed297d64f8de",
            ),
        )?;

        assert_eq!(3, step.next());
        assert_step_state(
            &step,
            (
                2,
                6,
                "a16c86e850d6a3efcfb6b7277a917c3dd94f76c4b327aee22c8d77980104394e",
            ),
        )?;

        assert_eq!(2, step.next());
        assert_step_state(
            &step,
            (
                2,
                5,
                "4494e6d7d334c6b9e06e4deba404bb3b75e7e7da69b538d7d1edb615b01149a7",
            ),
        )?;

        assert_eq!(1, step.next());
        assert_step_state(
            &step,
            (
                2,
                4,
                "481fed31e113b32de5b3027c57dfd3c3821b89dd74426fbb7d89e0ec9157ce4d",
            ),
        )?;

        assert_eq!(1, step.next());
        assert_step_state(
            &step,
            (
                2,
                3,
                "429d08756497635db54cf3bedd57d463f912733e697f9ab8c6c96858f0bbed47",
            ),
        )?;

        assert_eq!(1, step.next());
        assert_step_state(
            &step,
            (
                2,
                2,
                "3bc0bb6978738088f8b01ffbc1ed1d3670efceb74bb29b33d2a21865ff297355",
            ),
        )?;

        assert_eq!(1, step.next());
        assert_step_state(
            &step,
            (
                2,
                1,
                "1f0024f5397212ac48aa419a6c211fe3c576b5b5ca11d788b56b1b32b3e773f8",
            ),
        )?;

        assert_eq!(1, step.next());
        assert_step_state(
            &step,
            (
                2,
                0,
                "1c847d14cd920b704c2127356b24ee5f56c59416230138de69c3d6fbd82c0661",
            ),
        )?;

        assert_eq!(2, step.next());
        assert_step_state(
            &step,
            (
                4,
                9,
                "75b7fa2466d9443833c12da615b01d3256780a1df643f93ff89bec58275a488b",
            ),
        )?;

        assert_eq!(3, step.next());
        assert_step_state(
            &step,
            (
                4,
                8,
                "f81889fea7490ebb43bc11a3c45e617c202d99a7c03da1cbbb9643474dbc497f",
            ),
        )?;

        assert_eq!(5, step.next());
        assert_step_state(
            &step,
            (
                4,
                7,
                "08317d1498dc2ee668a3dc335d439f92dec14b1a1497b9839ff9bebdbc39eed7",
            ),
        )?;

        assert_eq!(3, step.next());
        assert_step_state(
            &step,
            (
                4,
                6,
                "efe8eb910dabdcf78d6e29bf5bb24abea7842de2e272d4cc9a536eb526e20377",
            ),
        )?;

        assert_eq!(6, step.next());
        assert_step_state(
            &step,
            (
                4,
                5,
                "8e6c2390504b77cd7f568f48e731acdd38e7d5ed41b6af6604551192d593fe8c",
            ),
        )?;

        assert_eq!(5, step.next());
        assert_step_state(
            &step,
            (
                4,
                4,
                "242b0874e823a99ee3da6b9d30996fa29611e9de2b917cd3da7b8847a7e182cf",
            ),
        )?;

        assert_eq!(2, step.next());
        assert_step_state(
            &step,
            (
                4,
                3,
                "54bf370021dd3f691dd07e5d7aa9b99baa7bc0c6743192c7002a708f74bb3a33",
            ),
        )?;

        assert_eq!(4, step.next());
        assert_step_state(
            &step,
            (
                4,
                2,
                "1c882c0a6ead6ad6175eebf673b8cef11a3d0a15169d26ca8f6683b7268fdb85",
            ),
        )?;

        assert_eq!(2, step.next());
        assert_step_state(
            &step,
            (
                4,
                1,
                "78c7214174dd8e1dbf70d493dbfd961796bcfb3b9cd822b18db00c2aab4df030",
            ),
        )?;

        assert_eq!(4, step.next());
        assert_step_state(
            &step,
            (
                4,
                0,
                "807931241e56c0c79c08a11b1cc42d3d631e16932c5f11a8d960dcd3671fdbbb",
            ),
        )?;

        Ok(())
    }

    fn assert_step_state(
        step: &Step,
        (expected_step, expected_counter, expected_seed_as_hex): (i32, i32, &str),
    ) -> Result<(), anyhow::Error> {
        assert_eq!(expected_step, step.step);
        assert_eq!(expected_counter, step.counter);
        assert_eq!(hex::decode(expected_seed_as_hex)?, step.seed);
        Ok(())
    }

    fn generate_key_string(c: char) -> CryptoboxPublicKeyHash {
        CryptoboxPublicKeyHash::try_from(
            std::iter::repeat(c)
                .take(HashType::CryptoboxPublicKeyHash.size())
                .collect::<String>()
                .into_bytes(),
        )
        .unwrap()
    }
}
