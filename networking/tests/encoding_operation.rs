use failure::Error;

use networking::p2p::encoding::prelude::*;
use networking::p2p::binary_message::BinaryMessage;
use tezos_encoding::hash::{HashEncoding, HashType};

#[test]
fn can_deserialize() -> Result<(), Error> {
    let message_bytes = hex::decode("10490b79070cf19175cd7e3b9c1ee66f6e85799980404b119132ea7e58a4a97e000008c387fa065a181d45d47a9b78ddc77e92a881779ff2cbabbf9646eade4bf1405a08e00b725ed849eea46953b10b5cdebc518e6fd47e69b82d2ca18c4cf6d2f312dd08")?;
    let operation = Operation::from_bytes(message_bytes)?;
    assert_eq!("BKqTKfGwK3zHnVXX33X5PPHy1FDTnbkajj3eFtCXGFyfimQhT1H", HashEncoding::new(HashType::BlockHash).bytes_to_string(&operation.branch()));
    Ok(assert_eq!("000008c387fa065a181d45d47a9b78ddc77e92a881779ff2cbabbf9646eade4bf1405a08e00b725ed849eea46953b10b5cdebc518e6fd47e69b82d2ca18c4cf6d2f312dd08", &hex::encode(&operation.data())))
}