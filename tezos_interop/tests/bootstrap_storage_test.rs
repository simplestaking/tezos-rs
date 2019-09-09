use tezos_interop::ffi;

// BMPtRJqFGQJRTfn8bXQR2grLE1M97XnUmG5vgjHMW7St1Wub7Cd
static BLOCK_HEADER_HASH_LEVEL_1: &str = "dd9fb5edc4f29e7d28f41fe56d57ad172b7686ed140ad50294488b68de29474d";
static BLOCK_HEADER_LEVEL_1: &str = include_str!("resources/block_header_level1.bytes");
fn block_header_level1_operations() -> Vec<Vec<String>> {
    vec![]
}

// BLwKksYwrxt39exDei7yi47h7aMcVY2kZMZhTwEEoSUwToQUiDV
static BLOCK_HEADER_HASH_LEVEL_2: &str = "60ab6d8d2a6b1c7a391f00aa6c1fc887eb53797214616fd2ce1b9342ad4965a4";
static BLOCK_HEADER_LEVEL_2: &str = "0000000201dd9fb5edc4f29e7d28f41fe56d57ad172b7686ed140ad50294488b68de29474d000000005c017cd804683625c2445a4e9564bf710c5528fd99a7d150d2a2a323bc22ff9e2710da4f6d0000001100000001000000000800000000000000029bd8c75dec93c276d2d8e8febc3aa6c9471cb2cb42236b3ab4ca5f1f2a0892f6000500000003ba671eef00d6a8bea20a4677fae51268ab6be7bd8cfc373cd6ac9e0a00064efcc404e1fb39409c5df255f7651e3d1bb5d91cb2172b687e5d56ebde58cfd92e1855aaafbf05";
fn block_header_level2_operations() -> Vec<Vec<String>> {
    vec![
        vec![],
        vec![],
        vec![],
        vec![]
    ]
}

// BLTQ5B4T4Tyzqfm3Yfwi26WmdQScr6UXVSE9du6N71LYjgSwbtc
static BLOCK_HEADER_HASH_LEVEL_3: &str = "a14f19e0df37d7b71312523305d71ac79e3d989c1c1d4e8e884b6857e4ec1627";
static BLOCK_HEADER_LEVEL_3: &str = "0000000301a14f19e0df37d7b71312523305d71ac79e3d989c1c1d4e8e884b6857e4ec1627000000005c017ed604dfcb6b41e91650bb908618b2740a6167d9072c3230e388b24feeef04c98dc27f000000110000000100000000080000000000000005f06879947f3d9959090f27054062ed23dbf9f7bd4b3c8a6e86008daabb07913e000c00000003e5445371002b9745d767d7f164a39e7f373a0f25166794cba491010ab92b0e281b570057efc78120758ff26a33301870f361d780594911549bcb7debbacd8a142e0b76a605";
fn block_header_level3_operations() -> Vec<Vec<String>> {
    vec![
        vec!["a14f19e0df37d7b71312523305d71ac79e3d989c1c1d4e8e884b6857e4ec1627000000000236663bacdca76094fdb73150092659d463fec94eda44ba4db10973a1ad057ef53a5b3239a1b9c383af803fc275465bd28057d68f3cab46adfd5b2452e863ff0a".to_string()],
        vec![],
        vec![],
        vec![]
    ]
}

#[test]
fn test_bootstrap_empty_storage_with_first_three_blocks() {

    // apply first block
    let ocaml_result = ffi::apply_block(
        BLOCK_HEADER_HASH_LEVEL_1.to_string(),
        BLOCK_HEADER_LEVEL_1.to_string(),
        block_header_level1_operations()
    );
    let ocaml_result = futures::executor::block_on(ocaml_result);
    assert_eq!("activate PsddFKi32cMJ", &ocaml_result);

    // apply second block
    let ocaml_result = ffi::apply_block(
        BLOCK_HEADER_HASH_LEVEL_2.to_string(),
        BLOCK_HEADER_LEVEL_2.to_string(),
        block_header_level2_operations()
    );
    let ocaml_result = futures::executor::block_on(ocaml_result);
    assert_eq!("lvl 2, fit 2, prio 5, 0 ops", &ocaml_result);

    // apply third block
    let ocaml_result = ffi::apply_block(
        BLOCK_HEADER_HASH_LEVEL_3.to_string(),
        BLOCK_HEADER_LEVEL_3.to_string(),
        block_header_level3_operations()
    );
    let ocaml_result = futures::executor::block_on(ocaml_result);
    assert_eq!("lvl 3, fit 5, prio 12, 1 ops", &ocaml_result);
}