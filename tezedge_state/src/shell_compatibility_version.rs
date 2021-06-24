use tezos_messages::p2p::encoding::prelude::NetworkVersion;
use tezos_messages::p2p::encoding::ack::NackMotive;

/// Holds informations about supported versions:
/// - all distributed_db_versions
/// - all p2p_versions
/// - version -> version used for bootstrap
#[derive(Clone, Debug)]
pub struct ShellCompatibilityVersion {
    /// All supported distributed_db_versions
    distributed_db_versions: Vec<u16>,

    /// All supported p2p_versions
    p2p_versions: Vec<u16>,

    /// version of network protocol, which we send to other peers
    version: NetworkVersion,
}

impl ShellCompatibilityVersion {
    const DEFAULT_VERSION: u16 = 0u16;

    pub fn new(
        chain_name: String,
        distributed_db_versions: Vec<u16>,
        p2p_versions: Vec<u16>,
    ) -> Self {
        Self {
            version: NetworkVersion::new(
                chain_name,
                *distributed_db_versions
                    .iter()
                    .max()
                    .unwrap_or(&Self::DEFAULT_VERSION),
                *p2p_versions.iter().max().unwrap_or(&Self::DEFAULT_VERSION),
            ),
            distributed_db_versions,
            p2p_versions,
        }
    }

    /// Returns Ok(version), if version is compatible, returns calculated compatible version for later use (NetworkVersion can contains feature support).
    /// Return Err(NackMotive), if something is wrong
    pub fn choose_compatible_version(
        &self,
        requested: &NetworkVersion,
    ) -> Result<NetworkVersion, NackMotive> {
        if !self.version.chain_name().eq(requested.chain_name()) {
            return Err(NackMotive::UnknownChainName);
        }

        Ok(NetworkVersion::new(
            self.version.chain_name().clone(),
            Self::select_compatible_version(
                &self.distributed_db_versions,
                requested.distributed_db_version(),
                NackMotive::DeprecatedDistributedDbVersion,
            )?,
            Self::select_compatible_version(
                &self.p2p_versions,
                requested.p2p_version(),
                NackMotive::DeprecatedP2pVersion,
            )?,
        ))
    }

    pub fn to_network_version(&self) -> NetworkVersion {
        self.version.clone()
    }

    fn select_compatible_version(
        supported_versions: &Vec<u16>,
        requested_version: &u16,
        nack_motive: NackMotive,
    ) -> Result<u16, NackMotive> {
        let best_supported_version = supported_versions
            .iter()
            .max()
            .unwrap_or(&Self::DEFAULT_VERSION);
        if best_supported_version <= requested_version {
            return Ok(*best_supported_version);
        }

        if supported_versions.contains(requested_version) {
            return Ok(*requested_version);
        }

        Err(nack_motive)
    }
}

#[cfg(test)]
mod tests {
    use tezos_messages::p2p::encoding::ack::NackMotive;
    use tezos_messages::p2p::encoding::version::NetworkVersion;

    use super::*;

    #[test]
    fn test_shell_version() {
        let tested =
            ShellCompatibilityVersion::new("TEST_CHAIN".to_string(), vec![3, 4], vec![1, 2]);

        assert!(matches!(
            tested.choose_compatible_version(&NetworkVersion::new("TEST_XYZ".to_string(), 0, 0)),
            Err(NackMotive::UnknownChainName)
        ));
        assert!(matches!(
            tested.choose_compatible_version(&NetworkVersion::new("TEST_CHAIN".to_string(), 0, 0)),
            Err(NackMotive::DeprecatedDistributedDbVersion)
        ));
        assert!(matches!(
            tested.choose_compatible_version(&NetworkVersion::new("TEST_CHAIN".to_string(), 1, 0)),
            Err(NackMotive::DeprecatedDistributedDbVersion)
        ));
        assert!(matches!(
            tested.choose_compatible_version(&NetworkVersion::new("TEST_CHAIN".to_string(), 2, 0)),
            Err(NackMotive::DeprecatedDistributedDbVersion)
        ));
        assert!(matches!(
            tested.choose_compatible_version(&NetworkVersion::new("TEST_CHAIN".to_string(), 3, 0)),
            Err(NackMotive::DeprecatedP2pVersion)
        ));
        assert!(matches!(
            tested.choose_compatible_version(&NetworkVersion::new("TEST_CHAIN".to_string(), 4, 0)),
            Err(NackMotive::DeprecatedP2pVersion)
        ));
        assert!(matches!(
            tested.choose_compatible_version(&NetworkVersion::new("TEST_CHAIN".to_string(), 5, 0)),
            Err(NackMotive::DeprecatedP2pVersion)
        ));

        assert_eq!(
            tested.choose_compatible_version(&NetworkVersion::new("TEST_CHAIN".to_string(), 3, 1)),
            Ok(NetworkVersion::new("TEST_CHAIN".to_string(), 3, 1))
        );
        assert_eq!(
            tested.choose_compatible_version(&NetworkVersion::new("TEST_CHAIN".to_string(), 3, 2)),
            Ok(NetworkVersion::new("TEST_CHAIN".to_string(), 3, 2))
        );
        assert_eq!(
            tested.choose_compatible_version(&NetworkVersion::new("TEST_CHAIN".to_string(), 3, 3)),
            Ok(NetworkVersion::new("TEST_CHAIN".to_string(), 3, 2))
        );

        assert_eq!(
            tested.choose_compatible_version(&NetworkVersion::new("TEST_CHAIN".to_string(), 4, 1)),
            Ok(NetworkVersion::new("TEST_CHAIN".to_string(), 4, 1))
        );
        assert_eq!(
            tested.choose_compatible_version(&NetworkVersion::new("TEST_CHAIN".to_string(), 4, 2)),
            Ok(NetworkVersion::new("TEST_CHAIN".to_string(), 4, 2))
        );
        assert_eq!(
            tested.choose_compatible_version(&NetworkVersion::new("TEST_CHAIN".to_string(), 4, 3)),
            Ok(NetworkVersion::new("TEST_CHAIN".to_string(), 4, 2))
        );
        assert_eq!(
            tested.choose_compatible_version(&NetworkVersion::new("TEST_CHAIN".to_string(), 5, 1)),
            Ok(NetworkVersion::new("TEST_CHAIN".to_string(), 4, 1))
        );
        assert_eq!(
            tested.choose_compatible_version(&NetworkVersion::new("TEST_CHAIN".to_string(), 5, 2)),
            Ok(NetworkVersion::new("TEST_CHAIN".to_string(), 4, 2))
        );
        assert_eq!(
            tested.choose_compatible_version(&NetworkVersion::new("TEST_CHAIN".to_string(), 5, 3)),
            Ok(NetworkVersion::new("TEST_CHAIN".to_string(), 4, 2))
        );

        assert_eq!(
            tested.to_network_version(),
            NetworkVersion::new("TEST_CHAIN".to_string(), 4, 2)
        );
    }
}
