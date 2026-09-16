use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

macro_rules! branded_bytes {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub struct $name([u8; 16]);

        impl $name {
            pub fn generate() -> Self {
                let mut bytes = [0u8; 16];
                getrandom::fill(&mut bytes).expect("system entropy");
                Self(bytes)
            }

            pub const fn from_bytes(bytes: [u8; 16]) -> Self {
                Self(bytes)
            }

            pub const fn as_bytes(&self) -> &[u8; 16] {
                &self.0
            }

            pub fn to_hex(&self) -> String {
                hex::encode(self.0)
            }

            pub fn from_hex(text: &str) -> Result<Self, String> {
                let bytes = hex::decode(text).map_err(|err| err.to_string())?;
                let arr: [u8; 16] = bytes
                    .try_into()
                    .map_err(|_| "expected 16 bytes".to_owned())?;
                Ok(Self(arr))
            }
        }

        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_str(&self.to_hex())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                let text = String::deserialize(deserializer)?;
                Self::from_hex(&text).map_err(serde::de::Error::custom)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.to_hex())
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}({})", stringify!($name), self.to_hex())
            }
        }
    };
}

macro_rules! branded_u64 {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Debug)]
        pub struct $name(u64);

        impl $name {
            pub const fn new(value: u64) -> Self {
                Self(value)
            }

            pub const fn get(self) -> u64 {
                self.0
            }

            pub const fn saturating_add(self, n: u64) -> Self {
                Self(self.0.saturating_add(n))
            }

            pub const fn next(self) -> Self {
                Self(self.0.saturating_add(1))
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

branded_bytes!(
    /// Persistent storage-member identity. Distinct from worker sessions and Raft terms.
    MemberId
);
branded_bytes!(
    /// Cluster identity bound into certificates and genesis.
    ClusterId
);
branded_bytes!(
    /// One admitted workflow run.
    RunId
);
branded_bytes!(
    /// Durable execution scope.
    ScopeId
);
branded_bytes!(
    /// Logical node invocation. Retries keep this identity.
    ActivationId
);
branded_bytes!(
    /// Worker process session. Distinct from member identity.
    WorkerSessionId
);
branded_bytes!(
    /// Durable wait occurrence.
    WaitId
);
branded_bytes!(
    /// Command identity reused across transport retries.
    CommandId
);
branded_bytes!(
    /// Client-supplied external event identity.
    EventId
);
branded_bytes!(
    /// Logical external effect. Distinct from attempts and Raft terms.
    EffectKey
);
branded_bytes!(
    /// Immutable payload artifact identity.
    ArtifactId
);

branded_u64!(
    /// OpenRaft term. Distinct from owner generation.
    RaftTerm
);
branded_u64!(
    /// Claim/session owner generation. Distinct from Raft term.
    OwnerGeneration
);
branded_u64!(
    /// Monotonic attempt number for one activation and role.
    AttemptNo
);
branded_u64!(
    /// Per-run workflow event sequence.
    RunSequence
);
branded_u64!(
    /// Claim lease revision.
    LeaseRevision
);
branded_u64!(
    /// Wait reservation revision.
    WaitRevision
);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionRole {
    Forward,
    Compensation,
    Reconciliation,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct DefinitionId {
    pub name: String,
    pub version: u32,
}

impl DefinitionId {
    pub fn new(name: impl Into<String>, version: u32) -> Self {
        Self {
            name: name.into(),
            version,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ActivityKey {
    pub name: String,
    pub version: u32,
}

impl ActivityKey {
    pub fn new(name: impl Into<String>, version: u32) -> Self {
        Self {
            name: name.into(),
            version,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct NodeKey(pub String);

impl NodeKey {
    pub fn parse(raw: &str) -> Result<Self, String> {
        if raw.len() > 64 {
            return Err("node key longer than 64 characters".to_owned());
        }
        let mut chars = raw.chars();
        match chars.next() {
            Some('a'..='z') => {}
            _ => return Err(format!("invalid node key {raw}")),
        }
        if !chars.all(|ch| matches!(ch, 'a'..='z' | '0'..='9' | '_')) {
            return Err(format!("invalid node key {raw}"));
        }
        Ok(Self(raw.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for NodeKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

pub fn valid_ascii_name(raw: &str) -> bool {
    !raw.is_empty()
        && raw.len() <= 64
        && raw.is_ascii()
        && raw
            .chars()
            .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '_' | '.' | '-'))
}
