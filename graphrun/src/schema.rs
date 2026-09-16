use crate::error::{Error, Result};
use crate::ids::valid_ascii_name;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SchemaKey {
    pub name: String,
    pub version: u32,
}

impl Serialize for SchemaKey {
    fn serialize<S: Serializer>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error> {
        serializer.serialize_str(&self.as_stable_name())
    }
}

impl<'de> Deserialize<'de> for SchemaKey {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        let text = String::deserialize(deserializer)?;
        SchemaKey::parse(&text).map_err(serde::de::Error::custom)
    }
}

impl SchemaKey {
    pub fn new(name: impl Into<String>, version: u32) -> Result<Self> {
        let name = name.into();
        if !valid_ascii_name(&name) {
            return Err(Error::invalid(format!("invalid schema name {name}")));
        }
        if version == 0 {
            return Err(Error::invalid("schema version must be positive"));
        }
        Ok(Self { name, version })
    }

    pub fn parse(raw: &str) -> Result<Self> {
        let (name, version) = raw
            .rsplit_once("/v")
            .ok_or_else(|| Error::invalid(format!("schema {raw} must look like name/vN")))?;
        let version: u32 = version
            .parse()
            .map_err(|_| Error::invalid(format!("schema {raw} has a non-integer version")))?;
        Self::new(name, version)
    }

    pub fn as_stable_name(&self) -> String {
        format!("{}/v{}", self.name, self.version)
    }
}

impl fmt::Display for SchemaKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_stable_name())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(tag = "ctor", rename_all = "snake_case")]
pub enum SchemaRef {
    Named { key: SchemaKey },
    Array { element: Box<SchemaRef> },
    Tuple { elements: Vec<SchemaRef> },
    Integer,
    String,
    Boolean,
    Null,
}

impl SchemaRef {
    pub fn named(name: &str, version: u32) -> Result<Self> {
        Ok(Self::Named {
            key: SchemaKey::new(name, version)?,
        })
    }

    pub fn array(element: SchemaRef) -> Self {
        Self::Array {
            element: Box::new(element),
        }
    }

    pub fn tuple(elements: Vec<SchemaRef>) -> Result<Self> {
        if elements.is_empty() || elements.len() > 16 {
            return Err(Error::invalid("tuple schemas must have 1 to 16 elements"));
        }
        Ok(Self::Tuple { elements })
    }

    pub fn is_integer(&self) -> bool {
        matches!(self, Self::Integer)
    }

    pub fn is_string(&self) -> bool {
        matches!(self, Self::String)
    }

    pub fn is_scalar(&self) -> bool {
        matches!(
            self,
            Self::Integer | Self::String | Self::Boolean | Self::Null
        )
    }
}

pub trait DurablePayload:
    serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static
{
    fn schema_ref() -> SchemaRef;
}

impl DurablePayload for () {
    fn schema_ref() -> SchemaRef {
        SchemaRef::named("unit", 1).expect("unit/v1")
    }
}

impl DurablePayload for i64 {
    fn schema_ref() -> SchemaRef {
        SchemaRef::Integer
    }
}

impl DurablePayload for String {
    fn schema_ref() -> SchemaRef {
        SchemaRef::String
    }
}

impl DurablePayload for bool {
    fn schema_ref() -> SchemaRef {
        SchemaRef::Boolean
    }
}

impl<T: DurablePayload> DurablePayload for Vec<T> {
    fn schema_ref() -> SchemaRef {
        SchemaRef::array(T::schema_ref())
    }
}

macro_rules! impl_tuple_payload {
    ($($T:ident),+) => {
        impl<$($T: DurablePayload),+> DurablePayload for ($($T,)+) {
            fn schema_ref() -> SchemaRef {
                SchemaRef::tuple(vec![$($T::schema_ref()),+]).expect("tuple arity")
            }
        }
    };
}

impl_tuple_payload!(T0);
impl_tuple_payload!(T0, T1);
impl_tuple_payload!(T0, T1, T2);
impl_tuple_payload!(T0, T1, T2, T3);
impl_tuple_payload!(T0, T1, T2, T3, T4);
impl_tuple_payload!(T0, T1, T2, T3, T4, T5);
impl_tuple_payload!(T0, T1, T2, T3, T4, T5, T6);
impl_tuple_payload!(T0, T1, T2, T3, T4, T5, T6, T7);
impl_tuple_payload!(T0, T1, T2, T3, T4, T5, T6, T7, T8);
impl_tuple_payload!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9);
impl_tuple_payload!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10);
impl_tuple_payload!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11);
impl_tuple_payload!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12);
impl_tuple_payload!(T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13);
impl_tuple_payload!(
    T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14
);
impl_tuple_payload!(
    T0, T1, T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, T12, T13, T14, T15
);
