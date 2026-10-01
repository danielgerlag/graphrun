use crate::catalog::Catalog;
use crate::error::{Error, Result};
use crate::ids::{ActivityKey, ExecutionRole, valid_ascii_name};
use crate::schema::SchemaRef;
use crate::value::canonical_json;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const PROTOCOL_VERSION: u32 = 1;
pub const CODEC_VERSION: u32 = 1;

/// Exact executable binding to an immutable catalog contract.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerCapability {
    pub activity_name: String,
    pub activity_version: u32,
    pub role: ExecutionRole,
    pub codec_version: u32,
    pub input_schema_digest: String,
    pub output_schema_digest: String,
    pub contract_digest: String,
}

impl WorkerCapability {
    pub fn from_wire(wire: crate::generated::WorkerCapability) -> Result<Self> {
        let role = parse_role(&wire.role)?;
        if !valid_ascii_name(&wire.activity_name)
            || wire.activity_version == 0
            || wire.codec_version != CODEC_VERSION
            || [
                &wire.input_schema_digest,
                &wire.output_schema_digest,
                &wire.contract_digest,
            ]
            .iter()
            .any(|digest| {
                digest.len() != 64
                    || !digest
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            })
        {
            return Err(Error::invalid("invalid worker capability"));
        }
        Ok(Self {
            activity_name: wire.activity_name,
            activity_version: wire.activity_version,
            role,
            codec_version: wire.codec_version,
            input_schema_digest: wire.input_schema_digest,
            output_schema_digest: wire.output_schema_digest,
            contract_digest: wire.contract_digest,
        })
    }

    pub fn to_wire(&self) -> crate::generated::WorkerCapability {
        crate::generated::WorkerCapability {
            activity_name: self.activity_name.clone(),
            activity_version: self.activity_version,
            role: role_name(self.role).to_owned(),
            codec_version: self.codec_version,
            input_schema_digest: self.input_schema_digest.clone(),
            output_schema_digest: self.output_schema_digest.clone(),
            contract_digest: self.contract_digest.clone(),
        }
    }
}

pub fn role_name(role: ExecutionRole) -> &'static str {
    match role {
        ExecutionRole::Forward => "forward",
        ExecutionRole::Compensation => "compensation",
        ExecutionRole::Reconciliation => "reconciliation",
    }
}

pub fn parse_role(role: &str) -> Result<ExecutionRole> {
    match role {
        "forward" => Ok(ExecutionRole::Forward),
        "compensation" => Ok(ExecutionRole::Compensation),
        "reconciliation" => Ok(ExecutionRole::Reconciliation),
        _ => Err(Error::invalid(format!("unknown execution role {role}"))),
    }
}

pub fn contract_schemas<'a>(
    catalog: &'a Catalog,
    key: &ActivityKey,
    role: ExecutionRole,
) -> Result<(&'a SchemaRef, &'a SchemaRef)> {
    let activity = if role == ExecutionRole::Reconciliation {
        &catalog
            .reconcilers
            .get(key)
            .ok_or_else(|| Error::invalid(format!("unknown reconciler {}", key.as_stable_name())))?
            .forward
    } else {
        key
    };
    let contract = catalog.activity(activity)?;
    Ok((&contract.input_schema, &contract.output_schema))
}

pub fn capability_for(
    catalog: &Catalog,
    key: &ActivityKey,
    role: ExecutionRole,
) -> Result<WorkerCapability> {
    let (input, output) = contract_schemas(catalog, key, role)?;
    let contract = if role == ExecutionRole::Reconciliation {
        let recon = catalog
            .reconcilers
            .get(key)
            .ok_or_else(|| Error::invalid("unknown reconciliation contract"))?;
        if catalog.activity(&recon.forward)?.reconciler.as_ref() != Some(key) {
            return Err(Error::invalid(
                "reconciler is not bound to its forward contract",
            ));
        }
        serde_json::json!({"activity": catalog.activity(&recon.forward)?, "reconciler": recon})
    } else {
        serde_json::to_value(catalog.activity(key)?)
            .map_err(|err| Error::invalid(err.to_string()))?
    };
    Ok(WorkerCapability {
        activity_name: key.name.clone(),
        activity_version: key.version,
        role,
        codec_version: CODEC_VERSION,
        input_schema_digest: digest("graphrun.worker-schema/v1", &catalog.schema_json(input)?)?,
        output_schema_digest: digest("graphrun.worker-schema/v1", &catalog.schema_json(output)?)?,
        contract_digest: digest("graphrun.worker-contract/v1", &contract)?,
    })
}

fn digest(domain: &str, value: &serde_json::Value) -> Result<String> {
    let mut hash = Sha256::new();
    hash.update(domain.as_bytes());
    hash.update([0]);
    hash.update(canonical_json(value)?);
    Ok(hex::encode(hash.finalize()))
}
