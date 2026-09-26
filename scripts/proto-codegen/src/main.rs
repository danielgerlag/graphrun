use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../..");
    tonic_prost_build::configure()
        .out_dir(root.join("graphrun/src/generated"))
        .compile_protos(&[root.join("proto/graphrun.proto")], &[root.join("proto")])?;
    Ok(())
}
