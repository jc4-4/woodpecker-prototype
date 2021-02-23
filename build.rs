fn main() -> Result<(), String> {
    // for use in docker build where file changes can be wonky
    println!("cargo:rerun-if-env-changed=FORCE_REBUILD");

    println!("cargo:rerun-if-changed=proto/woodpecker.proto");
    tonic_build::compile_protos("proto/woodpecker.proto")
        .map_err(|e| format!("protobuf compilation failed: {}", e))?;

    Ok(())
}
