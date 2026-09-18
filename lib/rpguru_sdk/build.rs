fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../proto");
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &[
                proto_root.join("base/config.proto"),
                proto_root.join("auth/auth.proto"),
                proto_root.join("orchestration/orchestration.proto"),
                proto_root.join("orchestration/agent.proto"),
                proto_root.join("notify/notify.proto"),
            ],
            &[proto_root],
        )?;
    println!("cargo:rerun-if-changed=../../proto");
    Ok(())
}
