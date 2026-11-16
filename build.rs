fn main() -> Result<(), Box<dyn std::error::Error>> {
    pyo3_build_config::add_extension_module_link_args();
    println!("cargo:rerun-if-changed=proto/asynq.proto");
    let fds = protox::compile(["asynq.proto"], ["proto"])?;
    prost_build::Config::new().compile_fds(fds)?;
    Ok(())
}
