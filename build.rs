fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/asynq.proto");
    let fds = protox::compile(["asynq.proto"], ["proto"])?;
    prost_build::Config::new().compile_fds(fds)?;
    Ok(())
}
