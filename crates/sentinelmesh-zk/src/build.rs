//! Build script for SentinelMesh ZK module

use std::env;
use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-changed=methods/");

    // Set the RISC Zero toolchain environment variable
    let risc0_toolchain = env::var("RISC0_TOOLCHAIN").unwrap_or_else(|_| "default".to_string());
    println!("cargo:rustc-env=RISC0_TOOLCHAIN={}", risc0_toolchain);

    // Add the methods directory to the search path
    let methods_dir = PathBuf::from("methods");
    if methods_dir.exists() {
        println!(
            "cargo:rustc-env=SENTINELMESH_METHODS_DIR={}",
            methods_dir.display()
        );
    }
}
