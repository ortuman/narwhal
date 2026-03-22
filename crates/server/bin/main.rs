// SPDX-License-Identifier: BSD-3-Clause

#[cfg(target_os = "windows")]
compile_error!("This application strictly does not support Windows.");

use clap::Parser;

use narwhal_common::runtime;
use narwhal_server::version::VERSION;

/// Command line arguments
#[derive(Parser, Debug)]
#[command(name = "narwhal")]
#[command(version = VERSION)]
#[command(about = "Narwhal server", long_about = None)]
struct Cli {
  /// Path to configuration file
  #[arg(short, long, value_name = "FILE")]
  config: Option<String>,
}

fn main() {
  narwhal_server::setup_panic_hook();

  runtime::block_on(async {
    let cli = Cli::parse();

    match narwhal_server::run(cli.config).await {
      Ok(_) => {},
      Err(e) => {
        eprintln!("error: {}", e);
        std::process::exit(1);
      },
    }
  });
}
