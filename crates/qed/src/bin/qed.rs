use qed::PipelineLoader;

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: qed <command> [args]");
        eprintln!("Commands:");
        eprintln!("  run <pipeline> [--param=value...]");
        eprintln!("  list");
        eprintln!("  pipelines");
        eprintln!("  status <run-id>");
        std::process::exit(1);
    }

    let command = &args[1];

    match command.as_str() {
        "run" => {
            if args.len() < 3 {
                eprintln!("Usage: qed run <pipeline>");
                std::process::exit(1);
            }

            let pipeline_name = &args[2];
            let loader = PipelineLoader::new(".yah/qed");

            match loader.load(pipeline_name) {
                Ok(_pipeline) => {
                    println!("Pipeline '{}' loaded (stub CLI)", pipeline_name);
                }
                Err(e) => {
                    eprintln!("Error: {}", e);
                    std::process::exit(1);
                }
            }
        }
        "list" => {
            println!("QED runs (stub CLI)");
        }
        "pipelines" => {
            let loader = PipelineLoader::new(".yah/qed");
            match loader.list_files() {
                Ok(pipelines) => {
                    println!("Available pipelines:");
                    for p in pipelines {
                        println!("  {}", p);
                    }
                    println!("Built-in: check, smoke, release-build");
                }
                Err(e) => {
                    eprintln!("Error: {}", e);
                    std::process::exit(1);
                }
            }
        }
        "status" => {
            if args.len() < 3 {
                eprintln!("Usage: qed status <run-id>");
                std::process::exit(1);
            }
            println!("Status for run '{}' (stub CLI)", &args[2]);
        }
        _ => {
            eprintln!("Unknown command: {}", command);
            std::process::exit(1);
        }
    }
}
