use yah_qed::PipelineLoader;
use std::path::Path;

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: qed <command> [args]");
        eprintln!("Commands:");
        eprintln!("  run <pipeline> [--param=value...]");
        eprintln!("  plan <pipeline|path>");
        eprintln!("  list");
        eprintln!("  pipelines");
        eprintln!("  status <run-id>");
        std::process::exit(1);
    }

    let command = &args[1];

    match command.as_str() {
        "plan" => {
            if args.len() < 3 {
                eprintln!("Usage: qed plan <pipeline-name|path-to.toml>");
                std::process::exit(1);
            }
            let target = &args[2];
            let loader = PipelineLoader::new(".yah/qed");
            let pipeline = if target.ends_with(".toml") && Path::new(target).exists() {
                // Load from an explicit file path so plan can preview drafts
                // outside `.yah/qed/` (e.g. noisetable's release.apple.toml).
                match loader.parse_from_path(Path::new(target)) {
                    Ok(p) => p,
                    Err(e) => {
                        eprintln!("Parse error in {target}: {e}");
                        std::process::exit(1);
                    }
                }
            } else {
                match loader.load(target) {
                    Ok(p) => p,
                    Err(e) => {
                        eprintln!("Error: {e}");
                        std::process::exit(1);
                    }
                }
            };
            let jobs = yah_qed::matrix::plan(&pipeline);
            println!("{} — {} expanded job(s)", pipeline.name, jobs.len());
            for (i, job) in jobs.iter().enumerate() {
                println!("  [{:>2}] {}", i + 1, job.label());
                for step in &job.pipeline.steps {
                    println!("       · {}", step.name);
                }
            }
        }
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
