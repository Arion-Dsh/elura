use std::env;
use std::ffi::OsString;
use std::fmt::Write as _;
use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

const PROFILE_ORDER: &[&str] = &["config", "gateway", "world", "docker", "k8s"];
const TARGETS: &[&str] = &[
    "config", "gateway", "world", "monolith", "module", "route", "sdk", "docker", "k8s", "all",
];
const APP_SKILL_NAME: &str = "elura-app-development";

#[derive(Clone, Copy)]
struct Artifact {
    path: &'static str,
    template: &'static str,
}

const CONFIG: &[Artifact] = &[
    Artifact {
        path: "config/elura.env.example",
        template: include_str!("templates/elura.env.example"),
    },
    Artifact {
        path: "config/application.env.example",
        template: include_str!("templates/application.env.example"),
    },
    Artifact {
        path: "config/gateway.json",
        template: include_str!("templates/gateway.json.example"),
    },
    Artifact {
        path: "config/world.json",
        template: include_str!("templates/world.json.example"),
    },
    Artifact {
        path: "config/distributed.json",
        template: include_str!("templates/distributed.json.example"),
    },
    Artifact {
        path: "config/realm-gateways.json",
        template: include_str!("templates/realm-gateways.json.example"),
    },
    Artifact {
        path: "config/README.md",
        template: include_str!("templates/config.README.md"),
    },
];
const PROJECT: &[Artifact] = &[
    Artifact {
        path: "Cargo.toml",
        template: include_str!("templates/project.Cargo.toml"),
    },
    Artifact {
        path: "Dockerfile",
        template: include_str!("templates/project.Dockerfile"),
    },
    Artifact {
        path: ".dockerignore",
        template: include_str!("templates/project.dockerignore"),
    },
    Artifact {
        path: ".gitignore",
        template: include_str!("templates/project.gitignore"),
    },
];
const GATEWAY: &[Artifact] = &[Artifact {
    path: "src/bin/gateway.rs",
    template: include_str!("templates/gateway.main.rs"),
}];
const WORLD: &[Artifact] = &[Artifact {
    path: "src/bin/world.rs",
    template: include_str!("templates/world.main.rs"),
}];
const MONOLITH: &[Artifact] = &[
    Artifact {
        path: "src/bin/monolith.rs",
        template: include_str!("templates/monolith.main.rs"),
    },
    Artifact {
        path: "config/monolith.json",
        template: include_str!("templates/monolith.json.example"),
    },
    Artifact {
        path: "deploy/docker-compose.monolith.yml",
        template: include_str!("templates/docker-compose.monolith.yml"),
    },
];
const DOCKER: &[Artifact] = &[
    Artifact {
        path: ".env.example",
        template: include_str!("templates/env.example"),
    },
    Artifact {
        path: "deploy/docker-compose.yml",
        template: include_str!("templates/docker-compose.yml"),
    },
    Artifact {
        path: "deploy/README.md",
        template: include_str!("templates/deploy.README.md"),
    },
];
const K8S: &[Artifact] = &[
    Artifact {
        path: "deploy/kubernetes/README.md",
        template: include_str!("templates/kubernetes.README.md"),
    },
    Artifact {
        path: "deploy/kubernetes/kustomization.yaml",
        template: include_str!("templates/kustomization.yaml"),
    },
    Artifact {
        path: "deploy/kubernetes/namespace.yaml",
        template: include_str!("templates/namespace.yaml"),
    },
    Artifact {
        path: "deploy/kubernetes/secret.example.yaml",
        template: include_str!("templates/secret.example.yaml"),
    },
    Artifact {
        path: "deploy/kubernetes/discovery-config.yaml",
        template: include_str!("templates/discovery-config.yaml"),
    },
    Artifact {
        path: "deploy/kubernetes/gateway.yaml",
        template: include_str!("templates/gateway.yaml"),
    },
    Artifact {
        path: "deploy/kubernetes/world.yaml",
        template: include_str!("templates/world.yaml"),
    },
    Artifact {
        path: "deploy/kubernetes/network-policy.yaml",
        template: include_str!("templates/network-policy.yaml"),
    },
];
const SDK_CPP: &[Artifact] = &[
    Artifact {
        path: "sdk/cpp/CMakeLists.txt",
        template: include_str!("templates/sdk/cpp/CMakeLists.txt"),
    },
    Artifact {
        path: "sdk/cpp/README.md",
        template: include_str!("templates/sdk/cpp/README.md"),
    },
    Artifact {
        path: "sdk/cpp/include/elura/elr2.hpp",
        template: include_str!("templates/sdk/cpp/include/elura/elr2.hpp"),
    },
    Artifact {
        path: "sdk/cpp/src/elr2.cpp",
        template: include_str!("templates/sdk/cpp/src/elr2.cpp"),
    },
    Artifact {
        path: "sdk/cpp/tests/elr2_test.cpp",
        template: include_str!("templates/sdk/cpp/tests/elr2_test.cpp"),
    },
    Artifact {
        path: "sdk/cpp/proto/session_control.proto",
        template: include_str!("templates/sdk/session_control.proto"),
    },
];
const SDK_CSHARP: &[Artifact] = &[
    Artifact {
        path: "sdk/csharp/README.md",
        template: include_str!("templates/sdk/csharp/README.md"),
    },
    Artifact {
        path: "sdk/csharp/Elura.Protocol/Elura.Protocol.csproj",
        template: include_str!("templates/sdk/csharp/Elura.Protocol/Elura.Protocol.csproj"),
    },
    Artifact {
        path: "sdk/csharp/Elura.Protocol/Elr2.cs",
        template: include_str!("templates/sdk/csharp/Elura.Protocol/Elr2.cs"),
    },
    Artifact {
        path: "sdk/csharp/Elura.Protocol/GatewayContracts.cs",
        template: include_str!("templates/sdk/csharp/Elura.Protocol/GatewayContracts.cs"),
    },
    Artifact {
        path: "sdk/csharp/Elura.Protocol.Tests/Elura.Protocol.Tests.csproj",
        template: include_str!(
            "templates/sdk/csharp/Elura.Protocol.Tests/Elura.Protocol.Tests.csproj"
        ),
    },
    Artifact {
        path: "sdk/csharp/Elura.Protocol.Tests/Program.cs",
        template: include_str!("templates/sdk/csharp/Elura.Protocol.Tests/Program.cs"),
    },
    Artifact {
        path: "sdk/csharp/proto/session_control.proto",
        template: include_str!("templates/sdk/session_control.proto"),
    },
];
const SDK_TYPESCRIPT: &[Artifact] = &[
    Artifact {
        path: "sdk/typescript/README.md",
        template: include_str!("templates/sdk/typescript/README.md"),
    },
    Artifact {
        path: "sdk/typescript/package.json",
        template: include_str!("templates/sdk/typescript/package.json"),
    },
    Artifact {
        path: "sdk/typescript/tsconfig.json",
        template: include_str!("templates/sdk/typescript/tsconfig.json"),
    },
    Artifact {
        path: "sdk/typescript/src/elr2.ts",
        template: include_str!("templates/sdk/typescript/src/elr2.ts"),
    },
    Artifact {
        path: "sdk/typescript/src/gateway.ts",
        template: include_str!("templates/sdk/typescript/src/gateway.ts"),
    },
    Artifact {
        path: "sdk/typescript/src/index.ts",
        template: include_str!("templates/sdk/typescript/src/index.ts"),
    },
    Artifact {
        path: "sdk/typescript/test/golden.ts",
        template: include_str!("templates/sdk/typescript/test/golden.ts"),
    },
    Artifact {
        path: "sdk/typescript/proto/session_control.proto",
        template: include_str!("templates/sdk/session_control.proto"),
    },
];
const APP_SKILL: &[Artifact] = &[
    Artifact {
        path: "SKILL.md",
        template: include_str!("skills/elura-app-development/SKILL.md"),
    },
    Artifact {
        path: "references/architecture.md",
        template: include_str!("skills/elura-app-development/references/architecture.md"),
    },
    Artifact {
        path: "references/implementation.md",
        template: include_str!("skills/elura-app-development/references/implementation.md"),
    },
];

#[derive(Default)]
struct Options {
    directory: PathBuf,
    force: bool,
    dry_run: bool,
    name: String,
    module: String,
    route_id: Option<u32>,
    language: String,
}

struct GeneratedArtifact {
    path: String,
    content: String,
}

fn main() {
    let code = run(
        env::args_os().skip(1).collect(),
        &mut io::stdout(),
        &mut io::stderr(),
    );
    std::process::exit(code);
}

fn run(arguments: Vec<OsString>, stdout: &mut dyn Write, stderr: &mut dyn Write) -> i32 {
    match try_run(arguments, stdout) {
        Ok(()) => 0,
        Err(RunError::Usage(message)) => {
            let _ = writeln!(stderr, "{message}");
            let _ = writeln!(stderr, "Try 'elura --help' for more information.");
            2
        }
        Err(RunError::Operation(message)) => {
            let _ = writeln!(stderr, "elura: {message}");
            1
        }
    }
}

enum RunError {
    Usage(String),
    Operation(String),
}

fn try_run(arguments: Vec<OsString>, stdout: &mut dyn Write) -> Result<(), RunError> {
    let arguments = arguments
        .into_iter()
        .map(|argument| {
            argument
                .into_string()
                .map_err(|_| usage_error("arguments must be valid UTF-8"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let Some(command) = arguments.first().map(String::as_str) else {
        return Err(usage_error("missing command"));
    };
    match command {
        "--help" | "-h" => return write_help(stdout, None),
        "--version" | "-V" => {
            return writeln!(stdout, "elura {}", env!("CARGO_PKG_VERSION"))
                .map_err(operation_error);
        }
        "help" => return help_command(&arguments[1..], stdout),
        "skill" => return skill_command(&arguments[1..], stdout),
        "init" => {}
        command => return Err(usage_error(format!("unknown command {command:?}"))),
    }
    let Some(target) = arguments.get(1).map(String::as_str) else {
        return Err(usage_error("missing init target"));
    };
    if matches!(target, "--help" | "-h") {
        return write_help(stdout, Some("init"));
    }
    let profile = match target {
        "kubernetes" => "k8s",
        profile => profile,
    };
    if !TARGETS.contains(&profile) {
        return Err(usage_error(format!(
            "unknown target {profile:?}; choose {}",
            TARGETS.join(", ")
        )));
    }
    if arguments[2..]
        .iter()
        .any(|argument| matches!(argument.as_str(), "--help" | "-h"))
    {
        return write_help(stdout, Some(profile));
    }

    let options = parse_options(&arguments[2..])?;
    validate_options(profile, &options)?;
    let root = std::path::absolute(if options.directory.as_os_str().is_empty() {
        Path::new(".")
    } else {
        &options.directory
    })
    .map_err(|error| RunError::Operation(error.to_string()))?;
    let artifacts = artifacts(profile, &options)?;
    if options.dry_run {
        return preview(profile, &root, &artifacts, options.force, stdout);
    }
    generate(&root, &artifacts, options.force).map_err(RunError::Operation)?;
    writeln!(
        stdout,
        "initialized Elura {profile} scaffold in {} ({} files)",
        root.display(),
        artifacts.len()
    )
    .map_err(|error| RunError::Operation(error.to_string()))?;
    Ok(())
}

fn help_command(arguments: &[String], stdout: &mut dyn Write) -> Result<(), RunError> {
    match arguments {
        [] => write_help(stdout, None),
        [command] if command == "init" => write_help(stdout, Some("init")),
        [command] if command == "skill" => write_help(stdout, Some("skill")),
        [command, action] if command == "skill" && action == "install" => {
            write_help(stdout, Some("skill-install"))
        }
        [command, target] if command == "init" => {
            let target = if target == "kubernetes" {
                "k8s"
            } else {
                target
            };
            if TARGETS.contains(&target) {
                write_help(stdout, Some(target))
            } else {
                Err(usage_error(format!("unknown target {target:?}")))
            }
        }
        _ => Err(usage_error(
            "usage: elura help [init [target] | skill [install]]",
        )),
    }
}

fn skill_command(arguments: &[String], stdout: &mut dyn Write) -> Result<(), RunError> {
    let Some(action) = arguments.first().map(String::as_str) else {
        return Err(usage_error("missing skill command"));
    };
    if matches!(action, "--help" | "-h") {
        return write_help(stdout, Some("skill"));
    }
    if action != "install" {
        return Err(usage_error(format!("unknown skill command {action:?}")));
    }
    if arguments[1..]
        .iter()
        .any(|argument| matches!(argument.as_str(), "--help" | "-h"))
    {
        return write_help(stdout, Some("skill-install"));
    }

    let options = parse_skill_options(&arguments[1..])?;
    let project = std::path::absolute(&options.directory)
        .map_err(|error| RunError::Operation(error.to_string()))?;
    let root = project.join(".agents").join("skills").join(APP_SKILL_NAME);
    let artifacts = render_artifacts(APP_SKILL.iter().collect());
    if options.dry_run {
        return preview("agent skill", &root, &artifacts, options.force, stdout);
    }
    generate(&root, &artifacts, options.force).map_err(RunError::Operation)?;
    writeln!(stdout, "installed {APP_SKILL_NAME} in {}", root.display()).map_err(operation_error)
}

fn write_help(stdout: &mut dyn Write, topic: Option<&str>) -> Result<(), RunError> {
    let help = match topic {
        None => String::from(concat!(
            "Elura project scaffolding tool\n",
            "\n",
            "Usage:\n",
            "  elura <COMMAND>\n",
            "  elura [OPTIONS]\n",
            "\n",
            "Commands:\n",
            "  init  Generate project files\n",
            "  skill Install development skills for coding agents\n",
            "  help  Print help for a command or target\n",
            "\n",
            "Options:\n",
            "  -h, --help     Print help\n",
            "  -V, --version  Print version\n",
        )),
        Some("init") => String::from(concat!(
            "Generate Elura project files\n",
            "\n",
            "Usage: elura init <TARGET> [OPTIONS]\n",
            "\n",
            "Targets:\n",
            "  config    Ready-to-run runtime configuration\n",
            "  gateway   Gateway server entry point\n",
            "  world     World server entry point\n",
            "  monolith  Monolith entry point, config, and Compose file\n",
            "  module    Named world module (--name required)\n",
            "  route     Typed route and protobuf definition\n",
            "  sdk       Gateway-to-client protocol SDKs\n",
            "  docker    Docker Compose deployment files\n",
            "  k8s       Kubernetes deployment files (alias: kubernetes)\n",
            "  all       Complete Rust project with config and deployment files\n",
            "\n",
            "Common options:\n",
            "  -d, --dir <PATH>  Output directory [default: .]\n",
            "  -f, --force       Overwrite existing files\n",
            "      --dry-run     Show planned changes without writing files\n",
            "  -h, --help        Print help\n",
        )),
        Some("skill") => String::from(concat!(
            "Install Elura development skills for coding agents\n",
            "\n",
            "Usage: elura skill <COMMAND>\n",
            "\n",
            "Commands:\n",
            "  install  Install the Elura application-development skill\n",
            "\n",
            "Options:\n",
            "  -h, --help  Print help\n",
        )),
        Some("skill-install") => String::from(concat!(
            "Install the Elura application-development skill\n",
            "\n",
            "Usage: elura skill install [OPTIONS]\n",
            "\n",
            "The skill is installed in <PROJECT>/.agents/skills/elura-app-development.\n",
            "\n",
            "Options:\n",
            "  -d, --dir <PATH>  Project root [default: .]\n",
            "  -f, --force       Overwrite existing skill files\n",
            "      --dry-run     Show planned changes without writing files\n",
            "  -h, --help        Print help\n",
        )),
        Some(target) => target_help(target),
    };
    stdout.write_all(help.as_bytes()).map_err(operation_error)
}

fn target_help(target: &str) -> String {
    let mut help = format!(
        "Generate the Elura {target} scaffold\n\nUsage: elura init {target} [OPTIONS]\n\nOptions:\n"
    );
    if target == "module" {
        help.push_str("      --name <NAME>  Module name in snake_case (required)\n");
    } else if target == "route" {
        help.push_str(concat!(
            "      --module <NAME>  Parent module name in snake_case (required)\n",
            "      --name <NAME>    Route name in snake_case (required)\n",
            "      --id <ID>        Numeric route ID, at least 100 (required)\n",
        ));
    } else if target == "sdk" {
        help.push_str("      --language <LANG>  cpp, csharp, typescript, or all [default: all]\n");
    }
    help.push_str(concat!(
        "  -d, --dir <PATH>  Output directory [default: .]\n",
        "  -f, --force       Overwrite existing files\n",
        "      --dry-run     Show planned changes without writing files\n",
        "  -h, --help        Print help\n",
    ));
    help
}

fn parse_options(arguments: &[String]) -> Result<Options, RunError> {
    let mut options = Options {
        directory: PathBuf::from("."),
        ..Options::default()
    };
    let mut index = 0;
    while index < arguments.len() {
        let argument = &arguments[index];
        if matches!(argument.as_str(), "--force" | "-f") {
            options.force = true;
            index += 1;
            continue;
        }
        if argument == "--dry-run" {
            options.dry_run = true;
            index += 1;
            continue;
        }
        let (flag, inline_value) = argument
            .split_once('=')
            .map_or((argument.as_str(), None), |(flag, value)| {
                (flag, Some(value))
            });
        if !matches!(
            flag,
            "--dir" | "-d" | "--name" | "--module" | "--id" | "--language"
        ) {
            return Err(usage_error(format!("unexpected argument {argument:?}")));
        }
        let value = match inline_value {
            Some(value) if !value.is_empty() => value,
            Some(_) => return Err(usage_error(format!("missing value for {flag}"))),
            None => {
                index += 1;
                arguments
                    .get(index)
                    .filter(|value| !value.starts_with("--"))
                    .map(String::as_str)
                    .ok_or_else(|| usage_error(format!("missing value for {flag}")))?
            }
        };
        match flag {
            "--dir" | "-d" => options.directory = PathBuf::from(value),
            "--name" => options.name = value.into(),
            "--module" => options.module = value.into(),
            "--id" => {
                options.route_id = Some(
                    value
                        .parse()
                        .map_err(|_| usage_error("route ID must be an unsigned 32-bit integer"))?,
                );
            }
            "--language" => options.language = value.into(),
            _ => unreachable!(),
        }
        index += 1;
    }
    Ok(options)
}

fn parse_skill_options(arguments: &[String]) -> Result<Options, RunError> {
    let options = parse_options(arguments)?;
    if !options.name.is_empty()
        || !options.module.is_empty()
        || options.route_id.is_some()
        || !options.language.is_empty()
    {
        return Err(usage_error(
            "skill install supports only --dir, --force, and --dry-run",
        ));
    }
    Ok(options)
}

fn validate_options(profile: &str, options: &Options) -> Result<(), RunError> {
    if profile != "module" && profile != "route" && !options.name.is_empty() {
        return Err(usage_error(format!(
            "--name is not supported for the {profile} target"
        )));
    }
    if profile != "route" && !options.module.is_empty() {
        return Err(usage_error(
            "--module is only supported for the route target",
        ));
    }
    if profile != "route" && options.route_id.is_some() {
        return Err(usage_error("--id is only supported for the route target"));
    }
    if profile != "sdk" && !options.language.is_empty() {
        return Err(usage_error(
            "--language is only supported for the sdk target",
        ));
    }
    Ok(())
}

fn preview(
    profile: &str,
    root: &Path,
    artifacts: &[GeneratedArtifact],
    force: bool,
    stdout: &mut dyn Write,
) -> Result<(), RunError> {
    writeln!(
        stdout,
        "Elura {profile} scaffold plan for {}:",
        root.display()
    )
    .map_err(operation_error)?;
    let mut conflicts = 0;
    for artifact in artifacts {
        let path = root.join(&artifact.path);
        let action = match fs::symlink_metadata(&path) {
            Ok(_) if force => "overwrite",
            Ok(_) => {
                conflicts += 1;
                "conflict "
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => "create   ",
            Err(error) => return Err(RunError::Operation(error.to_string())),
        };
        writeln!(stdout, "  {action} {}", artifact.path).map_err(operation_error)?;
    }
    if conflicts > 0 {
        return Err(RunError::Operation(format!(
            "{conflicts} planned file(s) already exist; use --force to overwrite"
        )));
    }
    writeln!(stdout, "No files written.").map_err(operation_error)
}

fn artifacts(profile: &str, options: &Options) -> Result<Vec<GeneratedArtifact>, RunError> {
    if profile == "module" {
        if options.name.is_empty() {
            return Err(usage_error("--name is required for the module target"));
        }
        validate_name(&options.name, "module name")?;
        return Ok(vec![GeneratedArtifact {
            path: format!("src/world/{}/mod.rs", options.name),
            content: render(
                include_str!("templates/module.rs.tmpl"),
                &[
                    ("MODULE", &options.name),
                    ("TYPE", &upper_camel(&options.name)),
                ],
            ),
        }]);
    }
    if profile == "route" {
        if options.module.is_empty() {
            return Err(usage_error("--module is required for the route target"));
        }
        if options.name.is_empty() {
            return Err(usage_error("--name is required for the route target"));
        }
        if options.route_id.is_none() {
            return Err(usage_error("--id is required for the route target"));
        }
        validate_name(&options.module, "route module")?;
        validate_name(&options.name, "route name")?;
        let route_id = options
            .route_id
            .filter(|route_id| *route_id >= 100)
            .ok_or_else(|| usage_error("route ID must be between 100 and 4294967295"))?;
        let type_name = upper_camel(&options.name);
        let route_id = route_id.to_string();
        let values = [
            ("MODULE", options.module.as_str()),
            ("ROUTE", options.name.as_str()),
            ("TYPE", type_name.as_str()),
            ("ROUTE_ID", route_id.as_str()),
        ];
        return Ok(vec![
            GeneratedArtifact {
                path: format!("proto/{}/v2/{}.proto", options.module, options.name),
                content: render(include_str!("templates/route.proto.tmpl"), &values),
            },
            GeneratedArtifact {
                path: format!("src/world/{}/{}.rs", options.module, options.name),
                content: render(include_str!("templates/route.rs.tmpl"), &values),
            },
        ]);
    }

    if profile == "sdk" {
        let language = match options.language.as_str() {
            "" | "all" => "all",
            "cpp" | "c++" => "cpp",
            "csharp" | "cs" | "c#" => "csharp",
            "typescript" | "ts" => "typescript",
            value => {
                return Err(usage_error(format!(
                    "unknown SDK language {value:?}; choose cpp, csharp, typescript, or all"
                )));
            }
        };
        let mut selected = Vec::new();
        if matches!(language, "all" | "cpp") {
            selected.extend(SDK_CPP);
        }
        if matches!(language, "all" | "csharp") {
            selected.extend(SDK_CSHARP);
        }
        if matches!(language, "all" | "typescript") {
            selected.extend(SDK_TYPESCRIPT);
        }
        return Ok(render_artifacts(selected));
    }

    let mut selected: Vec<&Artifact> = Vec::new();
    if profile == "all" {
        selected.extend(PROJECT);
        for name in PROFILE_ORDER {
            selected.extend(profile_artifacts(name));
        }
    } else {
        selected.extend(profile_artifacts(profile));
    }
    Ok(render_artifacts(selected))
}

fn render_artifacts(selected: Vec<&Artifact>) -> Vec<GeneratedArtifact> {
    let wire_version = elura_core::protocol::VERSION.to_string();
    selected
        .into_iter()
        .map(|artifact| GeneratedArtifact {
            path: artifact.path.into(),
            content: render(
                artifact.template,
                &[
                    ("ELURA_VERSION", env!("CARGO_PKG_VERSION")),
                    ("ELR2_VERSION", wire_version.as_str()),
                    (
                        "PROTOCOL_IDENTIFIER",
                        elura_core::protocol::PROTOCOL_IDENTIFIER,
                    ),
                ],
            ),
        })
        .collect()
}

fn profile_artifacts(profile: &str) -> &'static [Artifact] {
    match profile {
        "config" => CONFIG,
        "gateway" => GATEWAY,
        "world" => WORLD,
        "monolith" => MONOLITH,
        "docker" => DOCKER,
        "k8s" => K8S,
        _ => &[],
    }
}

fn validate_name(name: &str, description: &str) -> Result<(), RunError> {
    let valid = name.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
        && name.bytes().all(|character| {
            character.is_ascii_lowercase() || character.is_ascii_digit() || character == b'_'
        });
    if valid {
        Ok(())
    } else {
        Err(usage_error(format!(
            "{description} must start with a lowercase letter and contain only lowercase letters, digits, or underscores"
        )))
    }
}

fn upper_camel(name: &str) -> String {
    let mut result = String::new();
    for part in name.split('_').filter(|part| !part.is_empty()) {
        let mut characters = part.chars();
        if let Some(first) = characters.next() {
            result.extend(first.to_uppercase());
            result.extend(characters);
        }
    }
    result
}

fn render(template: &str, values: &[(&str, &str)]) -> String {
    values
        .iter()
        .fold(template.to_owned(), |content, (key, value)| {
            content.replace(&format!("{{{{{key}}}}}"), value)
        })
}

fn generate(root: &Path, artifacts: &[GeneratedArtifact], force: bool) -> Result<(), String> {
    fs::create_dir_all(root).map_err(|error| error.to_string())?;
    if !force {
        for artifact in artifacts {
            let path = root.join(&artifact.path);
            match fs::symlink_metadata(&path) {
                Ok(_) => {
                    return Err(format!(
                        "{} already exists; use --force to overwrite",
                        path.display()
                    ));
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.to_string()),
            }
        }
    }
    for artifact in artifacts {
        let path = root.join(&artifact.path);
        let parent = path
            .parent()
            .ok_or_else(|| format!("invalid generated path {}", path.display()))?;
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
        fs::write(&path, artifact.content.as_bytes()).map_err(|error| error.to_string())?;
    }
    Ok(())
}

fn usage_error(message: impl AsRef<str>) -> RunError {
    let mut output = String::from("elura: ");
    let _ = write!(output, "{}", message.as_ref());
    RunError::Usage(output)
}

fn operation_error(error: io::Error) -> RunError {
    RunError::Operation(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    struct TestDir(PathBuf);

    impl TestDir {
        fn new(name: &str) -> Self {
            let nonce = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path =
                env::temp_dir().join(format!("elura-cli-{name}-{}-{nonce}", std::process::id()));
            fs::create_dir_all(&path).unwrap();
            Self(path)
        }
    }

    impl Drop for TestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn execute(arguments: &[&str]) -> (i32, String, String) {
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let code = run(
            arguments.iter().map(OsString::from).collect(),
            &mut stdout,
            &mut stderr,
        );
        (
            code,
            String::from_utf8(stdout).unwrap(),
            String::from_utf8(stderr).unwrap(),
        )
    }

    #[test]
    fn init_all_generates_upper_project_scaffold() {
        let directory = TestDir::new("all");
        let (code, stdout, stderr) =
            execute(&["init", "all", "--dir", directory.0.to_str().unwrap()]);
        assert_eq!(code, 0, "{stderr}");
        assert!(stdout.contains("initialized Elura all scaffold"));
        for path in [
            "Cargo.toml",
            "Dockerfile",
            ".dockerignore",
            ".gitignore",
            "config/elura.env.example",
            "config/gateway.json",
            "config/world.json",
            "config/distributed.json",
            "config/realm-gateways.json",
            "src/bin/gateway.rs",
            "src/bin/world.rs",
            "deploy/README.md",
            "deploy/docker-compose.yml",
            "deploy/kubernetes/README.md",
            "deploy/kubernetes/kustomization.yaml",
        ] {
            assert!(
                !fs::read(directory.0.join(path)).unwrap().is_empty(),
                "{path}"
            );
        }

        let manifest = fs::read_to_string(directory.0.join("Cargo.toml")).unwrap();
        assert!(manifest.contains("name = \"elura-game\""));
        assert!(manifest.contains(&format!(
            "elura = {{ version = \"{}\", features = [\"adapters\", \"monolith\"] }}",
            env!("CARGO_PKG_VERSION")
        )));

        let compose = fs::read_to_string(directory.0.join("deploy/docker-compose.yml")).unwrap();
        assert!(compose.contains("../config/gateway.json:/etc/elura/gateway.json:ro"));
        assert!(compose.contains("../config/world.json:/etc/elura/world.json:ro"));
        assert!(!compose.contains("../../config"));

        let kubernetes_config =
            fs::read_to_string(directory.0.join("deploy/kubernetes/discovery-config.yaml"))
                .unwrap();
        let kubernetes_world =
            fs::read_to_string(directory.0.join("deploy/kubernetes/world.yaml")).unwrap();
        let network_policy =
            fs::read_to_string(directory.0.join("deploy/kubernetes/network-policy.yaml")).unwrap();
        assert!(kubernetes_config.contains("name: elura-runtime-config"));
        assert!(kubernetes_config.contains("world.json: |"));
        assert!(kubernetes_world.contains("APP_WORLD_CONFIG"));
        assert!(kubernetes_world.contains("name: elura-runtime-config"));
        assert!(network_policy.contains("kubernetes.io/metadata.name: kube-system"));
        assert!(network_policy.contains("protocol: UDP"));
    }

    #[test]
    fn installs_project_level_cross_agent_skill() {
        let directory = TestDir::new("skill-install");
        let dir = directory.0.to_str().unwrap();
        let (code, stdout, stderr) = execute(&["skill", "install", "--dir", dir]);
        assert_eq!(code, 0, "{stderr}");
        assert!(stdout.contains("installed elura-app-development"));

        let root = directory.0.join(".agents/skills/elura-app-development");
        for path in [
            "SKILL.md",
            "references/architecture.md",
            "references/implementation.md",
        ] {
            assert!(!fs::read(root.join(path)).unwrap().is_empty(), "{path}");
        }
        let skill = fs::read_to_string(root.join("SKILL.md")).unwrap();
        assert!(skill.contains("name: elura-app-development"));
        assert!(!skill.contains("{{"));
    }

    #[test]
    fn skill_install_supports_dry_run_and_requires_force_for_conflicts() {
        let directory = TestDir::new("skill-dry-run");
        let dir = directory.0.to_str().unwrap();
        let skill = directory
            .0
            .join(".agents/skills/elura-app-development/SKILL.md");

        let (code, stdout, stderr) = execute(&["skill", "install", "--dir", dir, "--dry-run"]);
        assert_eq!(code, 0, "{stderr}");
        assert!(stdout.contains("create    SKILL.md"));
        assert!(!skill.exists());

        fs::create_dir_all(skill.parent().unwrap()).unwrap();
        fs::write(&skill, "existing").unwrap();
        let (code, _, stderr) = execute(&["skill", "install", "--dir", dir]);
        assert_eq!(code, 1);
        assert!(stderr.contains("use --force to overwrite"));
        assert_eq!(fs::read_to_string(&skill).unwrap(), "existing");

        let (code, _, stderr) = execute(&["skill", "install", "--dir", dir, "--force"]);
        assert_eq!(code, 0, "{stderr}");
        assert!(
            fs::read_to_string(&skill)
                .unwrap()
                .contains("name: elura-app-development")
        );
    }

    #[test]
    fn existing_files_require_force_and_preflight_is_atomic() {
        let directory = TestDir::new("force");
        let path = directory.0.join("src/bin/world.rs");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, "existing").unwrap();
        let dir = directory.0.to_str().unwrap();
        let (code, _, stderr) = execute(&["init", "world", "--dir", dir]);
        assert_eq!(code, 1, "{stderr}");
        assert_eq!(fs::read_to_string(&path).unwrap(), "existing");
        let (code, _, stderr) = execute(&["init", "world", "--dir", dir, "--force"]);
        assert_eq!(code, 0, "{stderr}");
        assert!(
            fs::read_to_string(&path)
                .unwrap()
                .contains("World::new(app.runtime)")
        );
    }

    #[test]
    fn init_module_generates_named_world_module() {
        let directory = TestDir::new("module");
        let dir = directory.0.to_str().unwrap();
        let (code, _, stderr) = execute(&["init", "module", "--name", "inventory", "--dir", dir]);
        assert_eq!(code, 0, "{stderr}");
        let module = fs::read_to_string(directory.0.join("src/world/inventory/mod.rs")).unwrap();
        assert!(module.contains("pub struct InventoryModule"));
        assert!(module.contains("\"inventory\""));
        assert_eq!(
            execute(&["init", "module", "--name", "Inventory", "--dir", dir]).0,
            2
        );
    }

    #[test]
    fn init_route_generates_proto_and_typed_handler() {
        let directory = TestDir::new("route");
        let dir = directory.0.to_str().unwrap();
        let (code, _, stderr) = execute(&[
            "init",
            "route",
            "--module",
            "inventory",
            "--name",
            "list_items",
            "--id",
            "120",
            "--dir",
            dir,
        ]);
        assert_eq!(code, 0, "{stderr}");
        let proto =
            fs::read_to_string(directory.0.join("proto/inventory/v2/list_items.proto")).unwrap();
        let handler =
            fs::read_to_string(directory.0.join("src/world/inventory/list_items.rs")).unwrap();
        assert!(proto.contains("message ListItemsRequest"));
        assert!(proto.contains("package game.inventory.v2;"));
        assert!(handler.contains("crate::proto::inventory::v2::ListItemsRequest"));
        assert!(handler.contains("const ID: u32 = 120"));
        assert!(handler.contains("world.route(ListItems, handle)"));
    }

    #[test]
    fn init_sdk_generates_gateway_client_protocols() {
        let directory = TestDir::new("sdk");
        let dir = directory.0.to_str().unwrap();
        let (code, stdout, stderr) = execute(&["init", "sdk", "--dir", dir]);
        assert_eq!(code, 0, "{stderr}");
        assert!(stdout.contains("initialized Elura sdk scaffold"));
        for path in [
            "sdk/cpp/include/elura/elr2.hpp",
            "sdk/cpp/src/elr2.cpp",
            "sdk/csharp/Elura.Protocol/Elr2.cs",
            "sdk/csharp/Elura.Protocol/GatewayContracts.cs",
            "sdk/typescript/src/elr2.ts",
            "sdk/typescript/src/gateway.ts",
        ] {
            let content = fs::read_to_string(directory.0.join(path)).unwrap();
            assert!(!content.contains("{{"), "unrendered placeholder in {path}");
        }
        for path in [
            "sdk/cpp/include/elura/elr2.hpp",
            "sdk/csharp/Elura.Protocol/GatewayContracts.cs",
            "sdk/typescript/src/gateway.ts",
        ] {
            assert!(
                fs::read_to_string(directory.0.join(path))
                    .unwrap()
                    .contains("SessionControl"),
                "{path}"
            );
        }
        let cpp = fs::read_to_string(directory.0.join("sdk/cpp/include/elura/elr2.hpp")).unwrap();
        assert!(cpp.contains(&format!("kElr2Version = {}", elura_core::protocol::VERSION)));
        assert!(cpp.contains(elura_core::protocol::PROTOCOL_IDENTIFIER));
    }

    #[test]
    fn init_sdk_selects_one_language_and_validates_the_name() {
        for (language, expected, absent) in [
            ("c++", "sdk/cpp/CMakeLists.txt", "sdk/csharp/README.md"),
            (
                "c#",
                "sdk/csharp/Elura.Protocol/Elr2.cs",
                "sdk/typescript/README.md",
            ),
            ("ts", "sdk/typescript/package.json", "sdk/cpp/README.md"),
        ] {
            let directory = TestDir::new("sdk-language");
            let dir = directory.0.to_str().unwrap();
            let (code, _, stderr) = execute(&["init", "sdk", "--language", language, "--dir", dir]);
            assert_eq!(code, 0, "{language}: {stderr}");
            assert!(directory.0.join(expected).exists(), "{language}");
            assert!(!directory.0.join(absent).exists(), "{language}");
        }
        let (code, _, stderr) = execute(&["init", "sdk", "--language", "java"]);
        assert_eq!(code, 2);
        assert!(stderr.contains("unknown SDK language"));
    }

    #[test]
    fn init_world_generates_config_driven_business_route() {
        let directory = TestDir::new("world-business");
        let dir = directory.0.to_str().unwrap();
        let (code, _, stderr) = execute(&["init", "world", "--dir", dir]);
        assert_eq!(code, 0, "{stderr}");
        let world = fs::read_to_string(directory.0.join("src/bin/world.rs")).unwrap();
        assert!(world.contains("struct AppConfig"));
        assert!(world.contains("World::new(app.runtime)"));
        assert!(world.contains("impl Route for GetPlayerProfile"));
        assert!(world.contains(".route(GetPlayerProfile"));
        assert!(world.contains("welcome_message"));
    }

    #[test]
    fn init_gateway_generates_full_runtime_configuration() {
        let directory = TestDir::new("gateway-config");
        let dir = directory.0.to_str().unwrap();
        let (code, _, stderr) = execute(&["init", "gateway", "--dir", dir]);
        assert_eq!(code, 0, "{stderr}");
        let gateway = fs::read_to_string(directory.0.join("src/bin/gateway.rs")).unwrap();
        assert!(gateway.contains("struct AppConfig"));
        assert!(gateway.contains("Gateway::new(app.runtime)"));
        assert!(gateway.contains(".transport(tcp)"));
        assert!(gateway.contains(".world_discovery(discovery)"));
    }

    #[test]
    fn init_monolith_generates_direct_runtime_entry() {
        let directory = TestDir::new("monolith");
        let dir = directory.0.to_str().unwrap();
        let (code, _, stderr) = execute(&["init", "monolith", "--dir", dir]);
        assert_eq!(code, 0, "{stderr}");
        let source = fs::read_to_string(directory.0.join("src/bin/monolith.rs")).unwrap();
        assert!(source.contains("Monolith::new(app.gateway, app.world)"));
        assert!(source.contains(".transport(tcp)"));
        assert!(source.contains("monolith.route("));
        assert!(source.contains("APP_INSTANCE_ID"));
        assert!(directory.0.join("config/monolith.json").exists());
    }

    #[test]
    fn invalid_commands_return_usage_exit_code() {
        for arguments in [
            vec![],
            vec!["init"],
            vec!["init", "unknown"],
            vec!["init", "world", "extra"],
            vec!["init", "module"],
            vec!["init", "route"],
        ] {
            assert_eq!(execute(&arguments).0, 2, "{arguments:?}");
        }
    }

    #[test]
    fn help_and_version_are_successful_and_contextual() {
        for arguments in [
            vec!["--help"],
            vec!["-h"],
            vec!["help"],
            vec!["init", "--help"],
            vec!["help", "init"],
            vec!["init", "route", "--help"],
            vec!["help", "init", "route"],
            vec!["init", "sdk", "--help"],
            vec!["help", "init", "sdk"],
            vec!["skill", "--help"],
            vec!["help", "skill"],
            vec!["skill", "install", "--help"],
            vec!["help", "skill", "install"],
        ] {
            let (code, stdout, stderr) = execute(&arguments);
            assert_eq!(code, 0, "{arguments:?}: {stderr}");
            assert!(!stdout.is_empty(), "{arguments:?}");
            assert!(stderr.is_empty(), "{arguments:?}: {stderr}");
        }

        let (_, route_help, _) = execute(&["init", "route", "--help"]);
        assert!(route_help.contains("--module <NAME>"));
        assert!(route_help.contains("--id <ID>"));

        let (_, sdk_help, _) = execute(&["init", "sdk", "--help"]);
        assert!(sdk_help.contains("--language <LANG>"));

        let (code, stdout, stderr) = execute(&["--version"]);
        assert_eq!(code, 0, "{stderr}");
        assert_eq!(stdout, format!("elura {}\n", env!("CARGO_PKG_VERSION")));
    }

    #[test]
    fn dry_run_previews_without_writing() {
        let parent = TestDir::new("dry-run");
        let directory = parent.0.join("new-project");
        let dir = directory.to_str().unwrap();
        let (code, stdout, stderr) = execute(&["init", "world", "--dir", dir, "--dry-run"]);
        assert_eq!(code, 0, "{stderr}");
        assert!(stdout.contains("create    src/bin/world.rs"));
        assert!(stdout.contains("No files written."));
        assert!(!directory.exists());
    }

    #[test]
    fn dry_run_reports_conflicts_and_force_plan() {
        let directory = TestDir::new("dry-run-conflict");
        let path = directory.0.join("src/bin/world.rs");
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(&path, "existing").unwrap();
        let dir = directory.0.to_str().unwrap();

        let (code, stdout, stderr) = execute(&["init", "world", "-d", dir, "--dry-run"]);
        assert_eq!(code, 1);
        assert!(stdout.contains("conflict  src/bin/world.rs"));
        assert!(stderr.contains("use --force to overwrite"));
        assert_eq!(fs::read_to_string(&path).unwrap(), "existing");

        let (code, stdout, stderr) = execute(&["init", "world", "-d", dir, "--dry-run", "-f"]);
        assert_eq!(code, 0, "{stderr}");
        assert!(stdout.contains("overwrite src/bin/world.rs"));
        assert_eq!(fs::read_to_string(&path).unwrap(), "existing");
    }

    #[test]
    fn rejects_target_specific_options_on_other_targets() {
        let (code, _, stderr) = execute(&["init", "world", "--name", "inventory"]);
        assert_eq!(code, 2);
        assert!(stderr.contains("--name is not supported for the world target"));

        let (code, _, stderr) = execute(&["init", "module"]);
        assert_eq!(code, 2);
        assert!(stderr.contains("--name is required for the module target"));

        let (code, _, stderr) = execute(&["init", "route", "--module", "inventory"]);
        assert_eq!(code, 2);
        assert!(stderr.contains("--name is required for the route target"));

        let (code, _, stderr) = execute(&["init", "world", "--language", "cpp"]);
        assert_eq!(code, 2);
        assert!(stderr.contains("--language is only supported for the sdk target"));
    }

    #[test]
    fn supports_kubernetes_alias_and_inline_options() {
        let directory = TestDir::new("alias");
        let argument = format!("--dir={}", directory.0.display());
        assert_eq!(execute(&["init", "kubernetes", &argument]).0, 0);
        assert!(directory.0.join("deploy/kubernetes/gateway.yaml").exists());
    }
}
