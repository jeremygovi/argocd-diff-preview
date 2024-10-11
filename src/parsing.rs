use crate::{Operator, Selector};
use log::{debug, info};
use regex::Regex;
use serde_yaml::Mapping;
use std::{error::Error, io::BufRead};

struct K8sResource {
    file_name: String,
    yaml: serde_yaml::Value,
}

struct Application {
    file_name: String,
    yaml: serde_yaml::Value,
    kind: ApplicationKind,
}

enum ApplicationKind {
    Application,
    ApplicationSet,
}

pub async fn get_applications_as_string(
    directory: &str,
    branch: &str,
    regex: &Option<Regex>,
    selector: &Option<Vec<Selector>>,
    repo: &str,
) -> Result<String, Box<dyn Error>> {
    debug!("Starting to fetch applications as string with directory: '{}', branch: '{}', regex: '{:?}', selector: '{:?}', repo: '{}'", directory, branch, regex, selector, repo);
    
    let yaml_files = get_yaml_files(directory, regex).await;
    debug!("Collected YAML files: {:?}", yaml_files);
    
    let k8s_resources = parse_yaml(yaml_files).await;
    debug!("Parsed K8s resources: {:?}", k8s_resources);
    
    let applications = get_applications(k8s_resources, selector);
    debug!("Filtered applications: {:?}", applications);
    
    let output = patch_applications(applications, branch, repo).await?;
    debug!("Final output: {}", output);
    
    Ok(output)
}

async fn get_yaml_files(directory: &str, regex: &Option<Regex>) -> Vec<String> {
    use walkdir::WalkDir;

    info!("🤖 Fetching all files in dir: {}", directory);

    let yaml_files: Vec<String> = WalkDir::new(directory)
        .into_iter()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().is_file())
        .filter(|e| {
            e.path()
                .extension()
                .and_then(|s| s.to_str())
                .map(|s| s == "yaml" || s == "yml")
                .unwrap_or(false)
        })
        .map(|e| format!("{}", e.path().display()))
        .filter(|f| regex.is_none() || regex.as_ref().unwrap().is_match(f))
        .collect();

    match regex {
        Some(r) => debug!(
            "🤖 Found {} yaml files matching regex: {}",
            yaml_files.len(),
            r.as_str()
        ),
        None => debug!("🤖 Found {} yaml files", yaml_files.len()),
    }

    yaml_files
}

async fn parse_yaml(files: Vec<String>) -> Vec<K8sResource> {
    debug!("Starting to parse YAML files: {:?}", files);

    files.iter()
        .flat_map(|f| {
            debug!("Opening file: {}", f);
            let file = std::fs::File::open(f).unwrap();
            let reader = std::io::BufReader::new(file);
            let lines = reader.lines().map(|l| l.unwrap());

            let mut raw_yaml_chunks: Vec<String> = lines.fold(vec!["".to_string()], |mut acc, s| {
                if s == "---" {
                    acc.push("".to_string());
                } else {
                    let last = acc.len() - 1;
                    acc[last].push('\n');
                    acc[last].push_str(&s);
                }
                acc
            });
            debug!("Raw YAML chunks: {:?}", raw_yaml_chunks);

            let yaml_vec: Vec<K8sResource> = raw_yaml_chunks.iter_mut().enumerate().map(|(i,r)| {
                let yaml = match serde_yaml::from_str(r) {
                    Ok(r) => r,
                    Err(e) => {
                        debug!("⚠️ Failed to parse element number {}, in file '{}', with error: '{}'", i+1, f, e);
                        serde_yaml::Value::Null
                    }
                };
                debug!("Parsed YAML resource in file '{}': {:?}", f, yaml);
                K8sResource {
                    file_name: f.clone(),
                    yaml,
                }
            }).collect();

            yaml_vec
        })
        .collect()
}

async fn patch_applications(
    applications: Vec<Application>,
    branch: &str,
    repo: &str,
) -> Result<String, Box<dyn Error>> {
    info!("🤖 Patching applications for branch: {}", branch);
    debug!("Applications before patching: {:?}", applications);

    let point_destination_to_in_cluster = |spec: &mut Mapping| {
        debug!("Patching destination to in-cluster...");
        if spec.contains_key("destination") {
            spec["destination"]["name"] = serde_yaml::Value::String("in-cluster".to_string());
            spec["destination"]
                .as_mapping_mut()
                .map(|a| a.remove("server"));
            debug!("Updated destination to in-cluster: {:?}", spec["destination"]);
        }
    };

    let set_project_to_default =
        |spec: &mut Mapping| {
            debug!("Setting project to default...");
            spec["project"] = serde_yaml::Value::String("default".to_string());
            debug!("Updated project: {:?}", spec["project"]);
        };

    let remove_sync_policy = |spec: &mut Mapping| {
        debug!("Removing syncPolicy...");
        spec.remove("syncPolicy");
        debug!("SyncPolicy removed.");
    };

    let redirect_sources = |spec: &mut Mapping, file: &str| {
        debug!("Redirecting sources in file: {}", file);
        if spec.contains_key("source") {
            if spec["source"]["chart"].as_str().is_some() {
                debug!("Source is a Helm chart, skipping repo URL update.");
                return;
            }
            match spec["source"]["repoURL"].as_str() {
                Some(url) if url.contains(repo) => {
                    spec["source"]["targetRevision"] = serde_yaml::Value::String(branch.to_string());
                    debug!("Updated targetRevision to branch '{}'", branch);
                }
                _ => debug!("Found no 'repoURL' under spec.source in file: {}", file),
            }
        } else if spec.contains_key("sources") {
            if let Some(sources) = spec["sources"].as_sequence_mut() {
                for source in sources {
                    if source["chart"].as_str().is_some() {
                        debug!("Source is a Helm chart, skipping repo URL update.");
                        continue;
                    }
                    match source["repoURL"].as_str() {
                        Some(url) if url.contains(repo) => {
                            source["targetRevision"] = serde_yaml::Value::String(branch.to_string());
                            debug!("Updated targetRevision to branch '{}'", branch);
                        }
                        _ => debug!("Found no 'repoURL' under spec.sources[] in file: {}", file),
                    }
                }
            }
        }
    };

    let applications: Vec<Application> = applications
        .into_iter()
        .map(|mut a| {
            // Update namespace
            a.yaml["metadata"]["namespace"] = serde_yaml::Value::String("argocd".to_string());
            debug!("Updated namespace for application in file '{}'", a.file_name);
            a
        })
        .filter_map(|mut a| {
            // Clean up the spec
            let spec = match a.kind {
                ApplicationKind::Application => a.yaml["spec"].as_mapping_mut()?,
                ApplicationKind::ApplicationSet => {
                    a.yaml["spec"]["template"]["spec"].as_mapping_mut()?
                }
            };
            remove_sync_policy(spec);
            set_project_to_default(spec);
            point_destination_to_in_cluster(spec);
            redirect_sources(spec, &a.file_name);
            debug!(
                "Processed application {:?} in file: {}",
                a.yaml["metadata"]["name"].as_str().unwrap_or("unknown"),
                a.file_name
            );
            Some(a)
        })
        .collect();

    info!(
        "🤖 Patching {} Argo CD Application[Sets] for branch: {}",
        applications.len(),
        branch
    );
    debug!("Applications after patching: {:?}", applications);

    // convert back to yaml string
    let mut output = String::new();
    for r in applications {
        output.push_str(&serde_yaml::to_string(&r.yaml)?);
        output.push_str("---\n");
    }
    debug!("Final YAML output: {}", output);

    Ok(output)
}

fn get_applications(
    k8s_resources: Vec<K8sResource>,
    selector: &Option<Vec<Selector>>,
) -> Vec<Application> {
    debug!("Getting applications from K8s resources: {:?}", k8s_resources);

    k8s_resources
        .into_iter()
        .filter_map(|r| {
            debug!("Processing file: {}", r.file_name);
            let kind =
                r.yaml["kind"]
                    .as_str()
                    .map(|s| s.to_string())
                    .and_then(|kind| match kind.as_str() {
                        "Application" => Some(ApplicationKind::Application),
                        "ApplicationSet" => Some(ApplicationKind::ApplicationSet),
                        _ => None,
                    })?;

            if r.yaml["metadata"]["annotations"]["argocd-diff-preview/ignore"].as_str()
                == Some("true")
            {
                debug!(
                    "Ignoring application {:?} due to 'argocd-diff-preview/ignore=true' in file: {}",
                    r.yaml["metadata"]["name"].as_str().unwrap_or("unknown"),
                    r.file_name
                );
                return None;
            }

            // loop over labels and check if the selector matches
            if let Some(selector) = selector {
                let labels: Vec<(&str, &str)> = {
                    match r.yaml["metadata"]["labels"].as_mapping() {
                        Some(m) => m.iter()
                            .flat_map(|(k, v)| Some((k.as_str()?, v.as_str()?)))
                            .collect(),
                        None => Vec::new(),
                    }
                };
                debug!("Application labels: {:?}", labels);

                let selected = selector.iter().all(|l| match l.operator {
                    Operator::Eq => labels.iter().any(|(k, v)| k == &l.key && v == &l.value),
                    Operator::Ne => labels.iter().all(|(k, v)| k != &l.key || v != &l.value),
                });
                if !selected {
                    debug!(
                        "Ignoring application {:?} due to selector mismatch in file: {}",
                        r.yaml["metadata"]["name"].as_str().unwrap_or("unknown"),
                        r.file_name
                    );
                    return None;
                } else {
                    debug!(
                        "Selected application {:?} due to selector match in file: {}",
                        r.yaml["metadata"]["name"].as_str().unwrap_or("unknown"),
                        r.file_name
                    );
                }
            }

            Some(Application {
                kind,
                file_name: r.file_name,
                yaml: r.yaml,
            })
        })
        .collect()
}
