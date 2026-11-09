//! One fresh durable Sandbox creation, with an owned intent before Docker effects.
use crate::{
    durable_records::Directory,
    job::{failed, refused},
    registry::{lock, random, text, Project, SandboxState, SANDBOXES},
    sandbox::{self, ENGINE, PORT, STARTUP},
};
use layerfs_api::{
    internal::split,
    sandbox::{ManagedSandbox, SandboxFailure, SandboxMilestone, SandboxObservation},
    SandboxApi, SandboxCreate,
};
use layerfs_bridge::{
    control::{Answer, Reply, SdkRefusal, SdkValue},
    daemon_setup::FreshWorkspaceSetup,
    provision::StoreManifest,
};
use layerfs_sandbox::backend::docker::{Docker, SandboxRequest};
use std::{
    collections::BTreeMap,
    ffi::OsStr,
    io,
    os::unix::fs::DirBuilderExt,
    path::{Path, PathBuf},
    sync::{Arc, LazyLock, Mutex},
};

/// Already validated deployment inputs, fixed for the original creation.
pub(crate) struct Inputs<'a> {
    pub docker: &'a str,
    pub image: String,
    pub volume: String,
    pub executable: PathBuf,
    pub max_workspaces: u32,
}
struct Dirs {
    project: Directory,
    root: Directory,
    sandbox: Directory,
    path: PathBuf,
    id: String,
}
impl Dirs {
    fn verify(&self) -> io::Result<()> {
        self.project.verify()?;
        self.root.verify()?;
        self.sandbox.verify()
    }
    fn claim(project_path: &Path) -> Result<Self, SdkRefusal> {
        let project = Directory::open(project_path).map_err(failed)?;
        let root_path = project.path().join("sandboxes");
        crate::discovery::directory(&root_path)?;
        project.sync().map_err(|error| {
            refused(
                "Unknown",
                format!("Sandbox directory parent sync failed: {error}"),
            )
        })?;
        let root = Directory::open(&root_path).map_err(failed)?;
        root.sync().map_err(|error| {
            refused("Unknown", format!("Sandbox directory sync failed: {error}"))
        })?;
        project.verify().map_err(failed)?;
        let id = (1..=9999u32)
            .find(|n| !root.path().join(n.to_string()).exists())
            .ok_or_else(|| refused("Capacity", "the project has 9999 Sandboxes"))?
            .to_string();
        let selected = root.path().join(&id);
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&selected)
            .map_err(failed)?;
        let sandbox = Directory::open(&selected).map_err(|error| {
            refused(
                "Unknown",
                format!("Sandbox number claimed but cannot be checked: {error}"),
            )
        })?;
        root.sync()
            .and_then(|()| sandbox.sync())
            .and_then(|()| project.verify())
            .map_err(|error| {
                refused(
                    "Unknown",
                    format!("Sandbox number claimed but directory sync failed: {error}"),
                )
            })?;
        Ok(Self {
            project,
            root,
            sandbox,
            path: project_path.join("sandboxes").join(&id),
            id,
        })
    }
}

struct Journal {
    dirs: Dirs,
    project: String,
    name: String,
    docker: String,
    image: String,
    volume: String,
    executable: PathBuf,
    capacity: u32,
    identity: [u8; 32],
    controller: [u8; 32],
    peer: [u8; 32],
    setup: Vec<u8>,
    container: Option<String>,
    status: Option<Vec<u8>>,
}
impl Journal {
    fn value(&self, phase: &str) -> SdkValue {
        SdkValue::map([
            ("kind", SdkValue::text("durableSandbox")),
            ("phase", SdkValue::text(phase)),
            ("id", SdkValue::text(&self.dirs.id)),
            ("project", SdkValue::text(&self.project)),
            ("name", SdkValue::text(&self.name)),
            ("docker", SdkValue::text(&self.docker)),
            ("image", SdkValue::text(&self.image)),
            ("volume", SdkValue::text(&self.volume)),
            (
                "executable",
                SdkValue::text(self.executable.to_string_lossy()),
            ),
            ("maxWorkspaces", SdkValue::Int(i64::from(self.capacity))),
            ("durableSandbox", SdkValue::Bytes(self.identity.to_vec())),
            ("controller", SdkValue::Bytes(self.controller.to_vec())),
            ("peer", SdkValue::Bytes(self.peer.to_vec())),
            ("setup", SdkValue::Bytes(self.setup.clone())),
            ("container", SdkValue::opt_text(self.container.clone())),
            (
                "status",
                self.status.clone().map(SdkValue::Bytes).unwrap_or_default(),
            ),
        ])
    }
    fn write(&self, phase: &str, create: bool) -> Result<(), SdkRefusal> {
        self.dirs.verify().map_err(|error| {
            refused(
                "Unknown",
                format!("original Sandbox directory changed: {error}"),
            )
        })?;
        crate::discovery::pinned_write(
            &self.dirs.project,
            "create.intent",
            self.value(phase),
            create,
        )?;
        self.dirs.verify().map_err(|error| {
            refused(
                "Unknown",
                format!("Sandbox intent written but its directory changed: {error}"),
            )
        })
    }
    fn observe(&mut self, at: SandboxObservation<'_>) -> io::Result<()> {
        let result = (|| {
            if let Some(runtime) = at.runtime {
                self.container = Some(runtime.identity().to_string());
            }
            if let Some(status) = at.status {
                self.status = Some(
                    Answer {
                        id: 1,
                        reply: Reply::Hello(status.clone()),
                    }
                    .encode()
                    .map_err(failed)?,
                );
            }
            if at.milestone == SandboxMilestone::BeforeCreate {
                return self.dirs.verify().map_err(failed);
            }
            self.write(&format!("{:?}", at.milestone), false)
        })();
        result.map_err(|error: SdkRefusal| io::Error::other(error.message))
    }
}

enum RetainedResult {
    Before(SdkRefusal),
    Create(Box<SandboxFailure>),
    Owner(Box<ManagedSandbox>, SdkRefusal),
    Ready(Arc<SandboxState>, SdkRefusal),
}
struct Retained {
    journal: Journal,
    result: RetainedResult,
}
static RETAINED: LazyLock<Mutex<BTreeMap<PathBuf, Retained>>> = LazyLock::new(Default::default);
fn retain(path: &Path, journal: Journal, result: RetainedResult) -> SdkRefusal {
    let message = match &result {
        RetainedResult::Before(error) => {
            format!("creation intent could not be confirmed: {}", error.message)
        }
        RetainedResult::Create(failure) => format!(
            "the original Sandbox creation stopped at {:?}",
            failure.phase
        ),
        RetainedResult::Owner(_, error) | RetainedResult::Ready(_, error) => format!(
            "the created Sandbox could not be fully recorded: {}",
            error.message
        ),
    };
    let kept = Retained { journal, result };
    match RETAINED.lock() {
        Ok(mut entries) => {
            entries.insert(path.to_owned(), kept);
        }
        Err(_) => std::mem::forget(kept),
    }
    refused(
        "Unknown",
        format!("{message}; its original owner and intent are retained"),
    )
}

/// No project may automatically retry an unresolved original creation.
pub(crate) fn ensure_available(project: &Path) -> Result<(), SdkRefusal> {
    if let Some(retained) = lock(&RETAINED)?.get(project) {
        let _ = &retained.journal;
        match &retained.result {
            RetainedResult::Owner(owner, _) => {
                let _ = &owner.runtime;
            }
            RetainedResult::Ready(state, _) => {
                let _ = &state.runtime;
            }
            _ => {}
        }
        return Err(refused(
            "RecoveryRequired",
            "a prior Sandbox creation is retained by this process",
        ));
    }
    let directory = Directory::open(project).map_err(failed)?;
    directory.verify().map_err(failed)?;
    match std::fs::symlink_metadata(directory.path().join("create.intent")) {
        Ok(_) => Err(refused("RecoveryRequired", "a prior Sandbox creation has an unresolved intent; its original resources are retained")),
        Err(error) if error.kind() == io::ErrorKind::NotFound => directory.verify().map_err(failed),
        Err(error) => Err(failed(error)),
    }
}

/// Starts exactly one fresh durable Sandbox; failures retain original ownership.
pub(crate) fn start(
    args: &SdkValue,
    project_path: &Path,
    project: &Project,
    installed: StoreManifest,
    inputs: Inputs<'_>,
) -> Result<Arc<SandboxState>, SdkRefusal> {
    ensure_available(project_path)?;
    let project_name = text(args, "project")?.to_owned();
    let controller = layerfs_bridge::duplex::generate_keypair().map_err(failed)?;
    let daemon = layerfs_bridge::duplex::generate_keypair().map_err(failed)?;
    let identity = random()?;
    let prefix = args
        .get("deploy")
        .and_then(|d| d.get_text("containerPrefix"))
        .filter(|prefix| !prefix.is_empty())
        .unwrap_or("layerfs-");
    Docker::new(inputs.docker, ENGINE)
        .map_err(failed)?
        .naming(&format!("{prefix}{project_name}-1"))
        .map_err(|_| refused("InvalidInput", "invalid Sandbox container name"))?;
    let dirs = Dirs::claim(project_path)?;
    let name = format!("{prefix}{project_name}-{}", dirs.id);
    let docker = Docker::new(inputs.docker, ENGINE)
        .map_err(failed)?
        .naming(&name)
        .map_err(|_| refused("InvalidInput", "invalid Sandbox container name"))?;
    let mut setup = sandbox::setup(daemon.private, controller.public, inputs.max_workspaces);
    let mut existing = project.manifest.clone();
    existing.daemon_sqlite = installed.daemon_sqlite;
    setup.existing_store = Some(existing);
    let base = format!(
        "/layerfs-store/layerfs-durable/{}",
        layerfs_bridge::sdk_values::hex(&identity)
    );
    setup.fresh_workspace = Some(FreshWorkspaceSetup {
        sandbox: identity,
        catalog_directory: format!("{base}/catalog"),
        overlay_directory: format!("{base}/overlays"),
        create_directories: true,
    });
    let encoded = setup.encode().map_err(failed)?;
    let mut journal = Journal {
        dirs,
        project: project_name.clone(),
        name: name.clone(),
        docker: inputs.docker.to_owned(),
        image: inputs.image.clone(),
        volume: inputs.volume.clone(),
        executable: inputs.executable.clone(),
        capacity: inputs.max_workspaces,
        identity,
        controller: controller.private,
        peer: daemon.public,
        setup: encoded,
        container: None,
        status: None,
    };
    if let Err(error) = journal.write("BeforeCreate", true) {
        return Err(retain(project_path, journal, RetainedResult::Before(error)));
    }
    let created = SandboxApi::new(docker).create_file_observed(
        SandboxCreate {
            deployment: SandboxRequest {
                image: inputs.image,
                store_volume: inputs.volume,
                port: PORT,
                #[cfg(target_os = "linux")]
                host_export: None,
                labels: sandbox::labels(&project_name, &journal.dirs.id),
            },
            setup,
            executable: inputs.executable,
            controller_private: controller.private,
            daemon_peer: daemon.public,
            startup_wait: STARTUP,
            connect_host: crate::pool::daemon_host(args),
        },
        &project.store,
        &project.manifest,
        |at| journal.observe(at),
    );
    let owner = match created {
        Ok(owner) => owner,
        Err(error) => return Err(retain(project_path, journal, RetainedResult::Create(error))),
    };
    let endpoint = match owner.runtime.endpoint() {
        Ok(endpoint) => endpoint,
        Err(error) => {
            return Err(retain(
                project_path,
                journal,
                RetainedResult::Owner(Box::new(owner), failed(error)),
            ))
        }
    };
    let instance = owner.startup.instance;
    let container = owner.runtime.identity().to_string();
    let (runtime, control, command_identity) = split(owner);
    let dial = crate::pool::Dial {
        endpoint,
        host: crate::pool::daemon_host(args),
        secret: controller.private,
        peer: daemon.public,
        instance,
    };
    let state = Arc::new(SandboxState {
        id: journal.dirs.id.clone(),
        project: project_name,
        name,
        max_workspaces: inputs.max_workspaces,
        durable_identity: Some(identity),
        directory: journal.dirs.path.clone(),
        runtime,
        identity: command_identity,
        controls: crate::pool::Controls::new(control, dial),
        mounts: Default::default(),
        processes: Default::default(),
    });
    let finish = (|| -> Result<(), SdkRefusal> {
        journal.dirs.verify().map_err(failed)?;
        crate::discovery::save_sandbox_pinned(
            &journal.dirs.sandbox,
            &state,
            args,
            controller.private,
            daemon.public,
            instance,
        )?;
        journal
            .dirs
            .sandbox
            .create(OsStr::new("container"), format!("{container}\n").as_bytes())
            .map_err(failed)?;
        journal.write("Recorded", false)?;
        lock(&SANDBOXES)?.insert(state.directory.clone(), state.clone());
        crate::discovery::pinned_write(
            &journal.dirs.sandbox,
            "create.receipt",
            SdkValue::map([
                ("id", SdkValue::text(&state.id)),
                ("durableSandbox", SdkValue::Bytes(identity.to_vec())),
                ("container", SdkValue::text(&container)),
                ("instance", SdkValue::Bytes(instance.to_vec())),
            ]),
            true,
        )?;
        journal
            .dirs
            .project
            .remove(OsStr::new("create.intent"))
            .map_err(|error| {
                refused(
                    "Unknown",
                    format!(
                        "Sandbox recorded but its creation intent could not be retired: {error}"
                    ),
                )
            })?;
        journal.dirs.verify().map_err(|error| {
            refused(
                "Unknown",
                format!("Sandbox recorded but its directory changed: {error}"),
            )
        })
    })();
    if let Err(error) = finish {
        return Err(retain(
            project_path,
            journal,
            RetainedResult::Ready(state, error),
        ));
    }
    Ok(state)
}
