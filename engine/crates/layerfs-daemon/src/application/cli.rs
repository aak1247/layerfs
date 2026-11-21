//! Thin actual deployment CLI; no launcher or shell command mode.
use super::{config, Application, ApplicationError};
use std::{
    io::{self, Write},
    net::TcpListener,
    path::Path,
    process::ExitCode,
};

/// Runs the Linux filesystem application from one protected configuration file.
/// Args/env carry no key and no command; ordinary runtime execution is separate.
pub fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(cause) => {
            eprintln!("{cause}");
            ExitCode::FAILURE
        }
    }
}
fn run() -> Result<(), ApplicationError> {
    let mut args = std::env::args_os();
    let _program = args.next();
    if args.next().as_deref() != Some(std::ffi::OsStr::new("--config")) {
        return Err(ApplicationError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: layerfs-daemon --config <protected-file>",
        )));
    }
    let path = args.next().ok_or_else(|| {
        ApplicationError::Io(io::Error::new(io::ErrorKind::InvalidInput, "config path"))
    })?;
    if args.next().is_some() {
        return Err(ApplicationError::Io(io::Error::new(
            io::ErrorKind::InvalidInput,
            "unexpected argument",
        )));
    }
    let setup = config::read(Path::new(&path)).map_err(ApplicationError::Io)?;
    #[cfg(target_os = "linux")]
    let mut setup = setup;
    #[cfg(target_os = "linux")]
    if let Some(guard) = setup.store_use.take() {
        guard.verify().map_err(ApplicationError::Io)?;
        // The application may return before detached connection or idle
        // threads finish. Keep this single CLI guard until the OS closes the
        // descriptor at actual process exit, including startup error paths.
        std::mem::forget(guard);
    }
    let listener = TcpListener::bind(&setup.daemon.listen).map_err(ApplicationError::Io)?;
    let application = Application::start(setup.daemon, setup.recovery, setup.recovery_anchor)?;
    println!(
        "LAYERFS_DAEMON_LISTEN {}",
        listener.local_addr().map_err(ApplicationError::Io)?
    );
    io::stdout().flush().map_err(ApplicationError::Io)?;
    application.watch_idle().map_err(ApplicationError::Io)?;
    application.serve(listener)
}
