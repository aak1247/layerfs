//! Read-only admission of one exact, never-started original Create result.
use super::{
    json::{once, Json},
    Docker, Sandbox, SandboxRequest,
};
use crate::{ContainerId, RuntimeError, WireFailure};
use std::io::Read;

impl Docker {
    /// Observes an exact Created container without Create, Upload or Start.
    /// The caller must already own and persist its original creation scope,
    /// selected ID and proof that no subsequent Upload has been attempted.
    /// Engine Created alone does not establish that latter fact.
    pub fn observe_created(
        &self,
        id: ContainerId,
        request: SandboxRequest,
    ) -> Result<Sandbox, Box<WireFailure>> {
        let owner = Sandbox {
            docker: self.clone(),
            id,
            request,
            upload_attempted: false,
            uploaded: false,
            start_attempted: false,
            started: false,
            listen_attempted: false,
            stop_attempted: false,
            stopped: false,
            delete_attempted: false,
            deleted: false,
        };
        owner
            .request
            .check()
            .map_err(|_| owner.invalid("original Created deployment"))?;
        owner.verify_export()?;
        self.plain_volume(&owner.request.store_volume)?;
        let mut response =
            self.exchange("GET", &format!("/containers/{id}/json"), false, |_| Ok(()))?;
        if response.status != 200 {
            return Err(owner.selected(response.fail(RuntimeError::Http(response.status), None)));
        }
        let result = (|| {
            let mut fields = 0;
            let mut valid = true;
            let mut j = Json::new(&mut response.body);
            j.object(|j, key| {
                if key.equals("Id") {
                    once(&mut fields, 1)?;
                    valid &= j.string_equals(&id.to_string())?;
                } else if key.equals("Image") {
                    once(&mut fields, 2)?;
                    valid &= j.string_equals(&owner.request.image)?;
                } else if key.equals("Config") {
                    once(&mut fields, 4)?;
                    valid &= super::topology::config(j, &owner.request.labels)?;
                } else if key.equals("HostConfig") {
                    once(&mut fields, 8)?;
                    valid &= super::topology::created_host(j, owner.request.port)?;
                } else if key.equals("Mounts") {
                    once(&mut fields, 16)?;
                    valid &= super::topology::mounts(j, &owner.request)?;
                } else if key.equals("State") {
                    once(&mut fields, 32)?;
                    valid &= never_started(j)?;
                } else if key.equals("RestartCount") {
                    once(&mut fields, 64)?;
                    valid &= j.integer()? == 0;
                } else {
                    j.skip(1)?;
                }
                Ok(())
            })?;
            j.finish()?;
            if fields != 127 || !valid {
                return Err(RuntimeError::Protocol("exact original Created container"));
            }
            owner
                .verify_export()
                .map_err(|_| RuntimeError::Protocol("original export changed"))?;
            Ok(())
        })();
        result.map_err(|error| owner.selected(response.fail(error, None)))?;
        Ok(owner)
    }
}

fn never_started<R: Read>(j: &mut Json<R>) -> Result<bool, RuntimeError> {
    let mut fields = 0_u16;
    let mut valid = true;
    j.object(|j, key| {
        if key.equals("Status") {
            state_field(&mut fields, 1)?;
            valid &= j.string_equals("created")?;
        } else if key.equals("Running")
            || key.equals("Paused")
            || key.equals("Restarting")
            || key.equals("OOMKilled")
            || key.equals("Dead")
        {
            let bit = if key.equals("Running") {
                2
            } else if key.equals("Paused") {
                4
            } else if key.equals("Restarting") {
                8
            } else if key.equals("OOMKilled") {
                16
            } else {
                32
            };
            state_field(&mut fields, bit)?;
            valid &= !j.boolean()?;
        } else if key.equals("Pid") || key.equals("ExitCode") {
            state_field(&mut fields, if key.equals("Pid") { 64 } else { 128 })?;
            valid &= j.integer()? == 0;
        } else if key.equals("StartedAt") || key.equals("FinishedAt") {
            state_field(&mut fields, if key.equals("StartedAt") { 256 } else { 512 })?;
            valid &= j.string_equals("0001-01-01T00:00:00Z")?;
        } else if key.equals("Error") {
            state_field(&mut fields, 1024)?;
            valid &= j.string_equals("")?;
        } else {
            j.skip(2)?;
        }
        Ok(())
    })?;
    Ok(fields == 2047 && valid)
}

fn state_field(bits: &mut u16, bit: u16) -> Result<(), RuntimeError> {
    if *bits & bit != 0 {
        return Err(RuntimeError::Protocol("duplicate Created state field"));
    }
    *bits |= bit;
    Ok(())
}
