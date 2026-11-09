//! One checked FUSE sync, retaining the original attempted failure.
use super::{
    accounting::Opcode,
    failure::{failed, KernelInput},
    terminal, KernelFilesystem,
};
use crate::{
    ports::{Fenced, SyncUnavailable},
    RequestDisposition,
};
use fuser::{Errno, FileHandle, INodeNo, ReplyEmpty, Request};
use std::io;

impl KernelFilesystem {
    pub(super) fn synchronize(
        &self,
        req: &Request,
        inode: INodeNo,
        handle: FileHandle,
        directory: bool,
        data_only: bool,
        reply: ReplyEmpty,
    ) {
        let opcode = if directory {
            Opcode::Fsyncdir
        } else {
            Opcode::Fsync
        };
        let Some((permit, reply)) = self.receive_reply(opcode, 0, reply, ReplyEmpty::error) else {
            return;
        };
        let mount = self.queue.identity();
        let services = self.services.clone();
        let fence = self.fence.clone();
        let serial = self.identity.serial(inode);
        let request = req.unique().0;
        self.serve(
            permit,
            Box::pin(async move {
                let outcome = async {
                    let serial =
                        serial.map_err(|error| io::Error::from_raw_os_error(error.code()))?;
                    let services = services.request(&fence)?;
                    services
                        .synchronize(mount, serial, handle.0, directory)
                        .await
                }
                .await;
                match outcome {
                    Ok(receipt) => {
                        reply.ok();
                        drop(receipt);
                        RequestDisposition::Complete
                    }
                    Err(error) if error.is::<Fenced>() => {
                        reply.error(terminal::STOPPED);
                        fence.replied();
                        RequestDisposition::Complete
                    }
                    Err(error) if error.is::<SyncUnavailable>() => {
                        reply.error(Errno::EAGAIN);
                        RequestDisposition::Complete
                    }
                    Err(error) => {
                        reply.error(Errno::EIO);
                        failed(
                            request,
                            mount,
                            KernelInput::Synchronize {
                                inode: inode.0,
                                handle: handle.0,
                                directory,
                                data_only,
                            },
                            error,
                        )
                    }
                }
            }),
        );
    }
}
