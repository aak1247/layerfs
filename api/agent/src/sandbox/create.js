// sandbox.create: a new Sandbox of the project, the lowest free number, on
// `image` (default LAYERFS_IMAGE), ready when this returns: { sandbox, image }.
// maxWorkspaces defaults to LAYERFS_MAX_WORKSPACES, then 16, and is fixed
// for this Sandbox's life.
// The only call that starts a Sandbox (no placement since 2026-10-23); it
// first reclaims the project's Sandboxes that closed themselves. The new one
// closes itself two minutes after it has no Workspace and no process, like
// every Sandbox; `sandbox.close` stops it now.
import { deployment, invokeClient } from "../client.js";
import { LayerFSError } from "../errors.js";
export async function create(client, options = {}) {
  if (options === null || typeof options !== "object" || Array.isArray(options) || (options.image !== undefined && typeof options.image !== "string")) {
    throw new LayerFSError("InvalidInput", "options are { image?, maxWorkspaces? }", null);
  }
  const fromEnvironment = options.maxWorkspaces === undefined;
  const configured = fromEnvironment ? client.options.maxWorkspaces : options.maxWorkspaces;
  const capacity = fromEnvironment && typeof configured === "string" && /^\d+$/.test(configured) ? Number(configured) : configured;
  if (capacity !== undefined && (!Number.isSafeInteger(capacity) || capacity < 1 || capacity > 0xffffffff)) {
    throw new LayerFSError("InvalidInput", "maxWorkspaces must be an integer from 1 to 4294967295", null);
  }
  const deploy = deployment({ ...client.options.deploy, ...(options.image ? { image: options.image } : {}) });
  const value = await invokeClient(client, "sandbox.create", { deploy, ...(capacity === undefined ? {} : { maxWorkspaces: capacity }) });
  return { sandbox: value.sandbox, image: value.image };
}
