// Correctness proof for dozens of simultaneous mounts, without a timing or
// throughput claim. Run in an owned E2E Store using the isolated launcher.
import assert from "node:assert/strict";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

if (process.argv[2] !== "inner") throw new Error("usage: workspace_capacity_real.mjs inner");
const { LayerFS } = await import("@layerfs/agent");
const project = `capacity-${Date.now().toString(36)}`;
const source = mkdtempSync(join(tmpdir(), "layerfs-capacity-"));
writeFileSync(join(source, "README.md"), "capacity test\n");
const client = await LayerFS.init(project, source);
const mounted = [];
try {
  await client.branch.fork("lane");
  const sandbox = await client.sandbox.create({ maxWorkspaces: 64 });
  for (let i = 0; i < 64; i++) mounted.push(await client.workspace.mount(sandbox.sandbox, "lane"));
  const info = (await client.project.list_sandboxes()).items[0];
  assert.deepEqual([info.workspaces, info.maxWorkspaces], [64, 64]);
  await assert.rejects(client.workspace.mount(sandbox.sandbox, "lane"), { code: "Capacity" });
  while (mounted.length) {
    await client.workspace.unmount(mounted.at(-1), true);
    mounted.pop();
  }
  await client.sandbox.close(sandbox.sandbox);
  console.log("64 simultaneous Workspaces: ok");
} finally {
  for (const workspace of mounted) await client.workspace.unmount(workspace, true).catch(() => {});
  await client.close();
  rmSync(source, { recursive: true, force: true });
}
