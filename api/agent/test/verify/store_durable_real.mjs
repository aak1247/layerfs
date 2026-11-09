// Proves the Durable global Store route through Init, another process's
// recorded-profile open, and a mounted Workspace's explicit Commit. The
// Workspace overlay itself is still disposable and is not a recovery proof.
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const { LayerFS } = await import("@layerfs/agent");
if (process.argv[2] === "reopen") {
  const client = await LayerFS.open(process.argv[3]);
  assert.match((await client.fs.read("lane", "/durable.txt")).text, /committed data/);
  await client.close();
} else {
  if (process.argv[2] !== "inner") throw new Error("usage: store_durable_real.mjs inner");
  assert.equal(process.env.LAYERFS_STORE_PROFILE, "durable");
  const source = mkdtempSync(join(tmpdir(), "layerfs-durable-"));
  writeFileSync(join(source, "README.md"), "durable Store test\n");
  const project = `durable-${Date.now().toString(36)}`;
  const client = await LayerFS.init(project, source);
  let workspace;
  try {
    await client.branch.fork("lane");
    await client.fs.write("lane", "/durable.txt", "committed data\n");
    const child = spawnSync(process.execPath, [fileURLToPath(import.meta.url), "reopen", project], {
      env: { ...process.env, LAYERFS_STORE_PROFILE: "disposable" },
      encoding: "utf8",
      timeout: 15_000,
    });
    assert.equal(child.status, 0, child.stderr || child.error?.message);
    const sandbox = await client.sandbox.create();
    workspace = await client.workspace.mount(sandbox.sandbox, "lane");
    await client.workspace.exec(workspace, "echo mounted >> durable.txt");
    await client.workspace.commit(workspace, "persist mounted changes");
    await client.workspace.unmount(workspace);
    workspace = undefined;
    await client.sandbox.close(sandbox.sandbox);
    assert.match((await client.fs.read("lane", "/durable.txt")).text, /mounted/);
    console.log("durable global Store: ok (Workspace recovery not tested)");
  } finally {
    if (workspace) await client.workspace.unmount(workspace, true).catch(() => {});
    await client.close();
    rmSync(source, { recursive: true, force: true });
  }
}
