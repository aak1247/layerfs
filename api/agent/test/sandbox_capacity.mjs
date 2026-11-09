// Sandbox capacity is selected at creation and is never silently clamped.
// This test uses the ordinary SDK over its stand-in native binding.
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import { ENV } from "./fixture/cases.mjs";

const fake = fileURLToPath(new URL("./fixture/fake-native.cjs", import.meta.url));
Object.assign(process.env, ENV, { LAYERFS_NATIVE: fake, LAYERFS_MAX_WORKSPACES: "32" });
const native = createRequire(import.meta.url)(fake);
const { LayerFS } = await import("../src/index.js");
const client = await LayerFS.open("site");
const last = () => native.args.at(-1);

await client.sandbox.create();
assert.equal(last().maxWorkspaces, 32);
await client.sandbox.create({ maxWorkspaces: 64 });
assert.equal(last().maxWorkspaces, 64);
for (const value of [0, -1, 1.5, "64", null, 4294967296]) {
  const before = native.calls.length;
  await assert.rejects(client.sandbox.create({ maxWorkspaces: value }), { code: "InvalidInput" });
  assert.equal(native.calls.length, before, "invalid capacity must not make a native call");
}
await client.close();

process.env.LAYERFS_MAX_WORKSPACES = "invalid";
const invalid = await LayerFS.open("site");
await assert.rejects(invalid.sandbox.create(), { code: "InvalidInput" });
await invalid.sandbox.create({ maxWorkspaces: 16 });
assert.equal(last().maxWorkspaces, 16, "an explicit capacity overrides the environment");
await invalid.close();
delete process.env.LAYERFS_MAX_WORKSPACES;
console.log("PASS sandbox capacity settings");
