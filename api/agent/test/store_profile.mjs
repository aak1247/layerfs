// A project's Store profile is selected before Init, with no native call for
// invalid input. Reopening is governed by its recorded manifest instead.
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import { fileURLToPath } from "node:url";
import { ENV } from "./fixture/cases.mjs";

const fake = fileURLToPath(new URL("./fixture/fake-native.cjs", import.meta.url));
Object.assign(process.env, ENV, { LAYERFS_NATIVE: fake, LAYERFS_STORE_PROFILE: "durable" });
const native = createRequire(import.meta.url)(fake);
const { LayerFS } = await import("../src/index.js");
const client = await LayerFS.init("site", "/fake/source");
const init = native.args[native.calls.lastIndexOf("project.init")];
assert.equal(init.storeProfile, "durable");
await client.close();

process.env.LAYERFS_STORE_PROFILE = "other";
const before = native.calls.length;
await assert.rejects(LayerFS.init("site", "/fake/source"), { code: "InvalidInput" });
assert.equal(native.calls.length, before);
delete process.env.LAYERFS_STORE_PROFILE;
console.log("PASS Store profile settings");
