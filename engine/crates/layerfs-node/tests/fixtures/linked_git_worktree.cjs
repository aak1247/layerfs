const fs = require('node:fs');
const assert = require('node:assert/strict');
const native = require('/proof/layerfs.node');
const inputs = JSON.parse(fs.readFileSync('/proof/inputs.json', 'utf8'));
const home = '/proof/home';
const storePath = '/layerfs-test-volume/global/store.sqlite';
const base = { home, project: inputs.project, storePath };
const deploy = {
  docker: '/var/run/docker.sock',
  image: 'sha256:644c03ea57592a10b1ccc3d16c46a38f33ae275d265ad5ad3e50e58b4da37d02',
  volume: inputs.volume,
  daemon: '/proof/layerfs-daemon',
};
async function call(op, args) {
  const result = await native.call(op, args);
  if (result.refused) throw new Error(`${op}: ${result.refused.code}: ${result.refused.message}`);
  return result.value;
}
function ownTree(path) {
  const info = fs.lstatSync(path);
  if (info.isDirectory()) for (const name of fs.readdirSync(path)) ownTree(`${path}/${name}`);
  fs.lchownSync(path, 501, 20);
}
(async () => {
  fs.chownSync(home, 0, 0);
  fs.chmodSync(home, 0o700);
  ownTree('/proof/project');
  ownTree('/proof/source');
  fs.mkdirSync('/layerfs-test-volume/global', { mode: 0o700 });
  await call('project.init', { home, name: inputs.project, path: '/proof/source', storePath, storeProfile: 'durable', timeoutMs: 60000 });
  const opened = await native.storeOpen(base);
  if (opened.refused) throw new Error(`storeOpen: ${opened.refused.code}: ${opened.refused.message}`);
  await call('branch.fork', { ...base, name: 'linked', from: 'main' });
  const sandbox = await call('sandbox.create', { ...base, deploy, maxWorkspaces: 16 });
  console.log(`durable sandbox ${sandbox.name} created`);
  const workspace = await call('workspace.mount', { ...base, sandbox: sandbox.sandbox, branch: 'linked' });
  const exec = (command) => call('workspace.exec', { ...base, sandbox: sandbox.sandbox, path: workspace.path, command });
  const content = await exec('timeout 30s cat tracked.txt');
  assert.equal(content.exitCode, 0);
  assert.equal(content.stdout, 'dirty user draft\n');
  const status = await exec('timeout 30s git status --porcelain');
  const head = await exec('timeout 30s git rev-parse HEAD');
  console.log(`linked Git status exit ${status.exitCode}: ${status.stderr.trim()}`);
  console.log(`linked Git HEAD exit ${head.exitCode}: ${head.stderr.trim()}`);
  await call('workspace.unmount', { ...base, sandbox: sandbox.sandbox, path: workspace.path, discard: true });
  fs.writeFileSync('/proof/result.json', JSON.stringify({ sandbox: sandbox.name, content: content.stdout, status, head }, null, 2));
})().catch(error => { console.error(error.stack || error.message); process.exitCode = 1; });
