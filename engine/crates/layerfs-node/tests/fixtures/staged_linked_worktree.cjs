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
async function command(sandbox, workspace, command) {
  const result = await call('workspace.exec', { ...base, sandbox, path: workspace.path, command: `timeout 30s sh -c ${JSON.stringify(command)}` });
  assert.equal(result.exitCode, 0, `${command}\nstdout: ${result.stdout}\nstderr: ${result.stderr}`);
  return result.stdout;
}
function ownTree(path) {
  const info = fs.lstatSync(path);
  if (info.isDirectory()) for (const name of fs.readdirSync(path)) ownTree(`${path}/${name}`);
  fs.lchownSync(path, 501, 20);
}
(async () => {
  fs.chownSync(home, 0, 0);
  fs.chmodSync(home, 0o700);
  ownTree('/proof/stage');
  fs.mkdirSync('/layerfs-test-volume/global', { mode: 0o700 });
  await call('project.init', { home, name: inputs.project, path: '/proof/stage', storePath, storeProfile: 'durable', timeoutMs: 60000 });
  const opened = await native.storeOpen(base);
  if (opened.refused) throw new Error(`storeOpen: ${opened.refused.code}: ${opened.refused.message}`);
  await call('branch.fork', { ...base, name: 'linked', from: 'main' });
  const sandbox = await call('sandbox.create', { ...base, deploy, maxWorkspaces: 16 });
  console.log(`durable sandbox ${sandbox.name} created`);
  const workspace = await call('workspace.mount', { ...base, sandbox: sandbox.sandbox, branch: 'linked' });
  assert.equal((await command(sandbox.sandbox, workspace, 'git rev-parse HEAD')).trim(), inputs.head);
  assert.equal((await command(sandbox.sandbox, workspace, 'git branch --show-current')).trim(), 'feature');
  assert.equal((await command(sandbox.sandbox, workspace, 'git status --porcelain')).trim(), inputs.status);
  assert.equal(await command(sandbox.sandbox, workspace, 'git show :tracked.txt'), 'staged\n');
  assert.equal(await command(sandbox.sandbox, workspace, 'cat tracked.txt draft.txt node_modules/example/marker.txt'), 'unstaged draft\nuntracked draft\nignored-dependency\n');
  await command(sandbox.sandbox, workspace, 'test ! -e .git/worktrees');
  await command(sandbox.sandbox, workspace, "git add tracked.txt draft.txt && git -c user.name='LayerFS test' -c user.email=test@example.invalid commit -m 'staged linked workspace'");
  const head = (await command(sandbox.sandbox, workspace, 'git rev-parse HEAD')).trim();
  assert.notEqual(head, inputs.head);
  const saved = await call('workspace.commit', { ...base, sandbox: sandbox.sandbox, path: workspace.path, message: 'linked workspace' });
  assert.equal(saved.kind, 'Committed');
  await call('workspace.unmount', { ...base, sandbox: sandbox.sandbox, path: workspace.path });
  const reopened = await call('workspace.mount', { ...base, sandbox: sandbox.sandbox, branch: 'linked' });
  assert.equal((await command(sandbox.sandbox, reopened, 'git rev-parse HEAD')).trim(), head);
  assert.equal(await command(sandbox.sandbox, reopened, 'cat tracked.txt draft.txt node_modules/example/marker.txt'), 'unstaged draft\nuntracked draft\nignored-dependency\n');
  await call('workspace.unmount', { ...base, sandbox: sandbox.sandbox, path: reopened.path, discard: true });
  fs.writeFileSync('/proof/result.json', JSON.stringify({ sandbox: sandbox.name, originalHead: inputs.head, committedHead: head }, null, 2));
  console.log('staged linked worktree preserved and remained a usable Git repository after LayerFS close/remount');
})().catch(error => { console.error(error.stack || error.message); process.exitCode = 1; });
