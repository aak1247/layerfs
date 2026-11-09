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
  if (info.isDirectory()) {
    for (const name of fs.readdirSync(path)) ownTree(`${path}/${name}`);
  }
  fs.lchownSync(path, 501, 20);
}

(async () => {
  fs.chownSync(home, 0, 0);
  fs.chmodSync(home, 0o700);
  ownTree('/proof/source');
  fs.mkdirSync('/layerfs-test-volume/global', { mode: 0o700 });
  await call('project.init', { home, name: inputs.project, path: '/proof/source', storePath, storeProfile: 'durable', timeoutMs: 60000 });
  const opened = await native.storeOpen(base);
  if (opened.refused) throw new Error(`storeOpen: ${opened.refused.code}: ${opened.refused.message}`);
  for (const name of ['git-alpha', 'git-beta']) await call('branch.fork', { ...base, name, from: 'main' });
  const sandbox = await call('sandbox.create', { ...base, deploy, maxWorkspaces: 16 });
  console.log(`durable sandbox ${sandbox.name} created`);
  const alpha = await call('workspace.mount', { ...base, sandbox: sandbox.sandbox, branch: 'git-alpha' });
  const beta = await call('workspace.mount', { ...base, sandbox: sandbox.sandbox, branch: 'git-beta' });
  assert.notEqual(alpha.path, beta.path);
  for (const workspace of [alpha, beta]) {
    assert.equal((await command(sandbox.sandbox, workspace, 'git rev-parse HEAD')).trim(), inputs.head);
    assert.equal(await command(sandbox.sandbox, workspace, 'git status --porcelain'), '');
    assert.equal(await command(sandbox.sandbox, workspace, 'cat node_modules/example/marker.txt'), 'ignored-dependency\n');
  }
  const identities = {};
  for (const [name, workspace] of [['alpha', alpha], ['beta', beta]]) {
    assert.equal(await command(sandbox.sandbox, workspace, 'cat tracked.txt'), 'base\n');
    const change = `printf '${name}\\n' > tracked.txt && git add -- tracked.txt && git -c user.name='LayerFS test' -c user.email=test@example.invalid commit -m '${name} commit'`;
    await command(sandbox.sandbox, workspace, change);
    identities[name] = (await command(sandbox.sandbox, workspace, 'git rev-parse HEAD')).trim();
    assert.notEqual(identities[name], inputs.head);
    assert.equal(await command(sandbox.sandbox, workspace, 'git status --porcelain'), '');
    const saved = await call('workspace.commit', { ...base, sandbox: sandbox.sandbox, path: workspace.path, message: `${name} workspace` });
    assert.equal(saved.kind, 'Committed');
    console.log(`${name} workspace made and saved an independent Git commit`);
  }
  assert.notEqual(identities.alpha, identities.beta);
  for (const workspace of [alpha, beta]) {
    await call('workspace.unmount', { ...base, sandbox: sandbox.sandbox, path: workspace.path });
  }
  for (const [name, branch] of [['alpha', 'git-alpha'], ['beta', 'git-beta']]) {
    const reopened = await call('workspace.mount', { ...base, sandbox: sandbox.sandbox, branch });
    assert.equal((await command(sandbox.sandbox, reopened, 'git rev-parse HEAD')).trim(), identities[name]);
    assert.equal(await command(sandbox.sandbox, reopened, 'cat tracked.txt'), `${name}\n`);
    assert.equal(await command(sandbox.sandbox, reopened, 'cat node_modules/example/marker.txt'), 'ignored-dependency\n');
    assert.equal(await command(sandbox.sandbox, reopened, 'git status --porcelain'), '');
    await call('workspace.unmount', { ...base, sandbox: sandbox.sandbox, path: reopened.path, discard: true });
  }
  fs.writeFileSync('/proof/result.json', JSON.stringify({ sandbox: sandbox.name, original: inputs.head, identities }, null, 2));
  console.log('Git workspaces remained independent and survived normal close/remount');
})().catch(error => {
  console.error(error.stack || error.message);
  process.exitCode = 1;
});
