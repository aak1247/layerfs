// layerfs sandbox create: sandbox.create({ image?, maxWorkspaces? }).

export default {
  args: [],
  options: { image: "text", "max-workspaces": "int" },
  summary: "start a Sandbox, the lowest free number (the only call that starts one)",
  run: (lf, a, o) => lf.sandbox.create({ image: o.image, maxWorkspaces: o.maxWorkspaces }),
};
