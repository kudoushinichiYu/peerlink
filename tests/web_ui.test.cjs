const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const test = require("node:test");
const vm = require("node:vm");

function loadPage(origin, apiRequest) {
  const nodes = new Map();
  function element(id) {
    if (!nodes.has(id)) {
      nodes.set(id, {
        value: "",
        addEventListener() {},
        replaceChildren(...options) { this.value = options[0]?.value || ""; }
      });
    }
    return nodes.get(id);
  }
  const context = vm.createContext({
    document: {getElementById: element, querySelectorAll: () => []},
    location: {origin},
    Option: class { constructor(text, value) { this.text = text; this.value = value; } },
    setInterval() {},
    fetch: async (url, request) => url === "/api/me"
      ? {ok: false, status: 401, json: async () => ({})}
      : {ok: true, json: async () => apiRequest(url, request)}
  });
  vm.runInContext(fs.readFileSync(path.join(__dirname, "../peerlink/static/app.js"), "utf8"), context);
  return {element, context};
}

test("the pairing command uses the Hub that issued the code", async () => {
  for (const origin of ["http://127.0.0.1:8000", "https://team.example.org:8443"]) {
    const {element} = loadPage(origin, (url, request) => {
      assert.equal(url, "/api/pairing-codes");
      assert.equal(request.method, "POST");
      return {code: "sample"};
    });
    element("device-name").value = "work laptop";
    await element("pair-device").onclick();
    assert.ok(element("connect-command").textContent.includes(`--hub '${origin}'`));
  }
});

test("switching recipients clears the previous project and ignores a late response", async () => {
  let finishFirst;
  const first = new Promise(resolve => { finishFirst = resolve; });
  const {element, context} = loadPage("https://hub.example.org", url => {
    if (url === "/api/projects?owner=first") return first;
    if (url === "/api/projects?owner=second") return [{id: "new", runtime: "mock"}];
    return [];
  });
  element("project").value = "legacy:old";
  element("peer").value = "first";
  const slow = context.loadPeerProjects();
  assert.equal(element("project").value, "");
  element("peer").value = "second";
  await context.loadPeerProjects();
  finishFirst([{id: "obsolete", runtime: "mock"}]);
  await slow;
  assert.equal(element("project").value, "legacy:new");
});
