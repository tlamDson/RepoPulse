// Test harness for tests/test_site_behaviour.py: runs site/app.js against a tiny fake DOM.
// argv[2] = path of app.js, stdin = JSON {meta, marts: {<mart>: doc}}; stdout = JSON per section.
"use strict";
const fs = require("fs");
const vm = require("vm");

class FakeNode {
  constructor(tag) {
    this.tag = tag;
    this.children = [];
    this.attrs = {};
    this.own = "";
    this.hidden = false;
    this.clientWidth = 640;
    this.classList = { toggle() {}, add() {}, remove() {} };
  }
  set textContent(v) {
    this.own = String(v);
    this.children = [];
  }
  get textContent() {
    return this.own + this.children.map((c) => (typeof c === "string" ? c : c.textContent)).join("");
  }
  append(...nodes) {
    this.children.push(...nodes);
  }
  replaceChildren(...nodes) {
    this.own = "";
    this.children = nodes;
  }
  setAttribute(k, v) {
    this.attrs[k] = v;
  }
  addEventListener() {}
  remove() {}
  getBoundingClientRect() {
    return { left: 0, top: 0, width: 640, height: 280 };
  }
  find(tag) {
    for (const c of this.children) {
      if (typeof c === "string") continue;
      if (c.tag === tag) return c;
      const inner = c.find(tag);
      if (inner) return inner;
    }
    return null;
  }
}

const ROLES = ["status", "chart", "table"];
const sections = {};
const roleNodes = {};
const byId = {};

function sectionFor(mart) {
  if (!sections[mart]) {
    const s = new FakeNode("section");
    s.querySelector = (sel) => s.roles[/data-role="([^"]+)"/.exec(sel)[1]];
    s.roles = Object.fromEntries(ROLES.map((r) => [r, new FakeNode("div")]));
    sections[mart] = s;
  }
  return sections[mart];
}

const document = {
  readyState: "complete",
  createElement: (tag) => new FakeNode(tag),
  createElementNS: (_ns, tag) => new FakeNode(tag),
  getElementById: (id) => (byId[id] = byId[id] || new FakeNode("span")),
  querySelector: (sel) => {
    const mart = /^section\[data-mart="([^"]+)"\]$/.exec(sel);
    if (mart) return sectionFor(mart[1]);
    const role = /^\[data-role="([^"]+)"\]$/.exec(sel);
    if (role) return (roleNodes[role[1]] = roleNodes[role[1]] || new FakeNode("span"));
    return null;
  },
  addEventListener() {},
};

const input = JSON.parse(fs.readFileSync(0, "utf8"));
const docs = { "data/meta.json": input.meta };
for (const [mart, doc] of Object.entries(input.marts)) docs[`data/${mart}.json`] = doc;

const context = {
  document,
  window: { innerWidth: 1024 },
  Intl,
  console,
  fetch: async (path) =>
    docs[path]
      ? { ok: true, json: async () => docs[path] }
      : { ok: true, json: async () => ({ columns: ["x"], rows: [] }) },
  requestAnimationFrame: (f) => f(),
};
vm.createContext(context);
vm.runInContext(fs.readFileSync(process.argv[2], "utf8"), context);

setTimeout(() => {
  const q2 = sectionFor("mart_cohort_retention");
  const table = q2.roles.table;
  const out = {
    status: q2.roles.status.textContent,
    chart_has_svg: q2.roles.chart.find("svg") !== null,
    chart_text: q2.roles.chart.textContent,
    table_text: table.textContent,
    window_start: (roleNodes["q2-window"] || { textContent: null }).textContent,
  };
  process.stdout.write(JSON.stringify(out));
}, 200);
