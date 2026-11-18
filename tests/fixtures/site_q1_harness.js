// Runs site/app.js against a tiny fake DOM and prints what the Q1 section drew, as JSON.
// usage: node site_q1_harness.js <app.js> <q1-rows-json>
"use strict";
const fs = require("fs");

const [appPath, rowsJson] = process.argv.slice(2);
const q1Rows = JSON.parse(rowsJson);

class Node {
  constructor(tag) {
    this.tag = tag;
    this.attrs = {};
    this.children = [];
    this.text = "";
    this.clientWidth = 640;
    this.hidden = false;
    this.style = {};
    this.classList = { toggle() {} };
    this.roles = {};
  }
  setAttribute(k, v) {
    this.attrs[k] = String(v);
  }
  set textContent(v) {
    this.text = String(v);
  }
  get textContent() {
    return this.text;
  }
  append(...c) {
    this.children.push(...c);
  }
  replaceChildren(...c) {
    this.children = [...c];
  }
  addEventListener() {}
  remove() {}
  getBoundingClientRect() {
    return { left: 0, top: 0, width: 640, height: 280 };
  }
  querySelector(sel) {
    const m = /data-role="(\w+)"/.exec(sel);
    if (!m) return null;
    this.roles[m[1]] = this.roles[m[1]] || new Node("div");
    return this.roles[m[1]];
  }
}

const sections = {};
const byId = {};
const document = {
  readyState: "complete",
  createElement: (t) => new Node(t),
  createElementNS: (_ns, t) => new Node(t),
  getElementById: (id) => (byId[id] = byId[id] || new Node("span")),
  querySelector(sel) {
    const m = /data-mart="(\w+)"/.exec(sel);
    return m ? (sections[m[1]] = sections[m[1]] || new Node("section")) : null;
  },
};
const meta = { generated_at: "2026-11-18T07:28:00+00:00", git_sha: null, simulated_sections: [] };
const fetch = async (path) => {
  if (path === "data/mart_daily_active_contributors.json") {
    return { ok: true, json: async () => ({ columns: [], rows: q1Rows }) };
  }
  if (path === "data/meta.json") return { ok: true, json: async () => meta };
  return { ok: true, json: async () => ({ columns: [], rows: [] }) };
};

new Function("document", "window", "fetch", fs.readFileSync(appPath, "utf8"))(
  document,
  { innerWidth: 1000 },
  fetch,
);

function walk(node, out) {
  out.push({ tag: node.tag, attrs: node.attrs, text: node.text });
  for (const c of node.children) walk(c, out);
  return out;
}

setTimeout(() => {
  const q1 = sections.mart_daily_active_contributors;
  console.log(
    JSON.stringify({
      chart: walk(q1.roles.chart, []),
      table: walk(q1.roles.table, []),
      status: q1.roles.status.text,
    }),
  );
}, 100);
