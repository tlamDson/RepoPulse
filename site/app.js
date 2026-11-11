// RepoPulse dashboard: reads the JSON written by `python -m repopulse.export_site` (relative
// paths, same origin) and draws SVG charts plus a table per section. No library, no request to any
// other site. Data values only ever reach the DOM through textContent / setAttribute.
"use strict";

(() => {
  const SVG_NS = "http://www.w3.org/2000/svg"; // XML namespace name, never fetched
  const MAX_SERIES = 8; // categorical palette size
  const TOP_N = 15; // bars per ranked chart; the table lists every row
  const TABLE_LIMIT = 50; // rows shown before "Show all"

  // ------------------------------------------------------------ formatting

  const nf = (opts) => new Intl.NumberFormat("en", opts);
  const fmtInt = nf({ maximumFractionDigits: 0 });
  const fmt1 = nf({ minimumFractionDigits: 1, maximumFractionDigits: 1 });
  const fmt2 = nf({ maximumFractionDigits: 2 });
  const fmt3 = nf({ minimumFractionDigits: 3, maximumFractionDigits: 3 });
  const fmtCompact = nf({ notation: "compact", maximumFractionDigits: 1 });
  const FORMAT = {
    text: (v) => String(v),
    raw: (v) => String(v),
    int: (v) => fmtInt.format(v),
    pct: (v) => fmt1.format(v) + "%",
    hours: (v) => fmt1.format(v) + " h",
    score: (v) => fmt3.format(v),
    secs: (v) => fmt2.format(v) + " s",
  };
  const NUMERIC = new Set(["int", "pct", "hours", "score", "secs"]);
  const INT_COLUMNS = new Set([
    "active_contributors", "new_contributors", "returning_contributors", "cohort_size",
    "retained_w4", "retained_w12", "forkers", "forkers_with_pr", "forkers_with_merge",
    "prs_opened", "prs_merged", "opened", "responded", "closed", "contributions",
    "bot_contributions", "human_contributors", "health_rank",
  ]);
  const RUNS_NUMERIC = /_seconds$|^peak_rss_mb$|^bytes_read$|^row_count$/;

  function isMissing(v) {
    return v === null || v === undefined || (typeof v === "number" && !Number.isFinite(v));
  }

  function show(v, kind) {
    return isMissing(v) ? "–" : FORMAT[kind](v);
  }

  function kindOf(column) {
    if (column.endsWith("_pct")) return "pct";
    if (column.includes("_hours_")) return "hours";
    if (column.endsWith("_score")) return "score";
    if (INT_COLUMNS.has(column)) return "int";
    return "text";
  }

  function fmtTick(v) {
    return Math.abs(v) >= 10000 ? fmtCompact.format(v) : fmt2.format(v);
  }

  const dayFormat = new Intl.DateTimeFormat("en", { month: "short", day: "numeric", timeZone: "UTC" });
  const monthFormat = new Intl.DateTimeFormat("en", { month: "short", year: "numeric", timeZone: "UTC" });

  function dayMs(iso) {
    return Date.parse(iso + "T00:00:00Z");
  }

  function formatStamp(iso) {
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return String(iso);
    return d.toISOString().slice(0, 16).replace("T", " ") + " UTC";
  }

  function truncate(text, max) {
    const s = String(text);
    return s.length > max && max > 1 ? s.slice(0, max - 1) + "…" : s;
  }

  // ------------------------------------------------------------ DOM helpers

  function el(tag, attrs, text) {
    const node = document.createElement(tag);
    for (const [k, v] of Object.entries(attrs || {})) node.setAttribute(k, String(v));
    if (text !== undefined && text !== null) node.textContent = String(text);
    return node;
  }

  function svg(tag, attrs, text) {
    const node = document.createElementNS(SVG_NS, tag);
    for (const [k, v] of Object.entries(attrs || {})) node.setAttribute(k, String(v));
    if (text !== undefined && text !== null) node.textContent = String(text);
    return node;
  }

  function keyIcon(shape, cls) {
    const icon = svg("svg", { viewBox: "0 0 16 10", "aria-hidden": "true", focusable: "false" });
    if (shape === "line") {
      icon.append(svg("line", { class: `line-key ${cls}`, x1: 1, y1: 5, x2: 15, y2: 5 }));
    } else {
      icon.append(svg("rect", { class: `key ${cls}`, x: 0, y: 1, width: 16, height: 8, rx: 2 }));
    }
    return icon;
  }

  function legend(series, shape) {
    const list = el("ul", { class: "legend" });
    series.forEach((s, i) => {
      const item = el("li");
      item.append(keyIcon(shape, `s${i + 1}`), el("span", null, s.name));
      list.append(item);
    });
    return list;
  }

  // ------------------------------------------------------------ tooltip

  function tooltip() {
    return document.getElementById("tooltip");
  }

  function showTip(point, title, rows) {
    const tip = tooltip();
    tip.replaceChildren();
    if (title) tip.append(el("p", { class: "tt-title" }, title));
    for (const r of rows) {
      const row = el("div", { class: "tt-row" });
      if (r.cls) row.append(keyIcon(r.shape || "rect", r.cls));
      row.append(el("span", { class: "tt-value" }, r.value));
      if (r.name) row.append(el("span", { class: "tt-name" }, r.name));
      tip.append(row);
    }
    tip.hidden = false;
    const w = tip.offsetWidth;
    const h = tip.offsetHeight;
    let left = point.x + 12;
    let top = point.y - h - 12;
    if (left + w > window.innerWidth - 8) left = Math.max(8, point.x - w - 12);
    if (top < 8) top = point.y + 16;
    tip.style.left = `${left}px`;
    tip.style.top = `${top}px`;
  }

  function hideTip() {
    tooltip().hidden = true;
  }

  function pointerAt(evt) {
    return { x: evt.clientX, y: evt.clientY };
  }

  function anchorOf(node) {
    const r = node.getBoundingClientRect();
    return { x: r.left + r.width / 2, y: r.top };
  }

  // ------------------------------------------------------------ scales

  function niceStep(span, count) {
    const raw = span / Math.max(1, count);
    const p = Math.pow(10, Math.floor(Math.log10(raw)));
    const m = raw / p;
    return (m <= 1 ? 1 : m <= 2 ? 2 : m <= 5 ? 5 : 10) * p;
  }

  function ticksTo(max, count) {
    const top = max > 0 ? max : 1;
    const step = niceStep(top, count);
    const end = Math.ceil(top / step) * step;
    const ticks = [];
    for (let i = 0; i * step <= end + step / 2; i += 1) ticks.push(Number((i * step).toFixed(10)));
    return ticks;
  }

  function chartWidth(host) {
    return Math.max(280, Math.floor(host.clientWidth || 640));
  }

  // ------------------------------------------------------------ bar chart (horizontal)

  function barPath(x, y, w, h, r) {
    if (w <= 0) return `M${x},${y}V${y + h}`;
    const rr = Math.min(r, w, h / 2);
    return (
      `M${x},${y}H${x + w - rr}Q${x + w},${y} ${x + w},${y + rr}` +
      `V${y + h - rr}Q${x + w},${y + h} ${x + w - rr},${y + h}H${x}Z`
    );
  }

  // spec: {title, rows: [{label, values: [...]}], series: [{name}], kind, note}
  function barChart(host, spec) {
    host.replaceChildren();
    const { rows, series, kind } = spec;
    if (series.length > 1) host.append(legend(series, "rect"));
    const width = chartWidth(host);
    const narrow = width < 520;
    const barH = series.length > 1 ? 12 : 16;
    const gap = 2; // surface gap between touching bars
    const groupH = series.length * barH + (series.length - 1) * gap;
    const labelH = narrow ? 18 : 0; // narrow: the name sits above its bars
    const rowH = groupH + labelH + 12;
    const longest = Math.max(4, ...rows.map((r) => String(r.label).length));
    const labelW = narrow ? 0 : Math.min(Math.round(width * 0.38), longest * 7 + 12);
    const valueW = 72;
    const top = 4;
    const axisH = 22;
    const plotX = labelW;
    const plotW = Math.max(60, width - labelW - valueW);
    const height = top + rows.length * rowH + axisH;
    const values = rows.flatMap((r) => r.values.filter((v) => !isMissing(v)));
    const ticks = ticksTo(Math.max(0, ...values), narrow ? 3 : 5);
    const domain = ticks[ticks.length - 1];
    const xs = (v) => plotX + (v / domain) * plotW;

    const root = svg("svg", { viewBox: `0 0 ${width} ${height}`, role: "img", "aria-label": spec.title });
    for (const t of ticks) {
      const x = xs(t);
      root.append(svg("line", { class: "grid-line", x1: x, x2: x, y1: top, y2: height - axisH }));
      root.append(svg("text", { class: "tick", x, y: height - 6, "text-anchor": "middle" }, fmtTick(t)));
    }
    rows.forEach((r, i) => {
      const y0 = top + i * rowH + 6;
      if (narrow) {
        root.append(svg("text", { class: "bar-label", x: plotX, y: y0 + 12 }, truncate(r.label, 48)));
      } else {
        const maxChars = Math.max(4, Math.floor((labelW - 12) / 7));
        root.append(
          svg(
            "text",
            { class: "bar-label", x: labelW - 8, y: y0 + groupH / 2 + 4, "text-anchor": "end" },
            truncate(r.label, maxChars),
          ),
        );
      }
      const tipRows = r.values.map((v, k) => ({ value: show(v, kind), name: series[k].name, cls: `s${k + 1}` }));
      r.values.forEach((v, j) => {
        const y = y0 + labelH + j * (barH + gap);
        if (isMissing(v)) {
          root.append(svg("text", { class: "value-label", x: plotX + 4, y: y + barH - 3 }, "no value"));
          return;
        }
        const w = Math.max(0, xs(v) - plotX);
        const bar = svg("path", {
          class: `mark-bar s${j + 1}`,
          d: barPath(plotX, y, w, barH, 4),
          tabindex: 0,
          "aria-label": `${r.label}, ${series[j].name}: ${show(v, kind)}`,
        });
        bar.addEventListener("pointermove", (e) => showTip(pointerAt(e), r.label, tipRows));
        bar.addEventListener("pointerleave", hideTip);
        bar.addEventListener("focus", (e) => showTip(anchorOf(e.target), r.label, tipRows));
        bar.addEventListener("blur", hideTip);
        root.append(bar);
        root.append(svg("text", { class: "value-label", x: plotX + w + 4, y: y + barH - 3 }, show(v, kind)));
      });
    });
    root.append(svg("line", { class: "axis-line", x1: plotX, x2: plotX, y1: top, y2: height - axisH }));
    host.append(root);
    if (spec.note) host.append(el("p", { class: "chart-note" }, spec.note));
  }

  // ------------------------------------------------------------ line chart (dates on x)

  // spec: {title, x: [ISO day], series: [{name, values: [...] aligned with x}], kind, note}
  function lineChart(host, spec) {
    host.replaceChildren();
    const { x, series, kind } = spec;
    if (series.length > 1) host.append(legend(series, "line"));
    const width = chartWidth(host);
    const narrow = width < 520;
    const height = narrow ? 220 : 280;
    const m = { top: 8, right: 16, bottom: 24, left: 48 };
    const pw = width - m.left - m.right;
    const ph = height - m.top - m.bottom;
    const values = series.flatMap((s) => s.values.filter((v) => !isMissing(v)));
    const ticks = ticksTo(Math.max(0, ...values), 4);
    const domain = ticks[ticks.length - 1];
    const t = x.map(dayMs);
    const t0 = t[0];
    const t1 = t[t.length - 1];
    const xs = (i) => m.left + (t1 === t0 ? pw / 2 : ((t[i] - t0) / (t1 - t0)) * pw);
    const ys = (v) => m.top + ph - (v / domain) * ph;
    const longRange = t1 - t0 > 300 * 86400000;
    const xLabel = (iso) => (longRange ? monthFormat : dayFormat).format(new Date(dayMs(iso)));

    const root = svg("svg", { viewBox: `0 0 ${width} ${height}`, role: "img", "aria-label": spec.title });
    for (const v of ticks) {
      const y = ys(v);
      root.append(svg("line", { class: "grid-line", x1: m.left, x2: width - m.right, y1: y, y2: y }));
      root.append(svg("text", { class: "tick", x: m.left - 6, y: y + 4, "text-anchor": "end" }, fmtTick(v)));
    }
    root.append(svg("line", { class: "axis-line", x1: m.left, x2: width - m.right, y1: ys(0), y2: ys(0) }));
    const n = Math.min(x.length, Math.max(2, Math.floor(pw / 96)));
    const tickIdx = new Set();
    for (let k = 0; k < n; k += 1) tickIdx.add(n === 1 ? 0 : Math.round((k * (x.length - 1)) / (n - 1)));
    for (const i of tickIdx) {
      const anchor = x.length === 1 ? "middle" : i === 0 ? "start" : i === x.length - 1 ? "end" : "middle";
      root.append(svg("text", { class: "tick", x: xs(i), y: height - 6, "text-anchor": anchor }, xLabel(x[i])));
    }

    series.forEach((s, j) => {
      const cls = `s${j + 1}`;
      let d = "";
      let open = false;
      s.values.forEach((v, i) => {
        if (isMissing(v)) {
          open = false;
          return;
        }
        d += `${open ? "L" : "M"}${xs(i)},${ys(v)}`;
        open = true;
      });
      if (d) root.append(svg("path", { class: `mark-line ${cls}`, d }));
      s.values.forEach((v, i) => {
        const before = i > 0 && !isMissing(s.values[i - 1]);
        const after = i < s.values.length - 1 && !isMissing(s.values[i + 1]);
        if (!isMissing(v) && !before && !after) {
          root.append(svg("circle", { class: `mark-dot ${cls}`, cx: xs(i), cy: ys(v), r: 4 }));
        }
      });
    });

    const cross = svg("line", { class: "crosshair", y1: m.top, y2: m.top + ph, visibility: "hidden" });
    const hover = svg("g", { "aria-hidden": "true" });
    root.append(cross, hover);
    const hit = svg("rect", {
      class: "hit",
      x: m.left,
      y: m.top,
      width: pw,
      height: ph,
      tabindex: 0,
      "aria-label": `${spec.title}: use the arrow keys to read values; the table below lists them all`,
    });
    root.append(hit);
    let current = x.length - 1;

    function focusIndex(i, point) {
      current = i;
      const cx = xs(i);
      cross.setAttribute("x1", cx);
      cross.setAttribute("x2", cx);
      cross.setAttribute("visibility", "visible");
      hover.replaceChildren();
      const rows = series.map((s, j) => {
        const v = s.values[i];
        if (!isMissing(v)) hover.append(svg("circle", { class: `mark-dot s${j + 1}`, cx, cy: ys(v), r: 4 }));
        return { value: show(v, kind), name: s.name, cls: `s${j + 1}`, shape: "line" };
      });
      showTip(point, xLabel(x[i]) + (longRange ? "" : ` (${x[i]})`), rows);
    }

    function clear() {
      cross.setAttribute("visibility", "hidden");
      hover.replaceChildren();
      hideTip();
    }

    function nearest(evt) {
      const box = root.getBoundingClientRect();
      const px = ((evt.clientX - box.left) * width) / box.width;
      let best = 0;
      for (let i = 1; i < x.length; i += 1) if (Math.abs(xs(i) - px) < Math.abs(xs(best) - px)) best = i;
      return best;
    }

    function pointFor(i) {
      const box = root.getBoundingClientRect();
      return { x: box.left + (xs(i) * box.width) / width, y: box.top + (m.top * box.height) / height };
    }

    hit.addEventListener("pointermove", (e) => focusIndex(nearest(e), pointerAt(e)));
    hit.addEventListener("pointerleave", clear);
    hit.addEventListener("focus", () => focusIndex(current, pointFor(current)));
    hit.addEventListener("blur", clear);
    hit.addEventListener("keydown", (e) => {
      if (e.key !== "ArrowLeft" && e.key !== "ArrowRight") return;
      e.preventDefault();
      const next = Math.min(x.length - 1, Math.max(0, current + (e.key === "ArrowRight" ? 1 : -1)));
      focusIndex(next, pointFor(next));
    });
    host.append(root);
    if (spec.note) host.append(el("p", { class: "chart-note" }, spec.note));
  }

  // ------------------------------------------------------------ table

  // cols: [{key, label, kind, get?}]
  function renderTable(host, cols, rows, caption) {
    host.replaceChildren();
    const table = el("table");
    if (caption) table.append(el("caption", null, caption));
    const head = el("tr");
    for (const c of cols) head.append(el("th", { scope: "col", class: NUMERIC.has(c.kind) || c.numeric ? "num" : "" }, c.label));
    const thead = el("thead");
    thead.append(head);
    table.append(thead);
    const body = el("tbody");
    table.append(body);
    let shown = Math.min(rows.length, TABLE_LIMIT);
    const fill = () => {
      body.replaceChildren();
      for (const r of rows.slice(0, shown)) {
        const tr = el("tr");
        for (const c of cols) {
          const v = c.get ? c.get(r) : r[c.key];
          const classes = [];
          if (NUMERIC.has(c.kind) || c.numeric) classes.push("num");
          if (isMissing(v)) classes.push("null");
          tr.append(el("td", classes.length ? { class: classes.join(" ") } : null, show(v, c.kind)));
        }
        body.append(tr);
      }
    };
    fill();
    host.append(table);
    host.setAttribute("tabindex", "0"); // the scroll box is reachable by keyboard
    if (rows.length > shown) {
      const more = el("button", { class: "more", type: "button" }, `Show all ${fmtInt.format(rows.length)} rows`);
      more.addEventListener("click", () => {
        shown = rows.length;
        fill();
        more.remove();
      });
      host.append(more);
    }
  }

  function martColumns(doc) {
    return doc.columns.map((c) => ({ key: c, label: c, kind: kindOf(c) }));
  }

  // ------------------------------------------------------------ sections

  function byDesc(get) {
    return (a, b) => get(b) - get(a);
  }

  function num(v) {
    return isMissing(v) ? -Infinity : v;
  }

  function q1(doc) {
    const rows = doc.rows.filter((r) => r.period === "day" && r.scope === "category");
    if (!rows.length) {
      return {
        empty: "No daily rows per category yet.",
        table: (host) => renderTable(host, martColumns(doc), doc.rows),
      };
    }
    const days = [...new Set(rows.map((r) => r.period_start))].sort();
    const totals = new Map();
    for (const r of rows) totals.set(r.scope_key, (totals.get(r.scope_key) || 0) + r.active_contributors);
    let names = [...totals.keys()].sort();
    let note = "";
    if (names.length > MAX_SERIES) {
      const keep = new Set([...names].sort((a, b) => totals.get(b) - totals.get(a)).slice(0, MAX_SERIES));
      names = names.filter((n) => keep.has(n));
      note = `Chart shows the ${MAX_SERIES} categories with the most activity; the table lists all.`;
    }
    const index = new Map(rows.map((r) => [`${r.scope_key}|${r.period_start}`, r.active_contributors]));
    // A missing (category, day) pair means no active contributor that day.
    const value = (name, day) => index.get(`${name}|${day}`) ?? 0;
    const all = [...totals.keys()].sort();
    return {
      chart: (host) =>
        lineChart(host, {
          title: "Daily active contributors per category",
          x: days,
          series: names.map((name) => ({ name, values: days.map((d) => value(name, d)) })),
          kind: "int",
          note,
        }),
      table: (host) =>
        renderTable(
          host,
          [{ key: "day", label: "day", kind: "text" }, ...all.map((name) => ({ key: name, label: name, kind: "int" }))],
          days.map((d) => Object.fromEntries([["day", d], ...all.map((n) => [n, value(n, d)])])),
          "Active contributors per day and category (scope = category, period = day).",
        ),
    };
  }

  function q2(doc) {
    const rows = doc.rows;
    return {
      chart: (host) =>
        lineChart(host, {
          title: "Cohort retention at W+4 and W+12",
          x: rows.map((r) => r.cohort_week),
          series: [
            { name: "Retained at W+4", values: rows.map((r) => r.retention_w4_pct) },
            { name: "Retained at W+12", values: rows.map((r) => r.retention_w12_pct) },
          ],
          kind: "pct",
        }),
      table: (host) => renderTable(host, martColumns(doc), rows),
    };
  }

  function topBy(rows, get, n) {
    return [...rows].sort((a, b) => byDesc(get)(a, b) || String(a.repo_name).localeCompare(String(b.repo_name))).slice(0, n);
  }

  function q3(doc) {
    const top = topBy(doc.rows, (r) => num(r.prs_opened), TOP_N);
    return {
      chart: (host) =>
        barChart(host, {
          title: "Pull requests opened and merged per repo",
          rows: top.map((r) => ({ label: r.repo_name, values: [r.prs_opened, r.prs_merged] })),
          series: [{ name: "PRs opened" }, { name: "PRs merged" }],
          kind: "int",
          note: doc.rows.length > top.length ? `Top ${top.length} of ${doc.rows.length} repos by PRs opened.` : "",
        }),
      table: (host) => renderTable(host, martColumns(doc), doc.rows),
    };
  }

  function q4(doc) {
    const repos = new Map();
    for (const r of doc.rows) {
      const entry = repos.get(r.repo_name) || { repo_name: r.repo_name, opened: 0, issue: null, pull_request: null };
      entry.opened += isMissing(r.opened) ? 0 : r.opened;
      entry[r.item_type] = r.response_hours_p50;
      repos.set(r.repo_name, entry);
    }
    const top = topBy([...repos.values()], (r) => r.opened, TOP_N);
    return {
      chart: (host) =>
        barChart(host, {
          title: "Median hours to first response per repo",
          rows: top.map((r) => ({ label: r.repo_name, values: [r.issue, r.pull_request] })),
          series: [{ name: "Issues" }, { name: "Pull requests" }],
          kind: "hours",
          note: repos.size > top.length ? `Top ${top.length} of ${repos.size} repos by opened items.` : "",
        }),
      table: (host) => renderTable(host, martColumns(doc), doc.rows),
    };
  }

  function q5(doc) {
    const top = topBy(doc.rows, (r) => num(r.contributions), TOP_N).sort(
      (a, b) => num(b.top1_share_pct) - num(a.top1_share_pct) || String(a.repo_name).localeCompare(String(b.repo_name)),
    );
    return {
      chart: (host) =>
        barChart(host, {
          title: "Share of human contributions by the top contributor",
          rows: top.map((r) => ({ label: r.repo_name, values: [r.top1_share_pct] })),
          series: [{ name: "Top contributor share" }],
          kind: "pct",
          note: doc.rows.length > top.length ? `${top.length} of ${doc.rows.length} repos (most contributions), highest share first.` : "",
        }),
      table: (host) => renderTable(host, martColumns(doc), doc.rows),
    };
  }

  function q6(doc) {
    const top = doc.rows.slice(0, TOP_N);
    return {
      chart: (host) =>
        barChart(host, {
          title: "Repo health score",
          rows: top.map((r) => ({ label: `${r.health_rank}. ${r.repo_name}`, values: [r.health_score] })),
          series: [{ name: "Health score" }],
          kind: "score",
          note: doc.rows.length > top.length ? `Top ${top.length} of ${doc.rows.length} repos.` : "",
        }),
      table: (host) => renderTable(host, martColumns(doc), doc.rows),
    };
  }

  function runs(doc) {
    const rows = doc.rows;
    const value = (r) => (r.total_seconds === null ? null : Number(r.total_seconds));
    return {
      chart: (host) =>
        barChart(host, {
          title: "Total seconds per run",
          rows: rows.map((r) => ({ label: `${r.variant} · ${r.data_date}`, values: [value(r)] })),
          series: [{ name: "total_seconds" }],
          kind: "secs",
          note: "Bars are labelled variant · data date; compare only rows with the same data date.",
        }),
      // runs.csv values are shown exactly as written in the file.
      table: (host) =>
        renderTable(
          host,
          doc.columns.map((c) => ({ key: c, label: c, kind: "raw", numeric: RUNS_NUMERIC.test(c) })),
          rows,
        ),
    };
  }

  const SECTIONS = [
    { mart: "mart_daily_active_contributors", file: "data/mart_daily_active_contributors.json", build: q1 },
    { mart: "mart_cohort_retention", file: "data/mart_cohort_retention.json", build: q2 },
    { mart: "mart_contribution_funnel", file: "data/mart_contribution_funnel.json", build: q3 },
    { mart: "mart_responsiveness", file: "data/mart_responsiveness.json", build: q4 },
    { mart: "mart_repo_concentration", file: "data/mart_repo_concentration.json", build: q5 },
    { mart: "mart_repo_health_score", file: "data/mart_repo_health_score.json", build: q6 },
    { mart: "runs", file: "data/runs.json", build: runs },
  ];

  // ------------------------------------------------------------ loading

  async function loadJSON(path) {
    const res = await fetch(path, { cache: "no-cache" });
    if (!res.ok) throw new Error(`${path}: HTTP ${res.status}`);
    return res.json();
  }

  function setStatus(node, text, isError) {
    node.textContent = text;
    node.classList.toggle("error", Boolean(isError));
  }

  function renderMeta(meta) {
    document.getElementById("generated-at").textContent = formatStamp(meta.generated_at);
    document.getElementById("latest-data-date").textContent = meta.latest_data_date || "no data yet";
    document.getElementById("git-sha").textContent = meta.git_sha ? meta.git_sha.slice(0, 7) : "–";
    const simulated = Array.isArray(meta.simulated_sections) ? meta.simulated_sections : [];
    if (simulated.length) {
      setStatus(document.getElementById("page-status"), `Contains simulated data in: ${simulated.join(", ")}.`, false);
    }
  }

  function watchWidth(host, draw) {
    let last = host.clientWidth;
    let pending = false;
    if (typeof ResizeObserver !== "function") return;
    new ResizeObserver(() => {
      const w = host.clientWidth;
      if (Math.abs(w - last) < 2 || pending) return;
      last = w;
      pending = true;
      requestAnimationFrame(() => {
        pending = false;
        draw(host);
      });
    }).observe(host);
  }

  async function renderSection(section) {
    const node = document.querySelector(`section[data-mart="${section.mart}"]`);
    if (!node) return;
    const status = node.querySelector('[data-role="status"]');
    const chart = node.querySelector('[data-role="chart"]');
    const table = node.querySelector('[data-role="table"]');
    setStatus(status, "Loading…", false);
    let doc;
    try {
      doc = await loadJSON(section.file);
    } catch (err) {
      setStatus(status, `Could not load ${section.file} (${err.message}). The rest of the page still works.`, true);
      return;
    }
    if (!doc || !Array.isArray(doc.rows) || !Array.isArray(doc.columns)) {
      setStatus(status, `${section.file} is not in the expected format.`, true);
      return;
    }
    if (!doc.rows.length) {
      setStatus(status, "No rows yet: this table is empty in the latest build.", false);
      return;
    }
    let view;
    try {
      view = section.build(doc);
      if (view.empty) {
        setStatus(status, view.empty, false);
      } else {
        setStatus(status, "", false);
        view.chart(chart);
        watchWidth(chart, view.chart);
      }
      view.table(table);
    } catch (err) {
      setStatus(status, `Could not draw this section (${err.message}).`, true);
    }
  }

  function boot() {
    loadJSON("data/meta.json")
      .then(renderMeta)
      .catch((err) => {
        document.getElementById("generated-at").textContent = "unknown";
        document.getElementById("latest-data-date").textContent = "unknown";
        setStatus(document.getElementById("page-status"), `Could not load data/meta.json (${err.message}).`, true);
      });
    for (const section of SECTIONS) renderSection(section);
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", boot);
  else boot();
})();
