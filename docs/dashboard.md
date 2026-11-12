# Dashboard: static GitHub Pages site + CSVs for Tableau

The public site `https://tlamdson.github.io/RepoPulse/` shows the 6 marts Q1–Q6 and `runs.csv`. The site
is plain HTML + CSS + JS in `site/` (no build tool, no npm, no CDN, no external fonts/analytics).
The data is GH Archive (real data); the site currently has **no** simulated data; any part that later
comes from the simulated app event stream must be labeled "simulated" (and recorded in
`meta.json.simulated_sections`).

| Component | File |
|---|---|
| Exporter (CLI) | `repopulse/export_site.py` (`python -m repopulse.export_site`) |
| Site | `site/index.html`, `site/style.css`, `site/app.js` |
| Workflow | `.github/workflows/pages.yml` |
| Tests | `tests/test_export_site.py`, `tests/test_site_static.py`, `tests/test_pages_workflow.py` |

## What the site reads

It only reads the **public** schema (`main_public`, the `public` role in `transform/governance.yml`,
`docs/governance.md`), never reads `main_gold`, and only reads the columns of the contract below (extra columns
in the table are not exported).

`python -m repopulse.export_site --db <db> --runs runs.csv --out <dir>` writes `<dir>/`:

| File | Content |
|---|---|
| `index.html`, `style.css`, `app.js` | copies of `site/` |
| `data/<mart>.json` | `{mart, query, columns, rows}`; rows sorted by the mart's key, NaN → `null`, ISO dates |
| `data/runs.json` | `{columns, rows}`; values keep the strings of `runs.csv`, empty cells → `null` |
| `data/meta.json` | `generated_at` (UTC), `git_sha`, `schema`, `source` (GH Archive, `real_data: true`), `simulated_sections` (empty), `latest_data_date` (the largest date of Q1 `period = 'day'`), row count of each mart |
| `tableau/<mart>.csv`, `tableau/runs.csv` | CSVs for Tableau Public; `runs.csv` is copied byte for byte |

Every number on the page comes from these files; the HTML contains no hand-written numbers (tested). JS builds the DOM with
`textContent`/`createElementNS`, never `innerHTML` (repo names are external data).

Each Q1–Q6 section has an SVG chart + a number table + a CSV link:

| Section | Chart |
|---|---|
| Q1 | line: active contributors per day by category (table = same values; per-repo/week rows are in the CSV) |
| Q2 | line: W+4 and W+12 retention rate by cohort |
| Q3 | grouped horizontal bars: PRs opened / PRs merged, the repos with the most opened PRs |
| Q4 | grouped horizontal bars: first response hours (median) for issues / PRs |
| Q5 | horizontal bars: share of the top contributor (bus factor) |
| Q6 | horizontal bars: health score, top of the ranking |
| Runs | horizontal bars of `total_seconds` for each `runs.csv` row (label variant · data_date; only compare rows of the same data day) |

Charts follow the dataviz skill: categorical colors in a fixed slot order (light/dark via
`prefers-color-scheme`), a legend when there are ≥ 2 series, text uses the text color, tooltips on hover/focus, and the number
table is always present. Charts redraw to the frame width (`viewBox` = the real width), narrow screens put the repo
name above the bar; wide tables scroll horizontally inside their own frame, the page does not scroll horizontally. A mart with 0 rows
or a broken file: that section shows a message, the other sections still work.

## Column contract (the build fails on mismatch)

`export_site.MART_COLUMNS` = the columns of the 6 marts in `transform/models/public/_public.yml` (a test keeps the two
in sync). The build **fails** (exit 1, with a message naming the problem) when:

- the schema does not exist;
- a mart is missing (`missing mart(s) in schema main_public: <mart>`);
- a mart lacks a column (`missing column(s): <mart>.<column>`);
- `runs.csv` is missing or its header differs from `repopulse.runs_log.COLUMNS`;
- the local database file does not exist (no empty file is created), `--generated-at` lacks a time zone, or `--git-sha` is wrong.

Everything is read and checked before writing; the output is built in a temporary directory and then replaces `--out` in one step. On an
error `--out` stays as it was. If `--out` already has files that are not an earlier export (no
`data/meta.json`), it refuses to overwrite.

To add a column to a public mart: edit `_public.yml` and `MART_COLUMNS` together, then `app.js` if you want to draw it.

## Local build and preview

```bash
python -m dbt.cli.main build --project-dir transform --profiles-dir transform --target ci
python -m repopulse.export_site --db ci.duckdb --runs runs.csv --out _site
python -m http.server 8000 --bind 127.0.0.1 --directory _site
```

Open `http://127.0.0.1:8000/`. It must go through `http.server` (the site uses `fetch`; opening `file://` is blocked
by the browser). `_site/` is gitignored. Other parameters: `--schema` (default `main_public`), `--site-dir`
(default `site/`), `--generated-at <ISO with time zone>` (used by tests for deterministic output), `--git-sha`.

A local database is opened read-only. With `--db md:repopulse`, DuckDB reads `MOTHERDUCK_TOKEN` from the
environment itself; the exporter does not read or print the token.

## Deploy workflow (`.github/workflows/pages.yml`)

| Trigger | Job | What it does |
|---|---|---|
| `workflow_run` of `Pipeline (scheduled)`, `completed`, branch `main` | `pages-build` → `pages-deploy` | Only when the pipeline run is `success`, is not a `pull_request` (a PR's smoke does not deploy), and is on `main`. Exports from `md:repopulse` (schema `main_public`), `upload-pages-artifact`, then `configure-pages` + `deploy-pages` |
| `workflow_dispatch` (on `main`) | same as above | Manual rebuild |
| `pull_request` (filtered by `paths:` `site/**`, exporter, workflow, `transform/models/public/**`) | `pages-preview` | `dbt build --target ci` on the fixture seed, export from `ci.duckdb`, upload `_site/` as a regular artifact `pages-preview-<PR number>`. No deploy, no secrets |

- `workflow_run` only runs when the file is on `main` (after release), and GitHub runs the version on `main`:
  checkout takes the default branch, not the code of the triggering run.
- Secret: only `MOTHERDUCK_TOKEN` (reads the `repopulse` database), only in the `env:` of the secret-check step and the
  export step; if missing the job stops with a clear message.
- Permissions: default `contents: read`; only `pages-deploy` has `pages: write` + `id-token: write`, environment
  `github-pages`. Deploys queue in the `pages-deploy` group and do not cancel the running one.
- DuckDB is pinned to the same version as `pipeline.yml` (MotherDuck rejects newer clients).
- Paths in the site are all relative (Pages serves under `/RepoPulse/`).

The `runs.csv` on the site is the version committed on `main`: pipeline rows sit in the artifact and only reach the
site after being merged with `benchmarks/record_run.py` and merged into the repo (`docs/pipeline.md`).

## Tableau Public (done manually by the repo owner)

1. Download the CSVs from the site (the "Download CSV (Tableau)" link in each section) or from the `pages-preview-*` artifact / a local
   build in `_site/tableau/`.
2. Tableau Public → Connect → Text file, one source per mart.
3. State the source as GH Archive (real data) with the latest data date (`data/meta.json`).
4. Publish to Tableau Public, and add the link to the README if you like.

This document contains no measurements; measurements come only from `runs.csv`.
