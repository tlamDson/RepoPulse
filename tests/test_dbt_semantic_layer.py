"""dbt semantic layer (MetricFlow) of the Q1-Q6 marts of schema ``public`` (docs/semantic-layer.md).

Two layers of checks:

- ``dbt parse`` writes ``semantic_manifest.json`` with one semantic model per public mart (Q1-Q6),
  each pointing at a model of schema ``public`` only, a metric for every numeric mart column,
  descriptions everywhere and a day time spine; no deprecation warning from the installed dbt.
- For each Q: the metrics of that Q, grouped by the full grain of its mart, are compiled to SQL by
  MetricFlow (``repopulse.semantic``, DuckDB dialect), run on the DuckDB built from the fixture
  (session fixture ``built``) and compared with ``benchmarks/queries/qN.sql`` on the same raw lines
  (session fixture ``baseline``), row by row and column by column.

MetricFlow comes with dbt-core (>= 1.12 depends on ``metricflow``). Locally the module skips when
it is missing; on CI (``CI`` set) a missing MetricFlow is an import error, never a skip.
"""

from __future__ import annotations

import importlib
import json
import os
from datetime import date

import duckdb
import pytest

pytest.importorskip("dbt.cli.main", reason="dbt extra not installed: pip install -e '.[dbt]'")
pytest.importorskip("dbt.adapters.duckdb", reason="dbt-duckdb not installed")
if not os.environ.get("CI"):
    pytest.importorskip("metricflow", reason="metricflow ships with dbt-core >= 1.12")

from dbt_helpers import (  # noqa: E402
    MARTS,
    PROJECT,
    PUBLIC_SCHEMA,
    output,
    require_ok,
    run_dbt,
    write_profiles,
)

# q -> (semantic model, primary entity, {mart grain column: MetricFlow group-by name})
SEMANTIC = {
    "q1": (
        "q1_daily_active_contributors",
        "q1_activity",
        {
            "period": "q1_activity__period",
            "period_start": "q1_activity__period_start__day",
            "scope": "q1_activity__scope",
            "scope_key": "q1_activity__scope_key",
        },
    ),
    "q2": (
        "q2_cohort_retention",
        "q2_cohort",
        {"cohort_week": "q2_cohort__cohort_week__week"},
    ),
    "q3": (
        "q3_contribution_funnel",
        "q3_funnel",
        {"repo_name": "q3_funnel__repo_name", "category": "q3_funnel__category"},
    ),
    "q4": (
        "q4_responsiveness",
        "q4_responsiveness",
        {"repo_name": "q4_responsiveness__repo_name", "item_type": "q4_responsiveness__item_type"},
    ),
    "q5": (
        "q5_repo_concentration",
        "q5_concentration",
        {"repo_name": "q5_concentration__repo_name", "category": "q5_concentration__category"},
    ),
    "q6": (
        "q6_repo_health_score",
        "q6_health",
        {"repo_name": "q6_health__repo_name", "category": "q6_health__category"},
    ),
}
# Ratios rebuilt from additive measures (same rounding as the Q query).
DERIVED_RATIOS = {"retention_w4_pct", "retention_w12_pct", "close_rate_pct", "bot_share_pct"}
# Only valid at the grain of their mart: distinct counts, percentiles, top-N shares, scores, rank.
NON_ADDITIVE = {
    "active_contributors",
    "new_contributors",
    "returning_contributors",
    "merge_hours_p50",
    "merge_hours_p90",
    "response_hours_p50",
    "response_hours_p90",
    "top1_share_pct",
    "top2_share_pct",
    "top3_share_pct",
    "health_rank",
    "activity_score",
    "retention_score",
    "merge_score",
    "response_score",
    "close_score",
    "diversity_score",
    "health_score",
}
NUMERIC_TYPES = ("BIGINT", "INTEGER", "HUGEINT", "DOUBLE", "FLOAT", "DECIMAL")


def metric_columns(q: str, baseline: dict) -> list[str]:
    """Mart columns of Q that are not grain keys (= the metrics of Q, same names)."""
    grain = SEMANTIC[q][2]
    return [c for c in baseline[q]["columns"] if c not in grain]


# ---------------------------------------------------------------- fixtures


@pytest.fixture(scope="module")
def parsed(tmp_path_factory) -> dict:
    """``dbt parse`` of transform/ with target/logs/profiles in tmp."""
    base = tmp_path_factory.mktemp("dbt_semantic_parse")
    profiles = write_profiles(base / "profiles", base / "parse.duckdb")
    proc = run_dbt(["parse"], PROJECT, profiles, base)
    target = base / "target"
    return {
        "proc": proc,
        "semantic_manifest": target / "semantic_manifest.json",
        "manifest": target / "manifest.json",
    }


@pytest.fixture(scope="module")
def sm(parsed) -> dict:
    """semantic_manifest.json of ``parsed`` (empty when missing: the parse test reports that)."""
    path = parsed["semantic_manifest"]
    if parsed["proc"].returncode != 0 or not path.is_file():
        return {"semantic_models": [], "metrics": [], "project_configuration": {"time_spines": []}}
    return json.loads(path.read_text(encoding="utf-8"))


@pytest.fixture
def gold(built):
    require_ok(built["proc"])
    con = duckdb.connect(str(built["db"]), read_only=True)
    yield con
    con.close()


def _semantic():
    """repopulse.semantic, imported in the test body so a missing module fails the test."""
    return importlib.import_module("repopulse.semantic")


def _built_semantic_manifest(built):
    require_ok(built["proc"])
    return built["base"] / "target" / "semantic_manifest.json"


def _metrics(sm: dict) -> dict[str, dict]:
    return {m["name"]: m for m in sm["metrics"]}


def _models(sm: dict) -> dict[str, dict]:
    return {m["name"]: m for m in sm["semantic_models"]}


# ---------------------------------------------------------------- dbt parse: semantic manifest


def test_dbt_parse_writes_semantic_manifest_without_deprecation(parsed, sm):
    require_ok(parsed["proc"])
    out = output(parsed["proc"])
    assert "deprecat" not in out.lower(), out[-4000:]
    assert parsed["semantic_manifest"].is_file(), "dbt parse wrote no semantic_manifest.json"
    assert sm["semantic_models"], "semantic manifest has no semantic model"
    assert sm["metrics"], "semantic manifest has no metric"


def test_one_semantic_model_per_public_mart(sm):
    models = _models(sm)
    assert set(models) == {name for name, _, _ in SEMANTIC.values()}
    for q, (name, entity, _) in SEMANTIC.items():
        relation = models[name]["node_relation"]
        assert relation["alias"] == MARTS[q][0], (name, relation)
        assert relation["schema_name"] == PUBLIC_SCHEMA, (name, relation)
        assert models[name]["primary_entity"] == entity, name


def test_semantic_models_ref_public_models_only(parsed, sm):
    manifest = json.loads(parsed["manifest"].read_text(encoding="utf-8"))
    nodes = manifest["nodes"]
    semantic_models = manifest["semantic_models"].values()
    assert {m["name"] for m in semantic_models} == {name for name, _, _ in SEMANTIC.values()}
    for model in semantic_models:
        deps = model["depends_on"]["nodes"]
        assert len(deps) == 1, (model["name"], deps)
        node = nodes[deps[0]]
        assert node["config"]["schema"] == "public", (model["name"], deps[0])
        assert node["original_file_path"].replace("\\", "/").startswith("models/public/pub_mart_")


def test_every_numeric_mart_column_has_a_metric(sm, gold, baseline):
    metrics = _metrics(sm)
    for q, (mart, _) in MARTS.items():
        types = dict(
            gold.execute(
                "SELECT column_name, data_type FROM information_schema.columns "
                "WHERE table_schema = ? AND table_name = ?",
                [PUBLIC_SCHEMA, mart],
            ).fetchall()
        )
        grain = SEMANTIC[q][2]
        numeric = {
            c for c, t in types.items() if t.upper().startswith(NUMERIC_TYPES) and c not in grain
        }
        assert numeric, (q, types)
        assert numeric == set(metric_columns(q, baseline)), q
        missing = sorted(numeric - set(metrics))
        assert missing == [], f"{q} ({mart}): numeric columns without a metric: {missing}"


def test_metrics_and_semantic_models_have_descriptions(sm):
    for kind in ("semantic_models", "metrics"):
        assert sm[kind], f"no {kind}: nothing to check"
        for item in sm[kind]:
            assert (item.get("description") or "").strip(), (kind, item["name"])


def test_ratio_metrics_are_derived_from_additive_metrics(sm):
    metrics = _metrics(sm)
    for name in sorted(DERIVED_RATIOS):
        metric = metrics[name]
        assert metric["type"] == "derived", name
        inputs = {m["name"] for m in metric["type_params"]["metrics"]}
        assert inputs and all(metrics[i]["type"] == "simple" for i in inputs), (name, inputs)
        assert not inputs & NON_ADDITIVE, (name, inputs)


def test_non_additive_metrics_say_so_in_description(sm):
    metrics = _metrics(sm)
    for name in sorted(NON_ADDITIVE):
        assert "not additive" in metrics[name]["description"].lower(), name
    for name in sorted(set(metrics) - NON_ADDITIVE):
        assert "not additive" not in metrics[name]["description"].lower(), name


def test_time_spine_is_a_public_day_spine(sm, gold):
    spines = sm["project_configuration"]["time_spines"]
    assert len(spines) == 1, spines
    spine = spines[0]
    assert spine["primary_column"]["time_granularity"] == "day"
    assert spine["node_relation"]["schema_name"] == PUBLIC_SCHEMA
    table, column = spine["node_relation"]["alias"], spine["primary_column"]["name"]
    lo, hi, n, distinct = gold.execute(
        f'SELECT min("{column}"), max("{column}"), count(*), count(DISTINCT "{column}") '
        f'FROM {PUBLIC_SCHEMA}."{table}"'
    ).fetchone()
    assert n == distinct == (hi - lo).days + 1  # one row per day, no gap
    q1, q2 = MARTS["q1"][0], MARTS["q2"][0]
    used = gold.execute(
        f"SELECT min(d), max(d) FROM (SELECT period_start AS d FROM {PUBLIC_SCHEMA}.{q1} "
        f"UNION ALL SELECT cohort_week FROM {PUBLIC_SCHEMA}.{q2})"
    ).fetchone()
    assert lo <= used[0] and used[1] <= hi, (lo, hi, used)
    assert lo <= date(2026, 1, 1) and date(2026, 12, 31) <= hi


# ---------------------------------------------------------------- metrics == Q1-Q6 on the fixture


@pytest.mark.parametrize("q", sorted(SEMANTIC))
def test_metrics_match_baseline_query(q, built, gold, baseline):
    semantic = _semantic()
    manifest = semantic.load_manifest(_built_semantic_manifest(built))
    grain = SEMANTIC[q][2]
    metrics = metric_columns(q, baseline)
    sql = semantic.compile_sql(manifest, metrics, list(grain.values()))
    cur = gold.execute(sql)
    names = [c[0] for c in cur.description]
    rows = [dict(zip(names, r, strict=True)) for r in cur.fetchall()]

    cols = list(baseline[q]["columns"])
    expected = [dict(zip(cols, r, strict=True)) for r in baseline[q]["rows"]]
    assert expected, f"{q} baseline is empty on the fixture: comparison would be vacuous"
    assert len(rows) == len(expected), (rows, expected)

    def key(row: dict, mf: bool) -> tuple:
        return tuple(row[grain[c] if mf else c] for c in grain)

    actual = {key(r, True): r for r in rows}
    wanted = {key(r, False): r for r in expected}
    assert len(actual) == len(rows), "grain keys are not unique in the metric result"
    assert sorted(actual, key=repr) == sorted(wanted, key=repr)
    for k, e_row in wanted.items():
        a_row = actual[k]
        for m in metrics:
            a, e = a_row[m], e_row[m]
            if isinstance(e, float):
                assert a == pytest.approx(e, abs=1e-9), (q, k, m, a, e)
            else:
                assert a == e, (q, k, m, a, e)


def test_ratio_metric_reaggregates_from_measures(built, gold):
    """close_rate_pct over all repos = 100 * sum(closed) / sum(opened), not an average of ratios."""
    semantic = _semantic()
    manifest = semantic.load_manifest(_built_semantic_manifest(built))
    sql = semantic.compile_sql(
        manifest, ["close_rate_pct", "opened"], ["q4_responsiveness__item_type"]
    )
    cur = gold.execute(sql)
    names = [c[0] for c in cur.description]
    got = {
        r["q4_responsiveness__item_type"]: r
        for r in (dict(zip(names, row, strict=True)) for row in cur.fetchall())
    }
    want = {
        item_type: (opened, rate)
        for item_type, opened, rate in gold.execute(
            "SELECT item_type, sum(opened), "
            "round(100 * CAST(sum(closed) AS DOUBLE) / sum(opened), 2) "
            f"FROM {PUBLIC_SCHEMA}.{MARTS['q4'][0]} GROUP BY item_type"
        ).fetchall()
    }
    assert want and set(got) == set(want)
    for item_type, (opened, rate) in want.items():
        assert got[item_type]["opened"] == opened
        assert got[item_type]["close_rate_pct"] == pytest.approx(rate, abs=1e-9)


# ---------------------------------------------------------------- CLI


def test_cli_sql_prints_runnable_metric_sql(built, gold, baseline, capsys):
    semantic = _semantic()
    path = _built_semantic_manifest(built)
    code = semantic.main(
        [
            "sql",
            "--manifest",
            str(path),
            "--metrics",
            "cohort_size,retention_w4_pct",
            "--group-by",
            "q2_cohort__cohort_week__week",
        ]
    )
    assert code == 0
    sql = capsys.readouterr().out
    rows = gold.execute(sql).fetchall()
    assert len(rows) == len(baseline["q2"]["rows"]) > 0


def test_cli_sql_unknown_metric_exits_2(built, capsys):
    semantic = _semantic()
    path = _built_semantic_manifest(built)
    code = semantic.main(["sql", "--manifest", str(path), "--metrics", "no_such_metric"])
    assert code == 2
    assert "no_such_metric" in capsys.readouterr().err
