# RepoPulse — Claude Code Project Instructions

RepoPulse: data platform nhỏ đo "sức khoẻ" các dự án open source hệ data (Spark, dbt, DuckDB, Airflow, Kafka...) từ dữ liệu sự kiện GitHub công khai (GH Archive), kèm một luồng event app mô phỏng theo logging spec. Stack: Python 3.11, PySpark + Delta Lake (Databricks Free Edition, hoặc chạy trên runner GitHub Actions nếu dùng đường dự phòng), dbt (DuckDB trên CI, Databricks SQL hoặc MotherDuck cho gold), GitHub Actions điều phối, dashboard tĩnh trên GitHub Pages.

**Đường đã chọn (bước thử 2026-11-11): dự phòng** — PySpark + Delta trên runner GitHub Actions, gold trên MotherDuck (khoá phiên bản DuckDB theo bản MotherDuck hỗ trợ). Databricks chưa kiểm được vì secret `DATABRICKS_TOKEN` không phải personal access token. Chi tiết và cách thử lại: `docs/probe-result.md`.

Kế hoạch: `docs/plan.md`. Tiến độ: `python scripts/orchestrate.py report` (nguồn sự thật là PR đã merge). Việc chờ chủ repo: `docs/lam-checklist.md`, `logs/run-<ngày>.md` và email báo cáo ngày (`logs/daily-<ngày>.md`; số đo vận hành ở `logs/orchestrator-metrics.csv`, không phải `runs.csv`).

Các rule chi tiết dưới đây áp dụng luôn, không chỉ khi được hỏi:

@.claude/rules/tech-defaults.md
@.claude/rules/workflow.md

## Quy tắc bắt buộc (không được vi phạm)

- **Git flow:** feature → `develop` (squash), release `develop` → `main` (merge commit). Không push thẳng lên `develop`/`main`, không force push, không `--admin`. `main` là nhánh mặc định (cron chỉ chạy trên đó).
- **Một PR một việc.** Mỗi PR feature/fix/docs chỉ làm đúng một việc: một task trong `scripts/prs.toml` (luôn tính là một việc, đúng như mô tả của nó), hoặc một cải tiến/sửa lỗi ngoài `prs.toml`. Việc khác gặp trong lúc làm không gộp vào: phiên tay mở PR riêng, phiên headless ghi vào Nợ kỹ thuật hoặc `## Giả định`. PR release và PR Dependabot không áp dụng. Chi tiết: `.claude/rules/developer-guide.md`.
- **Merge feature vào `develop` khi CI xanh cả 3 job** (`Lint`, `Unit Tests`, `dbt build (DuckDB)`) **và `code-reviewer` PASS**. Đủ thì merge ngay, không hỏi lại.
- **Release `develop` → `main`** ở các mốc `release_after` trong `scripts/prs.toml`, khi CI của PR release xanh.
- **PR sửa `.claude/settings.json`, `.claude/auto-mode.json` hoặc hook để chủ repo duyệt**, không tự merge.
- **Kiểu lệnh theo `.claude/rules/command-style.md`** — phiên headless từ chối ngay mọi lệnh lẽ ra phải hỏi.
- **TDD bắt buộc** (Red → Green → Refactor), phải thấy đỏ thật và chép output đỏ vào mục `## RED` của mô tả PR. Chi tiết: `.claude/rules/tdd.md`.
- **Số đo chỉ lấy từ `runs.csv`.** README, mô tả PR và báo cáo không được trích số nào không có trong `runs.csv`. Mọi tối ưu so với `variant=baseline` trên cùng ngày dữ liệu và bộ query Q1–Q6.
- **Dữ liệu mô phỏng phải ghi rõ là mô phỏng** ("simulated"). GH Archive là dữ liệu thật.
- **Không đọc, không in giá trị secret** (`DATABRICKS_*`, `MOTHERDUCK_TOKEN`, `SNOWFLAKE_*`, file `.env`). Chủ repo tự thêm secret.
- **Cập nhật `CLAUDE.md` + `.claude/rules/*`** khi task làm thay đổi convention, tooling hay quy trình.
- **Không tạo file `.md` mới ở thư mục gốc** — chỉ `README.md` và `CLAUDE.md`. Tài liệu khác để trong `docs/`.

## Lệnh hay dùng

Trên máy Windows của chủ repo, `pytest`, `ruff`, `dbt` không nằm trong PATH: luôn gọi dạng `python -m …` như dưới (CI Linux vẫn dùng lệnh trần trong `ci.yml`).

```bash
pip install -e ".[dev,dbt]"          # cài một lần
pre-commit install                   # hook ruff + gitleaks
python -m ruff check .
python -m ruff format --check .
python -m pytest
python -m dbt.cli.main build --project-dir transform --profiles-dir transform --target ci
python -m dbt.cli.main build --project-dir transform --profiles-dir transform --target ci --vars "{silver_gharchive_parquet: '<glob>/*.parquet'}"   # đọc silver parquet thay seed fixture (docs/dbt-models.md)
python -m pytest tests/test_dbt_marts.py         # mart Q1-Q6 so khớp benchmarks/queries/q*.sql, contract, PII
python -m repopulse.governance scan --db ci.duckdb --schema main_gold --schema main_public   # email thô ở gold/public? exit 0 sạch, 1 có finding (docs/governance.md)
python -m repopulse.governance check --manifest transform/target/manifest.json --policy transform/governance.yml --db ci.duckdb   # nhãn meta.pii + quyền theo vai
gh pr checks <số-PR> --watch --interval 30

python benchmarks/fetch_reference.py             # tải + kiểm sha256 ngày chuẩn (benchmarks/reference.toml)
python benchmarks/naive_duckdb.py --runs 5 --git-sha <sha> --out out/benchmark-row.csv
python benchmarks/record_run.py <benchmark-row.csv> [<pipeline-row.csv> ...]   # gộp dòng từ artifact vào runs.csv (một file hỏng thì không ghi gì)
python benchmarks/scale_prepare.py               # tải + lọc + rút gọn 30 ngày GH Archive cho benchmark scale (docs/benchmark-scale.md)
python benchmarks/scale_bench.py --git-sha <sha> --out out/scale-rows.csv   # Spark vs DuckDB 1/7/30 ngày; gộp bằng record_run.py
python benchmarks/snowflake_port.py --engine duckdb --run-results transform/target/run_results.json --git-sha <sha> --out out/snowflake-port-rows.csv   # dòng runs.csv cho một dbt build (port Snowflake, docs/snowflake-port.md); target snowflake chỉ chạy trong workflow snowflake-port.yml

python -m eventspec.generate                     # sinh lại eventspec/schemas/ từ catalog (docs/eventspec.md)
python -m simkit.generate --seed 42 --users 100 --days 7 --null 5 --bot 2   # event simulated + manifest lỗi vào data/simulated/ (docs/simkit.md)

python -m repopulse.ingest --date 2026-01-05 --hour 15 --hours 2   # ingest GH Archive vào data/landing/gharchive
python -m repopulse.ingest --summary out/ingest-summary.json      # giờ (now UTC - 2h); --force để làm lại
python -m repopulse.spark.job --source gharchive --date 2026-01-05 --hour 16 --hours 2   # bronze/silver Delta (cần PII_HASH_SALT, Java 17, extra spark); docs/spark-jobs.md
python -m repopulse.spark.export --lake-dir data/lake --out data/silver-export   # silver Delta -> parquet cho dbt (var silver_gharchive_parquet)
python -m repopulse.pipeline {window,timed,counts,row,md-reset} --help   # helper của .github/workflows/pipeline.yml (incremental batch theo giờ); docs/pipeline.md
python -m dbt.cli.main build --project-dir transform --profiles-dir transform --target ci --full-refresh   # dựng lại fct_events (staging giữ lịch sử); khi nào cần: docs/dbt-models.md
python -m repopulse.quality faults --seed 42 --users 30 --days 2 --clean-runs 2 --git-sha <sha> --report out/dq-faults.json --out out/dq-fault-row.csv   # 6 lỗi simkit (simulated) qua Spark; exit 1 khi sót lỗi/báo động giả (cần PII_HASH_SALT, Java 17); docs/data-quality.md
python -m repopulse.quality check --database ci.duckdb --report out/dq-report.json   # khối lượng + độ tươi của stg_gharchive_events (md:repopulse cần MOTHERDUCK_TOKEN); ngưỡng ở repopulse/quality.toml
python -m repopulse.quality gap --ingest --report out/dq-gap.json   # giờ GH Archive thiếu đã biết phải bị bắt (giờ khai ở [volume.reference])
python -m repopulse.quality alert --report out/dq-report.json --run-url <url> --dry-run   # Issue data-incident; bỏ --dry-run thì gọi gh (GH_TOKEN), một issue mở mỗi loại kiểm tra

python -m repopulse.export_site --db ci.duckdb --runs runs.csv --out _site   # 6 mart public + runs.csv -> trang + CSV Tableau; thiếu mart/cột thì exit 1 (docs/dashboard.md)
python -m http.server 8000 --bind 127.0.0.1 --directory _site                # xem thử trang local

python scripts/orchestrate.py run --dry-run      # xem sẽ chạy việc nào
python scripts/orchestrate.py run                # chạy headless, không cần người (mặc định 5 phiên)
python scripts/orchestrate.py report             # tiến độ theo PR đã merge
python scripts/orchestrate.py daily              # báo cáo ngày: logs/daily-<ngày>.md + email (tự gửi 22h khi đang chạy)
python scripts/orchestrate.py quota              # quota gần nhất đọc từ log
python scripts/orchestrate.py clean --dry-run    # worktree nào sẽ bị dọn
```

Dừng orchestrator: tạo file `.claude/state/STOP` (việc đang chạy làm nốt rồi dừng).

## Vai và model

| Vai | Model / effort | Làm gì |
|---|---|---|
| `scripts/orchestrate.py` | (Python, không phải agent) | Đọc DAG `scripts/prs.toml`, kiểm cổng (secret, harness), tạo worktree, chạy tối đa 5 phiên `claude -p` song song (dừng mở việc khi CI `develop` đỏ), chờ quota reset rồi `--resume`, dừng phiên treo, thử lại kèm lý do lần trước, chạy phiên review độc lập rồi tự merge khi PASS, release, dọn worktree, viết báo cáo |
| Phiên PR (headless) | Fable, high | Một phiên mỗi PR, trong worktree của PR: kế hoạch ngắn có mục test phải đỏ, giao agent chuyên trách, chờ CI xanh, kết thúc bằng `RESULT: ready <số PR>`. Không tự review, không merge |
| Phiên review (headless) | Opus, medium | `claude -p --agent code-reviewer` mới cho mỗi lần review, không thấy ngữ cảnh phiên PR; đăng comment, kết thúc bằng `REVIEW: PASS` hoặc `REVIEW: FAIL <finding>` |
| Agent chuyên trách | Opus, medium | `pipeline-ops`, `event-schema-engineer`, `spark-engineer`, `analytics-engineer`, `data-quality-governance`, `dashboard-builder` — xem `.claude/agents/README.md` |
| `code-reviewer` | Opus, medium | Review có bằng chứng: chạy lại test, commit đầu chỉ có test, mutation, PASS/FAIL |
| `release-manager`, `qa-testing`, `researcher` | Sonnet, high | Release khi script thất bại, README kết quả; test bổ sung; đọc code |

## Cổng cần chủ repo

- Thêm secret repo (Databricks và/hoặc MotherDuck) — thiếu thì orchestrator làm hết việc không cần cloud rồi dừng ở `probe`.
- Duyệt `docs/harness-check.md` (dòng `Lam đã duyệt`) trước PR B, hoặc chạy với `--auto-approve-harness`.
- Duyệt và merge PR sửa `.claude/settings.json` / `.claude/auto-mode.json` / hook.
- Đăng ký Snowflake trial khi muốn làm PR 13 (đồng hồ 30 ngày chạy ngay); thiếu thì PR 13 bị bỏ qua.
- Publish dashboard lên Tableau Public.
