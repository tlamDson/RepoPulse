# Tech defaults — RepoPulse

> Stack, layout, luật dữ liệu, cạm bẫy và nợ kỹ thuật. Kế hoạch và lý do chọn stack: `docs/plan.md`.

## Stack

| Lớp | Công nghệ | Ghi chú |
|---|---|---|
| Ngôn ngữ | Python 3.11 | `pyproject.toml`; lint/format bằng `ruff` (line-length 100) |
| Test | `pytest` | `tests/`, xem `.claude/rules/tdd.md` |
| Điều phối | GitHub Actions | Cron chỉ chạy trên `main`, có thể trễ, tự tắt sau 60 ngày repo không hoạt động |
| Điều phối (phương án đo cạnh cron) | Airflow 3.4.0 (`airflow standalone` trên runner, venv riêng) | `dags/`, spec thuần `repopulse/dagspec.py`; chỉ chạy trên Linux (`.github/workflows/airflow.yml`), không có trong extras của `pyproject.toml`; `docs/airflow.md` |
| Bronze/Silver | PySpark + Delta Lake | Databricks Free Edition (serverless, chỉ batch `availableNow`). Dự phòng: chạy trên runner Actions (4 CPU, 16 GB RAM, 14 GB đĩa) |
| Streaming (giai đoạn 2) | Apache Kafka (`apache/kafka`, KRaft, 3 broker, docker compose) + Spark Structured Streaming + Delta | Chỉ trong workflow `streaming.yml` trên runner; client `confluent-kafka` (extra `streaming`, import lười); trigger `availableNow`; `docs/streaming.md` |
| Gold | dbt | `transform/`; CI chạy trên DuckDB (`--target ci`); gold thật trên Databricks SQL warehouse hoặc MotherDuck |
| Semantic layer | MetricFlow (đi kèm dbt-core >= 1.12) | `transform/models/public/_semantic.yml` (spec `semantic_models:` + `metrics:`) trên 6 mart public; `repopulse/semantic.py` sinh SQL DuckDB; `docs/semantic-layer.md` |
| Dashboard | Trang tĩnh trên GitHub Pages + Tableau Public (publish tay) | Databricks dashboard không chia sẻ công khai được |
| SQL agent (AI2) | `repopulse/sql_agent.py`: LLM qua API OpenAI-compatible (`urllib`, không SDK; local thường là `llama-server`), DuckDB chỉ đọc `main_public` | Chỉ file DuckDB local, không MotherDuck; guard dựa trên parser DuckDB (`json_serialize_sql`); CI chỉ dùng LLM giả lập; `docs/sql-agent.md` |
| Bảo mật | pre-commit (`ruff`, `gitleaks`), secret scanning + push protection, Dependabot (`pip`, `github-actions`) | |

Bước thử số 1 (`docs/plan.md`) quyết định đường chính (Databricks) hay đường dự phòng (runner + MotherDuck). Kết quả ghi ở `docs/progress.md`; cập nhật bảng trên khi đã chốt.

## Layout

```
repopulse/        package Python chính (runs.csv, orchestrator.py = lõi thuần của orchestrator)
eventspec/        catalog event YAML → JSON Schema, hàm kiểm event       (PR 2)
simkit/           sinh dữ liệu mô phỏng có seed, chèn lỗi biết trước     (PR 3)
benchmarks/       ngày dữ liệu chuẩn, pipeline ngây thơ, queries/q1..q6  (PR B)
tests/            pytest; fixtures/ chứa dữ liệu mẫu nhỏ
transform/        project dbt (dbt_project.yml, profiles.yml, models/, seeds/)
site/             dashboard tĩnh (HTML/CSS/JS thuần) cho GitHub Pages; export_site.py ghi data/ + tableau/ vào _site/ (PR 11)
scripts/          orchestrate.py (CLI headless), prs.toml (DAG việc → agent → cổng)
dags/             DAG Airflow (repopulse_pipeline.py, dựng từ repopulse/dagspec.py); không đặt tên thư mục là airflow/ (P2d)
.github/workflows CI (ci.yml), benchmark (PR B), pipeline theo lịch (PR 7), deploy Pages (PR 11), airflow backfill (P2d)
.claude/          rules, agents, hooks/log_permission.py, auto-mode.json; state/ và worktrees/ không commit
docs/             plan, progress, checklist, harness-check
logs/             log stream-json mỗi PR, báo cáo mỗi lần chạy (không commit)
runs.csv          sổ số đo, mỗi lần chạy một dòng
```

Cập nhật sơ đồ này khi thêm thư mục mới.

## Luật dữ liệu

- **Số đo chỉ lấy từ `runs.csv`.** Không trích số ước lượng, số "khoảng", hay số chạy tay không ghi lại.
- **Dữ liệu mô phỏng ghi rõ "simulated"** ở README, dashboard, mô tả PR. GH Archive là dữ liệu thật.
- **Băm PII trước silver.** Gold không bao giờ chứa email/giá trị PII thô; có test tự động kiểm điều này (PR 9).
- **Idempotent theo (ngày, giờ).** Chạy lại cùng giờ dữ liệu không sinh bản ghi trùng.
- **Event sai schema vào bảng quarantine**, không bị bỏ im lặng.
- Gọi đúng tên: giai đoạn 1 là "incremental batch", không gọi là real-time/streaming.

## Secret

`DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_HTTP_PATH`, `MOTHERDUCK_TOKEN`, `PII_HASH_SALT` (salt băm PII ở silver), và cho PR 13: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER` (`REPOPULSE_CI`, user SERVICE, đăng nhập bằng key-pair), `SNOWFLAKE_PRIVATE_KEY` (PKCS#8 PEM), `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE` (`REPOPULSE_WH`, XSMALL, auto-suspend 60s, resource monitor 20 credit), `SNOWFLAKE_DATABASE` (`REPOPULSE`) — chủ repo tự thêm bằng `gh secret set` hoặc trên web. Agent không đọc `.env`, không in giá trị secret, không chạy `gh secret` (lệnh này ở nhóm `ask`). Biến mới thêm vào `.env.example` với giá trị rỗng.

Biến môi trường local (không phải GitHub secret) cho email báo cáo ngày của orchestrator: `REPOPULSE_SMTP_USER`, `REPOPULSE_SMTP_APP_PASSWORD` (Gmail App password), `REPOPULSE_REPORT_TO` (tuỳ chọn). Chủ repo đặt bằng `setx`; script chỉ chuyển giá trị cho `smtplib`, không in. Agent không đọc, không in các biến này.

Biến môi trường local cho SQL agent (`python -m repopulse.sql_agent`, `docs/sql-agent.md`): `REPOPULSE_LLM_BASE_URL` (mặc định `http://127.0.0.1:8080/v1`), `REPOPULSE_LLM_MODEL`, `REPOPULSE_LLM_API_KEY` (tuỳ chọn). Module chỉ đọc từ môi trường, không đọc `.env`, không đưa key vào lỗi/log/report. Agent không đọc, không in `REPOPULSE_LLM_API_KEY`.

## Cạm bẫy đã biết

- **Databricks Free Edition**: tài liệu chính thức chưa xác nhận tạo token/điều khiển từ ngoài; truy cập internet theo danh sách domain (chưa biết `data.gharchive.org` có được phép); vượt quota thì compute tắt tới hết ngày; tối đa 5 job task đồng thời. Lịch chạy thưa, job idempotent để hôm sau chạy bù.
- **Đĩa runner 14 GB**: mỗi lần chạy chỉ xử lý vài giờ dữ liệu, lọc loại event trước khi lưu.
- **Snowflake trial 30 ngày**: chỉ đăng ký khi tới PR 13.
- **Windows**: mở phiên với `D:` viết hoa (xem `developer-guide.md`). Hook `ruff format` cần `ruff` đã cài trong Python mà lệnh `python` trỏ tới; chưa cài thì hook bỏ qua im lặng.
- **Spark trên Windows**: PySpark cần JDK 17 (máy đang có Java 24, không hỗ trợ) và ghi Delta cần `winutils`. Test Spark đánh dấu `@pytest.mark.spark`, tự skip local khi thiếu, luôn chạy trên CI Linux.
- **Kill cứng một tiến trình PySpark**: JVM là tiến trình con của Python; SIGKILL riêng Python thì JVM thấy stdin đóng và thoát qua shutdown hook (không phải crash). Muốn crash thật: chạy tiến trình con với `start_new_session=True` rồi `os.killpg(pid, SIGKILL)` (chỉ POSIX; xem `ConsumerProcess` trong `benchmarks/streaming_fault.py`, `docs/streaming-fault.md`).
- **Auto mode bỏ qua allow rule rộng** (`Bash(python *)`, `npm *`, `npx *`, allow `Agent`/`Monitor`); allow rule phải cụ thể. `autoMode` không đọc từ `.claude/settings*.json` — nằm ở `.claude/auto-mode.json`, nạp bằng `--settings`.
- **Ghi vào `.claude/` (ngoài `worktrees/`) là đường dẫn được bảo vệ**: allow rule không mở được, auto mode chặn tự sửa cấu hình. Thay đổi harness do chủ repo duyệt.
- **Quota**: stream-json có `rate_limit_event` (`status: rejected`, `resetsAt` epoch giây); `errorCode: credits_required` thì chờ cũng vô ích. Orchestrator đặt `CLAUDE_CODE_RETRY_WATCHDOG=1` để limit 5 giờ được chờ ngay trong phiên, và `--resume <session_id>` khi phiên thoát vì limit. `autoContinueAtUsageLimit` không áp dụng cho `-p`.
- **Mutation trên file chưa commit**: `git checkout --` không hoàn tác được file chưa được theo dõi; chỉ thử mutation trên file đã commit.

## Nợ kỹ thuật

(mỗi khoản đủ ba phần: Triệu chứng, Nguyên nhân kèm `file:line`, Điều kiện kích hoạt — xem `workflow.md`)

- Chưa có.
