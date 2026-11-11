# RepoPulse — kế hoạch triển khai

## RepoPulse làm gì

RepoPulse là một data platform nhỏ đo "sức khoẻ" của các dự án open source từ dữ liệu sự kiện GitHub công khai (GH Archive), kèm một luồng event ứng dụng theo logging spec.

- **Đầu vào:** event GitHub thật theo giờ; event app mô phỏng sinh theo một catalog event có schema.
- **Xử lý:** pipeline batch ELT chạy theo lịch: làm sạch và băm PII bằng PySpark + Delta Lake, dựng star schema bằng dbt, có test, contract và cảnh báo bất thường.
- **Đầu ra:** các metric như contributor hoạt động hằng ngày, retention theo cohort, funnel fork → PR → merge; một dashboard công khai; và sổ số đo `runs.csv` ghi lại mọi lần chạy.

## Kết quả dự án: trả lời câu hỏi gì

Gold và dashboard tập trung vào khoảng 50–150 **repo hệ data** (Spark, dbt, DuckDB, Airflow, Kafka, Trino, Delta, Iceberg, Polars, pandas…), danh sách ở seed `transform/seeds/repos_data_ecosystem.csv` kèm `category` (compute, orchestration, storage-format, warehouse, streaming, dataframe, bi). Ingest vẫn xử lý toàn bộ event GH Archive để có số quy mô (M1).

Bộ query cố định Q1–Q6 vừa là câu hỏi dự án trả lời, vừa là bộ đo tốc độ dùng lại ở mọi lần đo:

| Q | Câu hỏi | Mart |
|---|---|---|
| Q1 | Mỗi ngày, mỗi tuần có bao nhiêu contributor hoạt động trên mỗi repo và mỗi category? Người mới so với người quay lại? | `mart_daily_active_contributors` |
| Q2 | Retention theo cohort: trong nhóm có PR đầu tiên ở tuần W, bao nhiêu % còn hoạt động ở W+4, W+12? | `mart_cohort_retention` |
| Q3 | Funnel fork → mở PR → PR merge; thời gian tới lần merge đầu p50/p90 | `mart_contribution_funnel` |
| Q4 | Thời gian tới phản hồi đầu tiên cho issue/PR p50/p90; tỉ lệ đóng | `mart_responsiveness` |
| Q5 | Bus factor: top 1–3 contributor chiếm bao nhiêu % đóng góp; tỉ lệ bot | `mart_repo_concentration` |
| Q6 | Xếp hạng sức khoẻ các repo hệ data (điểm tổng hợp từ Q1–Q5, công thức ghi trong README) | `mart_repo_health_score` |

**Giá trị cho DE/DA.** DE: mỗi kỹ thuật (incremental, partition, Spark so với DuckDB, data contract, cảnh báo chất lượng) có số trước/sau đo trên cùng bộ Q1–Q6 và cùng ngày dữ liệu. DA: định nghĩa metric, cohort, funnel, dashboard công khai.

**Baseline đo trước tiên (PR B, ngay sau S0.4).** Một ngày GH Archive cố định (24 file, ngày và checksum ghi trong `benchmarks/reference.toml`), pipeline ngây thơ `benchmarks/naive_duckdb.py` (DuckDB đọc thẳng `*.json.gz`, full refresh, không partition, không khử trùng) tính Q1–Q6 bằng `benchmarks/queries/q1.sql`…`q6.sql`. Đo tổng thời gian, thời gian từng query, peak RAM, dữ liệu đọc, số dòng; 5 lần lấy median; workflow `benchmark.yml` trên runner chuẩn; ghi `runs.csv` với `variant=baseline`. PR 5, 7, 10, 12 chạy lại đúng workflow này với `variant=<tên>`, README so mỗi biến thể với baseline.

Làm theo hai giai đoạn. Giai đoạn 1 chạy hoàn toàn trên dịch vụ cloud miễn phí và GitHub Actions, không cần Docker trên máy. Giai đoạn 2 chạy trên máy, thêm phần cloud miễn phí không làm được: Kafka, Trino/Hive, Airflow và thử chịu lỗi.

## Những gì đã kiểm về dịch vụ cloud miễn phí (2026-11-10)

| Dịch vụ | Kết quả kiểm | Hệ quả cho thiết kế |
|---|---|---|
| Databricks Free Edition | Chỉ có serverless; 1 SQL warehouse cỡ 2X-Small; tối đa 5 job task đồng thời; vượt quota thì compute tắt tới hết ngày; cấm mục đích thương mại. **Tài liệu chính thức không xác nhận** việc tạo token và điều khiển từ bên ngoài (CLI, GitHub Actions); chỉ có một bài trên diễn đàn nói làm được | Bước đầu tiên là một bước thử. Không qua thì dùng đường dự phòng |
| Databricks: truy cập internet | Bị giới hạn theo danh sách domain; chưa biết `data.gharchive.org` có tải được không | GitHub Actions tải dữ liệu rồi đẩy lên Databricks |
| Databricks: streaming | Chỉ chạy kiểu batch (`availableNow`) | Giai đoạn 1 gọi đúng tên là "incremental batch", không gọi là real-time |
| Databricks: dashboard | Không chia sẻ công khai được | Dashboard công khai làm bằng cách khác |
| Airflow managed miễn phí | Không có lựa chọn miễn phí lâu dài | Giai đoạn 1 điều phối bằng GitHub Actions. Airflow để giai đoạn 2 |
| Kafka managed miễn phí | Chỉ có trial 30 ngày, hoặc gói rất nhỏ tự tắt | Kafka để giai đoạn 2 |
| GitHub Actions (repo public) | Miễn phí; runner 4 CPU, 16 GB RAM, 14 GB đĩa; mỗi job tối đa 6 giờ; cron ngắn nhất 5 phút, chỉ chạy trên nhánh mặc định, có thể trễ, tự tắt sau 60 ngày repo không hoạt động | Runner đủ mạnh để chạy PySpark. Đây là đường dự phòng cho Databricks |
| MotherDuck Lite | Miễn phí lâu dài: 10 GB, 10 giờ compute mỗi tháng; dùng được với dbt qua token | Warehouse dự phòng |
| Snowflake trial | 30 ngày, hết hạn thì khoá | Chỉ đăng ký khi pipeline đã chạy ổn |
| Tableau Public | Đọc CSV; chỉ tự làm mới với Google Sheets; còn lại phải publish lại bằng tay | Publish bằng tay ở các mốc. Trang dashboard tự cập nhật làm bằng trang tĩnh |

## Giai đoạn 1 (cloud): thiết kế

**Mục tiêu:** một pipeline batch ELT chạy theo lịch trên cloud, có logging spec, data modeling, data quality, governance và dashboard, với số đo thật ghi trong `runs.csv`.

```
Nguồn dữ liệu
  GH Archive (event GitHub thật, công khai) ──┐
  Event app mô phỏng (simkit, theo logging spec) ──┤  GitHub Actions (cron)
  Event thật của TravelPlan (tuỳ chọn, bước cuối) ──┘        │ tải, kiểm schema, đẩy lên
                                                             ▼
Bronze/Silver: PySpark + Delta Lake trên Databricks (serverless)
   kiểu dữ liệu, khử trùng, sự kiện đến muộn, băm PII, bảng quarantine cho event sai
                                                             ▼
Gold: dbt star schema (fct_events, dim_user SCD2, dim_repo; mart DAU, retention, funnel)
   test, contract, kiểm độ tươi; CI chạy cùng project dbt trên DuckDB
                                                             ▼
Dashboard: trang tĩnh tự cập nhật trên GitHub Pages + Tableau Public (publish tay)
Sổ số đo: runs.csv trong repo, mỗi lần chạy một dòng
```

**Đường dự phòng nếu Databricks không điều khiển được từ ngoài:** PySpark + Delta chạy ngay trên runner của GitHub Actions (16 GB RAM), gold nằm trên MotherDuck. Khi đó vẫn dùng Spark và Delta Lake, chỉ không có Databricks; README ghi rõ điều này.

**Những thứ cố ý không làm ở giai đoạn 1** (để giữ phạm vi gọn): agent hỏi đáp SQL, Terraform, Kafka, Airflow, Trino/Hive, thử chịu lỗi.

## Subplan S0: dựng repo, GitHub DevOps và harness trước khi chạy thật

Repo tạo bằng `gh`; harness `.claude` lấy từ TravelPlan rồi chỉnh cho Python và dbt; harness được kiểm bằng một PR mẫu đi hết vòng. S0 phải xong và báo cáo kiểm harness phải sạch thì mới làm PR 2 trở đi.

### S0.1 Tạo repo

- `gh repo create tlamDson/RepoPulse --public --clone` vào `D:\Desktop\RepoPulse`, kèm mô tả và topics.
- Git flow như TravelPlan: feature → `develop` (squash), release `develop` → `main` (merge commit). `main` là nhánh mặc định vì workflow chạy theo lịch chỉ chạy trên nhánh mặc định; pipeline bắt đầu chạy sau release. Orchestrator tự release ở các mốc sau PR B, 7, 11, 14 khi CI xanh (cập nhật 2026-11-11, thay cho "chỉ một nhánh `main`").
- Cấu hình repo: chỉ cho squash merge, tự xoá nhánh sau merge.
- File gốc: `README.md`, `LICENSE` (MIT), `.gitignore` (Python, dbt, `.env`, `.claude/settings.local.json`, `.claude/worktrees/`), `.gitattributes`, `.env.example`, `pyproject.toml` có khoá phiên bản.

### S0.2 GitHub DevOps

| Hạng mục | Làm gì |
|---|---|
| CI trên PR | `.github/workflows/ci.yml`: ruff (lint và format), pytest, `dbt build` trên DuckDB. Tên job cố định để làm check bắt buộc |
| Bảo vệ nhánh `main` | Bắt buộc qua PR; bắt buộc các job CI xanh; cấm force push và cấm xoá nhánh; không bắt buộc người duyệt (repo một người, review do agent làm và ghi vào PR) |
| Bảo mật | Bật secret scanning và push protection (TravelPlan đang bật); Dependabot cho `pip` và `github-actions`; workflow khai `permissions` tối thiểu, có `timeout-minutes` và `concurrency` |
| Mẫu | Mẫu PR có mục `## Summary`, `## RED` (output test đỏ), `## Test plan`; mẫu issue cho sự cố dữ liệu |
| GitHub Pages | Bật nguồn là GitHub Actions, để PR dashboard dùng sau |
| Secret | Lam tự thêm bằng `gh secret set` hoặc trên web: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `MOTHERDUCK_TOKEN`. Agent không đọc và không in giá trị |
| pre-commit | ruff và một bộ quét secret, chạy cả ở local lẫn trong CI |

Workflow chạy pipeline theo lịch và workflow deploy Pages thuộc PR 7 và PR 11, không làm ở S0.

### S0.3 Cài harness `.claude` từ TravelPlan

| Từ TravelPlan | Sang RepoPulse | Điều chỉnh |
|---|---|---|
| `.claude/rules/tdd.md` | Giữ | Đổi sang pytest và test của dbt; giữ Red → Green → Refactor và yêu cầu thấy đỏ thật |
| `.claude/rules/workflow.md`, `developer-guide.md` | Giữ | Nhánh gốc là `main`; worktree mới rẽ từ `origin/main` là đúng mặc định, không cần bước `reset --hard origin/develop`; merge khi CI xanh và review PASS; cập nhật nhánh bằng `gh pr update-branch`, không rebase |
| `.claude/rules/tech-defaults.md` | Viết mới | Stack của RepoPulse; luật "số đo chỉ lấy từ `runs.csv`"; luật ghi rõ dữ liệu mô phỏng; mục nợ kỹ thuật |
| `.claude/agents/code-reviewer.md` | Giữ | Thêm: chạy lại test và chép output, kiểm commit đầu chỉ có test, thử mutation, kết luận PASS hoặc FAIL |
| `.claude/agents/qa-testing.md`, `researcher.md` | Giữ | Đổi lệnh test sang Python và dbt |
| Hook định dạng sau khi sửa file | Giữ ý tưởng | Đổi `prettier` thành `ruff format` cho file `.py` |
| Hook âm báo (dừng, hỏi quyền, chờ nhập) | Giữ nguyên | — |
| `.claude/commands/pr.md`, `ci-watch.md`, `scripts/ci-stop-guard.ps1` | **Không mang sang** | Cơ chế này chỉ theo dõi được một PR và bị xoá khi mở phiên mới. Phiên điều phối theo dõi CI bằng `gh pr checks --watch` |
| `rules/design.md`, `debug-chrome-mcp.md`, các skill UI, `itinerary-builder`, `rag-destinations`, `travelplan-devops` | Không mang sang | Không liên quan tới project data |
| `CLAUDE.md` | Viết mới | Tổng quan, lệnh hay dùng, bảng vai và model, các cổng cần Lam, trỏ tới rules |

**File `.claude/settings.json` cần Lam cho phép rõ ràng.** Chế độ Auto chặn agent tự ghi cấu hình quyền và hook, trừ khi chính Lam viết trong tin nhắn đúng thay đổi đó. Vì vậy câu mở đầu của S0 (Lam dán) có ghi rõ: "cho phép ghi `.claude/settings.json` của repo này với danh sách allow, ask, deny và hook như trong `docs/plan.md` mục S0.3". Nội dung:
- `allow`: `git`, `gh`, `python`, `pip`, `uv`, `pytest`, `ruff`, `dbt`, `databricks`, `duckdb`, `npm`/`npx` (cho trang dashboard), `curl` tới localhost.
- `ask`: `gh secret *`, `gh repo delete*`, `databricks bundle destroy*`, lệnh ghi lên Snowflake.
- `deny`: đọc `.env`, `git push` thẳng vào `main`, `git push --force`, `gh pr merge --admin`.
- Hook: `ruff format` sau khi sửa file `.py`; ba âm báo.
- Chế độ Auto bật bằng cờ lúc mở phiên, không đặt trong file này.

### S0.4 Kiểm harness bằng một PR mẫu

PR mẫu là thứ đằng nào cũng cần: module ghi `runs.csv` (hàm thêm một dòng số đo, có kiểm schema). Nó đi hết vòng thật:

1. Phiên chính viết kế hoạch ngắn cho PR, có mục test phải đỏ.
2. Subagent Opus trong worktree viết test đỏ, commit, rồi code, mở PR.
3. CI chạy trên PR.
4. Subagent Opus khác review: chạy lại test, thử mutation, kết luận.
5. Phiên chính merge khi CI xanh và review PASS.

Trong lúc đó ghi lại vào `docs/harness-check.md`:

| Kiểm | Đạt khi |
|---|---|
| Hỏi quyền | Liệt kê mọi lần bị hỏi; mục tiêu là 0 lần ngoài các lệnh thuộc `ask` |
| Worktree của subagent | Tạo được, đúng nhánh gốc `main`; lỗi lệch `d:`/`D:` có xảy ra không |
| Model và effort của subagent | Đúng Opus medium và Sonnet high như đã giao |
| Hook | `ruff format` chạy sau khi sửa file; âm báo kêu |
| Bảo vệ nhánh | API báo `main` bắt buộc PR và bắt buộc check; push thẳng bị từ chối bởi luật `deny` |
| CI | Đỏ trên commit chỉ có test, xanh sau khi có code |
| Review | Reviewer chép được output test thật và bắt được mutation |
| Secret | Push protection đang bật; file `.env` không đọc được |

Chỗ nào không đạt thì sửa harness rồi chạy lại phần đó. Lam xem `docs/harness-check.md` trước khi cho chạy PR 2.

### Danh sách PR giai đoạn 1 (repo mới)

| # | Nhánh | Nội dung | Test đỏ viết trước |
|---|---|---|---|
| S0 | (subplan ở trên) | Tạo repo, DevOps, harness, PR mẫu `feature/runs-log` | Test của module `runs.csv` |
| 1 | (bước thử, không PR) | Sau khi Lam thêm secret: từ GitHub Actions chạy thử đẩy file lên Volume, chạy một job serverless, `dbt debug` tới SQL warehouse; trong notebook thử tải `data.gharchive.org`. Ghi kết quả vào `docs/progress.md`, chọn đường chính hay đường dự phòng | — |
| B | `feature/baseline-naive` | Ngày dữ liệu chuẩn + checksum, pipeline ngây thơ DuckDB, bộ query Q1–Q6, `benchmark.yml` (5 lần, median), ghi `runs.csv` `variant=baseline` | Parser kết quả đo đúng schema `runs.csv`; checksum sai thì dừng; Q1–Q6 trên fixture nhỏ ra đúng số tính tay |
| 2 | `feature/event-spec` | `eventspec/`: catalog event dạng YAML sinh JSON Schema; hàm kiểm event; quy tắc khoá chống trùng | Event thiếu trường bắt buộc bị từ chối; event trùng khoá bị nhận diện |
| 3 | `feature/simkit-generator` | `simkit/`: sinh người dùng mô phỏng theo persona, có seed; chèn 6 loại lỗi dữ liệu biết trước (trùng, null, sai kiểu, đến muộn, đổi schema, bot) | Cùng seed ra cùng dữ liệu; số lỗi chèn đúng bằng số yêu cầu |
| 4 | `feature/ingest-gharchive` | Tải file theo giờ, lọc loại event, idempotent theo (ngày, giờ), đẩy lên vùng landing; workflow cron | Chạy lại cùng giờ không sinh bản ghi trùng |
| 5 | `feature/bronze-silver-spark` | Job PySpark: bronze raw, silver có kiểu, khử trùng, cửa sổ đến muộn, băm PII trước silver, quarantine | Test PySpark trên dữ liệu nhỏ: trùng bị loại, email không còn dạng thô, event sai vào quarantine |
| 6 | `feature/dbt-star-schema` | Project dbt: staging, marts, `dim_user` SCD2, mart DAU, retention, funnel; test, contract, unit test | `dbt build` trên DuckDB đỏ khi vi phạm contract |
| 7 | `feature/incremental-and-schedule` | Model incremental thay full refresh; workflow chạy theo lịch nối ingest, job Spark, `dbt build`; ghi `runs.csv` | So khớp số dòng giữa incremental và full refresh trên fixture |
| 8 | `feature/data-quality-alerts` | Chạy bộ lỗi chèn qua pipeline; phát hiện bất thường về khối lượng và độ tươi; cảnh báo bằng GitHub Issue | Mỗi loại lỗi có test bắt được; chạy sạch không báo động |
| 9 | `feature/governance` | Phân quyền theo vai, gắn nhãn cột PII, che cột; kiểm tự động rằng gold không chứa email thô. Dùng Unity Catalog nếu Free Edition có (chưa kiểm), không thì làm ở tầng dbt | Test tìm email thô trong gold phải đỏ khi cố ý bỏ bước băm |
| 10 | `feature/query-optimization` | Đo query dashboard trước và sau khi partition và clustering; ghi số | Parser kết quả đo ra đúng schema |
| 11 | `feature/dashboard-site` | Trang tĩnh trên GitHub Pages đọc dữ liệu gold đã tổng hợp, tự cập nhật sau mỗi lần chạy; xuất CSV cho Tableau | Build trang thất bại khi thiếu mart |
| 12 | `feature/spark-vs-duckdb-benchmark` | Chạy cùng phép biến đổi bằng PySpark và DuckDB ở 1, 7, 30 ngày dữ liệu trên runner | Parser benchmark |
| 13 | `feature/snowflake-port` | Chạy cùng project dbt trên Snowflake trial; ghi thời gian, số dòng phải sửa | `dbt build` profile Snowflake trong workflow chạy tay |
| 14 | `docs/results` | README có bảng kết quả lấy từ `runs.csv`, sơ đồ, quyết định thiết kế | — |

Một PR ở **repo TravelPlan** (tuỳ chọn, làm riêng bên đó) để có event thật của một app:

| Nhánh | Nội dung |
|---|---|
| `feature/product-event-logging` | SDK ghi event ở client và server theo đúng catalog của `eventspec`; endpoint nhận event có kiểm schema; lưu vào một collection Mongo có hạn; đo độ phủ hành động quan trọng. Sau đó một workflow ở RepoPulse xuất event thật mỗi ngày (cần Lam thêm secret chỉ đọc) |

### Số đo giai đoạn 1

| ID | Số đo | Điều kiện | Baseline |
|---|---|---|---|
| M1 | Số event và dung lượng đã xử lý | Đếm ở bronze, silver, gold | Số quy mô |
| M2 | Thời gian build khi thêm một ngày dữ liệu | Median 5 lần | Full refresh (`variant=baseline`) |
| M3 | Độ tươi dữ liệu p50/p95 | ≥168 lần chạy theo lịch. Chạy trên cloud nên tự tích luỹ, không cần máy Lam | Mục tiêu đặt trước |
| M4 | Số lỗi chèn vào bị bắt; số báo động giả | 6 loại lỗi; N lần chạy sạch | Không có test: 0 |
| M5 | Điểm giao nhau Spark và DuckDB | 1, 7, 30 ngày; 3 lần mỗi mức | DuckDB |
| M7 | Port sang Snowflake: số dòng sửa, thời gian | Cùng `dbt build` | Bản chính |
| M9 | Tỉ lệ event bị loại vì sai schema; tỉ lệ trùng | Event mô phỏng (ghi rõ là mô phỏng) và event thật nếu có | Trước khi có spec |
| M10 | p50 thời gian query và lượng dữ liệu quét | Bộ Q1–Q6, 10 lần | Bảng chưa partition (`variant=baseline`) |

## Plan v2 (2026-11-11): so sánh hai đường, mở rộng engine, giai đoạn 2, AI

Giai đoạn 1 đã xong (16/16 việc, release #45). Lần chạy lại probe với token đúng cho kết quả `primary`: cả 9 bước Databricks đều pass. Plan v2 đo hai đường đối đầu, rồi chọn đường chính theo một luật đã ghi **trước khi đo**. Ngoài ra nó thêm ma trận engine, Iceberg so với Delta, giai đoạn 2 và một SQL agent chạy bằng model local. Chủ repo đã chốt ngày 2026-11-11: mọi thứ phải miễn phí, giai đoạn 2 chạy trên runner GitHub Actions, orchestrator chạy tới hết quota.

### Luật chọn đường chính (chốt trước khi đo, PR D4 áp dụng y nguyên)

Gọi A là runner + MotherDuck (đường hiện tại), B là Databricks serverless + SQL warehouse. Mỗi đường chạy cùng pipeline end-to-end (ingest → bronze/silver → gold → bộ Q1–Q6) trên cùng dữ liệu, đủ số lần chạy theo lịch để có p50 và tỉ lệ thành công.

1. Loại đường nào **không chạy đủ 24 lần/ngày trong quota miễn phí**, hoặc có **tỉ lệ thành công < 95%**.
2. Trong các đường còn lại, chọn đường có **p50 thời gian end-to-end nhỏ hơn**.
3. Nếu hai đường chênh nhau **dưới 10%**, giữ đường hiện tại (A).

D4 ghi bảng số liệu, luật và kết luận vào `docs/probe-result.md`. Bảng chỉ dùng số trong `runs.csv`. D4 chỉ đổi `pipeline.yml` khi luật nói đổi.

### Các việc (DAG ở `scripts/prs.toml`)

| Việc | Nhánh | Nội dung | Test đỏ viết trước | Số đo vào `runs.csv` |
|---|---|---|---|---|
| D1 | `feature/databricks-bronze-silver` | Job bronze/silver chạy serverless trên Databricks: Delta trong Unity Catalog, dùng chung logic với `repopulse/spark/`, nạp bằng Files API + Jobs API từ GitHub Actions | Logic gọi API (mock HTTP): dựng payload job, retry, idempotent theo (ngày, giờ) | `variant=databricks-bronze-silver` |
| D2 | `feature/dbt-databricks-target` | Target dbt `databricks` dựng gold trên SQL warehouse; giữ nguyên model và contract; profile lấy secret qua env | `dbt parse --target databricks` thành công; contract giữ nguyên | — |
| D3 | `feature/pipeline-path-comparison` | Workflow chạy A và B end-to-end theo lịch, ghi thời gian từng bước, thời gian khởi động/xếp hàng, kết quả thành công; script tổng hợp p50/p95 và tỉ lệ thành công | Parser và hàm tổng hợp trên fixture | `variant=path-a-runner`, `variant=path-b-databricks` |
| D4 | `feature/primary-path-switch` | Áp luật chọn ở trên; cập nhật `docs/probe-result.md`; đổi `pipeline.yml` nếu luật nói đổi | Hàm luật chọn (thuần) trên các bộ số mẫu, gồm cả trường hợp chênh dưới 10% | — |
| E1 | `feature/engine-matrix` | Cùng bộ Q1–Q6 trên DuckDB, Spark trên runner, Databricks serverless, Databricks SQL, MotherDuck, Snowflake (lấy lại số đã có). README sinh một bảng duy nhất | Parser ma trận; cảnh báo khi thiếu engine | `variant=matrix-<engine>` |
| E2 | `feature/iceberg-vs-delta` | Cùng dữ liệu ghi bằng Delta và Apache Iceberg (PySpark trên runner): ghi, đọc Q1–Q6, MERGE, time travel | Test vai trò hai format trên fixture nhỏ | `variant=format-delta`, `variant=format-iceberg` |
| P2a | `feature/streaming-kafka` | **Apache Kafka** 3 broker chế độ KRaft (image chính thức `apache/kafka`, không ZooKeeper) làm Docker service trong workflow; producer phát event GH Archive của ngày chuẩn vào topic có N partition; Spark Structured Streaming đọc Kafka, ghi Delta (checkpoint, exactly-once tới sink); đối chiếu số dòng với batch | Producer/consumer trên fixture; đối chiếu đếm | `variant=streaming-throughput-p<N>` |
| P2b | `feature/streaming-fault-tolerance` | Kill một broker và một consumer giữa chừng; đo số event mất, trùng, thời gian phục hồi | Hàm đối soát mất/trùng | `variant=streaming-fault-<case>` |
| P2c | `feature/trino-hive` | Trino + Hive Metastore + MinIO làm Docker service; gold dạng Hive-partitioned; Q1–Q6 trên Trino trước và sau tối ưu | Sinh DDL partition; parser kết quả | `variant=trino-<layout>` |
| P2d | `feature/airflow-dags` | Airflow standalone trong workflow; DAG có backfill, retry, SLA thay cho cron; đo backfill N ngày | DAG import được, không lỗi chu trình; thứ tự task | `variant=airflow-backfill-<N>d` |
| AI1 | `feature/dbt-semantic-layer` | Semantic model và metric (MetricFlow) cho Q1–Q6 trên gold public | `dbt parse` kiểm semantic manifest; metric khớp SQL của Q1–Q6 trên fixture | — |
| AI2 | `feature/sql-agent` | SQL agent gọi qua API OpenAI-compatible (`base_url` và model là cấu hình): chỉ đọc schema `public`, chặn mọi câu lệnh ghi, giới hạn số dòng; bộ câu hỏi đánh giá có đáp án; CI dùng LLM giả lập | Chặn câu lệnh ghi; sinh và chạy SQL với LLM giả; chấm đáp án | — (đo ở AI3) |
| R2 | `docs/results-v2` | README kết quả v2 sinh từ `runs.csv`: bảng hai đường, ma trận engine, Iceberg/Delta, giai đoạn 2, agent | Generator báo lỗi khi thiếu biến thể | — |

**AI3, chạy tay trên máy chủ repo, không qua orchestrator:** model `Qwen3.6-35B-A3B` (GGUF 18 GB) không chạy được trên runner. Sau khi AI2 merge, phiên chính xin phép chủ repo, rồi bật `llama-server` cục bộ và chạy bộ đánh giá. Độ chính xác và thời gian mỗi câu ghi vào `runs.csv` với `variant=sql-agent-qwen3.6-35b-a3b`, đưa vào qua một PR.

### Ràng buộc

- **Chỉ dịch vụ miễn phí:** Databricks Free Edition (quota theo ngày, tối đa 5 job chạy cùng lúc), MotherDuck Lite, Snowflake trial không thẻ, runner GitHub Actions (job ≤ 6 giờ, 16 GB RAM, 14 GB đĩa). Không dùng API LLM trả phí.
- **Giai đoạn 2 chạy trên runner:** dùng `services:` hoặc `docker compose` trong workflow chạy tay hay theo lịch, không chạy trên máy chủ repo.
- **Số đo chỉ lấy từ `runs.csv`:** mọi tối ưu so với `variant=baseline` trên cùng ngày dữ liệu và cùng bộ Q1–Q6.

Plan v2 là phần mở rộng. Bỏ đi cũng không ảnh hưởng kết quả của giai đoạn 1.

## Việc Lam cần làm

**Trước khi bắt đầu (khoảng 30 phút):**

| # | Việc | Ghi chú |
|---|---|---|
| 1 | Tên project đã chốt: **RepoPulse** | Agent tạo repo public `tlamDson/RepoPulse` bằng `gh` và clone về `D:\Desktop\RepoPulse` |
| 2 | Đăng ký Databricks Free Edition | Không cần thẻ |
| 3 | Đăng ký MotherDuck Lite | Dự phòng; không cần thẻ |
| 4 | Đăng ký Tableau Public và cài Tableau Desktop Public Edition | Dùng ở bước dashboard |
| 5 | Sau S0: thêm secret vào repo (token và host của Databricks, token MotherDuck) | Lam tự thêm; agent không cần thấy giá trị |
| 6 | Xem `docs/harness-check.md` rồi cho chạy tiếp | Cổng duy nhất giữa S0 và phần còn lại |

**Trong lúc chạy:**
- Đăng ký Snowflake trial **khi agent báo tới PR 13**, vì đồng hồ 30 ngày tính ngay.
- Publish dashboard lên Tableau Public (khoảng 30–60 phút, một lần).
- Thêm secret chỉ đọc tới Mongo staging nếu muốn có event thật của TravelPlan.

## Cách chạy

**Cập nhật 2026-11-11:** chạy headless bằng `scripts/orchestrate.py` (DAG ở `scripts/prs.toml`, mỗi PR một phiên `claude -p` trong worktree riêng, tối đa 5 song song (trước đây 2), agent chuyên trách theo mảng, tự chờ quota, tự release). Cách dùng: `docs/kickoff.md`. Mô tả cũ bên dưới giữ để tham khảo vai trò.

- **Phiên chính Fable (effort high):** viết kế hoạch ngắn cho từng PR có mục test phải đỏ, giao việc, quyết định merge, ghi `docs/progress.md`.
- **Subagent Opus (effort medium):** mỗi PR một agent trong worktree riêng, viết test đỏ rồi code; một agent Opus khác review có bằng chứng.
- **Subagent Sonnet (effort high):** lint, cập nhật nhánh, chép bằng chứng, PR chỉ có dữ liệu.
- Merge khi CI xanh và review PASS. Dừng khi mọi việc còn lại chờ Lam, và in checklist.

Vì mọi thứ nặng chạy trên GitHub Actions và Databricks, máy Lam chỉ chạy unit test nhỏ. Không cần Docker.

## Thứ tự làm S0

1. **S0.1:** tạo repo public `tlamDson/RepoPulse` bằng `gh`, đặt cấu hình merge, commit đầu gồm file gốc và tài liệu trong `docs/`.
2. **S0.2:** PR `chore/devops-bootstrap` (CI, mẫu PR, Dependabot, pre-commit, khung Python và dbt). Sau khi merge thì bật bảo vệ nhánh `main` và các cài đặt bảo mật.
3. **S0.3:** PR `chore/claude-harness` (rules, agents, `CLAUDE.md`, `.claude/settings.json`). PR này **để mở cho chủ repo duyệt**.
4. **S0.4:** PR mẫu `feature/runs-log`, làm trong một phiên Claude Code mở tại repo này để harness được nạp và kiểm đúng nơi nó chạy. Câu mở đầu nằm ở `docs/kickoff.md`.

## Kiểm chứng

- **Bước 1 (bước thử)** quyết định đường chính hay dự phòng; kết quả ghi vào `docs/progress.md` trước khi làm PR 4. Cập nhật 2026-11-11: bước thử làm qua PR `docs/probe-result`, kết quả ở [`probe-result.md`](probe-result.md) — chọn **đường dự phòng**; Databricks chưa kiểm được vì token đang lưu không phải personal access token.
- Mỗi PR: CI xanh (lint, test, `dbt build` trên DuckDB), có output test đỏ trong mô tả PR, review PASS.
- Pipeline: workflow theo lịch chạy xanh nhiều ngày liền; `runs.csv` tăng dòng; trang GitHub Pages cập nhật.
- Chạy lại cùng một giờ dữ liệu không sinh bản ghi trùng.
- README chỉ trích số có trong `runs.csv`.

## Rủi ro và điều chưa kiểm chứng

| Rủi ro | Xử lý |
|---|---|
| Databricks Free Edition không cho điều khiển từ ngoài | Đường dự phòng: PySpark trên runner + MotherDuck; README ghi rõ không dùng Databricks |
| Vượt quota Databricks trong ngày | Lịch chạy thưa (vài giờ một lần), job idempotent để hôm sau chạy bù |
| Tài khoản Databricks bị xoá vì lâu không dùng | Code và dữ liệu gold đã tổng hợp luôn có bản trong repo |
| Cron GitHub Actions trễ, hoặc tự tắt sau 60 ngày không hoạt động | Ghi thời điểm chạy thật vào `runs.csv`; M3 báo đúng độ trễ đo được |
| Đĩa runner chỉ 14 GB | Mỗi lần chạy chỉ xử lý vài giờ dữ liệu; lọc loại event trước khi lưu |
| Dữ liệu app là mô phỏng | README và mọi báo cáo ghi rõ "simulated"; GH Archive là dữ liệu thật |
| GH Archive gần đây thiếu không đều (đo ngày 2026-11-11, file giờ 12:00) | Có giờ đủ: 2026-10-25 có 81.569 event, 82% PushEvent. Có giờ mất hẳn PushEvent, CreateEvent, DeleteEvent: 2026-10-15, 2026-11-03 (3.398 event), 2026-11-09, 2026-11-10; dung lượng 2,8–15 MB/giờ. Khối lượng cũng giảm dần theo năm: 2024-01 là 123 MB/giờ, 2025-06 là 85 MB/giờ, 2026-06 là 21 MB/giờ. Hệ quả: (1) PR B chọn ngày chuẩn là ngày đủ 24 giờ có PushEvent, kiểm bằng script và ghi tiêu chí vào `benchmarks/reference.toml`; (2) PR 12 đo Spark và DuckDB ở 1, 7, 30 ngày trên dữ liệu năm 2024–2025, khi đó một ngày khoảng 2–3 GB, vừa đĩa 14 GB; (3) Q1–Q6 dựa trên PR, issue, review, comment, fork, watch là những loại luôn có, không dựa vào PushEvent; (4) PR 8 dùng chính các giờ thiếu dữ liệu này làm bất thường thật cho phần phát hiện khối lượng. |

**Chưa kiểm chứng:** token và CLI trên Free Edition; `data.gharchive.org` có nằm trong danh sách domain được phép không; dung lượng Volume; Free Edition có phân quyền, che cột và lineage của Unity Catalog không; con số quota theo ngày; số credit của Snowflake trial; thời gian mỗi PR ở repo mới.
