# So sánh hai đường end-to-end (D3)

Việc D3 của Plan v2 (`docs/plan.md`, mục "Luật chọn đường chính"): chạy **cùng** pipeline
incremental batch (ingest → bronze/silver → gold → bộ Q1–Q6) trên hai đường, trên **cùng** cửa sổ
dữ liệu GH Archive thật (không phải simulated), đủ nhiều lần theo lịch, rồi tổng hợp p50/p95 và tỉ
lệ thành công chỉ từ `runs.csv`. Luật chọn đường do D4 áp dụng; PR này không chọn.

| Đường | `variant` | Bronze/silver | Gold | Q1–Q6 đọc từ |
|---|---|---|---|---|
| A | `path-a-runner` | PySpark + Delta trên runner GitHub Actions | `dbt build --target motherduck`, database `repopulse_path_a` | MotherDuck `main_gold.mart_*` |
| B | `path-b-databricks` | Job serverless Databricks (D1, `docs/databricks-job.md`) | `dbt build --target databricks` (D2, `docs/dbt-databricks.md`), schema `repopulse_dbt_*` | SQL warehouse `workspace.repopulse_dbt_gold.mart_*` |

## Workflow `.github/workflows/path-comparison.yml`

- **Trigger:** `schedule` hằng giờ phút 41 (`pipeline.yml` chạy phút 23), `workflow_dispatch`
  (input `date`, `hour`, `hours` tuỳ chọn), `push` chỉ trên nhánh `feature/pipeline-path-comparison`
  (lọc `paths`) để chạy được trong PR này. Không có `pull_request`. **Cron và dispatch chỉ chạy khi
  file đã lên `main`**, tức sau release kế tiếp; trước đó chỉ có lần chạy do push trên nhánh PR.
- **Cửa sổ dữ liệu quyết định một lần** ở job `window` (`python -m repopulse.pathcompare window`),
  hai job đường đọc chung `needs.window.outputs`:
  - input `date` + `hour` (bắt buộc đi cùng nhau), `hours` 1..6, mặc định 1;
  - `push`: ngày chuẩn `benchmarks/reference.toml`, giờ cuối 23, 1 giờ (như D1);
  - `schedule` hoặc dispatch không có `date`: giờ `floor(now UTC) - 2h`, 1 giờ.
- **Hai job độc lập** (`path-a`, `path-b`, cùng `needs: window`): đường này hỏng không làm mất dòng
  của đường kia.
- **Không đụng production của A:** gold của A ghi vào database MotherDuck `repopulse_path_a`
  (`MOTHERDUCK_DATABASE`, `transform/profiles.yml`), tạo bằng
  `python -m repopulse.pathcompare md-ensure` (`CREATE DATABASE IF NOT EXISTS`; từ chối `repopulse`
  và `*_smoke`). `repopulse` của `pipeline.yml` và kiểm độ tươi không bị ghi. Khoá `duckdb==1.5.6`
  như `pipeline.yml`. Database này giữ lịch sử staging qua các lần chạy như `repopulse`
  (idempotent theo `event_id`).
- **Đường B dùng lại bảng của D1/D2:** silver `workspace.repopulse.silver_gharchive_events` (MERGE
  theo event id, idempotent theo (ngày, giờ)) và schema `repopulse_dbt_*`. Một lần chạy D1 duy nhất,
  không `--verify-rerun` (D1 đã chứng minh chạy lại không thêm dòng).
- **Concurrency:** cả workflow một nhóm `path-comparison` (xếp hàng, không huỷ lần đang chạy;
  GitHub chỉ giữ lần chờ mới nhất, lần chờ cũ hơn bị huỷ và không có dòng). Job B thêm nhóm
  `databricks-bronze-silver` của D1, nên không cùng ghi silver với `databricks-bronze-silver.yml`.
  `dbt-databricks.yml` (D2, chạy tay) không chung nhóm: staging chỉ chèn `event_id` chưa có.
- **Secret:** chỉ vào qua `env:` của step cần; bước "Detect secrets" ghi `HAS_SECRETS=true` vào
  `$GITHUB_ENV` khi đủ. Thiếu secret của một đường thì mọi bước của đường đó bỏ qua ở cấp step, có
  `::notice::`, workflow vẫn xanh, **không ghi dòng**. A cần `MOTHERDUCK_TOKEN`, `PII_HASH_SALT`; B
  cần `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `PII_HASH_SALT` (`DATABRICKS_HTTP_PATH` tuỳ chọn). Http
  path của warehouse do `python -m repopulse.probe http-path --github-env` giải và mask như D2.
- **Quyền:** `contents: read`; hai job đường thêm `actions: read` để đọc thời gian xếp hàng của
  chính job qua GitHub API (`GH_TOKEN` là `github.token`, không phải secret repo).
- **Đĩa runner:** mỗi lần chạy 1 giờ dữ liệu (tối đa 6), ingest lọc loại event trước khi lưu và xoá
  file tải về trong `$RUNNER_TEMP`.
- **Artifact** `path-comparison-a-<run_id>` / `path-comparison-b-<run_id>`: `out/path-a-row.csv`
  hoặc `out/path-b-row.csv` cùng các JSON đo; Job summary in bảng tổng hợp của dòng đó.

Lần chạy do push trên nhánh PR dùng ngày chuẩn: silver của B có thể đã có các giờ đó từ D1 (chèn 0
dòng), còn `repopulse_path_a` bắt đầu trống. Dòng của lần đó là thử workflow; số so sánh cho D4 lấy
từ các lần chạy theo lịch (`trigger=schedule` trong `notes`).

## Đo gì

Mỗi bước pipeline chạy qua `python -m repopulse.pipeline timed --name <bước>` (wall time của tiến
trình con, ghi vào `out/timings.json`, mã thoát của lệnh là kết quả của step). Bước của A:
`ingest`, `spark`, `export`, `dbt`, `queries`; của B: `ingest`, `databricks`, `warehouse`, `dbt`,
`queries`.

| Số đo | Nguồn | Ở đâu trong dòng |
|---|---|---|
| End-to-end | tổng thời gian các bước đã chạy (không gồm checkout, cài đặt, xếp hàng) | `total_seconds` |
| Từng bước | `pipeline timed` | `notes`: `<bước>_seconds` |
| Xếp hàng runner | GitHub jobs API của lần chạy: `started_at - created_at` của job | `queue_seconds` |
| Khởi động runner | `now - started_at` ngay trước ingest (checkout, setup Python/Java, pip) | `setup_seconds` |
| Xếp hàng / setup job serverless (B) | Jobs API qua dòng D1 (`queue_s`, `setup_s` của `databricks_job`) | `job_queue_seconds`, `job_setup_seconds` |
| Khởi động SQL warehouse (B) | `pathcompare warehouse-start`: từ lúc gọi tới khi `RUNNING` (0 nếu đang chạy) | `warehouse_start_seconds`; bước `warehouse_seconds` là wall time của cả bước |
| Q1–Q6 | `pathcompare queries`: `SELECT *` toàn bộ mart của câu hỏi (A: DuckDB `fetchall`; B: Statement Execution API tới `SUCCEEDED`) | `q1_seconds` … `q6_seconds` |
| Số dòng | `silver_inserted` của bước bronze/silver | `row_count` |
| Số dòng đọc vào | `bronze_rows` của bước bronze/silver | `bronze_rows` trong `notes` |

`row_count` là số dòng silver **nhận thêm**: đường A dựng lake mới mỗi lần chạy nên luôn chèn hết;
silver của đường B nằm lâu dài trong Unity Catalog nên giờ đã nạp trước đó (ví dụ giờ của ngày
chuẩn do D1 nạp) cho `row_count=0`, toàn bộ dòng bị khử trùng. `bronze_rows` là số dòng bước
bronze/silver đọc vào: hai đường xử lý cùng cửa sổ dữ liệu thì hai số này bằng nhau. Lần chạy theo
lịch lấy một giờ mới nên cả hai đường đều chèn thật.

`notes` còn có `status=success|failed`, `failed_step=<bước>` (bước đầu tiên có kết quả khác
`success`; bước không chạy tính như `skipped`), `hour`, `hours`, `missing_hours` (giờ GH Archive
chưa có trên server), `run_id`, `trigger`. Thời điểm `run_at` là lúc job bắt đầu (UTC).

**Lần chạy thất bại vẫn ghi một dòng** (bước `if: always()`), để tỉ lệ thành công tính được chỉ từ
`runs.csv`. Không có dòng khi: thiếu secret (bỏ qua có chủ đích), job bị huỷ trước khi chạy (nhóm
concurrency), hoặc lỗi trước bước ghi dòng làm Python/package không dùng được (cài đặt hỏng). Các
trường hợp này cũng không có dòng ở đường còn lại nếu nguyên nhân chung (ví dụ lần chờ bị huỷ).

Giới hạn đã biết: thời gian trễ của chính cron GitHub (giờ hẹn → lúc tạo run) không được đo; Q1–Q6
trên B đo tới khi câu lệnh xong ở warehouse, kết quả inline phần đầu, không tải hết mọi chunk.

## Tổng hợp

```bash
python -m repopulse.pathcompare summary --runs runs.csv --out out/path-summary.md
```

Hàm thuần `read_runs` (parser) và `summarize` (tổng hợp) trong `repopulse/pathcompare.py`; D4 gọi
thẳng hai hàm này.

- `read_runs`: chỉ giữ dòng `path-a-runner` / `path-b-databricks`; tách `notes` thành dict (mỗi
  từ phải là `key=value`, khoá lặp là lỗi), kiểu hoá số. Dòng hỏng (thiếu `status`, `failed` không
  có `failed_step`, số không đọc được, sai số cột) báo `PathCompareError` kèm số dòng, không bỏ im
  lặng. Dòng của variant khác bị bỏ qua.
- `summarize`, mỗi đường (luôn có cả hai):
  - số lần chạy, số thành công, tỉ lệ thành công (`None` khi chưa có lần nào);
  - p50/p95 end-to-end và từng bước: **chỉ lần thành công**;
  - p50/p95 xếp hàng/khởi động: mọi lần có số đó;
  - đếm bước hỏng (`failed_steps`);
  - số lần chạy theo ngày UTC của `run_at`, mọi ngày từ lần đầu tới lần cuối (ngày không có lần
    nào tính 0), cùng min/max mỗi ngày — để D4 kiểm "24 lần/ngày". Ngày đầu và ngày cuối có thể chỉ
    là một phần ngày.

**Percentile nearest-rank:** với n giá trị đã sắp xếp, percentile p là giá trị ở hạng
`ceil(p / 100 * n)` (đếm từ 1, ít nhất 1). Kết quả luôn là một số đo có thật trong `runs.csv`
(không nội suy); n = 1 thì p50 = p95 = giá trị đó; danh sách rỗng thì không có percentile.

## Gộp dòng vào `runs.csv`

Tải artifact của các lần chạy rồi:

```bash
python benchmarks/record_run.py out/path-a-row.csv out/path-b-row.csv
python -m repopulse.readme_results
```

`record_run.py` bỏ qua dòng đã có (gộp lại cùng artifact không sinh trùng). Mục Results của README
liệt kê các dòng này ở "Other runs" cho tới khi R2 dựng bảng riêng.

## Tắt cron khi D4 đủ số

Workflow chỉ chạy theo lịch sau khi release lên `main`. Khi D4 đã đủ số lần chạy: tắt ở tab
Actions (hoặc `gh workflow disable path-comparison.yml`, chủ repo làm), hoặc mở PR bỏ khối
`schedule:`. Free Edition của Databricks có quota ngày; vượt quota thì compute tắt tới hết ngày,
các lần chạy đó ghi `status=failed` ở đường B, đúng là dữ liệu D4 cần.

Tài liệu này không chép số; số đo chỉ đọc ở `runs.csv`.
