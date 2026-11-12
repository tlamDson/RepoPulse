# Job bronze/silver trên Databricks serverless (D1)

Việc D1 của Plan v2 (`docs/plan.md`): chạy **cùng logic** bronze → silver của
`repopulse/spark/` (`repopulse.spark.batch.run_batch`) dưới dạng một job serverless trên
Databricks, ghi bảng Delta trong Unity Catalog. Đây là **incremental batch** theo (ngày, giờ),
không phải streaming hay real-time. Đường chính của deployment hiện tại vẫn là dự phòng (Spark trên
runner, gold trên MotherDuck, `docs/probe-result.md`); job này là bản chạy song song để so sánh.

Dữ liệu là GH Archive thật (không phải simulated).

## Đường chạy

```
GitHub Actions runner                         Databricks (serverless)
---------------------                         -----------------------
python -m repopulse.ingest  -> data/landing/gharchive/date=.../hour=.../
pip wheel --no-deps         -> dist/repopulse-<version>-py3-none-any.whl
python -m repopulse.databricks_job run
  1. Unity Catalog: schema + managed Volume  (đã có thì bỏ qua)
  2. salt PII -> secret scope                (job chỉ mang tên scope/key, không mang giá trị)
  3. Files API: landing + wheel -> Volume    (overwrite=true)
  4. Workspace API: import notebook          (%pip install wheel, gọi databricks_entry.run)
  5. Jobs API: runs/submit (serverless, idempotency_token) -> chờ -> runs/get-output (JSON summary)
  6. --verify-rerun: submit lại cùng giờ, silver phải thêm 0 dòng
  7. một dòng runs.csv (variant=databricks-bronze-silver) -> out/databricks-row.csv
```

- Client (`repopulse/databricks_job.py`) chỉ dùng thư viện chuẩn; mọi HTTP qua một `transport`
  tiêm được nên unit test không gọi Databricks thật (`tests/test_databricks_job.py`).
- Notebook gọi `repopulse.spark.databricks_entry.run`, hàm này kiểm tham số rồi gọi
  `run_batch(..., catalog=..., schema=..., persist=False)`: serverless là Spark Connect, không có
  `persist` và không có API theo đường dẫn Delta, nên bảng được đọc/ghi theo tên.
- Run thất bại: thông báo lỗi gồm `state_message` và dòng `error` của task (lấy từ
  `runs/get-output`, cắt ngắn); mọi thông báo đi qua `Redactor` (host, token, user, salt).

## Chạy

Workflow `.github/workflows/databricks-bronze-silver.yml`:

- `workflow_dispatch` (chỉ hiện ở tab Actions sau khi file lên `main`): input `date`, `hour`,
  `hours` đều tuỳ chọn. Để trống `date` thì dùng ngày chuẩn của benchmark
  (`benchmarks/reference.toml`, `data_date`), giờ cuối 23, 3 giờ.
- `push` lên nhánh `feature/databricks-bronze-silver` (lọc `paths`) để chạy được ngay trong PR, luôn
  với cửa sổ ngày chuẩn nói trên. Không có `pull_request`. Nhánh bị xoá sau merge thì trigger này
  hết tác dụng.
- Thiếu `DATABRICKS_HOST`: bước Databricks bị bỏ qua ở cấp step, có `::notice`, workflow vẫn xanh,
  không có dòng `runs.csv`.
- Đường B của `.github/workflows/path-comparison.yml` (D3, `docs/path-comparison.md`) gọi cùng lệnh
  `run` (không `--verify-rerun`) theo lịch hằng giờ sau release, trên cùng bảng
  `workspace.repopulse.*`. Job đó dùng chung nhóm concurrency `databricks-bronze-silver` với
  workflow này, nên hai bên không cùng ghi silver một lúc.

Chạy tay (cần ba biến môi trường dưới đây, một thư mục landing đã ingest và một wheel):

```bash
python -m repopulse.ingest --date 2026-10-25 --hour 23 --hours 3 --landing-dir data/landing/gharchive --summary out/ingest-summary.json
python -m pip wheel --no-deps --wheel-dir dist .
python -m repopulse.databricks_job run --source gharchive --date 2026-10-25 --hour 23 --hours 3 --landing-dir data/landing/gharchive --wheel dist/repopulse-0.0.1-py3-none-any.whl --git-sha <sha> --verify-rerun --summary out/databricks-summary.json --out out/databricks-row.csv
```

Cờ khác: `--catalog` (mặc định `workspace`), `--schema` (`repopulse`), `--volume`
(`repopulse_landing`), `--secret-scope` (`repopulse`), `--secret-key` (`pii_hash_salt`),
`--late-window-hours`, `--job-timeout` (giây mỗi run; workflow đặt 1800), `--poll-interval`,
`--add-mask` (in `::add-mask::` cho hostname và user name, dùng trên GitHub Actions). Mã thoát: 0
thành công, 1 lỗi (thiếu biến, lỗi API, run thất bại, re-run thêm dòng), 2 sai tham số.

## Secret

| Biến | Dùng cho |
|---|---|
| `DATABRICKS_HOST` | URL workspace; trống thì workflow bỏ qua bước Databricks |
| `DATABRICKS_TOKEN` | personal access token (Bearer) cho REST API |
| `PII_HASH_SALT` | salt SHA-256 băm PII ở silver; client đưa vào secret scope, không vào payload job |

Chủ repo tự thêm secret. Secret chỉ vào workflow qua `env:` của step, không step nào in ra.

## Bảng Unity Catalog

Mặc định `workspace.repopulse` (`repopulse/spark/tables.py`):

| Bảng | Nội dung | Partition |
|---|---|---|
| `bronze_gharchive_events` | mỗi dòng landing nguyên văn (`raw_json`, `raw_sha256`) + nguồn gốc | `data_date` |
| `quarantine_gharchive_events` | lý do + thông báo lỗi + nguồn gốc, không có payload | `data_date` |
| `silver_gharchive_events` | event hợp lệ, khử trùng theo `event_id`, email/tên tác giả commit đã băm | `event_date` |

Volume `repopulse_landing`: `landing/gharchive/date=YYYY-MM-DD/hour=HH/` (cùng layout với
`repopulse.ingest`) và `wheels/<git_sha>/`. Notebook nằm ở `/Users/<user>/repopulse/`.
Quy tắc phân loại, khử trùng, cửa sổ đến muộn và băm PII giống hệt bản trên runner:
`docs/spark-jobs.md`.

## Idempotent theo (ngày, giờ)

- Files API ghi `overwrite=true` vào đường dẫn chỉ phụ thuộc (nguồn, ngày, giờ, tên file).
- Bronze và quarantine ghi đè đúng các lát (ngày, giờ) của lần chạy bằng `replaceWhere`.
- Silver dùng MERGE chỉ chèn khoá chưa có, nên chạy lại cùng giờ thêm 0 dòng; `--verify-rerun`
  kiểm điều này trên Databricks thật, re-run thêm dòng thì exit 1 và không ghi dòng `runs.csv`.
- `runs/submit` mang `idempotency_token` từ (nguồn, ngày, giờ, số giờ, khoá chạy); khoá chạy là
  `GITHUB_RUN_ID-GITHUB_RUN_ATTEMPT`, nên request gửi lại do retry không sinh run thứ hai.

## Giới hạn Free Edition

- Chỉ compute serverless, chỉ batch; vượt quota thì compute tắt tới hết ngày, hôm sau chạy lại
  cùng giờ an toàn vì job idempotent.
- Tối đa 5 job task đồng thời; workflow dùng một `concurrency` group, không huỷ run đang chạy.
- Truy cập internet theo danh sách domain: vì vậy runner tải GH Archive rồi đẩy lên Volume, job
  không tự tải.
- Đĩa runner 14 GB giới hạn số giờ mỗi lần ingest (`repopulse.ingest` tối đa 6 giờ).

## Gộp số đo vào `runs.csv`

Artifact `databricks-bronze-silver` chứa `out/databricks-row.csv`, `out/databricks-summary.json`,
`out/ingest-summary.json`. Tải artifact rồi:

```bash
python benchmarks/record_run.py out/databricks-row.csv
python -m repopulse.readme_results
```

Dòng có `variant=databricks-bronze-silver`; `total_seconds` là từ submit tới khi run đầu kết thúc,
`row_count` là số dòng silver chèn thêm; `notes` ghi số dòng bronze/quarantine, số dòng trùng bị
bỏ, thời gian upload, thời gian từng pha của run (nếu API trả về), và `rerun_silver_inserted`.
Số đo chỉ trích từ `runs.csv`.
