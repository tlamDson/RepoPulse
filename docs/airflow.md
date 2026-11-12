# Airflow DAG: backfill, retry, SLA

`dags/repopulse_pipeline.py` là phương án điều phối bằng **Airflow 3.4.0** cho cùng chuỗi
**incremental batch** của `.github/workflows/pipeline.yml` (không phải streaming hay real-time):

```
ingest -> spark_bronze_silver -> export_silver -> dbt_build
```

Cron của `pipeline.yml` vẫn giữ nguyên: hai cách điều phối được đo cạnh nhau. Việc thay hẳn cron
(nếu có) thuộc việc khác. GH Archive là dữ liệu thật.

## Cấu trúc

| File | Vai trò |
|---|---|
| `repopulse/dagspec.py` | Spec thuần (stdlib, không import `airflow`): task, lệnh bash, retry, timeout, SLA, cửa sổ backfill, parser `list-runs`, vòng chờ, dòng `runs.csv`. Test ở `tests/test_dagspec.py` |
| `dags/repopulse_pipeline.py` | Dựng DAG Airflow từ spec (`BashOperator`, `DeadlineAlert`). Test DagBag ở `tests/test_airflow_dag.py` |
| `.github/workflows/airflow.yml` | `airflow standalone` trên runner, backfill N ngày, ghi dòng đo vào artifact |

Thư mục là `dags/`, không bao giờ đặt tên `airflow/` (sẽ che package `airflow` thật vì pytest chạy
với `pythonpath = ["."]`).

## DAG

- `schedule="@daily"`, `catchup=False`, `max_active_runs=1`, `start_date=2025-01-01`.
- **Run của ngày logical D xử lý dữ liệu ngày D − 1** (`{{ macros.ds_add(ds, -1) }}`): ngày đã đủ
  khi run bắt đầu. Airflow 3 mặc định dùng `CronTriggerTimetable`, nên `ds` là ngày run được kích
  hoạt.
- Mỗi run chỉ xử lý `REPOPULSE_AIRFLOW_HOURS_PER_DAY` giờ (mặc định **1**) kết thúc ở giờ
  `REPOPULSE_AIRFLOW_HOUR` (mặc định 12) của ngày dữ liệu: đĩa runner 14 GB, một giờ GH Archive đầy
  đủ đã nặng. Đây là mẫu của ngày, không phải cả ngày; dòng đo ghi rõ `hours_per_day=` và
  `last_hour=`.
- Lệnh của task giống hệt `pipeline.yml`, chạy bằng Python của project (`REPOPULSE_PYTHON`) với
  `cwd` là gốc repo (`REPOPULSE_HOME`). Summary của từng ngày ghi vào `out/airflow/<ngày>/`.
- Idempotent theo (ngày, giờ): ingest bỏ qua giờ đã có manifest, silver dedup theo event id, staging
  dbt giữ một dòng mỗi event. DAG không thêm trạng thái nào mới.

### Retry

| Task | retries | retry_delay | backoff | timeout |
|---|---|---|---|---|
| `ingest` | 3 | 120 s | exponential | 30 phút |
| `spark_bronze_silver` | 1 | 60 s | không | 45 phút |
| `export_silver` | 1 | 60 s | không | 15 phút |
| `dbt_build` | 1 | 60 s | không | 30 phút |

`ingest` thêm `python -m repopulse.dagspec check-ingest`: giờ `missing` (GH Archive chưa publish)
hay `error` làm task fail, để retry có backoff chờ file xuất hiện, thay vì để Spark chạy trên
landing rỗng.

### SLA = Deadline Alerts

Airflow 3 đã gỡ `sla=` và `sla_miss_callback`. DAG dùng **Deadline Alert** (từ 3.1, còn
experimental): `DeadlineAlert(reference=DeadlineReference.DAGRUN_QUEUED_AT,
interval=REPOPULSE_AIRFLOW_SLA_MINUTES (mặc định 30 phút), callback=AsyncCallback(...))`.
Callback là `repopulse.dagspec.record_sla_miss` (hàm `async` top-level, Triggerer import được),
ghi một dòng JSON vào `REPOPULSE_SLA_MISS_FILE` (mặc định `out/airflow/sla-miss.jsonl`).

Chọn `AsyncCallback` vì là dạng có từ bản đầu của Deadline Alerts và không chiếm slot executor.
Với backfill, mọi run được xếp hàng cùng lúc nên `DAGRUN_QUEUED_AT` tính cả thời gian chờ run
trước: callback có thể báo trễ dù run nhanh. Vì vậy **số `sla_missed` của dòng đo không lấy từ
callback** mà tính từ thời lượng run (`end_date − start_date > SLA`) trong
`airflow dags list-runs`.

## Backfill trên runner

`.github/workflows/airflow.yml`:

| Trigger | Dữ liệu |
|---|---|
| `workflow_dispatch` (chỉ khi file có trên `main`, tức sau release) | input `days` (mặc định 3, tối đa 7), `last_data_date` (mặc định 2026-10-25), `hour` (15), `hours_per_day` (1) |
| `pull_request` (lọc `paths:`, không phải required check) | smoke 2 ngày tới 2026-10-25, giờ 15, 1 giờ/ngày (cố định để lặp lại được) |

Các bước: cài project (`.[dev,spark,dbt]`), tạo **venv riêng** cho Airflow (`apache-airflow==3.4.0`
với constraints 3.11, cộng `pip install --no-deps -e .` để DAG import được `repopulse.dagspec`),
test DagBag với `REPOPULSE_REQUIRE_AIRFLOW=1` (thiếu Airflow là fail), `dagspec window` (đặt
`BACKFILL_FROM/TO` và `REPOPULSE_AIRFLOW_END_DATE`), `airflow db migrate`, `airflow standalone`
chạy nền, chờ DAG được nạp, `airflow dags unpause`, `airflow backfill create --max-active-runs 1`,
`dagspec wait` (thăm dò `airflow dags list-runs -o json`), `dagspec row`.

`REPOPULSE_AIRFLOW_END_DATE` đặt `end_date` của DAG ở trưa ngày logical cuối: các run backfill (nửa
đêm) nằm trong khoảng, còn scheduler không tự tạo run theo lịch khi DAG được unpause, nên không có
run nào chạy chen với backfill.

dbt ghi DuckDB local (`--target ci`) với salt giả `not-a-secret-pr-salt`; workflow không cần
secret, không đụng MotherDuck. Job fail khi có run backfill `failed` hoặc hết giờ chờ
(`25 × days` phút).

Artifact `airflow-backfill-<run_id>`: `out/` (dòng đo, `backfill-summary.json`, JSON thô của
`list-runs`, summary từng ngày, `sla-miss.jsonl` nếu có) và `out/airflow/airflow-logs.tar.gz`
(`standalone.log` + log task của Airflow; đóng gói vì đường dẫn log task chứa dấu `:` mà
`upload-artifact` không nhận).

Lệnh tương đương chạy tay (Linux; Airflow không chạy trên Windows):

```bash
python -m repopulse.dagspec window --last-data-date 2026-10-25 --days 3      # in BACKFILL_FROM/TO...
airflow backfill create --dag-id repopulse_pipeline --from-date <FROM> --to-date <TO> --max-active-runs 1
python -m repopulse.dagspec wait --last-data-date 2026-10-25 --days 3 --started-at <ISO> \
  --timeout-minutes 75 --summary out/airflow/backfill-summary.json --raw out/airflow/list-runs.json
python -m repopulse.dagspec row --summary out/airflow/backfill-summary.json --run-at <ISO> \
  --git-sha <sha> --run-id <id> --trigger manual --out out/airflow-backfill-row.csv
```

## Dòng đo

`variant=airflow-backfill-<N>d`, `data_date` = ngày dữ liệu cuối, `total_seconds` = wall clock từ
lúc tạo backfill tới khi run cuối xong (gồm cả độ trễ lập lịch của Airflow), `row_count` = tổng
`silver_inserted` của các ngày (trống nếu thiếu summary một ngày), `notes`:
`engine=airflow-3.4.0 days= hours_per_day= last_hour= runs_success= runs_failed= sla_minutes=
sla_missed= max_active_runs= target=ci run_id= trigger=`. Chỉ backfill thành công mới có dòng.

Gộp vào `runs.csv` từ artifact:

```bash
python benchmarks/record_run.py out/airflow-backfill-row.csv
python -m repopulse.readme_results
```

## Giới hạn

- Airflow standalone + SQLite chỉ để dev; không phải triển khai thật.
- Deadline Alerts còn experimental; API có thể đổi giữa các bản 3.x.
- Mỗi ngày chỉ 1 giờ dữ liệu (mặc định): số đo là của điều phối + chuỗi xử lý, không phải xử lý cả
  ngày GH Archive.
- Airflow không có trong extras của `pyproject.toml` (constraints xung đột dbt/pyspark): bản pin
  nằm trong workflow.
