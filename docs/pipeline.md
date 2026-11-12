# Pipeline theo lịch: ingest → Spark → export → dbt → runs.csv

`.github/workflows/pipeline.yml` nối các bước đã có thành một **incremental batch** chạy mỗi giờ
(không phải streaming hay real-time). Đường dự phòng (`docs/probe-result.md`): Spark chạy trên
runner, gold nằm trên MotherDuck. GH Archive là dữ liệu thật.

```
python -m repopulse.ingest            data/landing/gharchive   (docs: repopulse/ingest.py)
python -m repopulse.pipeline window   DATA_DATE / DATA_HOUR / DATA_HOURS -> $GITHUB_ENV
python -m repopulse.spark.job         data/lake (bronze/silver/quarantine)   docs/spark-jobs.md
python -m repopulse.spark.export      data/silver-export/*.parquet           docs/spark-jobs.md
dbt build --vars silver_gharchive_parquet=...   MotherDuck (incremental)     docs/dbt-models.md
python -m repopulse.pipeline row      out/pipeline-row.csv (artifact)
```

Bốn bước nặng chạy qua `python -m repopulse.pipeline timed --name <bước> --timings out/timings.json
-- <lệnh>`, ghi thời gian tường của từng bước; dòng `runs.csv` lấy tổng các bước làm
`total_seconds`.

## Trigger

| Trigger | Dữ liệu | Target dbt | Secret |
|---|---|---|---|
| `schedule` (phút 23 mỗi giờ) | 3 giờ kết thúc ở now − 2h | `motherduck`, database `repopulse` | `MOTHERDUCK_TOKEN`, `PII_HASH_SALT` (thiếu thì dừng ngay bước đầu) |
| `workflow_dispatch` | input `date`/`hour`/`hours` (mặc định 3), `force`, `full_refresh` | như trên | như trên |
| `pull_request` (lọc `paths:`) | 1 giờ thật (now − 2h) | `ci` (DuckDB local), salt giả cố định | không cần; có `MOTHERDUCK_TOKEN` thì thêm bước smoke |

`PII_HASH_SALT` vào cả bước Spark (băm PII ở silver) lẫn mọi bước `dbt build`: các model `public`
che `user_key`/`actor_id`/`actor_login` bằng `mask_pii` với cùng salt (`docs/governance.md`), và
ngoài target `ci` thiếu salt là lỗi compile. Bước chỉ chạy trên pull request dùng salt giả cố định,
không nhận secret thật.

`schedule` và `workflow_dispatch` chỉ chạy khi file đã có trên `main` (sau release). Hai trigger này
chung một nhóm concurrency (`pipeline-motherduck`, xếp hàng, không huỷ): mỗi lúc chỉ một lần ghi
MotherDuck. Mỗi PR một nhóm riêng.

Mỗi lần chạy lấy 3 giờ dù cron chạy mỗi giờ: giờ GH Archive công bố muộn (lúc chạy còn `missing`)
được lần sau nhặt lại. Giờ đã nạp thì staging bỏ qua theo `event_id`, nên chạy chồng không sinh
trùng. Cron hourly của `ingest.yml` đã bỏ (nó chỉ còn `workflow_dispatch` và `pull_request`), để
không tải mỗi giờ hai lần.

## Pull request

Không đụng database `repopulse`. Chuỗi chạy trên `ci.duckdb` rồi:

1. `dbt build` lần hai trên cùng parquet, so `python -m repopulse.pipeline counts` trước/sau bằng
   `diff`: chạy lại không được đổi số dòng của staging và gold.
2. Có `MOTHERDUCK_TOKEN`: `md-reset --database repopulse_smoke` (chỉ nhận tên `*_smoke`), build
   target `motherduck` hai lần với `MOTHERDUCK_DATABASE=repopulse_smoke`, so số dòng.

Dòng `runs.csv` của PR có `variant=pipeline-pr-smoke`, của lịch/dispatch là `pipeline-incremental`.

## runs.csv

Workflow chỉ đọc repo (`contents: read`), không commit. Dòng nằm ở artifact
`pipeline-run-<run_id>` (`out/pipeline-row.csv`, cùng summary ingest/Spark/export và
`timings.json`; không upload lake hay landing). Gộp vào `runs.csv` bằng:

```bash
python benchmarks/record_run.py <thư mục artifact>/pipeline-row.csv [<artifact khác>.csv ...]
```

Mọi dòng của mọi artifact được kiểm trước; một artifact hỏng thì không dòng nào được ghi.

| Cột | Giá trị |
|---|---|
| `data_date` | ngày của giờ dữ liệu cuối |
| `total_seconds` | tổng thời gian các bước trong `timings.json` |
| `row_count` | `silver_inserted` của job Spark (số event vào silver của lần chạy) |
| `q1_seconds`…`q6_seconds`, `peak_rss_mb`, `bytes_read` | để trống (pipeline không đo query) |
| `notes` | `key=value` cách nhau bởi dấu cách: `last_hour`, `hours`, `ingested`/`skipped`/`missing`, `<bước>_s`, `run_id`, `trigger`, `target`, `full_refresh` |

Tài liệu này không chứa số đo; số đo chỉ lấy từ `runs.csv`.

Sau mỗi lần chạy theo lịch, `.github/workflows/data-quality.yml` (`workflow_run`) kiểm khối lượng
và độ tươi của staging và mở/cập nhật Issue `data-incident`: `docs/data-quality.md`.

## Lệnh

```bash
python -m repopulse.pipeline window --ingest-summary out/ingest-summary.json
python -m repopulse.pipeline timed --name dbt --timings out/timings.json -- <lệnh>
python -m repopulse.pipeline counts --database ci.duckdb --out out/counts.json
python -m repopulse.pipeline row --run-at <ISO có múi giờ> --git-sha <sha> --ingest-summary out/ingest-summary.json --spark-summary out/spark-summary.json --timings out/timings.json --run-id <id> --trigger <tên> --out out/pipeline-row.csv
python -m repopulse.pipeline md-reset --database repopulse_smoke   # cần MOTHERDUCK_TOKEN
```

Test: `tests/test_pipeline.py` (helper, không mạng, không Spark), `tests/test_pipeline_workflow.py`
(kiểm tĩnh `pipeline.yml`), `tests/test_spark_export.py`, `tests/test_dbt_incremental.py`.
