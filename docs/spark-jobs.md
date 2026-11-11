# Spark jobs: bronze → silver (incremental batch)

`repopulse/spark/` biến các giờ đã land (`repopulse.ingest` cho GH Archive, landing app events)
thành bảng Delta bronze / silver / quarantine. Đây là **incremental batch** (mỗi lần chạy xử lý một
danh sách (ngày, giờ) đã land), không phải streaming hay real-time. Đường đã chọn là dự phòng:
PySpark 3.5 + Delta 3.3 chạy local trên runner GitHub Actions (4 CPU, 16 GB RAM, 14 GB đĩa),
không chạy trên Databricks.

- GH Archive là dữ liệu thật (sự kiện GitHub công khai).
- App events là dữ liệu **simulated** (app lập kế hoạch chuyến đi mô phỏng, catalog `eventspec`),
  không phải người dùng thật; bảng silver có cột `simulated` (luôn `true`).

## Chạy

```bash
pip install -e ".[dev,spark]"     # cần Java 17 (PySpark 3.5); Windows cần thêm winutils
PII_HASH_SALT=<salt> python -m repopulse.spark.job --source gharchive --date 2026-01-05 --hour 15 --hours 2 --summary out/spark-summary.json
python -m repopulse.spark.job --source app --date 2026-01-05 --hour 15   # landing mặc định data/landing/app
```

| Tham số | Mặc định | Ghi chú |
|---|---|---|
| `--source` | (bắt buộc) | `gharchive` hoặc `app` |
| `--date`, `--hour` | (bắt buộc) | giờ **cuối** (UTC) |
| `--hours` | 1 | 1..24 giờ liên tiếp kết thúc ở `--hour` (giống `repopulse.ingest`) |
| `--landing-dir` | `data/landing/gharchive` / `data/landing/app` | |
| `--lake-dir` | `data/lake` | |
| `--late-window-hours` | 24 | cửa sổ nhận event đến muộn |
| `--summary` | | ghi summary JSON |

`PII_HASH_SALT` bắt buộc, không có giá trị mặc định; thiếu hoặc rỗng thì job thoát mã 1 **trước
khi** khởi động Spark. Salt không bao giờ được in ra log hay summary (hash ở silver chạy bằng UDF
Python nên salt không nằm trong query plan).

Session: `local[*]`, `spark.sql.shuffle.partitions=8`, múi giờ phiên UTC, driver 8 GB.

## Landing đọc vào

```
<landing>/date=YYYY-MM-DD/hour=HH/
    events.json.gz         GH Archive (repopulse.ingest), đọc bằng Spark theo đường dẫn file
    _quarantine.jsonl.gz   GH Archive, dòng ingest đã loại; đọc bằng Python trên driver
    events.jsonl.gz        app events (simulated)
```

Spark bỏ qua file tên bắt đầu bằng `_`/`.` khi liệt kê thư mục, nên `_quarantine.jsonl.gz` đọc trên
driver. File đọc bằng đường dẫn tường minh để `date=`/`hour=` không bị suy diễn thành cột partition.
Giờ chưa có file landing thì không có dòng nào cho giờ đó.

## Bảng (`<lake>/…`)

| Bảng | Partition | Cột |
|---|---|---|
| `bronze/gharchive_events`, `bronze/app_events` | `data_date` | `raw_json` (nguyên văn), `raw_sha256`, `data_date`, `hour`, `source_file`, `line_number` (từ 1, theo file), `ingested_at` |
| `silver/gharchive_events` | `event_date` | `event_id`, `event_type`, `created_at` (timestamp), `event_date`, `actor_id`, `actor_login`, `repo_id`, `repo_name`, `action`, `pr_number`, `issue_number`, `pr_merged`, `commit_author_email_hashes`, `commit_author_name_hashes`, `landing_date`, `landing_hour`, `is_late` |
| `silver/app_events` (simulated) | `event_date` | `dedup_key`, `event_id`, `event_name`, `event_version`, `occurred_at`, `received_at`, `user_id`, `session_id`, `simulated`, `event_date`, `properties` (JSON trường riêng, PII đã băm), `landing_date`, `landing_hour`, `is_late` |
| `quarantine/gharchive_events`, `quarantine/app_events` | `data_date` | `reason`, `errors`, `raw_sha256`, `data_date`, `hour`, `source_file`, `line_number`, `event_type` / `event_name` |

Bronze là vùng raw hạn chế: được giữ PII thô. Silver và quarantine **không** giữ payload thô; muốn
xem dòng gốc của một bản ghi quarantine thì tra bronze theo `raw_sha256`.

`pr_number` = `payload.pull_request.number`, không có thì `payload.number`; `issue_number` =
`payload.issue.number` (Q4/Q6 coalesce hai cột này).

## Luật

**Schema → quarantine** (`reason`, kèm `errors` chi tiết):

| reason | Khi nào |
|---|---|
| `malformed_json` | GH: mọi dòng trong `_quarantine.jsonl.gz` của landing; app: dòng không parse được JSON |
| `schema_invalid` | GH: thiếu `id`/`type`/`actor.id`/`repo.id`/`repo.name`, id không phải số nguyên, `created_at` không phải RFC 3339; app: `eventspec.validate` báo lỗi (thông báo giữ nguyên văn) |
| `unknown_event` | app: `event_name` lạ hoặc `event_version` chưa khai báo |
| `late_beyond_window` | đến muộn quá cửa sổ |

**Khử trùng.** GH theo `id`; app theo `eventspec.dedup_key` (`event_id`, không có thì băm
`dedup_fields`). Trong một lô giữ bản đầu theo (`data_date`, `hour`, `source_file`,
`line_number`). So với silver đã có thì bản trong silver thắng: MERGE chỉ chèn khoá chưa có
(silver GH cắt partition theo các `event_date` của lô). Bản trùng không vào quarantine, chỉ đếm ở
`duplicates_dropped`.

**Đến muộn** (`repopulse.spark.rules.classify_arrival`). Arrival: GH = đầu giờ landing (UTC); app =
`received_at`, không có thì đầu giờ landing. Event time: GH `created_at`, app `occurred_at`. Event
muộn khi nằm ở giờ UTC sớm hơn giờ của arrival; muộn mà `arrival - event_time <= cửa sổ` (bằng
cửa sổ vẫn nhận) thì vào silver ở partition `event_date` của event, `is_late = true`; quá cửa sổ thì
quarantine `late_beyond_window`.

**PII.** Băm trước khi ghi silver: `sha256(salt + value.strip().lower())` hex
(`repopulse.spark.pii.hash_pii`). GH: `payload.commits[].author.email` và `.name` của PushEvent
chỉ còn dạng mảng hash. App: mọi trường `pii: true` của catalog (`email`, `recipient_email`) được
thay bằng hash trong `properties`.

**Idempotent.** Bronze và quarantine ghi đè đúng lát (ngày, giờ) bằng `replaceWhere`; silver chỉ
chèn khoá mới. Chạy lại cùng giờ không đổi số dòng của bảng nào.

## Summary

```json
{"source": "gharchive", "hours": [["2026-01-05", 10]], "bronze_rows": 0, "silver_inserted": 0,
 "duplicates_dropped": 0, "quarantined": {"schema_invalid": 0}, "quarantined_total": 0,
 "late_accepted": 0}
```

(Giá trị trên chỉ minh hoạ dạng, không phải số đo.) Với lake trống:
`bronze_rows == silver_inserted + duplicates_dropped + quarantined_total` — không dòng nào bị bỏ
im lặng. `late_accepted` là số event muộn được chèn vào silver.

## Export silver cho dbt

```bash
python -m repopulse.spark.export --lake-dir data/lake --out data/silver-export --summary out/export-summary.json
```

Đọc `silver/gharchive_events` **qua Delta** (không glob thẳng thư mục Delta, vì như thế bỏ qua
`_delta_log`) rồi ghi đè `--out` bằng parquet thường, đúng 17 cột silver theo thứ tự ở bảng trên,
timestamp dạng `TIMESTAMP_MICROS` (UTC). Lake chưa có silver (mọi giờ đều thiếu) thì ghi parquet
rỗng vẫn mang schema, summary `{"silver_table": "missing", "rows": 0, ...}`. dbt đọc kết quả qua var
`silver_gharchive_parquet` (`docs/dbt-models.md`); cả chuỗi theo lịch ở `docs/pipeline.md`.

## Test

`tests/test_spark_*.py`: phần thuần Python (`pii`, `rules`, CLI) chạy ở mọi máy; test đánh dấu
`@pytest.mark.spark` dùng fixture `spark` (`local[1]`) trong `tests/conftest.py`, tự skip ở local khi
thiếu pyspark/delta, Java 17 hoặc winutils, và luôn chạy trên CI (job `Unit Tests` cài JDK 17).
