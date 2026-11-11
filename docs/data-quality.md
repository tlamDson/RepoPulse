# Chất lượng dữ liệu: lỗi chèn, khối lượng, độ tươi, cảnh báo Issue

`repopulse/quality.py` + `.github/workflows/data-quality.yml` kiểm chất lượng dữ liệu của
**incremental batch** theo giờ (không phải streaming hay real-time). Hai nguồn, hai loại kiểm:

- **App events — dữ liệu simulated** (`simkit`, không có người dùng thật): chèn 6 loại lỗi biết
  trước, cho chạy qua job Spark bronze/silver thật, mỗi loại lỗi phải bị một kiểm tra riêng bắt
  được; chạy sạch (không chèn lỗi) thì không được có báo động nào.
- **GH Archive — dữ liệu thật**: khối lượng theo giờ landing và độ tươi của bảng
  `main_staging.stg_gharchive_events`; có bất thường thì mở/cập nhật GitHub Issue nhãn
  `data-incident`.

Tài liệu này không chứa số đo; số đo (M4) chỉ lấy từ `runs.csv`.

## Ngưỡng ở đâu

Mọi ngưỡng nằm trong `repopulse/quality.toml` (đóng gói cùng package, nạp bằng
`repopulse.quality.load_config`; thiếu khoá hoặc sai kiểu thì lỗi nêu đúng tên khoá). Code không
hardcode ngưỡng và **không** lấy hằng số từ `simkit`: kiểm tra độc lập với bộ sinh lỗi. Muốn chạy
với ngưỡng khác: `python -m repopulse.quality --config <file.toml> <lệnh> ...`.

| Mục | Khoá | Nghĩa |
|---|---|---|
| `[faults]` | `late_threshold_seconds`, `bot_events_per_window`, `bot_window_seconds` | ngưỡng của kiểm tra lỗi |
| `[faults.inject]` | `duplicate`, `null`, `wrong_type`, `late`, `schema_drift`, `bot` | số lỗi harness chèn mỗi loại |
| `[volume]` | `table`, `window_hours`, `min_history`, `min_ratio`, `evaluate_hours`, `type_min_median` | kiểm khối lượng |
| `[volume.reference]` | `history`, `evaluate` | giờ GH Archive chuẩn (đủ dữ liệu) và giờ thiếu đã biết |
| `[freshness]` | `table`, `column`, `max_lag_hours` | kiểm độ tươi |
| `[alert]` | `label`, `template` | nhãn Issue, file mẫu Issue |

## Sáu loại lỗi → kiểm tra bắt được

Kiểm tra (`detect_*`, gộp trong `run_fault_checks`) chỉ đọc dòng của bảng lake
(`docs/spark-jobs.md`), **không** nhìn manifest của simkit. Manifest là đáp án, chỉ dùng ở bước
chấm điểm `score`.

| Loại lỗi simkit | Kiểm tra (`check`) | Hàm | Bắt bằng |
|---|---|---|---|
| `duplicate` | `duplicate_key_in_bronze` | `detect_duplicates` | khoá `eventspec.dedup_key` xuất hiện > 1 lần ở bronze (bronze giữ mọi bản) |
| `null` | `quarantine_null_field` | `detect_null` | quarantine `schema_invalid` có lỗi `must be of type …, got null` |
| `wrong_type` | `quarantine_wrong_type` | `detect_wrong_type` | quarantine `schema_invalid` có `must be of type T, got U` (U khác `null`) |
| `schema_drift` | `quarantine_schema_drift` | `detect_schema_drift` | quarantine có `unexpected field …`, hoặc reason `unknown_event` (version chưa khai) |
| `late` | `late_arrival`, `late_beyond_window` | `detect_late` | silver có `received_at - occurred_at > late_threshold_seconds`; hoặc quarantine `late_beyond_window` |
| `bot` | `bot_rate` | `detect_bots` | user ở silver có > `bot_events_per_window` event trong một cửa sổ `bot_window_seconds` bất kỳ |

Kiểm tra phụ, không ứng với loại lỗi nào (có finding là báo động giả hoặc pipeline sai):
`silver_duplicate_key` (silver còn > 1 dòng một khoá), `duplicates_not_dropped`
(`duplicates_dropped` của summary nhỏ hơn số bản trùng hợp lệ ở bronze), `quarantine_other`
(quarantine vì lý do khác, ví dụ `malformed_json`).

`late` **không** dùng cột `is_late` của silver: Spark coi event là muộn khi nằm ở giờ UTC sớm hơn
giờ arrival, nên event sạch vắt qua ranh giới giờ (trễ vài giây) cũng `is_late = true` và sẽ thành
báo động giả. Kiểm theo độ trễ so với ngưỡng trong config.

**Chấm điểm** (`score`): một lỗi trong manifest là "bắt được" khi có finding **cùng loại** chỉ
đúng event của nó (`event_id`), hoặc với bot là đúng `user_id`. Quarantine không giữ payload nên
finding quarantine mang `raw_sha256`; harness giữ bảng `sha256 dòng landing → event_id` khi ghi
landing để nối. Finding không khớp lỗi nào cùng loại là **báo động giả**.

## Harness `faults`

```bash
PII_HASH_SALT=<salt> python -m repopulse.quality faults --seed 42 --users 30 --days 2 \
    --clean-runs 2 --work-dir data/tmp/dq-faults --git-sha <sha> \
    --report out/dq-faults.json --out out/dq-fault-row.csv
```

1. `simkit.simulate` + `inject_faults` (counts từ `[faults.inject]`, seed cố định).
2. Ghi landing app `<work>/faulted/landing/date=YYYY-MM-DD/hour=HH/events.jsonl.gz`; giờ landing =
   giờ của `received_at` (trường này hỏng vì lỗi null/wrong_type thì dùng giờ của event đứng trước).
   Event `late` có thể đến muộn tới vài ngày nên landing kéo dài quá khoảng mô phỏng.
3. `repopulse.spark.job.run_batch` nguồn `app` trên mọi giờ đã ghi (một lô), đọc lại đúng các lát
   đó của bronze/silver/quarantine → `run_fault_checks` → `score`.
4. `--clean-runs K` lần chạy sạch (seed+1 … seed+K, không chèn lỗi): mọi finding đều là báo động
   giả.

Cần Java 17 + `pip install -e ".[dev,spark]"`, và `PII_HASH_SALT` (thiếu thì thoát mã 1 **trước
khi** khởi động Spark). Exit 1 khi có loại lỗi bị sót hoặc có báo động giả. Lake và landing nằm ở
`--work-dir` (mặc định `data/tmp/dq-faults`, đã gitignore; không phải `out/`, không upload).

### Dòng `runs.csv` (M4)

`--out` ghi CSV (header + một dòng, cùng định dạng `repopulse.pipeline.write_row`):

| Cột | Giá trị |
|---|---|
| `variant` | `dq-fault-injection` |
| `data_date` | ngày bắt đầu mô phỏng (`--start`, mặc định `2026-11-01`) |
| `total_seconds` | thời gian tường của cả harness |
| `row_count` | số event (đã chèn lỗi) đưa vào pipeline |
| `notes` | `simulated=true seed= users= days= injected= caught= false_alarms= clean_runs=` và mỗi loại `duplicate=<bắt>/<chèn>` … `bot=<bắt>/<chèn>`; `false_alarms` gồm cả finding của các lần chạy sạch |

Gộp vào `runs.csv` từ artifact `data-quality-<run_id>`:

```bash
python benchmarks/record_run.py <thư mục artifact>/dq-fault-row.csv
```

## Khối lượng (GH Archive, dữ liệu thật)

`check_volume(hourly_counts, cfg)` với `{giờ UTC: {event_type: số event}}` (đếm theo
`landing_date`/`landing_hour` của bảng staging):

- Dựng chuỗi giờ **liên tục** từ giờ đầu tới giờ cuối; giờ không có dòng nào = 0.
- Mỗi giờ trong `evaluate_hours` giờ mới nhất so với **trung vị** của tối đa `window_hours` giờ
  liền trước nó:
  - tổng < `min_ratio` × trung vị tổng → `volume_drop`;
  - loại event có trung vị ≥ `type_min_median` mà giờ đang xét = 0 → `volume_type_missing`.
- Ít hơn `min_history` giờ lịch sử → không kết luận, không báo động (report ghi
  `insufficient_history`).

**Giờ thiếu thật** (`gap`): từ 2026-11 có giờ GH Archive mất hẳn PushEvent/CreateEvent/DeleteEvent
và khối lượng tụt mạnh (`docs/plan.md`). `[volume.reference]` khai giờ chuẩn đủ dữ liệu của ngày
chuẩn benchmark (`benchmarks/reference.toml`) làm lịch sử và một giờ thiếu làm giờ đang xét. `gap
--ingest` tải các giờ đó bằng `repopulse.ingest`, đếm theo `counts_by_type` của `_manifest.json`
(chỉ các loại event đã ingest), so như trên; exit 1 nếu giờ thiếu **không** bị bắt.

## Độ tươi

`check_freshness(latest_event_at, now, cfg)`: `now - max(created_at)` > `max_lag_hours` →
`freshness_stale` (bằng đúng ngưỡng thì chưa báo); bảng rỗng → `freshness_empty`. `now` luôn truyền
vào (CLI `--now` ISO-8601 có múi giờ, mặc định giờ hệ thống UTC).

```bash
python -m repopulse.quality check --database ci.duckdb --report out/dq-report.json
python -m repopulse.quality check --database md:repopulse --report out/dq-report.json   # cần MOTHERDUCK_TOKEN
```

`check` thoát 0 kể cả khi có báo động (báo động đi qua Issue), 1 khi lỗi chạy (không có database,
thiếu bảng, `--now` thiếu múi giờ).

## Cảnh báo GitHub Issue

```bash
python -m repopulse.quality alert --report out/dq-report.json --run-url <url lần chạy> --dry-run
python -m repopulse.quality alert --report out/dq-report.json --run-url <url> --repo owner/name   # cần GH_TOKEN
```

- `build_issue`: body có **đủ** mọi heading `## ` của `.github/ISSUE_TEMPLATE/data-incident.md`
  theo đúng thứ tự (đọc từ file mẫu, bỏ front matter): Triệu chứng (kèm link lần chạy Actions),
  Phạm vi (bảng, khoảng giờ dữ liệu), Nguyên nhân (gợi ý điều tra), Điều kiện kích hoạt (ngưỡng ở
  đâu), Cách xử lý và chạy bù (`workflow_dispatch` của `Pipeline (scheduled)` với `date`/`hour`/
  `hours`).
- Mỗi báo động có marker ẩn tất định `<!-- dq:<check>:<giờ dữ liệu> -->`.
- `publish`: mỗi loại kiểm tra (`volume`, `freshness`) tối đa **một** issue đang mở nhãn
  `data-incident` (tiêu đề bắt đầu `[dq:volume]` / `[dq:freshness]`):
  - chưa có → `gh label create data-incident --force` rồi `gh issue create`;
  - đã có và marker của báo động chưa có trong body/comment → `gh issue comment` chỉ với báo
    động mới;
  - mọi marker đã có → không làm gì. Chạy lại mỗi giờ không spam.
- `--dry-run` chỉ in tiêu đề và body, không gọi `gh`. Unit test luôn tiêm `gh` giả, không bao giờ
  tạo Issue thật. Token chỉ qua biến môi trường `GH_TOKEN`, không in ra.

## Workflow `.github/workflows/data-quality.yml`

| Job | Trigger | Làm gì | Quyền |
|---|---|---|---|
| `checks` | `workflow_run` (sau `Pipeline (scheduled)`), `workflow_dispatch`, `pull_request` | lịch/dispatch: `check --database md:repopulse` (thiếu `MOTHERDUCK_TOKEN` thì `::error::` và dừng); PR: `dbt build --target ci` (seed fixture) rồi `check --database ci.duckdb`. Report qua artifact `data-quality-report-<run_id>` | `contents: read` |
| `alert` | sau `checks` | PR: `alert --dry-run`; còn lại: `alert --repo $GITHUB_REPOSITORY` với `GH_TOKEN` | `contents: read`, **`issues: write`** (job duy nhất) |
| `fault-injection` | `pull_request`, `workflow_dispatch` | `faults` trên dữ liệu **simulated** với salt giả cố định `not-a-secret-pr-salt`; artifact `data-quality-<run_id>` (chỉ `out/`: report + `dq-fault-row.csv`, 30 ngày) | `contents: read` |
| `reference-gap` | `pull_request`, `workflow_dispatch` | `gap --ingest` trên giờ GH Archive thật; đỏ nếu giờ thiếu không bị bắt | `contents: read` |

- `workflow_run` bắt nguồn từ lần chạy pipeline của pull request bị bỏ qua (job-level `if`).
- `workflow_run` và `workflow_dispatch` chỉ chạy khi file đã có trên `main` (sau release).
- `pull_request` lọc `paths:` (workflow này không phải required check). Không `push`, `schedule`,
  `pull_request_target`.
- Concurrency: lịch/dispatch chung nhóm `data-quality-alerts` (xếp hàng, không huỷ) để hai lần chạy
  không cùng tạo Issue; mỗi PR một nhóm.
- DuckDB pin `duckdb==1.5.6` như `pipeline.yml`/`probe.yml` (MotherDuck từ chối client mới hơn).
- Không `${{ }}` trong `run:`; secret chỉ tới step qua `env:`.

## Test

| File | Phủ |
|---|---|
| `tests/test_quality.py` | từng kiểm tra lỗi trên dòng dựng tay (bắt được + dòng sạch, kể cả event sạch vắt ranh giới giờ), `score`, khối lượng, độ tươi, đếm từ DuckDB file tạm, landing manifest, `build_issue` so heading với file mẫu, `publish` với `gh` giả, ngưỡng đổi trong file config tạm thì kết luận đổi theo, dòng `runs.csv`, mã thoát CLI. Không Spark, không mạng |
| `tests/test_quality_faults_spark.py` (`@pytest.mark.spark`) | 6 loại lỗi simkit qua `run_batch` thật: mỗi loại bị bắt đủ bởi kiểm tra của nó, 0 báo động giả; chạy sạch riêng → 0 finding; chạy lại cùng giờ → cùng finding |
| `tests/test_data_quality_workflow.py` | kiểm tĩnh workflow (trigger, quyền, secret, dry-run trên PR, artifact không chứa lake) |
