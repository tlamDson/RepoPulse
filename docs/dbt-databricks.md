# dbt trên Databricks SQL warehouse (D2)

Việc D2 của Plan v2 (`docs/plan.md`): chạy **cùng** project dbt `transform/` trên một SQL
warehouse của Databricks Free Edition (`dbt build --target databricks`), đọc bảng silver Delta mà
job bronze/silver của D1 ghi vào Unity Catalog (`docs/databricks-job.md`). Đầu vào là GH Archive
thật (không phải simulated, không phải seed CI). Đường chính của deployment hiện tại vẫn là dự
phòng (runner + MotherDuck, `docs/probe-result.md`); đây là bản gold song song để so sánh (D3).

## Chạy

Workflow `.github/workflows/dbt-databricks.yml` (chạy tay):

- `workflow_dispatch` (khi file đã lên `main`), hoặc `push` lên đúng nhánh
  `feature/dbt-databricks-target` (lọc `paths`), để chạy được ngay trong PR. Không có
  `pull_request`, không có lịch: Free Edition có quota ngày.
- Đường B của `.github/workflows/path-comparison.yml` (D3, `docs/path-comparison.md`) chạy cùng
  `dbt build --target databricks` với cùng `DBT_DATABRICKS_SCHEMA=repopulse_dbt` theo lịch hằng giờ
  (sau release), nên schema `repopulse_dbt_*` cũng được ghi từ đó. Staging chỉ chèn `event_id` mới,
  nên hai workflow ghi xen kẽ không sinh dòng trùng.
- Thiếu `DATABRICKS_HOST` hoặc `DATABRICKS_TOKEN`: các bước Databricks bị bỏ qua ở cấp step, có
  `::notice`, workflow vẫn xanh, không có dòng `runs.csv`.
- Các bước:
  1. `python -m repopulse.probe http-path --github-env`: http path của warehouse = secret
     `DATABRICKS_HTTP_PATH`, trống thì warehouse đầu tiên của `GET /api/2.0/sql/warehouses` (cùng
     hàm `resolve_warehouse` với check SQL warehouse của probe). Ghi `DBT_DATABRICKS_HTTP_PATH` vào
     `$GITHUB_ENV`, `::add-mask::` hostname, http path và id warehouse. Warehouse đang dừng tự khởi
     động khi dbt kết nối.
  2. `dbt debug --target databricks`.
  3. `dbt build --target databricks`: seed, model, data test, unit test; staging đọc
     `workspace.repopulse.silver_gharchive_events`.
  4. `dbt show --inline … --output json --log-format json --target-path target/show`: số dòng và
     khoảng `event_date` của `stg_gharchive_events` sau build (`--target-path` riêng để giữ
     `run_results.json` của build). Log nằm trên runner, không vào artifact.
  5. `python benchmarks/dbt_databricks.py` dựng dòng `variant=dbt-build-databricks`
     (`if: always()`: build hỏng vẫn ghi node lỗi vào summary, không ghi dòng).
  6. Job summary và artifact `dbt-databricks` (`out/dbt-databricks-rows.csv`,
     `out/dbt-databricks-summary.md`).

Không chạy target `databricks` trên máy local (không có credential, `dbt-databricks` không cài
trong extra `dbt`). Cài adapter bằng extra riêng: `pip install -e ".[dbt,databricks]"`.

### Secret và biến

| Tên | Loại | Dùng cho |
|---|---|---|
| `DATABRICKS_HOST` | secret | URL workspace; trống thì bỏ qua |
| `DATABRICKS_TOKEN` | secret | personal access token; trống thì bỏ qua |
| `DATABRICKS_HTTP_PATH` | secret, tuỳ chọn | `/sql/1.0/warehouses/<id>`; trống thì tự tìm qua API |
| `PII_HASH_SALT` | secret | `mask_pii` của schema public (target khác `ci` không có salt thì compile lỗi) |
| `DBT_DATABRICKS_SCHEMA` | biến workflow (`repopulse_dbt`) | schema gốc của dbt; rỗng/không đặt thì `repopulse_probe` (giữ nguyên hành vi `dbt debug` của probe) |

Secret chỉ vào qua `env:`; không step nào in giá trị.

### Schema

dbt ghép `<schema target>_<schema tuỳ chỉnh>` trong catalog `workspace`: seed ở `repopulse_dbt`,
model ở `repopulse_dbt_staging`, `repopulse_dbt_intermediate`, `repopulse_dbt_gold`,
`repopulse_dbt_public`. Không có gì được ghi vào `workspace.repopulse` (bronze/silver/quarantine
của D1); test `tests/test_dbt_databricks_workflow.py` chặn đặt schema trùng.

## Nguồn của staging

| Var | Mặc định | Nghĩa |
|---|---|---|
| `silver_gharchive_source` | `unity_catalog` khi `target.type == 'databricks'`, `seed` ở mọi target khác | `unity_catalog` → dbt source `silver.silver_gharchive_events`; `seed` → seed `fixture_silver_gharchive_events` |
| `silver_gharchive_catalog` / `_schema` / `_table` | `workspace` / `repopulse` / `silver_gharchive_events` | vị trí bảng silver (khớp `repopulse/spark/tables.py`) |
| `silver_gharchive_parquet` | không đặt | chỉ DuckDB (`read_parquet`); trên Databricks báo lỗi compile chỉ sang nguồn Unity Catalog |

Trên DuckDB hành vi không đổi: mặc định seed, hoặc parquet khi đặt var. Muốn build seed CI trên
Databricks (so fixture): `--vars "{silver_gharchive_source: seed}"`.

Seed lưu hai cột mảng hash commit dưới dạng text (rỗng trong fixture); Databricks không cast được
string sang array, nên staging truyền `from_text` cho `cast_string_array` khi nguồn là seed
(`from_json(…, 'array<string>')`), còn bảng Delta có `array<string>` thật thì `cast(… as
array<string>)`.

**Idempotent:** staging là incremental `append`, chỉ chèn `event_id` chưa có (`not exists`), nên
chạy lại workflow trên cùng silver không thêm dòng; `fct_events` dùng `merge` theo `event_id` trên
Databricks (xem dưới). Workflow không bao giờ `--full-refresh`.

## Cú pháp đã đổi

Như port Snowflake (`docs/snowflake-port.md`): SQL chuẩn ở model; chỗ không có cách viết chung đi
qua macro `adapter.dispatch` trong `transform/macros/cross_db.sql`, mỗi macro có `duckdb__`,
`snowflake__` và `databricks__`. `tests/test_dbt_databricks.py` chặn cú pháp Databricks không hiểu
ngoài nhánh engine, render từng nhánh `databricks__` bằng Jinja và chạy được thì chạy trên DuckDB
cạnh nhánh `duckdb__` (hàm chỉ Databricks có được thay bằng macro DuckDB cùng nghĩa).

| Trước | Sau (DuckDB, Snowflake, Databricks) | Databricks | Ở đâu |
|---|---|---|---|
| `cast(x as varchar)`, contract/seed `varchar` | `cast(x as string)`, `data_type: string` | `varchar` không độ dài bị từ chối; `string` là alias của `VARCHAR` trên DuckDB và Snowflake | mọi model, `_gold.yml`, `_marts.yml`, `seeds/schema.yml` |
| macro `hours_between` | thêm `databricks__` | `cast(unix_micros(b) - unix_micros(a) as double) / 3600000000` (đúng tới micro giây) | Q3, Q4 |
| macro `utc_timestamp` | thêm `databricks__` | `cast(x as timestamp)` | staging |
| macro `cast_string_array` | thêm tham số `from_text` | `from_json(cast(x as string), 'array<string>')` (seed) / `cast(x as array<string>)` (Delta) | staging |
| macro `silver_parquet_relation` | thêm `databricks__` | lỗi compile, chỉ sang nguồn Unity Catalog | staging |
| `cohort_week + 28` | macro `add_days(d, n)` | `date_add(d, n)` | Q2 |
| `sha256(x)` | macro `sha256_hex(x)` | `sha2(x, 256)` (Snowflake cũng vậy) | `mask_pii` |
| `trim(x, chars)` | macro `trim_chars(x, chars)` | `btrim(x, chars)` (`trim(a, b)` hai đối số của Spark cũ hiểu ngược thứ tự) | `mask_pii` |
| `regexp_full_match(x, p)` | macro `regex_full_match(x, p)` | `x rlike '^(?:p)\z'` (backslash nhân đôi trong literal) | `hashed_pii` |
| `regexp_matches(x, p)` | macro `regex_contains(x, p)` | `x rlike 'p'` (backslash nhân đôi) | `no_raw_email` |
| `unnest(arr)` | macro `string_array_elements(rel, col)` | `select explode(col) as element …` | `hashed_pii` |
| `fct_events` `delete+insert` | `merge` khi `target.type == 'databricks'` | batch có một dòng mỗi `event_id`, nên upsert cho đúng bảng như delete+insert | `fct_events` |

Giữ nguyên vì Databricks SQL hiểu như DuckDB: `qualify`, `min_by`, `any_value`,
`percentile_cont(p) within group (order by …)`, `count(distinct case …)`, `is distinct from`,
`right()`, `date_trunc('week', …)` (thứ Hai), `union`, `using (…)`, inline `values … as t (…)`
trong unit test.

## Cùng kết quả trên hai adapter

Các `unit_tests:` dbt ở `_gold.yml` (SCD2 `dim_user`, `fct_events`) và `_marts.yml` (6 mart Q1–Q6)
có `given`/`expect` cố định, chạy trong `dbt build` của cả hai target: job CI `dbt build (DuckDB)`
và workflow này. Cùng `expect` xanh ở cả hai là bằng chứng mart cho cùng kết quả trên fixture ở
hai adapter. So khớp mart với `benchmarks/queries/q*.sql` trên dữ liệu (`tests/test_dbt_marts.py`)
vẫn chỉ chạy trên DuckDB.

## Dòng `runs.csv`

`benchmarks/dbt_databricks.py` (dùng chung lõi đọc `run_results.json` với
`benchmarks/snowflake_port.py`):

| Cột | Giá trị |
|---|---|
| `variant` | `dbt-build-databricks` |
| `total_seconds` | `elapsed_time` của `run_results.json` (cả `dbt build`) |
| `q1_seconds` … `q6_seconds` | `execution_time` của mart tương ứng (thời gian build model) |
| `row_count` | số dòng `stg_gharchive_events` sau build (từ `dbt show`) |
| `data_date` | `event_date` nhỏ nhất của staging |
| `notes` | `engine=databricks; source=<bảng silver>; input=gharchive-silver-delta; max_event_date=…; lines_changed=<n>; q_seconds=mart build time; total=dbt build elapsed_time; base=<sha>` |

`lines_changed` = tổng thêm + xoá của `git diff --numstat <base_sha> HEAD -- transform`, `base_sha`
ở `benchmarks/dbt_databricks.toml` (đầu `develop` trước D2). Script từ chối `run_results.json` của
target khác `databricks`, của lệnh khác `build`, hoặc có node `error`/`fail`/`runtime error`/
`skipped`.

README có mục riêng "dbt build on a Databricks SQL warehouse (real GH Archive data)"
(`python -m repopulse.readme_results`), chỉ hiện khi `runs.csv` có dòng này; không lẫn với mục
seed fixture của DuckDB/Snowflake.

Gộp vào `runs.csv` sau khi tải artifact `dbt-databricks`:

```bash
python benchmarks/record_run.py out/dbt-databricks-rows.csv
python -m repopulse.readme_results
```

Tài liệu này không chép số; số đo chỉ đọc ở `runs.csv`.

## Kết quả

Lần chạy workflow trên nhánh `feature/dbt-databricks-target` (commit `4553df1`,
[run 43166502216](https://github.com/tlamDson/RepoPulse/actions/runs/43166502216)) build xanh trên
SQL warehouse: cùng bộ seed, model, data test và unit test như target `ci`, không node nào lỗi hay
bị bỏ qua. Đầu vào là silver Delta của D1 (GH Archive thật, không phải simulated). Dòng của lần
chạy đó đã gộp vào `runs.csv` với `variant=dbt-build-databricks`.

Đây là một lần build, không phải so sánh tốc độ với DuckDB/Snowflake (hai dòng kia build từ seed
fixture). So sánh hai đường end-to-end là việc D3.

## Giả định và giới hạn

- **Múi giờ phiên là UTC.** `timestamp` của Databricks là thời điểm hiển thị theo múi giờ phiên;
  mặc định của SQL warehouse là UTC. Workspace đổi `TIMEZONE` thì literal (`loaded_at`, seed,
  unit test) bị hiểu theo múi giờ khác và `event_week` có thể lệch.
- `date_trunc('week', …)` trên Databricks luôn về thứ Hai.
- `round(x, 2)` của Spark làm tròn HALF_UP, DuckDB làm tròn xa số 0: với số dương giống nhau; ca
  biên đúng `.xx5` trên double có thể khác ở chữ số cuối (unit test tránh ca này).
- CI không có `dbt-databricks`: SQL của nhánh `databricks__` chỉ được kiểm thật khi workflow chạy;
  lỗi SQL của Databricks nằm trong log job, kết quả build ở Job summary và `runs.csv`.
