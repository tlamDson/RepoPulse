# Port project dbt sang Snowflake (PR 13)

Chạy **cùng** project dbt `transform/` trên Snowflake trial và so với bản chính (DuckDB, target
`ci`) bằng cùng một lệnh `dbt build` trên cùng đầu vào. Số đo M7 (`docs/plan.md`): số dòng phải
sửa và thời gian build. Đầu vào của cả hai lần build là seed `fixture_silver_gharchive_events`:
**CI fixture, không phải số đo trên dữ liệu thật**.

## Chạy

Workflow `.github/workflows/snowflake-port.yml` (chạy tay):

- `workflow_dispatch` (khi file đã lên `main`), hoặc `push` lên đúng nhánh
  `feature/snowflake-port` (để chạy được ngay trong PR, trước release). Không có `pull_request`;
  Snowflake chỉ được ghi trong workflow này.
- Bước 1: `dbt build --target ci` (DuckDB) rồi dựng dòng `dbt-build-duckdb`.
- Bước 2 (gate cấp step, chỉ khi có secret `SNOWFLAKE_ACCOUNT`): `dbt debug --target snowflake`,
  `dbt build --target snowflake`, dựng dòng `dbt-build-snowflake`. Thiếu secret thì có notice,
  workflow vẫn xanh, chỉ có dòng DuckDB.
- Artifact `snowflake-port`: `out/snowflake-port-rows.csv` (định dạng `runs.csv`) và
  `out/snowflake-port-summary.md` (cũng ghi vào Job summary).

Không chạy target `snowflake` trên máy local (không có credential; lệnh nằm ở nhóm `ask`).

### Secret

| Secret | Nội dung |
|---|---|
| `SNOWFLAKE_ACCOUNT` | account identifier |
| `SNOWFLAKE_USER` | `REPOPULSE_CI` (user SERVICE) |
| `SNOWFLAKE_PRIVATE_KEY` | PEM PKCS#8 không mật khẩu (đăng nhập key-pair, không có password) |
| `SNOWFLAKE_ROLE` | role của user CI |
| `SNOWFLAKE_WAREHOUSE` | `REPOPULSE_WH` (XSMALL, auto-suspend 60s) |
| `SNOWFLAKE_DATABASE` | `REPOPULSE` |

Profile `snowflake` trong `transform/profiles.yml` đọc cả 6 giá trị bằng `env_var(...)`; schema cố
định `repopulse` (dbt ghép thành `repopulse_staging`, `repopulse_intermediate`, `repopulse_gold`).
Cài adapter bằng extra riêng: `pip install -e ".[dbt,snowflake]"` (extra `dbt` của CI không đổi).

### Gộp vào `runs.csv`

Tải artifact `snowflake-port` rồi:

```bash
python benchmarks/record_run.py out/snowflake-port-rows.csv
```

## Dòng `runs.csv`

`benchmarks/snowflake_port.py` dựng một dòng từ `transform/target/run_results.json`, ngay sau mỗi
lần build (lần build sau ghi đè file này; script kiểm `args.target` để không lấy nhầm kết quả).

| Cột | Giá trị |
|---|---|
| `variant` | `dbt-build-duckdb` / `dbt-build-snowflake` |
| `total_seconds` | `elapsed_time` của `run_results.json` (cả `dbt build`: seed, model, test, unit test) |
| `q1_seconds` … `q6_seconds` | `execution_time` của mart tương ứng: `mart_daily_active_contributors`, `mart_cohort_retention`, `mart_contribution_funnel`, `mart_responsiveness`, `mart_repo_concentration`, `mart_repo_health_score` (thời gian build model, không phải query Q trên dữ liệu thật) |
| `row_count` | số dòng seed fixture |
| `data_date` | `event_date` nhỏ nhất trong seed fixture (quy ước, fixture không phải một ngày GH Archive) |
| `notes` | `engine=…; fixture=seed; lines_changed=<n>; q_seconds=mart build time; total=dbt build elapsed_time; base=<sha>` |

Build có node `error`/`fail`/`runtime error`/`skipped` thì không ghi dòng, script thoát mã khác 0
và ghi các node lỗi vào summary. Log dbt (kể cả lỗi SQL của Snowflake) nằm trong log job.

### Số dòng phải sửa

`base_sha` trong `benchmarks/snowflake_port.toml` là đầu `develop` trước khi port. Số dòng sửa =
tổng cột thêm + xoá của `git diff --numstat <base_sha> HEAD -- transform` (bỏ file nhị phân). Con
số gồm mọi thay đổi dưới `transform/` (model, test, macro, profile), không gồm test pytest, script
hay workflow. Checkout trong workflow dùng `fetch-depth: 0` để có commit gốc.

## Cú pháp đã đổi

Ưu tiên SQL chuẩn chạy được ở cả hai engine; chỉ chỗ không có cách viết chung mới qua macro
`adapter.dispatch` trong `transform/macros/cross_db.sql` (mỗi macro có `duckdb__` và
`snowflake__`). Test `tests/test_dbt_portability.py` chặn cú pháp chỉ DuckDB có ngoài `macros/`.

| DuckDB (trước) | Sau (cả hai engine) | Ở đâu |
|---|---|---|
| `count(*) filter (where c)` | `count(case when c then 1 end)` | Q1, Q5, Q6, `assert_dim_user_scd2_intervals` (cả dạng window) |
| `count(distinct x) filter (where c)` | `count(distinct case when c then x end)` | Q3 |
| `sum(n) filter (where c)` | `sum(case when c then n end)` | Q5 |
| `arg_min(x, y)` | `min_by(x, y)` (DuckDB có alias) | Q3, Q4, Q6 |
| `quantile_cont(x, q)` | `percentile_cont(q) within group (order by x)` | Q3, Q4 |
| `(epoch(b) - epoch(a)) / 3600` | macro `hours_between(a, b)`: DuckDB giữ nguyên biểu thức, Snowflake `datediff(microsecond, a, b) / 3.6e9` | Q3, Q4 |
| `x::double` | `cast(x as double)` | Q2, Q4, Q5, Q6 |
| `bool_or(c)` | `max(case when c then 1 else 0 end) = 1` | Q6 |
| `ends_with(s, '[bot]')` | `right(s, 5) = '[bot]'` | `dim_user` |
| `cast(x as varchar[])` | macro `cast_string_array(x)`: Snowflake `cast(try_parse_json(...) as array)` | staging |
| `make_timestamp(epoch_us(x))` | macro `utc_timestamp(x)`: DuckDB giữ nguyên biểu thức (giờ UTC của `TIMESTAMPTZ` từ parquet), Snowflake `cast(x as timestamp)` (nguồn luôn là seed) | staging |
| `read_parquet('<glob>')` | macro `silver_parquet_relation(glob)`: Snowflake báo lỗi compile (đọc parquet cần stage, ngoài phạm vi) | staging |
| alias `merge`, `close` | `merge_rate`, `close_rate` (cột ra của mart không đổi) | Q6 |

Kết quả trên DuckDB không đổi: `tests/test_dbt_marts.py` vẫn so khớp 6 mart với
`benchmarks/queries/q*.sql`.

Sau port này, target Databricks (D2, `docs/dbt-databricks.md`) đổi thêm vài chỗ dùng chung cho
cả ba engine: `varchar` thành `string` (model, contract, seed), `cohort_week + n` thành macro
`add_days`, và `mask_pii`/`hashed_pii`/`no_raw_email` đi qua macro `sha256_hex`, `trim_chars`,
`regex_full_match`, `regex_contains`, `string_array_elements` (nhánh `snowflake__` của chúng là
`sha2(x, 256)`, `trim(x, chars)`, `regexp_like`, `regexp_instr`, `lateral flatten`). Các nhánh
`snowflake__` mới chưa chạy trên Snowflake (workflow `snowflake-port.yml` chạy tay).

Giả định của port:

- `date_trunc('week', …)` trên Snowflake theo tham số `WEEK_START`; mặc định (0) là tuần bắt đầu
  thứ Hai như DuckDB. Account đổi tham số này thì `event_week` lệch.
- Kiểu `timestamp` trên Snowflake là `TIMESTAMP_NTZ` (theo `TIMESTAMP_TYPE_MAPPING` mặc định).
- Contract (`data_type` `varchar`, `bigint`, `double`, `date`, `timestamp`, `boolean`) dùng tên
  kiểu cả hai engine hiểu; dbt so kiểu bằng chính engine nên alias được chuẩn hoá.

## Kết quả

Lần chạy workflow trên nhánh `feature/snowflake-port` (commit `ee38cf0`) build xanh trên cả hai
engine, cùng bộ seed, model, data test và unit test. Hai dòng của lần chạy đó đã gộp vào
`runs.csv`: `variant=dbt-build-duckdb` và `variant=dbt-build-snowflake`, cùng `git_sha`. Thời gian
build ở cột `total_seconds`, thời gian từng mart ở `q1_seconds`…`q6_seconds`, số dòng phải sửa ở
`lines_changed=` trong `notes`. Tài liệu này không chép số: đọc ở `runs.csv`.

Đây là build trên CI fixture (seed), không phải số đo trên dữ liệu thật, nên chỉ so được chi phí
cố định của một lần `dbt build` giữa hai engine, không nói gì về tốc độ ở khối lượng lớn.
