# Semantic layer (MetricFlow) cho Q1–Q6

Lớp định nghĩa metric trên 6 mart của schema `public` (`main_public` trên DuckDB/MotherDuck), để
công cụ đọc metric theo tên thay vì tự viết SQL (việc AI2: SQL agent chỉ đọc `public`). Định nghĩa
ở `transform/models/public/_semantic.yml`; mã sinh SQL ở `repopulse/semantic.py`; test ở
`tests/test_dbt_semantic_layer.py`. Mọi số liệu của metric là của mart, tức là kết quả của
`benchmarks/queries/qN.sql`; tài liệu này không có số đo nào (số đo chỉ lấy từ `runs.csv`).

## Spec và phiên bản

- dbt-core 1.12 phụ thuộc trực tiếp `metricflow` (0.213 khi viết), gói này mang theo validator
  semantic manifest (`metricflow_semantic_interfaces`) và engine sinh SQL. Không cần
  `dbt-metricflow` hay lệnh `mf`.
- Dùng spec `semantic_models:` cấp trên cùng (có `measures`) + `metrics:` với `type_params`. dbt
  1.12 cũng nhận spec mới gắn semantic model vào model (`semantic_model:`, `metrics:` trong
  model), nhưng spec cũ: `dbt parse` của bản đang cài không cảnh báo deprecated, mọi dbt-core
  >= 1.8 mà các workflow có thể cài (pipeline MotherDuck, port Snowflake, probe Databricks) đều
  parse được, và không phải sửa `_public.yml` (file governance đọc nhãn PII).
- `dbt parse`/`dbt build` validate semantic manifest (lỗi validate làm parse thất bại) và ghi
  `target/semantic_manifest.json`. MetricFlow đọc file đó qua
  `parse_manifest_from_dbt_generated_manifest` (đổi measure thành simple metric khi nạp).

## Semantic model

Mỗi semantic model một mart, một dòng mỗi grain của mart, primary entity riêng (không join giữa
các mart). Tên metric = tên cột của mart. Tên nhóm (group-by) theo dạng
`<primary entity>__<dimension>`, dimension thời gian thêm `__<granularity>`.

| Semantic model | Mart (`ref`) | Primary entity | Grain = group-by |
|---|---|---|---|
| `q1_daily_active_contributors` | `pub_mart_daily_active_contributors` | `q1_activity` | `q1_activity__period`, `q1_activity__period_start__day`, `q1_activity__scope`, `q1_activity__scope_key` |
| `q2_cohort_retention` | `pub_mart_cohort_retention` | `q2_cohort` | `q2_cohort__cohort_week__week` |
| `q3_contribution_funnel` | `pub_mart_contribution_funnel` | `q3_funnel` | `q3_funnel__repo_name` (+ `q3_funnel__category`) |
| `q4_responsiveness` | `pub_mart_responsiveness` | `q4_responsiveness` | `q4_responsiveness__repo_name`, `q4_responsiveness__item_type` |
| `q5_repo_concentration` | `pub_mart_repo_concentration` | `q5_concentration` | `q5_concentration__repo_name` (+ `q5_concentration__category`) |
| `q6_repo_health_score` | `pub_mart_repo_health_score` | `q6_health` | `q6_health__repo_name` (+ `q6_health__category`) |

Semantic model chỉ `ref` model `pub_mart_*` (schema `public`), không trỏ gold/staging/intermediate;
test kiểm cả `node_relation.schema_name` lẫn `depends_on` trong `manifest.json`.

## Metric theo Q

| Q | Cộng dồn được (simple, `sum`) | Tỉ lệ (derived, từ measure cộng được) | Không cộng dồn được (đúng ở grain mart) |
|---|---|---|---|
| Q1 | — | — | `active_contributors`, `new_contributors`, `returning_contributors` |
| Q2 | `cohort_size`, `retained_w4`, `retained_w12` | `retention_w4_pct`, `retention_w12_pct` | — |
| Q3 | `forkers`, `forkers_with_pr`, `forkers_with_merge`, `prs_opened`, `prs_merged` | — | `merge_hours_p50`, `merge_hours_p90` |
| Q4 | `opened`, `responded`, `closed` | `close_rate_pct` | `response_hours_p50`, `response_hours_p90` |
| Q5 | `contributions`, `bot_contributions`, `human_contributors` | `bot_share_pct` | `top1_share_pct`, `top2_share_pct`, `top3_share_pct` |
| Q6 | — | — | `health_rank` và 7 cột điểm (`*_score`, `health_score`) |

- **Tỉ lệ**: `round(100 * cast(a as double) / b, 2)` trên measure đã cộng, đúng công thức và làm
  tròn của `qN.sql`, nên khi gộp nhiều cohort/repo vẫn ra tỉ lệ gộp đúng (không phải trung bình
  các tỉ lệ; test `test_ratio_metric_reaggregates_from_measures`).
- **Không cộng dồn được**: số người distinct của Q1 (cộng qua ngày/scope sẽ đếm một người nhiều
  lần), percentile, top-N share (mart không có số đếm theo người để tính lại), điểm và hạng của
  Q6. Measure dùng `max` (mỗi grain một dòng nên ra đúng giá trị mart); `description` của metric
  ghi "Not additive" và grain đúng. Nhóm rộng hơn grain thì kết quả chỉ là max, không có nghĩa.
- Đếm distinct của Q3/Q5 (`forkers`, `human_contributors`) cộng qua repo ra số cặp (người, repo),
  ghi trong `description`.

## Time spine và mốc thời gian quy ước

- `metricflow_time_spine` (schema `public`, cột `date_day`, granularity `day`, 2020-01-01 tới
  2030-12-31) dựng bằng macro `dbt.date_spine` của dbt (chạy được trên DuckDB/MotherDuck,
  Snowflake, Databricks). Không có dữ liệu cá nhân (`pii: none`); `tests/test_governance.py` đã
  thêm bảng này vào danh sách quan hệ của `main_public`.
- MetricFlow bắt mỗi measure có aggregation time dimension. Q1 dùng `period_start` (granularity
  ngày; dòng tuần có `period_start` là thứ Hai), Q2 dùng `cohort_week` (granularity tuần).
- Q3–Q6 là tổng hợp toàn thời gian theo repo, không có cột thời gian. Chúng dùng dimension
  `all_time_anchor` khai bằng `expr: cast('1970-01-01' as date)`: **mốc quy ước, hằng số, không
  phải thời gian sự kiện**. `metric_time` của các metric này luôn là 1970-01-01; đừng lọc hay nhóm
  theo nó. Cột của mart public không đổi.

## Sinh và chạy SQL metric local

```bash
python -m dbt.cli.main build --project-dir transform --profiles-dir transform --target ci   # ghi ci.duckdb + transform/target/semantic_manifest.json
python -m repopulse.semantic sql --metrics cohort_size,retention_w4_pct --group-by q2_cohort__cohort_week__week
```

`repopulse.semantic sql` in SQL DuckDB (dùng được cho MotherDuck) rồi thoát 0; metric/group-by sai
thì in lỗi ra stderr và thoát 2. SQL trỏ tới `"<database>"."main_public"."mart_…"` với database
của target lúc dbt ghi manifest (`ci` với `ci.duckdb`): chạy nó bằng `duckdb` trên đúng database
đó. Trong Python: `load_manifest(path)` rồi `compile_sql(manifest, metrics, group_by)`.
Module chỉ sinh SQL, không chạy; `metricflow` import lười.

## Test

`python -m pytest tests/test_dbt_semantic_layer.py`:

- `dbt parse` (target/log trong tmp) thành công, không cảnh báo deprecated, ghi semantic manifest
  có đúng 6 semantic model trỏ mart public, metric cho mọi cột số của mỗi mart, `description` không
  rỗng, một time spine ngày ở `main_public` phủ mọi `period_start`/`cohort_week`.
- Từng Q1–Q6: metric của Q nhóm theo đủ grain, sinh SQL bằng MetricFlow, chạy trên DuckDB build
  từ fixture (fixture session `built`), so với kết quả `qN.sql` (fixture `baseline`): cùng số dòng,
  cùng khoá grain, từng cột số (float `abs=1e-9`).
- Tỉ lệ là derived từ simple metric cộng được; metric không cộng dồn được ghi "Not additive";
  CLI in SQL chạy được và thoát 2 khi metric không tồn tại.

Trên máy local thiếu `metricflow` thì file test skip; trên CI (`CI` có giá trị) thiếu là lỗi.
