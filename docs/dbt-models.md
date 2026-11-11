# dbt models: staging → star schema → mart Q1–Q6

Project dbt nằm ở `transform/`. Gold là star schema (`fct_events`, `dim_user` SCD2, `dim_repo`) và
sáu mart trả lời Q1–Q6 (`docs/plan.md`). Định nghĩa chuẩn của từng metric là
`benchmarks/queries/qN.sql`; mart cho **cùng tên cột, cùng ngữ nghĩa và cùng kết quả** với query
tương ứng trên cùng dữ liệu (đã khử trùng theo `event_id`), có test so khớp
(`tests/test_dbt_marts.py`). Materialization hiện là `table` (full refresh); incremental là PR 7.

## Sơ đồ

```
seed fixture_silver_gharchive_events ─┐   (mặc định, CI)
read_parquet(var silver_gharchive_parquet) ─┤   (khi đặt var)
                                       ▼
                    staging.stg_gharchive_events (view)
                         │                    │
seed repos_data_ecosystem ──► gold.dim_repo   │
                         │         │          ▼
                         │         │    gold.dim_user (SCD2)
                         ▼         ▼          │
                    gold.fct_events ◄─────────┘
                         │
                         ▼
          intermediate.int_events_enriched (view: fct + login/is_bot + repo_name/category)
                         │
     ┌──────────┬────────┼─────────┬──────────────┬──────────────┐
     ▼          ▼        ▼         ▼              ▼              ▼
   mart_daily_  mart_    mart_     mart_          mart_repo_     mart_repo_
   active_      cohort_  contrib-  responsiveness concentration  health_score
   contributors retention ution_funnel
   (Q1)         (Q2)     (Q3)      (Q4)           (Q5)           (Q6)
```

Schema trên DuckDB: `main` (seed), `main_staging`, `main_intermediate`, `main_gold` (dbt ghép
`<schema target>_<schema tuỳ chỉnh>`).

## Chọn nguồn bằng var

| Var | Nguồn của `stg_gharchive_events` | Dùng khi |
|---|---|---|
| (không đặt) | seed `fixture_silver_gharchive_events` | job CI `dbt build (DuckDB)`: không có lake, không có mạng dữ liệu |
| `silver_gharchive_parquet: "<glob>"` | `read_parquet('<glob>')` | test so khớp (`tests/test_dbt_marts.py`), chạy tay trên file parquet có cột silver |

```bash
python -m dbt.cli.main build --project-dir transform --profiles-dir transform --target ci
python -m dbt.cli.main build --project-dir transform --profiles-dir transform --target ci --vars "{silver_gharchive_parquet: 'path/to/silver/*.parquet'}"
```

Seed `fixture_silver_gharchive_events` là **fixture cho CI, không phải số đo**: 36 event viết tay
(`tests/fixtures/gharchive_small` đã khử trùng theo `id` + `tests/fixtures/gharchive_scd2`) theo
đúng cột của `silver/gharchive_events` (`docs/spark-jobs.md`). Sinh lại từ JSON bằng
`silver_select_sql` trong `tests/test_dbt_marts.py`.

Đọc thẳng bảng Delta silver và nối vào pipeline theo lịch là PR 7. Lưu ý cho PR 7: Spark ghi
`created_at` dạng timestamp UTC; staging ép về `timestamp` không múi giờ, nên phiên DuckDB đọc
Delta cần múi giờ UTC để `event_date`/tuần không lệch.

## Grain và khoá

| Model | Grain | Khoá | Ghi chú |
|---|---|---|---|
| `stg_gharchive_events` | 1 event silver | `event_id` | Đủ 17 cột silver, kể cả mảng hash commit (silver được phép giữ hash) |
| `dim_repo` | 1 repo trong seed | `repo_key` = `lower(repo_name)` | `repo_name` giữ cách viết của seed; event khớp không phân biệt hoa thường như baseline |
| `dim_user` | 1 phiên bản login của 1 `actor_id` | `user_key` = `'<actor_id>-<version_number>'` | SCD2, xem dưới |
| `fct_events` | 1 event trên repo seed | `event_id` | FK `user_key` → `dim_user`, `repo_key` → `dim_repo`; `event_week` = tuần ISO (thứ Hai); `item_number` = `coalesce(pr_number, issue_number)`. **Không** mang cột hash email/tên commit sang gold |
| mart Q1–Q6 | như `qN.sql` | như `qN.sql` | |

Event trên repo ngoài seed không vào `fct_events` (không Q nào dùng), nhưng vẫn góp vào lịch sử
login của `dim_user`.

## Luật SCD2 của `dim_user`

Dựng từ lịch sử event (gaps-and-islands), tất định, **không** dùng dbt snapshot (snapshot phụ thuộc
thời điểm chạy, không idempotent khi chạy bù):

1. Xếp event của mỗi `actor_id` theo (`created_at`, `event_id`).
2. Một phiên bản mới bắt đầu khi `actor_login` khác event liền trước (so sánh null-safe). `a → b → a`
   cho 3 phiên bản.
3. `valid_from` = event đầu của phiên bản; `valid_to` = `valid_from` của phiên bản kế tiếp (nửa mở
   `[valid_from, valid_to)`), `NULL` cho phiên bản hiện tại; `is_current = valid_to IS NULL`.
4. `is_bot`: login kết thúc bằng `[bot]` hoặc `-bot` (không phân biệt hoa thường), như `q1.sql`.

`fct_events.user_key` là phiên bản của cùng `actor_id`, cùng login với event, có `valid_from` lớn
nhất `<=` `created_at`. Vì baseline đếm theo `actor.login`, mart dùng login của phiên bản có hiệu
lực lúc event, nên kết quả khớp baseline cả khi actor đổi tên.

## Mart

| Mart | Query | Câu hỏi |
|---|---|---|
| `mart_daily_active_contributors` | `q1.sql` | Contributor hoạt động theo ngày/tuần, theo repo và category, mới so với quay lại |
| `mart_cohort_retention` | `q2.sql` | Retention cohort tại đúng W+4, W+12 |
| `mart_contribution_funnel` | `q3.sql` | Funnel fork → PR → merge, giờ tới merge p50/p90 |
| `mart_responsiveness` | `q4.sql` | Thời gian phản hồi đầu tiên p50/p90, tỉ lệ đóng |
| `mart_repo_concentration` | `q5.sql` | Bus factor (tỉ trọng top-N) và tỉ lệ bot |
| `mart_repo_health_score` | `q6.sql` | Điểm sức khoẻ tổng hợp và xếp hạng |

Công thức điểm sức khoẻ (thành phần, trọng số, làm tròn, thứ tự xếp hạng) ghi ở đầu
`benchmarks/queries/q6.sql`; `mart_repo_health_score` dùng nguyên công thức đó.

Khác biệt duy nhất với baseline: baseline không khử trùng (dòng raw trùng `id` đếm hai lần, ảnh
hưởng Q5/Q6), còn silver giữ một dòng mỗi `event_id`. Test so khớp chạy baseline trên đầu vào đã
khử trùng.

## Test

| Loại | Ở đâu | Kiểm gì |
|---|---|---|
| Contract | `contract: {enforced: true}` cho `fct_events`, `dim_user`, `dim_repo` và 6 mart | Tên cột, kiểu, `not_null` |
| Generic | `models/**/_*.yml` | `not_null`, `unique`, `relationships` (fct → dim, funnel → dim_repo), `accepted_values` |
| Singular | `transform/tests/` | Khoảng hiệu lực SCD2 không hở, không chồng, đúng một phiên bản hiện tại; fct trỏ đúng phiên bản; fct đủ mọi event trên repo seed, mỗi event một lần |
| Unit (`unit_tests:`) | `_gold.yml`, `_marts.yml` | SCD2 (đổi login → 2 phiên bản; `a → b → a` → 3), cohort Q2, funnel Q3 |
| pytest | `tests/test_dbt_marts.py` | So khớp 6 mart với `qN.sql` từng dòng từng cột; vi phạm contract làm `dbt build` thoát mã khác 0; gold không có cột/giá trị email; build lại cùng đầu vào cho cùng số dòng |

Không có số đo nào trong tài liệu này; số đo chỉ lấy từ `runs.csv`.
