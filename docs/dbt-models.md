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

gold.{dim_repo, dim_user, fct_events, 6 mart}
                         │  pub_* (table, alias = tên gold)
                         ▼
     public.{dim_repo, 6 mart}            đi thẳng (toàn cột pii: none)
     public.{dim_user, fct_events}        user_key / actor_id / actor_login che bằng mask_pii
```

Schema trên DuckDB: `main` (seed), `main_staging`, `main_intermediate`, `main_gold`, `main_public`
(dbt ghép `<schema target>_<schema tuỳ chỉnh>`). `main_public` là thứ duy nhất vai `public`
(dashboard, Tableau Public) được đọc; nhãn PII, vai và cách che: `docs/governance.md`.

SQL của model và singular test chạy được trên cả DuckDB và Snowflake: không dùng cú pháp chỉ
DuckDB có (`filter (where …)`, `arg_min`, `quantile_cont`, `epoch`, `::`, `bool_or`, …) ngoài
macro `transform/macros/cross_db.sql`; `tests/test_dbt_portability.py` kiểm điều này. Bảng ánh xạ
và workflow port: `docs/snowflake-port.md`. Var `silver_gharchive_parquet` chỉ dùng được trên
DuckDB (Snowflake báo lỗi compile).

## Chọn nguồn bằng var

| Var | Nguồn của `stg_gharchive_events` | Dùng khi |
|---|---|---|
| (không đặt) | seed `fixture_silver_gharchive_events` | job CI `dbt build (DuckDB)`: không có lake, không có mạng dữ liệu |
| `silver_gharchive_parquet: "<glob>"` | `read_parquet('<glob>')` (macro `silver_parquet_relation`, chỉ DuckDB) | test so khớp (`tests/test_dbt_marts.py`), chạy tay trên file parquet có cột silver |

```bash
python -m dbt.cli.main build --project-dir transform --profiles-dir transform --target ci
python -m dbt.cli.main build --project-dir transform --profiles-dir transform --target ci --vars "{silver_gharchive_parquet: 'path/to/silver/*.parquet'}"
```

Seed `fixture_silver_gharchive_events` là **fixture cho CI, không phải số đo**: 36 event viết tay
(`tests/fixtures/gharchive_small` đã khử trùng theo `id` + `tests/fixtures/gharchive_scd2`) theo
đúng cột của `silver/gharchive_events` (`docs/spark-jobs.md`). Sinh lại từ JSON bằng
`silver_select_sql` trong `tests/dbt_helpers.py`; test
`test_ci_seed_equals_silver_built_from_fixture_json` khẳng định seed bằng đúng dữ liệu đó (từng
`event_id`, từng cột). Parquet mà pytest tạo có thêm `tests/fixtures/gharchive_parquet_only` (3
event không có trong seed, gồm ca đổi login cùng giây), nên test phân biệt được đường đọc parquet
với đường đọc seed; baseline chạy trên cùng các dòng raw với parquet.

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

1. Xếp event của mỗi `actor_id` theo (`created_at`, `event_id`). `event_id` là varchar nên hoà
   `created_at` được phân theo thứ tự chuỗi (`'10' < '9'`), không theo số; vẫn tất định.
2. Một phiên bản mới bắt đầu khi `actor_login` khác event liền trước (so sánh null-safe). `a → b → a`
   cho 3 phiên bản.
3. `valid_from` = event đầu của phiên bản; `valid_to` = `valid_from` của phiên bản kế tiếp (nửa mở
   `[valid_from, valid_to)`), `NULL` cho phiên bản hiện tại; `is_current = valid_to IS NULL`.
4. `is_bot`: login kết thúc bằng `[bot]` hoặc `-bot` (không phân biệt hoa thường), như `q1.sql`.
5. **Đổi login trong cùng một giây** (event login `a` rồi event login `b` của cùng actor, cùng
   `created_at` = `t`): phiên bản `a` có `valid_from = valid_to = t` (phiên bản độ dài 0). Phiên
   bản độ dài 0 có hiệu lực đúng tại thời điểm `[t, t]`, không phải khoảng rỗng `[t, t)`; mọi
   phiên bản khác vẫn nửa mở. Không dời mốc thời gian, không gộp phiên bản, nên `dim_user` vẫn chỉ
   phụ thuộc event. Singular test `assert_fct_events_user_version_matches_event` áp đúng luật này;
   `assert_dim_user_scd2_intervals` vốn đã chấp nhận `valid_to = valid_from`.

`fct_events.user_key` là phiên bản của cùng `actor_id`, cùng login với event, có `valid_from` lớn
nhất `<=` `created_at` (hoà thì lấy `version_number` lớn nhất). Ở ca đổi login cùng giây, event `a`
chỉ khớp phiên bản `a` độ dài 0, event `b` khớp phiên bản `b`. Giới hạn đã biết: nếu cùng một giây
có `a → b → a`, event `a` đầu tiên trỏ phiên bản `a` sau (cũng bắt đầu tại `t`, cùng login) thay vì
phiên bản đầu; vẫn tất định, vẫn qua singular test và không đổi mart (mart đếm theo login). Vì
baseline đếm theo `actor.login`, mart dùng login của phiên bản có hiệu lực lúc event, nên kết quả
khớp baseline cả khi actor đổi tên.

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

Khác biệt với baseline:

1. **Khử trùng.** Baseline không khử trùng (dòng raw trùng `id` đếm hai lần, ảnh hưởng Q5/Q6), còn
   silver giữ một dòng mỗi `event_id`. Test so khớp chạy baseline trên đầu vào đã khử trùng.
2. **`actor_login` NULL.** Baseline tính `ends_with(lower(NULL), …)` ra NULL nên `NOT is_bot` loại
   event đó khỏi mọi tập "human" (forkers, cohort, `per_actor` của Q5/Q6, `opener_is_bot`), nhưng Q5
   vẫn đếm nó trong `contributions`. Mart coi event đó là human vì `dim_user.is_bot` coalesce về
   `false`, nên nó lọt vào cohort Q2, `prs_opened` của Q3, `human_contributors`/top-N của Q5 và
   activity/diversity của Q6. Silver chỉ bắt buộc `actor.id`, nhưng GH Archive thực tế luôn có
   `actor.login`. Ca này **chưa có test** và fixture không có event login NULL.

## Test

| Loại | Ở đâu | Kiểm gì |
|---|---|---|
| Contract | `contract: {enforced: true}` cho `fct_events`, `dim_user`, `dim_repo` và 6 mart | Tên cột, kiểu, `not_null` |
| Generic | `models/**/_*.yml` | `not_null`, `unique`, `relationships` (fct → dim, funnel → dim_repo, public fct → public dim_user), `accepted_values` |
| Generic tự viết | `transform/tests/generic/` | `hashed_pii`: cột hash của staging và cột đã che ở public chỉ chứa SHA-256 hex 64 ký tự (silver quên băm thì build đỏ, model sau SKIP); `no_raw_email`: cột nhãn khác `none` ở gold/public không chứa email (`docs/governance.md`) |
| Singular | `transform/tests/` | Khoảng hiệu lực SCD2 không hở, không chồng, đúng một phiên bản hiện tại; fct trỏ đúng phiên bản; fct đủ mọi event trên repo seed, mỗi event một lần |
| Unit (`unit_tests:`) | `_gold.yml`, `_marts.yml` | SCD2 (đổi login → 2 phiên bản; `a → b → a` → 3; đổi login cùng giây → phiên bản độ dài 0), `fct_events` chọn phiên bản theo cận `valid_from` (`a → b → a`) và ở ca cùng giây, cohort Q2, funnel Q3 |
| pytest | `tests/test_dbt_marts.py` | So khớp 6 mart với `qN.sql` từng dòng từng cột; vi phạm contract làm `dbt build` thoát mã khác 0; gold không có cột/giá trị email; build lại cùng đầu vào cho cùng số dòng |
| pytest | `tests/test_governance.py` | Nhãn `meta.pii` đủ và đúng policy (`check`), không email thô ở gold/public (`scan`), silver chưa băm làm build đỏ, che cột ở public đúng công thức `hash_pii`, public = gold về số dòng và join |

Build dùng chung cho hai file pytest là fixture session `built` trong `tests/conftest.py`; helper
(`run_dbt`, `write_profiles`, `silver_select_sql`, …) ở `tests/dbt_helpers.py`.

Không có số đo nào trong tài liệu này; số đo chỉ lấy từ `runs.csv`.
