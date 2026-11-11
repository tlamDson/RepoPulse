# Governance: nhãn PII, vai, che cột

Governance của RepoPulse nằm ở tầng dbt + Python, vì đường đã chọn là **dự phòng**
(`docs/probe-result.md`): không có Unity Catalog, gold chạy trên DuckDB (CI) và MotherDuck. Không
dùng tag hay `GRANT` của Databricks.

| Thành phần | File |
|---|---|
| Policy (nguồn duy nhất: lớp PII, tầng, vai) | `transform/governance.yml` |
| Nhãn cột | `config: {meta: {pii: <lớp>}}` trên mọi cột trong `transform/models/**/_*.yml` |
| Che cột | macro `transform/macros/mask_pii.sql`, model `transform/models/public/` |
| Test dbt | `transform/tests/generic/hashed_pii.sql`, `transform/tests/generic/no_raw_email.sql` |
| Kiểm tự động | `repopulse/governance.py` (`python -m repopulse.governance scan` / `check`) |
| pytest | `tests/test_governance.py` |

## Lớp PII

| Lớp | Nghĩa | Được có ở |
|---|---|---|
| `none` | Không phải dữ liệu cá nhân: id event, loại event, thời điểm, tên repo, số đếm, điểm | mọi tầng |
| `public_identifier` | Login hoặc id GitHub (`actor_login`, `actor_id`, `user_key`). Công khai trên GitHub nhưng vẫn là dữ liệu cá nhân dạng bút danh | staging, intermediate, gold; ở `public` chỉ khi đã che |
| `hashed` | SHA-256 có salt làm ở silver (`commit_author_email_hashes`, `commit_author_name_hashes`) | staging, intermediate. **Không** vào gold/public |
| `direct` | Email hoặc tên thật ở dạng thô | **không ở đâu** từ staging trở đi: silver băm trước (`repopulse/spark/pii.py`) |

## Vai → quyền

| Vai | Ai | Đọc | Ghi | Thấy thô |
|---|---|---|---|---|
| `pipeline` | Job theo lịch, dbt (GitHub Actions) | staging, intermediate, gold, public | mọi tầng | `none`, `public_identifier`, `hashed` |
| `analyst` | Người phân tích, đọc file DuckDB hoặc database MotherDuck chỉ-đọc | gold, public | không | `none`, `public_identifier` |
| `public` | Dashboard GitHub Pages, Tableau Public, bất kỳ ai | **chỉ** `public` | không | `none` |

Luật: vai đọc được một cột mà lớp của cột không nằm trong "thấy thô" của vai thì cột đó phải được
che (`mask_pii`) và khai `masked: true`. `check` báo lỗi nếu không.

## Nhãn cột (tóm tắt)

| Model | Cột không phải `none` |
|---|---|
| `stg_gharchive_events` | `actor_id`, `actor_login` (`public_identifier`); `commit_author_email_hashes`, `commit_author_name_hashes` (`hashed`, có test `hashed_pii`) |
| `int_events_enriched` | `actor_login` (`public_identifier`) |
| `dim_user` | `user_key`, `actor_id`, `actor_login` (`public_identifier`, có test `no_raw_email`) |
| `fct_events` | `user_key`, `actor_id` (`public_identifier`, có test `no_raw_email`) |
| `dim_repo`, 6 mart Q1–Q6 | không có (toàn `none`) |
| `public.dim_user` | `user_key`, `actor_id`, `actor_login`: `public_identifier`, `masked: true`, test `hashed_pii` + `no_raw_email` |
| `public.fct_events` | `user_key`, `actor_id`: `public_identifier`, `masked: true`, test `hashed_pii` + `no_raw_email` |
| `public.dim_repo`, `public.mart_*` | không có (toàn `none`, đi thẳng từ gold) |

Seed (`fixture_silver_gharchive_events`, `repos_data_ecosystem`) không nằm trong phạm vi `check`:
`check` chỉ xét model dbt.

## Schema `public` và cách che

Model ở `transform/models/public/` (tên `pub_*`, alias giữ tên gold) vào schema `public`
(`main_public` trên DuckDB), materialized **table**:

| Relation | Nguồn | Che |
|---|---|---|
| `main_public.dim_repo` | `dim_repo` | không |
| `main_public.mart_daily_active_contributors` … `mart_repo_health_score` (6 mart) | mart gold | không |
| `main_public.dim_user` | `dim_user` | `user_key`, `actor_id`, `actor_login` |
| `main_public.fct_events` | `fct_events` | `user_key`, `actor_id` |

`mask_pii(x)` = `sha256(salt || lower(strip(cast(x as varchar))))`, hex chữ thường 64 ký tự, NULL
giữ NULL: **đúng công thức** `repopulse.spark.pii.hash_pii(value, salt)` của silver
(`tests/test_governance.py` so từng dòng). Cùng salt cho mọi khoá đã che, nên
`public.fct_events JOIN public.dim_user USING (user_key)` cho đúng số dòng như ở gold. `is_bot` và
các cột SCD2 giữ nguyên.

Salt:

- `PII_HASH_SALT` có đặt (mọi target): dùng salt đó, cùng secret với job silver.
- Không đặt, target `ci`: dùng var `ci_pii_hash_salt` trong `transform/dbt_project.yml`, một salt
  fixture ghi rõ **không phải secret**, chỉ cho CI và test.
- Không đặt, target khác (`motherduck`, `databricks`, …): lỗi compile nêu `PII_HASH_SALT`, không bao
  giờ lặng lẽ dùng mặc định. `dbt parse` vẫn chạy được khi thiếu salt (chỉ kiểm lúc execute).

Vì sao `table` chứ không `view`: SQL của view (có literal salt) được lưu trong database; chia sẻ
database/schema public sẽ để lộ salt. Table chỉ lưu kết quả đã che. Salt vẫn có trong SQL đã compile
(`target/`) và log debug của dbt: hai thư mục này đã gitignore và hiện không workflow nào upload
chúng làm artifact; job theo lịch (PR 7) phải giữ nguyên điều đó.

Giới hạn của che bằng hash: login GitHub là công khai, nên ai có salt có thể dò lại login từ hash.
Salt thật chỉ nằm trong secret của pipeline; không chia sẻ cho vai `analyst`/`public`.

## Test và kiểm tự động

| Kiểm | Ở đâu | Bắt được |
|---|---|---|
| `hashed_pii` (generic) | `stg_gharchive_events.commit_author_*_hashes` (`is_array: true`); cột đã che ở `public` | Job silver quên băm (email/tên thô trong cột `*_hashes`): `dbt build` đỏ và **mọi model sau bị SKIP**, gold không được dựng. Cột public chưa che. NULL (seed CI) qua |
| `no_raw_email` (generic) | mọi cột nhãn khác `none` ở gold và public | Email thô trong cột định danh (ép varchar nên bắt cả mảng/struct) |
| `scan` | `python -m repopulse.governance scan` | Email thô ở **bất kỳ** cột chuỗi, mảng, struct, map nào của schema được quét, kể cả model mới không có yml. Không in giá trị, chỉ in schema/bảng/cột/số dòng. Hash 64 hex và login không bị báo |
| `check` | `python -m repopulse.governance check` | Cột thiếu `meta.pii`, nhãn lạ, `direct` từ staging trở đi, `hashed` ở gold/public, vai đọc lớp không được thấy mà cột chưa `masked: true`, model ngoài các tầng của policy; với `--db`: cột có trong bảng đã build mà chưa khai trong yml |
| pytest | `tests/test_governance.py` | Tất cả các ý trên trên build thật và trên bản sao project cố ý làm hỏng (email lọt vào gold, bỏ nhãn một cột gold, view public lộ `actor_login`), cùng công thức che, số dòng public = gold, target không phải `ci` thiếu salt |

Hai test dbt chạy trong job CI `dbt build (DuckDB)` sẵn có; `scan`/`check` chạy trong pytest (job
`Unit Tests`). Mẫu regex email là `repopulse.governance.EMAIL_PATTERN`, chép nguyên vào
`no_raw_email.sql` (có test giữ hai chỗ bằng nhau).

## Chạy tay

```bash
python -m dbt.cli.main build --project-dir transform --profiles-dir transform --target ci
python -m repopulse.governance scan --db ci.duckdb --schema main_gold --schema main_public
python -m repopulse.governance check --manifest transform/target/manifest.json --policy transform/governance.yml --db ci.duckdb
```

Mã thoát: `0` sạch, `1` có finding (in bảng), `2` sai tham số, thiếu file, hoặc schema không tồn tại
(gõ nhầm schema không được coi là "sạch"). `check` không cần `--db` (chỉ kiểm tĩnh trên manifest,
`dbt parse` là đủ); thêm `--db` để kiểm cả cột chưa khai.

## Giới hạn: phân quyền trên DuckDB/MotherDuck

DuckDB và MotherDuck **không có `GRANT` theo cột hay theo schema**. Vai trong policy là thiết kế,
thực thi bằng cách chỉ chia sẻ đúng phần cần:

- `public`: chỉ xuất/chia sẻ schema `public` (dashboard tĩnh đọc file xuất từ `main_public`; trên
  MotherDuck là một share chỉ chứa dữ liệu public). Vì `public` là table, không phụ thuộc gold.
- `analyst`: share chỉ-đọc của database gold.
- `pipeline`: token MotherDuck trong secret của Actions.

Chưa triển khai trên cloud trong PR này: chưa tạo share MotherDuck, chưa có job xuất dữ liệu cho
dashboard (PR 11). Ánh xạ policy sang `grants` của dbt trên Snowflake (role theo vai, quyền theo
schema, masking policy cho cột `public_identifier`) để dành cho PR 13.
