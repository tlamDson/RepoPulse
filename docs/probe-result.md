# RepoPulse · Kết quả bước thử số 1 (cloud probe)

Bước thử trong [`plan.md`](plan.md) (dòng "bước thử" của danh sách PR): từ GitHub Actions có điều khiển được Databricks Free Edition không, hay dùng đường dự phòng (PySpark + Delta trên runner, gold trên MotherDuck).

## Kết luận

**Đường chọn: dự phòng (`fallback`)** — PySpark + Delta Lake chạy trên runner GitHub Actions, gold trên MotherDuck.

- **MotherDuck dùng được từ GitHub Actions:** kết nối bằng token, tạo database `repopulse`, `select 1`, và `dbt debug --target motherduck` đều qua.
- **Databricks chưa dùng được, và chưa kiểm được tới nơi:** mọi API trả HTTP 401 ngay ở bước xác thực, nên các bước Volume, job serverless, SQL warehouse chưa hề được thử.
- **Nguyên nhân 401 nằm ở secret, chưa phải ở Free Edition:** giá trị `DATABRICKS_TOKEN` đang lưu không bắt đầu bằng `dapi`, tức không phải personal access token. Lần thử này vì vậy **không chứng minh** Free Edition chặn điều khiển từ ngoài; câu hỏi đó vẫn mở.

Vì đường dự phòng đã chạy thật còn đường chính thì chưa, các PR sau (4, 5, 7, 9) làm theo đường dự phòng. Quyết định này đổi lại được: xem mục "Muốn thử lại Databricks".

## Lần chạy làm căn cứ

| | |
|---|---|
| Ngày | 2026-11-11 |
| Workflow | `Cloud probe` (`.github/workflows/probe.yml`), runner `ubuntu-latest` |
| Run | <https://github.com/tlamDson/RepoPulse/actions/runs/42980750248> |
| Commit | `2d1df17` trên nhánh `docs/probe-result` |
| Artifact | `probe-result` (`probe-result.json`, `probe-result.md`) |

Bảng dưới chép từ `probe-result.json` của lần chạy đó (đã bỏ mã `ReqId` trong thông báo lỗi; artifact ghi `HTTP 401 401` vì Databricks trả `error_code` cũng là `401`, ở đây viết gọn `HTTP 401`). Báo cáo chỉ có pass/fail/skipped, không có số đo (số đo chỉ lấy từ `runs.csv`).

| Check | Trạng thái | Chi tiết | Vai trò |
|---|---|---|---|
| `databricks_secret_shape` | fail | `DATABRICKS_TOKEN does not start with dapi, so it is not a personal access token` | chỉ tham khảo |
| `databricks_auth` | fail | `HTTP 401: Credential was not sent or was of an unsupported type for this API.` | quyết định đường |
| `databricks_volume_upload` | fail | `HTTP 401: Credential was not sent or was of an unsupported type for this API.` | quyết định đường |
| `databricks_serverless_job` | fail | `current user unknown (auth check failed); cannot place the probe notebook` | quyết định đường |
| `notebook_gharchive_download` | skipped | `serverless job did not finish successfully; download not attempted` | chỉ tham khảo |
| `databricks_sql_warehouse` | fail | `HTTP 401: Credential was not sent or was of an unsupported type for this API.` | quyết định đường |
| `motherduck_connect` | pass | `connected to MotherDuck; database repopulse exists; select 1 ok` | quyết định đường |
| `dbt_debug_databricks` | fail | `dbt debug --target databricks failed` (cùng lỗi 401 ở bước `OpenSession`) | quyết định đường |
| `dbt_debug_motherduck` | pass | `dbt debug --target motherduck succeeded` | quyết định đường |

Luật chọn đường (`repopulse.probe.decide_path`): `primary` khi đủ bốn check Databricks (auth, Volume, job serverless, SQL warehouse) pass và `dbt debug --target databricks` không fail; nếu không thì `fallback` khi MotherDuck kết nối được và `dbt debug --target motherduck` không fail; còn lại là `blocked`.

## Điều chưa kiểm được

Các mục "chưa kiểm chứng" của `plan.md` về Databricks vẫn nguyên trạng, vì lần thử dừng ở xác thực:

- Free Edition có cho tạo token và điều khiển từ ngoài (REST, CLI, GitHub Actions) không.
- Đẩy file lên Volume và dung lượng Volume.
- Chạy job serverless từ ngoài.
- `data.gharchive.org` có nằm trong danh sách domain được phép của serverless không.
- SQL warehouse có nhận `dbt` từ ngoài không.

Lần thử không tạo tài nguyên nào trên Databricks. Trên MotherDuck có thêm database `repopulse` (rỗng).

## Cạm bẫy tìm thấy

- **MotherDuck từ chối client DuckDB quá mới.** Lần chạy đầu (<https://github.com/tlamDson/RepoPulse/actions/runs/42980241119>) cài DuckDB mới nhất và MotherDuck báo: `Your DuckDB version (v2.0.0) is not yet supported by MotherDuck. The latest supported version is v1.5.6.` Sau khi khoá `duckdb==1.5.6` trong workflow thì qua. Mọi workflow nối MotherDuck (PR 7, 9, 11) phải khoá phiên bản DuckDB theo bản MotherDuck đang hỗ trợ; CI trên DuckDB local không bị ảnh hưởng.
- **GitHub chỉ che đúng nguyên văn giá trị secret.** `dbt debug` in hostname không có `https://`, nên workflow đăng ký thêm `::add-mask::` cho hostname trần, user, http path và id warehouse trước khi chạy `dbt debug`. Log của repo public đã kiểm: `host` và `http_path` hiện `***`.
- **`workflow_dispatch` chỉ thấy workflow đã có trên `main`.** Trong PR này workflow chạy nhờ trigger `push` giới hạn ở nhánh `docs/probe-result`; sau khi nhánh bị xoá, chạy lại bằng `workflow_dispatch` sau lần release kế tiếp.

## Hệ quả cho các PR sau

| PR | Theo đường dự phòng |
|---|---|
| 4 `feature/ingest-gharchive` | Vùng landing nằm trên runner (hoặc artifact/cache của Actions), không phải Volume của Databricks |
| 5 `feature/bronze-silver-spark` | PySpark + Delta chạy trên runner (4 CPU, 16 GB RAM, 14 GB đĩa): mỗi lần vài giờ dữ liệu, lọc trước khi lưu |
| 7 `feature/incremental-and-schedule` | Gold trên MotherDuck qua target dbt `motherduck` (`transform/profiles.yml`), token `MOTHERDUCK_TOKEN`, khoá phiên bản DuckDB |
| 9 `feature/governance` | Không có Unity Catalog: phân quyền, nhãn và che cột PII làm ở tầng dbt |
| 14 `docs/results` | README ghi rõ dự án dùng Spark và Delta Lake nhưng **không chạy trên Databricks** |

## Muốn thử lại Databricks (việc của chủ repo, không bắt buộc)

1. Trong workspace Free Edition: User Settings → Developer → Access tokens → tạo personal access token (chuỗi bắt đầu bằng `dapi`). Nếu Free Edition không có mục này thì đó chính là câu trả lời: không điều khiển được từ ngoài bằng token.
2. `gh secret set DATABRICKS_TOKEN` (dán token, không kèm dấu nháy). `DATABRICKS_HOST` là URL workspace dạng `https://<tên>.cloud.databricks.com`. `DATABRICKS_HTTP_PATH` không bắt buộc: thiếu thì probe tự tìm warehouse.
3. Sau lần release kế tiếp (khi `probe.yml` đã có trên `main`): `gh workflow run "Cloud probe"`, rồi xem Job summary hoặc artifact `probe-result`.
4. Nếu ra `primary`: mở PR cập nhật file này và bảng Stack trong `.claude/rules/tech-defaults.md`, rồi chuyển bronze/silver và gold sang Databricks.

Việc còn chờ chủ repo dù theo đường nào: cập nhật bảng Stack ở `.claude/rules/tech-defaults.md` theo kết quả này (phiên headless không được ghi vào `.claude/`).
