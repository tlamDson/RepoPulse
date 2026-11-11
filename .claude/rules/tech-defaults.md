# Tech defaults — RepoPulse

> Stack, layout, luật dữ liệu, cạm bẫy và nợ kỹ thuật. Kế hoạch và lý do chọn stack: `docs/plan.md`.

## Stack

| Lớp | Công nghệ | Ghi chú |
|---|---|---|
| Ngôn ngữ | Python 3.11 | `pyproject.toml`; lint/format bằng `ruff` (line-length 100) |
| Test | `pytest` | `tests/`, xem `.claude/rules/tdd.md` |
| Điều phối | GitHub Actions | Cron chỉ chạy trên `main`, có thể trễ, tự tắt sau 60 ngày repo không hoạt động |
| Bronze/Silver | PySpark + Delta Lake | Databricks Free Edition (serverless, chỉ batch `availableNow`). Dự phòng: chạy trên runner Actions (4 CPU, 16 GB RAM, 14 GB đĩa) |
| Gold | dbt | `transform/`; CI chạy trên DuckDB (`--target ci`); gold thật trên Databricks SQL warehouse hoặc MotherDuck |
| Dashboard | Trang tĩnh trên GitHub Pages + Tableau Public (publish tay) | Databricks dashboard không chia sẻ công khai được |
| Bảo mật | pre-commit (`ruff`, `gitleaks`), secret scanning + push protection, Dependabot (`pip`, `github-actions`) | |

Bước thử số 1 (`docs/plan.md`) quyết định đường chính (Databricks) hay đường dự phòng (runner + MotherDuck). Kết quả ghi ở `docs/progress.md`; cập nhật bảng trên khi đã chốt.

## Layout

```
repopulse/        package Python chính (ghi runs.csv, tiện ích chung)
eventspec/        catalog event YAML → JSON Schema, hàm kiểm event       (PR 2)
simkit/           sinh dữ liệu mô phỏng có seed, chèn lỗi biết trước     (PR 3)
tests/            pytest; fixtures/ chứa dữ liệu mẫu nhỏ
transform/        project dbt (dbt_project.yml, profiles.yml, models/)
.github/workflows CI (ci.yml), pipeline theo lịch (PR 7), deploy Pages (PR 11)
docs/             plan, progress, checklist, harness-check
runs.csv          sổ số đo, mỗi lần chạy một dòng
```

Cập nhật sơ đồ này khi thêm thư mục mới.

## Luật dữ liệu

- **Số đo chỉ lấy từ `runs.csv`.** Không trích số ước lượng, số "khoảng", hay số chạy tay không ghi lại.
- **Dữ liệu mô phỏng ghi rõ "simulated"** ở README, dashboard, mô tả PR. GH Archive là dữ liệu thật.
- **Băm PII trước silver.** Gold không bao giờ chứa email/giá trị PII thô; có test tự động kiểm điều này (PR 9).
- **Idempotent theo (ngày, giờ).** Chạy lại cùng giờ dữ liệu không sinh bản ghi trùng.
- **Event sai schema vào bảng quarantine**, không bị bỏ im lặng.
- Gọi đúng tên: giai đoạn 1 là "incremental batch", không gọi là real-time/streaming.

## Secret

`DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `MOTHERDUCK_TOKEN` — chủ repo tự thêm bằng `gh secret set` hoặc trên web. Agent không đọc `.env`, không in giá trị secret, không chạy `gh secret` (lệnh này ở nhóm `ask`). Biến mới thêm vào `.env.example` với giá trị rỗng.

## Cạm bẫy đã biết

- **Databricks Free Edition**: tài liệu chính thức chưa xác nhận tạo token/điều khiển từ ngoài; truy cập internet theo danh sách domain (chưa biết `data.gharchive.org` có được phép); vượt quota thì compute tắt tới hết ngày; tối đa 5 job task đồng thời. Lịch chạy thưa, job idempotent để hôm sau chạy bù.
- **Đĩa runner 14 GB**: mỗi lần chạy chỉ xử lý vài giờ dữ liệu, lọc loại event trước khi lưu.
- **Snowflake trial 30 ngày**: chỉ đăng ký khi tới PR 13.
- **Windows**: mở phiên với `D:` viết hoa (xem `developer-guide.md`). Hook `ruff format` cần `ruff` đã cài trong Python mà lệnh `python` trỏ tới; chưa cài thì hook bỏ qua im lặng.

## Nợ kỹ thuật

(mỗi khoản đủ ba phần: Triệu chứng, Nguyên nhân kèm `file:line`, Điều kiện kích hoạt — xem `workflow.md`)

- Chưa có.
