# RepoPulse · Tiến độ

Plan: [`plan.md`](plan.md). Phiên điều phối cập nhật file này sau mỗi PR, để phiên mới đọc là làm tiếp được.

## Subplan S0

| Bước | Việc | Trạng thái | PR | Ghi chú |
|---|---|---|---|---|
| S0.1 | Tạo repo, cấu hình merge, file gốc, tài liệu | xong | — | 2026-11-10: repo public `tlamDson/RepoPulse`, chỉ squash merge, tự xoá nhánh sau merge |
| S0.2 | GitHub DevOps: CI, mẫu PR, Dependabot, pre-commit, bảo vệ nhánh `main`, bảo mật, Pages | đang làm | | |
| S0.3 | Harness `.claude` lấy từ TravelPlan | đang làm | | Chờ chủ repo duyệt |
| S0.4 | Kiểm harness bằng PR mẫu `feature/runs-log` | chưa làm | | Làm trong phiên mở tại repo này; kết quả ghi `harness-check.md` |

## Giai đoạn 1

| # | PR | Trạng thái | Số PR | Ghi chú |
|---|---|---|---|---|
| 1 | Bước thử Databricks Free Edition (không PR) | chờ secret | — | |
| 2 | `feature/event-spec` | chưa làm | | |
| 3 | `feature/simkit-generator` | chưa làm | | |
| 4 | `feature/ingest-gharchive` | chưa làm | | |
| 5 | `feature/bronze-silver-spark` | chưa làm | | |
| 6 | `feature/dbt-star-schema` | chưa làm | | |
| 7 | `feature/incremental-and-schedule` | chưa làm | | |
| 8 | `feature/data-quality-alerts` | chưa làm | | |
| 9 | `feature/governance` | chưa làm | | |
| 10 | `feature/query-optimization` | chưa làm | | |
| 11 | `feature/dashboard-site` | chưa làm | | |
| 12 | `feature/spark-vs-duckdb-benchmark` | chưa làm | | |
| 13 | `feature/snowflake-port` | chờ đăng ký trial | | |
| 14 | `docs/results` | chưa làm | | |

## Việc chờ chủ repo

Xem [`lam-checklist.md`](lam-checklist.md).

## Giả định của người thực thi

(mỗi quyết định nhỏ một dòng)

- S0.1: phiên bản thư viện Python mới khai theo khoảng (`>=`), chưa có file khoá. Việc khoá phiên bản làm trong S0.4, khi có môi trường cài được `pip-tools` hoặc `uv`.

## Số đo đã có

(chỉ ghi số lấy từ `runs.csv`)
