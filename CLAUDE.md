# RepoPulse — Claude Code Project Instructions

RepoPulse: data platform nhỏ đo "sức khoẻ" dự án open source từ dữ liệu sự kiện GitHub công khai (GH Archive) và một luồng event app mô phỏng theo logging spec. Stack: Python 3.11, PySpark + Delta Lake (Databricks Free Edition, hoặc chạy trên runner GitHub Actions nếu dùng đường dự phòng), dbt (DuckDB trên CI, Databricks SQL hoặc MotherDuck cho gold), GitHub Actions điều phối, dashboard tĩnh trên GitHub Pages.

Kế hoạch đầy đủ: `docs/plan.md`. Tiến độ: `docs/progress.md`. Việc chờ chủ repo: `docs/lam-checklist.md`.

Các rule chi tiết dưới đây áp dụng luôn, không chỉ khi được hỏi:

@.claude/rules/tech-defaults.md
@.claude/rules/workflow.md

## Quy tắc bắt buộc (không được vi phạm)

- **Mọi thay đổi đi qua PR vào `main`.** Không push thẳng lên `main`, không force push. Repo chỉ có một nhánh dài hạn là `main` (workflow chạy theo lịch của GitHub Actions chỉ chạy trên nhánh mặc định).
- **Merge = squash, chỉ khi CI xanh cả 3 job** (`Lint`, `Unit Tests`, `dbt build (DuckDB)`) **và review PASS** của agent `code-reviewer`. Đủ hai điều kiện thì phiên chính tự merge, không hỏi lại. Không bao giờ dùng `gh pr merge --admin`.
- **PR `chore/claude-harness` (và mọi PR sửa `.claude/settings.json`) để mở cho chủ repo duyệt**, không tự merge.
- **TDD bắt buộc** (Red → Green → Refactor), phải thấy đỏ thật và chép output đỏ vào mục `## RED` của mô tả PR. Chi tiết: `.claude/rules/tdd.md`.
- **Số đo chỉ lấy từ `runs.csv`.** README, mô tả PR và báo cáo không được trích số nào không có trong `runs.csv`.
- **Dữ liệu mô phỏng phải ghi rõ là mô phỏng** ("simulated") ở mọi chỗ báo cáo. GH Archive là dữ liệu thật.
- **Không đọc, không in giá trị secret** (`DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `MOTHERDUCK_TOKEN`, file `.env`). Chủ repo tự thêm secret.
- **Cập nhật `CLAUDE.md` + `.claude/rules/*`** khi task làm thay đổi convention, tooling hay quy trình.
- **Không tạo file `.md` mới ở thư mục gốc** — chỉ `README.md` và `CLAUDE.md`. Tài liệu khác để trong `docs/`.

## Lệnh hay dùng

```bash
pip install -e ".[dev,dbt]"          # cài một lần
pre-commit install                   # hook ruff + gitleaks
ruff check . && ruff format --check .
pytest
dbt build --project-dir transform --profiles-dir transform --target ci
gh pr checks <số-PR> --watch         # theo dõi CI
```

## Vai và model

| Vai | Model / effort | Làm gì |
|---|---|---|
| Phiên chính (điều phối) | Fable, high | Ở checkout chính, không vào worktree. Viết kế hoạch ngắn cho từng PR (có mục test phải đỏ), giao việc, theo dõi CI, quyết định merge, cập nhật `docs/progress.md`. Chỉ sửa file trong `docs/`. |
| Subagent thực thi | Opus, medium | Mỗi PR một agent trong worktree riêng: viết test đỏ, commit, rồi code, mở PR. |
| `code-reviewer` | Opus, medium | Review có bằng chứng: chạy lại test, chép output, kiểm commit đầu chỉ có test, thử mutation, kết luận PASS/FAIL. |
| `qa-testing`, việc nhẹ | Sonnet, high | Lint, cập nhật nhánh (`gh pr update-branch`), chép bằng chứng, PR chỉ có dữ liệu. |
| `researcher` | Sonnet, high | Đọc code/tài liệu, trả lời câu hỏi, không viết code. |

## Cổng cần chủ repo

- Thêm secret repo (Databricks, MotherDuck) — sau S0.
- Duyệt `docs/harness-check.md` trước khi làm PR 2.
- Duyệt và merge PR sửa `.claude/settings.json`.
- Đăng ký Snowflake trial khi tới PR 13 (đồng hồ 30 ngày chạy ngay).
- Publish dashboard lên Tableau Public.

Khi mọi việc còn lại đều chờ chủ repo: dừng và in checklist.
