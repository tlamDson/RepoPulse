# Workflow — hub

> Quy tắc bất di bất dịch + chạy headless + nợ kỹ thuật. Chi tiết git/PR/commit, quy trình test và
> kiểu lệnh nằm ở 3 file import bên dưới — sửa ở đó, không lặp lại ở đây.

@.claude/rules/developer-guide.md
@.claude/rules/tdd.md
@.claude/rules/command-style.md

## Quy tắc bất di bất dịch

1. **Mọi thay đổi đều qua PR.** Feature → `develop` (squash). Release `develop` → `main` (merge commit). Không push thẳng lên `develop`/`main`, không force push, không `--admin`.
2. **Merge feature vào `develop` khi đủ hai điều kiện:** CI xanh cả 3 job (`Lint`, `Unit Tests`, `dbt build (DuckDB)`) và `code-reviewer` kết luận PASS. Đủ thì merge ngay, không hỏi.
3. **Release `develop` → `main`** do `scripts/orchestrate.py` (hoặc `release-manager`) làm ở các mốc trong `scripts/prs.toml` (`release_after`), khi CI của PR release xanh. Merge commit thật, không squash.
4. **PR sửa `.claude/settings.json`, `.claude/auto-mode.json` hoặc hook** để chủ repo duyệt và tự merge.
5. **TDD bắt buộc**, commit đầu chỉ có test, thấy đỏ thật. Chi tiết: `.claude/rules/tdd.md`.
6. **Số đo chỉ lấy từ `runs.csv`; dữ liệu mô phỏng ghi rõ "simulated".**
7. **Cập nhật `CLAUDE.md` + `.claude/rules/*`** khi task làm thay đổi convention, tooling hay quy trình.

## Chạy headless (`scripts/orchestrate.py`)

Mỗi PR là một phiên `claude -p --permission-mode auto --permission-prompts none` chạy trong worktree riêng `.claude/worktrees/pr-<id>` do script tạo từ `origin/develop`. Không có người để hỏi:

1. **Không hỏi.** `AskUserQuestion` bị gỡ; mọi quyết định nhỏ ghi một dòng vào mục `## Giả định` của mô tả PR.
2. **Không tạo worktree con.** Phiên PR và mọi subagent làm ngay trong worktree của PR; không dùng `isolation: "worktree"`, không `EnterWorktree`.
3. **Giao việc cho agent chuyên trách** ghi ở cột `agent` của `scripts/prs.toml`, dán nguyên văn `.claude/rules/command-style.md` vào brief.
4. **Chờ CI** bằng `gh pr checks <số> --watch --interval 30` foreground, timeout 600000, lặp tới khi xong. CI đỏ thì sửa và push lại.
5. **Review** bằng `code-reviewer`; FAIL thì sửa rồi review lại, tối đa 2 vòng.
6. **Bị từ chối quyền** thì làm theo mục "Bị từ chối thì sao" của `command-style.md`.
7. **Không sửa `docs/progress.md`** trong PR feature (chạy song song sẽ conflict); script tự sinh lại tiến độ.
8. **Dòng cuối cùng** của phiên luôn là `RESULT: merged <số PR>` | `RESULT: failed <lý do>` | `RESULT: blocked <việc cần chủ repo>`. Script đọc dòng này để đi tiếp.

Mọi lần bị hỏi/bị từ chối được hook `log_permission.py` ghi vào `.claude/state/permission-log.jsonl`; báo cáo mỗi đêm ở `logs/run-<ngày>.md`.

## Phiên tương tác

Khi Lam mở phiên tay: ở checkout chính chỉ đọc và sửa `docs/`; việc sửa code vẫn đi qua worktree và PR như trên. Có câu hỏi thật thì hỏi ngay bằng `AskUserQuestion`.

## Nợ kỹ thuật

Bug tìm thấy ngoài phạm vi task → viết test khẳng định hành vi **hiện tại** (kèm `# [BUG]`), không tự sửa. Ghi ngay vào `.claude/rules/tech-defaults.md` mục "Nợ kỹ thuật", mỗi khoản đủ ba phần:

- **Triệu chứng** — hệ thống/người dùng thấy gì.
- **Nguyên nhân** — chỗ nào trong code, kèm `file:line`.
- **Điều kiện kích hoạt** — khi nào nó thành lỗi thật.

Sửa xong thì xoá khỏi danh sách trong cùng commit với bản sửa.
