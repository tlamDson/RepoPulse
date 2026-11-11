# RepoPulse · Kiểm harness (S0.4)

Phiên PR S0.4 (`feature/runs-log`) điền bảng này bằng bằng chứng thật (lệnh, output, đường dẫn log).
Chủ repo đọc xong thì thêm dòng `Lam đã duyệt` ở cuối file; orchestrator chỉ chạy PR B trở đi khi có
dòng đó (hoặc khi chạy với `--auto-approve-harness` và log sạch).

Điền ngày 2026-11-11 trong phiên PR S0.4, PR [#10](https://github.com/tlamDson/RepoPulse/pull/10).
Bảng ghi trạng thái lúc kiểm, trước khi merge; log còn dài thêm sau thời điểm đó.

| Kiểm | Đạt khi | Kết quả | Bằng chứng |
|---|---|---|---|
| Hỏi quyền / bị từ chối | 0 lần ngoài lệnh cố ý cấm (`gh secret`) | Đạt: 0 dòng của task `S0.4` | `.claude/state/permission-log.jsonl` có 4 dòng, cả 4 là `"task": "manual"` (phiên tay của chủ repo, 05:49–06:16Z). Ba agent con (`pipeline-ops`, `code-reviewer` hai vòng) đều báo không bị từ chối lệnh nào |
| Worktree | Phiên chạy trong `.claude/worktrees/pr-S0-4`, nhánh rẽ từ `origin/develop` | Đạt | Thư mục làm việc `D:\Desktop\RepoPulse\.claude\worktrees\pr-S0-4`, nhánh `feature/runs-log`; `git log --oneline -1 origin/develop` → `9289337 chore: add headless orchestrator and Claude Code harness (#2)`. Agent con làm ngay trong thư mục này, không tạo worktree con. Không gặp lỗi lệch `d:`/`D:` |
| Model và effort | Phiên PR Fable/high; `pipeline-ops` và `code-reviewer` Opus/medium | Model đạt; effort chưa kiểm được | `logs/pr-S0.4.jsonl`: message của phiên chính mang `"model":"claude-fable-5-2"`, message của agent con mang `"model":"claude-opus-5-5"`. Không tìm thấy trường effort nào trong log (đã tìm `effort`, `effortLevel`, `reasoning_effort`), nên effort chỉ biết qua frontmatter `effort: medium` của agent, chưa có bằng chứng lúc chạy |
| Hook | `ruff format` chạy sau khi sửa `.py`; hook log ghi được dòng | Đạt | Ghi file thử `hook_probe.py` (ngoài repo) với nội dung `x=1` / `def f( a,b ):` bằng tool Write; đọc lại ra `x = 1` / `def f(a, b):`, và harness báo `PostToolUse hook modified ...hook_probe.py`. Hook log: `permission-log.jsonl` có dòng `PermissionRequest` và `PermissionDenied` |
| Bảo vệ nhánh | `develop` và `main` bắt buộc PR và 3 check; push thẳng bị deny | API đạt; luật deny có nhưng không thử | `gh api repos/tlamDson/RepoPulse/branches/develop/protection` và `.../main/protection` cùng trả `checks: ["Lint","Unit Tests","dbt build (DuckDB)"]`, `pr_required: true`, `enforce_admins: true`, `force_push: false`, `deletions: false`, `strict: false`. Luật deny `git push origin develop`/`main` nằm ở `.claude/settings.json:38-47`; phiên này không thử push thẳng |
| CI | Đỏ trên commit chỉ có test, xanh sau khi có code | Đạt | Commit test `e8ff0b4`: `Unit Tests` fail vì `ModuleNotFoundError: No module named 'repopulse.runs_log'`, `Lint` và `dbt build (DuckDB)` pass — [run 42972278922](https://github.com/tlamDson/RepoPulse/actions/runs/42972278922). Commit code `6e44f9b`: 3 job pass — [run 42972370013](https://github.com/tlamDson/RepoPulse/actions/runs/42972370013). Commit bổ sung test `850d489`: 3 job pass — [run 42973204178](https://github.com/tlamDson/RepoPulse/actions/runs/42973204178) |
| Review | `code-reviewer` chép output test thật và bắt được mutation | Đạt, có lệch quy trình (xem ghi chú 3, 4) | Vòng 1 [FAIL](https://github.com/tlamDson/RepoPulse/pull/10#issuecomment-6788864712): chép output pytest/ruff, thử 13 mutation, 11 bị bắt, 2 sống sót (một cái chặn merge: thiếu case `datetime` cho `data_date`). Sau commit `850d489`, vòng 2 [PASS](https://github.com/tlamDson/RepoPulse/pull/10#issuecomment-6789118704): cả 2 mutation đó bị bắt |
| Secret | Push protection bật; `.env` không đọc được | Push protection đạt; `.env` có luật deny nhưng không thử | `gh api repos/tlamDson/RepoPulse --jq .security_and_analysis` → `secret_scanning: enabled`, `secret_scanning_push_protection: enabled`. Luật deny `Read(./.env)`, `Read(./**/.env)`, `Read(./.env.local)` nằm ở `.claude/settings.json:35-37`; phiên này không thử đọc `.env` |
| Quota | Ghi lại lần đầu thấy `rate_limit_event` thật (nếu có) | Có event, chưa lần nào bị chặn | `logs/pr-S0.4.jsonl` lúc kiểm có 19 dòng chứa `rate_limit_event`; không dòng nào có `"status":"rejected"` hay `"errorCode":"credits_required"` |

## Ghi chú cho chủ repo

Những chỗ harness chạy khác với tài liệu, thấy trong phiên này. Phiên PR không được sửa `.claude/`,
nên chỉ ghi lại ở đây.

1. **`pytest` và `ruff` không có trong PATH của Git Bash.** `pytest -q` và `ruff --version` trả
   exit code 127 (`command not found`); `python -m pytest` và `python -m ruff` chạy được. Các lệnh
   trong `CLAUDE.md` và `.claude/rules/tdd.md` viết dạng trần (`pytest`, `ruff check .`), nên phiên
   nào cũng mất một lần thử hỏng. Brief của agent trong phiên này đã ghi rõ dạng `python -m`.
2. **Commit đầu của PR không thể "chỉ có test" nếu test import module first-party chưa tồn tại.**
   Với ruff 0.17.4, `from repopulse.runs_log import ...` bị xếp vào nhóm bên thứ ba khi file
   `repopulse/runs_log.py` chưa có, và báo `I001`; khi file có rồi thì thứ tự đúng lại đảo ngược.
   PR #10 thêm `[tool.ruff.lint.isort] known-first-party = ["repopulse"]` vào `pyproject.toml` ngay
   trong commit test. `code-reviewer` đã tự tái hiện và xác nhận. Từ PR sau cấu hình này đã có sẵn,
   nhưng package mới (`eventspec`, `simkit`, `benchmarks`) sẽ gặp lại đúng lỗi này cho tới khi được
   thêm vào danh sách.
3. **`code-reviewer` không có tool Edit**, trong khi `command-style.md` quy định thử mutation bằng
   Edit rồi `git checkout --`. Reviewer đã thử mutation trong bộ nhớ bằng `python -c` (đọc file, thay
   một chuỗi, `exec` vào module, gọi `pytest.main`), file trên đĩa không đổi. Cách này chạy được
   nhưng lệch chữ của rule "không dùng `python -c`".
4. **Một lượt `code-reviewer` bị treo.** Lượt tiếp nối vòng 2 (gửi qua `SendMessage` vào agent của
   vòng 1) dừng với `Agent stalled: no progress for 600s (stream watchdog did not recover)` trước khi
   kết luận. Phiên điều phối mở một `code-reviewer` mới với brief gọn hơn; lượt đó xong trong khoảng
   một phút. Tính là vòng review thứ 2, không phải vòng thứ 3.
5. **`gh pr checks <số> --watch` ngay sau khi push** có thể trả `no checks reported on the
   'feature/runs-log' branch` với exit code 1 vì check chưa kịp đăng ký; gọi lại lần hai thì chạy.
   Lệnh này cũng trả exit code 1 khi có job đỏ, kể cả lúc đỏ là điều mong đợi.
6. **Dòng `Co-Authored-By`** của hai commit do `pipeline-ops` tạo ghi `Claude Opus 5.5`, commit của
   phiên điều phối ghi `Claude Fable 5.2`. Squash merge gộp cả hai.
