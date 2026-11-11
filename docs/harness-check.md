# RepoPulse · Kiểm harness (S0.4)

Phiên PR S0.4 (`feature/runs-log`) điền bảng này bằng bằng chứng thật (lệnh, output, đường dẫn log).
Chủ repo đọc xong thì thêm dòng `Lam đã duyệt` ở cuối file; orchestrator chỉ chạy PR B trở đi khi có
dòng đó (hoặc khi chạy với `--auto-approve-harness` và log sạch).

| Kiểm | Đạt khi | Kết quả | Bằng chứng |
|---|---|---|---|
| Hỏi quyền / bị từ chối | 0 lần ngoài lệnh cố ý cấm (`gh secret`) | | `.claude/state/permission-log.jsonl` (task `S0.4`) |
| Worktree | Phiên chạy trong `.claude/worktrees/pr-S0-4`, nhánh rẽ từ `origin/develop` | | `git -C <worktree> log --oneline -1 origin/develop` |
| Model và effort | Phiên PR Fable/high; `pipeline-ops` và `code-reviewer` Opus/medium | | `logs/pr-S0.4.jsonl` (event `system/init`, model của subagent) |
| Hook | `ruff format` chạy sau khi sửa `.py`; hook log ghi được dòng | | |
| Bảo vệ nhánh | `develop` và `main` bắt buộc PR và 3 check; push thẳng bị deny | | `gh api repos/tlamDson/RepoPulse/branches/develop/protection` |
| CI | Đỏ trên commit chỉ có test, xanh sau khi có code | | link run CI |
| Review | `code-reviewer` chép output test thật và bắt được mutation | | comment trên PR |
| Secret | Push protection bật; `.env` không đọc được | | |
| Quota | Ghi lại lần đầu thấy `rate_limit_event` thật (nếu có) | | `python scripts/orchestrate.py quota` |
