# RepoPulse · Câu mở đầu cho phiên Claude Code

Mở phiên từ thư mục repo, gõ `D:` viết hoa:

```
cd D:\Desktop\RepoPulse
claude --permission-mode auto --model fable --effort high
```

## Chạy S0.4 (kiểm harness bằng PR mẫu)

> Thực thi bước S0.4 trong `docs/plan.md`: kiểm harness bằng PR mẫu `feature/runs-log` (module ghi `runs.csv`), đi hết vòng mini-plan → subagent Opus viết test đỏ rồi code trong worktree → CI → subagent Opus khác review có mutation → merge. Ghi kết quả từng mục kiểm vào `docs/harness-check.md`, cập nhật `docs/progress.md`, rồi dừng chờ tôi xem. Không làm PR 2 khi tôi chưa đồng ý.

## Chạy giai đoạn 1 (sau khi đã xem `docs/harness-check.md`)

> Điều phối giai đoạn 1 theo `docs/plan.md`. Bạn là phiên chính: ở checkout chính, không vào worktree, không sửa file được git theo dõi ngoài `docs/`. Đọc `docs/progress.md`, `gh pr list`, `git worktree list` trước. Với mỗi PR: viết kế hoạch ngắn có mục test phải đỏ; giao subagent Opus (effort medium, worktree riêng) viết test đỏ rồi code và mở PR; theo dõi CI bằng `gh pr checks --watch`; giao `code-reviewer` (model opus, effort medium) review có bằng chứng và mutation; CI xanh cộng review PASS thì squash-merge. Việc nhẹ giao Sonnet (effort high). Bắt đầu từ bước thử số 1 nếu secret đã có, không thì làm PR 2 và PR 3 trước. Dừng khi mọi việc còn lại chờ tôi, và in checklist.

## Mở phiên mới để làm tiếp

> Tiếp tục RepoPulse: đọc `docs/progress.md` và `docs/plan.md`, đối chiếu `gh pr list` và `git worktree list`, rồi làm tiếp.
