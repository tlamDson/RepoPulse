# Workflow — hub

> Quy tắc bất di bất dịch + quy ước phiên làm việc + nợ kỹ thuật. Chi tiết git/PR/commit và
> quy trình test nằm ở 2 file import bên dưới — sửa ở đó, không lặp lại ở đây.

@.claude/rules/developer-guide.md
@.claude/rules/tdd.md

## Quy tắc bất di bất dịch

1. **Mọi thay đổi đều phải qua PR vào `main`.** Không push thẳng lên `main`, không force push. Subagent viết code **vào worktree riêng (`EnterWorktree`) trước khi sửa file đầu tiên**; worktree mặc định rẽ từ `origin/main`, đúng nhánh gốc, không cần reset. Phiên chính ở checkout chính và chỉ sửa file trong `docs/`.
2. **Merge khi đủ hai điều kiện: CI xanh cả 3 job (`Lint`, `Unit Tests`, `dbt build (DuckDB)`) và `code-reviewer` kết luận PASS.** Đủ thì squash-merge ngay, không hỏi lại. Thiếu một trong hai → không merge. Không bao giờ `gh pr merge --admin`.
3. **PR sửa `.claude/settings.json` hoặc harness (`chore/claude-harness`) để mở cho chủ repo duyệt.**
4. **TDD bắt buộc**, commit đầu chỉ có test, thấy đỏ thật. Chi tiết: `.claude/rules/tdd.md`.
5. **Số đo chỉ lấy từ `runs.csv`; dữ liệu mô phỏng ghi rõ "simulated".**
6. **Cập nhật `CLAUDE.md` + `.claude/rules/*`** khi task làm thay đổi convention, tooling hay quy trình.

## Phiên làm việc tự động

Áp dụng sau khi plan đã được duyệt.

1. **Thực thi liên tục tới hết plan.** Không dừng để hỏi quyết định implementation nhỏ — tự quyết theo rule đã có và ghi một dòng vào `docs/progress.md` mục "Giả định của người thực thi".
2. **Xong PR → theo dõi CI bằng `gh pr checks <số-PR> --watch`** (chạy nền nếu cần) → giao `code-reviewer` → merge theo rule 2. Không báo "đang đợi CI" rồi dừng im lặng.
3. **Cập nhật nhánh bằng `gh pr update-branch <số-PR>`**, không rebase, không force push.
4. **Câu hỏi thật** (nghiệp vụ mơ hồ, hành động ngoài phạm vi đã cho phép, thiếu thông tin) → hỏi ngay bằng `AskUserQuestion`.
5. **Cuối phiên**: cập nhật `docs/progress.md`; nếu mọi việc còn lại chờ chủ repo thì dừng và in checklist.

## Nợ kỹ thuật

Bug tìm thấy ngoài phạm vi task → viết test khẳng định hành vi **hiện tại** (kèm `# [BUG]`), không tự sửa. Ghi ngay vào `.claude/rules/tech-defaults.md` mục "Nợ kỹ thuật", mỗi khoản đủ ba phần:

- **Triệu chứng** — hệ thống/người dùng thấy gì.
- **Nguyên nhân** — chỗ nào trong code, kèm `file:line`.
- **Điều kiện kích hoạt** — khi nào nó thành lỗi thật.

Sửa xong thì xoá khỏi danh sách trong cùng commit với bản sửa.
