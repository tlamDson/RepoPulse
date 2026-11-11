---
description: Dọn worktree của các PR đã merge (xem trước rồi mới xoá)
---

Dọn worktree dưới `.claude/worktrees/` bằng script, không tự `rm -rf`:

1. Chạy `python scripts/orchestrate.py clean --dry-run` và cho tôi xem bảng: worktree nào sẽ bị
   xoá (PR đã merge, cây sạch, không còn phiên chạy), worktree nào giữ lại và vì sao.
2. Nếu có ít nhất một dòng `remove`, chạy `python scripts/orchestrate.py clean`.
3. Báo lại kết quả và các worktree còn giữ (kèm lý do: PR còn mở, còn thay đổi chưa commit...).

Không xoá worktree đang có phiên `claude` chạy trong đó. Nếu Windows báo "Permission denied" khi
xoá thì có tiến trình còn mở thư mục đó — báo tôi, không thử xoá bằng cách khác.
