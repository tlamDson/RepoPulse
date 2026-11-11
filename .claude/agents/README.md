# Custom Subagents — RepoPulse

## Agent hiện có

| Agent (`name`) | File | Model / effort | Tools | Dùng khi |
|---|---|---|---|---|
| `code-reviewer` | `code-reviewer.md` | opus / medium | Read, Grep, Glob, Bash | Review PR có bằng chứng (chạy lại test, commit đầu chỉ có test, mutation), kết luận PASS/FAIL trước khi merge. |
| `qa-testing` | `qa-testing.md` | sonnet / high | Read, Grep, Glob, Bash, Edit, Write | Viết/chạy test theo TDD — pytest, PySpark local, test dbt trên DuckDB. |
| `researcher` | `researcher.md` | sonnet / high | Read, Grep, Glob | Khám phá code/tài liệu trước khi lập kế hoạch hoặc sửa code. |

Subagent thực thi PR (Opus, medium, worktree riêng) dùng agent `general-purpose` với model/effort giao lúc gọi, không cần file riêng.

## Convention

Mỗi subagent là 1 file `.md` trong thư mục này. **Định danh thật là field `name` trong frontmatter, không phải tên file.**

```markdown
---
name: agent-slug              # bắt buộc
description: Agent làm gì và khi nào nên dùng.
tools: Read, Grep, Glob        # tuỳ chọn — chuỗi phân tách dấu phẩy, KHÔNG phải YAML list. Bỏ trống = kế thừa toàn bộ tool.
model: sonnet                  # tuỳ chọn — sonnet, opus, haiku, fable (hoặc full model ID)
effort: high                   # tuỳ chọn — low, medium, high, xhigh, max
---

System prompt của agent, viết như hướng dẫn cho đồng nghiệp mới nhận việc.
```
