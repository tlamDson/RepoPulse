# Custom Subagents — RepoPulse

## Agent hiện có

| Agent (`name`) | Model / effort | Mảng | Dùng cho |
|---|---|---|---|
| `pipeline-ops` | opus / medium | GitHub Actions, ingest GH Archive, bước thử cloud, `runs.csv`, deploy Pages | S0.4, 4, probe |
| `event-schema-engineer` | opus / medium | eventspec (catalog → JSON Schema), simkit (dữ liệu mô phỏng có seed, chèn lỗi) | 2, 3 |
| `spark-engineer` | opus / medium | PySpark + Delta bronze/silver, PII, quarantine, benchmark Spark/DuckDB | 5, 12 |
| `analytics-engineer` | opus / medium | baseline Q1–Q6, dbt star schema, mart, incremental, tối ưu query, Snowflake | B, 6, 7, 10, 13 |
| `data-quality-governance` | opus / medium | phát hiện bất thường, cảnh báo, PII, phân quyền, che cột | 8, 9 |
| `dashboard-builder` | opus / medium | trang tĩnh Pages, xuất CSV cho Tableau | 11 |
| `release-manager` | sonnet / high | release develop → main, progress, README kết quả | 14, release thất bại |
| `code-reviewer` | opus / medium | review có bằng chứng: chạy lại test, commit đầu chỉ có test, mutation, PASS/FAIL | mọi PR |
| `qa-testing` | sonnet / high | viết/chạy test bổ sung | khi được giao |
| `researcher` | sonnet / high | đọc code/tài liệu, không viết code | khi được giao |

Bảng việc → agent nằm ở `scripts/prs.toml` (cột `agent`); `tests/test_orchestrator.py` kiểm mọi agent
trong đó đều có file ở đây. Mọi agent tuân `.claude/rules/command-style.md`.

## Convention

Mỗi subagent là 1 file `.md` trong thư mục này. **Định danh thật là field `name` trong frontmatter, không phải tên file** — nhưng giữ tên file trùng `name` để test kiểm được.

```markdown
---
name: agent-slug              # bắt buộc, trùng tên file
description: Agent làm gì và khi nào nên dùng.
tools: Read, Grep, Glob        # tuỳ chọn — chuỗi phân tách dấu phẩy, KHÔNG phải YAML list. Bỏ trống = kế thừa toàn bộ tool.
model: sonnet                  # tuỳ chọn — sonnet, opus, haiku, fable (hoặc full model ID)
effort: high                   # tuỳ chọn — low, medium, high, xhigh, max
---

System prompt của agent, viết như hướng dẫn cho đồng nghiệp mới nhận việc.
```
