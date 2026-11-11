# Developer Guide — Git Flow, Worktree, Commit, PR

> Quy ước đóng góp code cho RepoPulse. Quy trình test: `.claude/rules/tdd.md`. Kiểu lệnh:
> `.claude/rules/command-style.md`.

## Git Flow

`develop ← <type>/<mô-tả>` (PR, **squash**), `main ← develop` (PR release, **merge commit**). Không push thẳng lên `develop` hay `main`.

- **`main` là nhánh mặc định** của GitHub. Workflow chạy theo lịch (`schedule`) chỉ chạy trên `main`, và `workflow_dispatch` chỉ thấy file workflow có trên `main`. Vì vậy pipeline/cron chỉ bắt đầu chạy sau release; mốc release ghi ở `scripts/prs.toml` (`release_after`): sau PR B (baseline), PR 7 (cron), PR 11 (Pages), PR 14.
- **Nhánh feature rẽ từ `origin/develop` mới nhất**, tự xoá sau merge.
- **Branch types:** `feature/`, `fix/`, `test/`, `docs/`, `chore/`. Chữ thường, phân tách bằng `-`, mô tả cụ thể phạm vi (`feature/ingest-gharchive`, không `feature/update`).

## Worktree

Mỗi việc viết code có một worktree riêng dưới `.claude/worktrees/`. Lý do: nhiều phiên chạy song song trên cùng thư mục thì `git checkout`/`git reset` của phiên này âm thầm xoá thay đổi chưa commit của phiên kia.

- **Chạy headless:** `scripts/orchestrate.py` tạo `.claude/worktrees/pr-<id>` từ `origin/develop` và chạy phiên PR trong đó. Phiên và subagent không tạo worktree con.
- **Phiên tay:** `git worktree add .claude/worktrees/<tên> -b <type>/<mô-tả> origin/develop`. Không dùng `EnterWorktree` (nó rẽ từ nhánh mặc định `main`, sai gốc).
- **Dọn:** `python scripts/orchestrate.py clean` (hoặc `/wt-clean`) xoá worktree có PR đã merge, cây sạch, không còn phiên chạy trong đó. Không tự `rm -rf` worktree.
- Worktree mới không có môi trường Python riêng: dùng chung `pip install -e ".[dev,dbt]"` đã cài. Không copy `.env` vào worktree.
- Cạm bẫy Windows: mở terminal bằng `cd D:\Desktop\RepoPulse` (chữ `D:` viết hoa). Đường dẫn `d:` và `D:` lệch nhau làm Claude Code coi là hai project khác nhau (memory, settings).

## Commit Messages

Imperative mood, hoàn thành câu _"If applied, this commit will… [message]"_. Một dòng, không prefix `feat:`/`fix:` (prefix chỉ dành cho PR title). Verb chuẩn: `add`, `fix`, `update`, `remove`, `refactor`, `test`, `docs`, `chore`.

```
add failing tests for runs.csv schema validation
add runs.csv append with schema check
fix late-event window off-by-one in silver dedup
```

Không commit: `.env`/credentials/tokens, `.venv/`, `transform/target/`, `*.duckdb`, dữ liệu thô (`data/raw/`, `*.json.gz`, `*.parquet`), `logs/`, `.claude/state/`, file không liên quan task.

## Pull Requests

Feature target `develop`; chỉ PR release target `main`. Title `<type>: <short description>` (< 72 ký tự): `feat:`, `fix:`, `test:`, `docs:`, `chore:`, `refactor:`, `release:`. Mô tả theo `.github/pull_request_template.md`, thêm mục `## Giả định` khi chạy headless:

```markdown
## Summary
- <thay đổi chính>
- <lý do / impact>

## RED
<output test đỏ thật, chép từ lần chạy trên commit chỉ có test>

## Giả định
- <quyết định nhỏ đã tự chọn>

## Test plan
- [ ] pytest pass locally
- [ ] dbt build --target ci pass
- [ ] ruff check + ruff format --check sạch
- [ ] Không commit secret / .env
```

### Điều kiện merge

| Target | Cách merge | Điều kiện |
|---|---|---|
| `develop` | `gh pr merge <số> --squash --delete-branch` | CI xanh cả 3 job + `code-reviewer` PASS (ghi comment trên PR) |
| `main` | `gh pr merge <số> --merge` (PR `develop → main`) | CI của PR release xanh; chỉ ở mốc `release_after` |
| PR sửa `.claude/settings.json`, `.claude/auto-mode.json`, hook | Chủ repo tự merge | Chủ repo duyệt |

Không bao giờ thêm `--admin`.

**Trước khi push thêm commit vào PR đã mở**, chạy `gh pr view <số> --json state` — PR đã `MERGED`/`CLOSED` thì push không trigger CI; tạo nhánh mới từ `origin/develop` cho phần việc còn lại.

Nhánh bị tụt sau `develop`: `gh pr update-branch <số>`, không rebase, không force push.

### Sửa `.github/workflows/ci.yml` — đừng làm mất required check

Tên ba job (`Lint`, `Unit Tests`, `dbt build (DuckDB)`) **chính là** định danh required status check của `develop` và `main`. Vì vậy:

- **Không đổi tên** ba job này. Cần thêm việc thì thêm job mới tên khác.
- **Không thêm `paths:` vào `on: pull_request`** — workflow không chạy, check treo ở _Expected_, PR không merge được.
- **Không đặt `if:` ở cấp job** cho ba job này. Muốn bỏ qua việc nặng thì gate ở cấp **step**.
- Không tự ý thêm job khác vào required checks.
