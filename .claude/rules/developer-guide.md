# Developer Guide — Git, Worktree, Commit, PR

> Quy ước đóng góp code cho RepoPulse. Quy trình test nằm ở `.claude/rules/tdd.md`.

## Nhánh

`main ← <type>/<mô-tả> (PR, squash merge)`. Một nhánh dài hạn duy nhất là `main`, không có `develop`: workflow chạy theo lịch (`schedule`) và `workflow_dispatch` của GitHub Actions chỉ được nhận diện trên nhánh mặc định, nên pipeline phải sống trên `main`. Nhánh feature tự xoá sau merge.

**Branch types:** `feature/`, `fix/`, `test/`, `docs/`, `chore/`. Chữ thường, phân tách bằng `-`, mô tả cụ thể phạm vi (`feature/ingest-gharchive`, không `feature/update`).

## Worktree

Subagent viết code **luôn** làm trong worktree riêng, gọi `EnterWorktree` trước khi sửa file đầu tiên. Lý do: nhiều phiên chạy song song trên cùng thư mục thì `git checkout`/`git reset` của phiên này âm thầm xoá thay đổi chưa commit của phiên kia.

`EnterWorktree` rẽ từ `origin/main`, đúng nhánh gốc của repo này, nên chỉ cần đổi tên nhánh:

```bash
git fetch origin main
git branch -m <type>/<mô-tả-kebab-case>
```

Worktree mới không có môi trường Python: `pip install -e ".[dev,dbt]"` (hoặc tạo `.venv` riêng). Không copy `.env` vào worktree trừ khi task thật sự cần gọi dịch vụ ngoài.

Cạm bẫy Windows: mở phiên bằng `cd D:\Desktop\RepoPulse` (chữ `D:` viết hoa). Đường dẫn `d:` và `D:` lệch nhau có thể làm Claude Code coi là hai project khác nhau (memory, settings, worktree).

## Commit Messages

Imperative mood, hoàn thành câu _"If applied, this commit will… [message]"_. Một dòng, không prefix `feat:`/`fix:` (prefix chỉ dành cho PR title). Verb chuẩn: `add`, `fix`, `update`, `remove`, `refactor`, `test`, `docs`, `chore`.

```
add failing tests for runs.csv schema validation
add runs.csv append with schema check
fix late-event window off-by-one in silver dedup
```

Không commit: `.env`/credentials/tokens, `.venv/`, `transform/target/`, file `*.duckdb`, dữ liệu thô (`data/raw/`, `*.json.gz`, `*.parquet`), file không liên quan task.

## Pull Requests

Target luôn là `main`. Title `<type>: <short description>` (< 72 ký tự): `feat:`, `fix:`, `test:`, `docs:`, `chore:`, `refactor:`. Mô tả theo `.github/pull_request_template.md`:

```markdown
## Summary
- <thay đổi chính>
- <lý do / impact>

## RED
<output test đỏ thật, chép từ lần chạy trên commit chỉ có test>

## Test plan
- [ ] pytest pass locally
- [ ] dbt build --target ci pass
- [ ] ruff check + ruff format --check sạch
- [ ] Không commit secret / .env
```

### Điều kiện merge

| Điều kiện | Ghi chú |
|---|---|
| CI xanh cả 3 job | `Lint`, `Unit Tests`, `dbt build (DuckDB)` |
| `code-reviewer` kết luận PASS | Ghi kết luận và bằng chứng vào PR (comment) |
| Không phải PR harness/settings | PR sửa `.claude/settings.json` để chủ repo duyệt |

```bash
gh pr checks <số-PR> --watch
gh pr merge <số-PR> --squash --delete-branch   # không bao giờ thêm --admin
```

**Trước khi push thêm commit vào PR đã mở**, chạy `gh pr view <số-PR> --json state` — PR đã `MERGED`/`CLOSED` thì push không trigger CI; tạo nhánh mới từ `main` cho phần việc còn lại.

Nhánh bị tụt sau `main`: `gh pr update-branch <số-PR>`, không rebase, không force push.

### Sửa `.github/workflows/ci.yml` — đừng làm mất required check

Tên ba job (`Lint`, `Unit Tests`, `dbt build (DuckDB)`) **chính là** định danh required status check của `main`. Vì vậy:

- **Không đổi tên** ba job này. Cần thêm việc thì thêm job mới tên khác.
- **Không thêm `paths:` vào `on: pull_request`** — workflow không chạy, check treo ở _Expected_, PR không merge được.
- **Không đặt `if:` ở cấp job** cho ba job này. Muốn bỏ qua việc nặng thì gate ở cấp **step**.
- Không tự ý thêm job khác vào required checks.
