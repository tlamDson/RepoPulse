# Command style — chạy headless không bị chặn

> Bắt buộc cho phiên chính và mọi subagent. Phiên điều phối dán **nguyên văn** file này vào brief
> của từng agent. Lý do: phiên headless chạy `--permission-prompts none`, lệnh nào lẽ ra phải hỏi
> sẽ bị **từ chối ngay**. Trong lịch sử Auto_Apply, 221/414 lần phải hỏi người đến từ đúng các kiểu
> lệnh bị cấm dưới đây.

## Làm

- **Một lệnh mỗi lần gọi Bash.** Không nối bằng `&&`, `;`, `|` nếu không thật cần. Cần pipe thì giữ
  ngắn và chỉ dùng lệnh đọc (`| head`, `| tail`, `| grep`).
- **Không `cd`.** Dùng `git -C <path>`, `dbt --project-dir transform --profiles-dir transform`,
  `pytest <đường dẫn>`, đường dẫn tuyệt đối hoặc tương đối từ thư mục hiện tại.
- **Ghi file bằng tool Write/Edit**, không dùng `cat > file <<EOF`, `echo > file`, `tee`.
  Bằng chứng (output test đỏ, kết quả review) cũng ghi bằng Write.
- **Đọc và tìm bằng Read/Grep/Glob**, không dùng `cat`, `grep -r`, `find`, `python -c` để dò code.
- **git và gh chạy qua Bash**, không qua PowerShell.
- **File tạm** để trong scratchpad của phiên (đường dẫn tuyệt đối), tạo bằng Write.
- **Thử mutation**: chỉ trên file **đã commit**, sửa bằng Edit, chạy test, rồi hoàn tác bằng
  `git checkout -- <file>` và kiểm `git status --porcelain` sạch. `git checkout --` **không** hoàn
  tác được file chưa được theo dõi.
- **Gọi công cụ Python bằng `python -m`**: `python -m pytest`, `python -m ruff`,
  `python -m dbt.cli.main build …`. Lệnh trần `pytest`/`ruff`/`dbt` không có trong PATH trên máy
  local (exit code 127).
- **Chờ CI**: `gh pr checks <số> --watch --interval 30` chạy foreground với timeout 600000; hết giờ
  thì gọi lại, tới khi xong. Ngay sau khi push, lệnh có thể báo `no checks reported` (exit 1) vì check
  chưa kịp đăng ký: đợi rồi gọi lại. Exit 1 cũng xảy ra khi có job đỏ — đọc output, đừng chỉ nhìn
  exit code.

## Không làm

- Không dùng biến shell cho đường dẫn (`S=...; ... "$S/x"`), không `$(...)` chứa lệnh ghi.
- Không `rm -rf`, không xoá thư mục. Worktree do `scripts/orchestrate.py clean` dọn.
- Không `git stash` (dùng chung giữa mọi worktree), không `git reset --hard`, không `git clean`,
  không rebase, không force push.
- Không push lên `develop` hay `main`; chỉ push nhánh của việc đang làm.
- Không sửa gì trong `.claude/` (ngoài `.claude/worktrees/`), không sửa `.github/workflows/ci.yml`
  trừ khi PR được giao đúng việc đó.
- Không đọc `.env`, không chạy `gh secret`, không in token.
- Không `AskUserQuestion`, không `EnterPlanMode`. Quyết định nhỏ ghi vào mục `## Giả định` của PR.

## Bị từ chối thì sao

1. Đọc lý do. Viết lại lệnh theo các luật trên (thường là tách lệnh, bỏ `cd`, đổi heredoc sang Write).
2. Thử lại **một** lần.
3. Vẫn bị từ chối: ghi lệnh và lý do vào PR (mục `## Giả định`), bỏ qua bước đó nếu không bắt buộc;
   nếu bắt buộc thì kết thúc với `RESULT: blocked <lệnh bị chặn>`.
