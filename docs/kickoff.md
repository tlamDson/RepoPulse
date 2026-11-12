# RepoPulse · Cách chạy

Mở terminal từ thư mục repo, gõ `D:` viết hoa:

```
cd D:\Desktop\RepoPulse
pip install -e ".[dev,dbt]"
```

## Chạy headless qua đêm (cách chính)

```
python scripts/orchestrate.py run --dry-run                          # xem sẽ chạy việc nào, bị chặn ở đâu
python scripts/orchestrate.py run --auto-approve-harness             # chạy, rồi đi chỗ khác
```

- Script đọc `scripts/prs.toml`, chạy tối đa 5 phiên `claude -p` song song (`--parallel N` để đổi),
  mỗi việc một worktree `.claude/worktrees/pr-<id>`. Số phiên thật bị giới hạn bởi độ rộng DAG.
  Không phiên nào chờ người: lệnh cần hỏi bị từ chối và ghi log.
- Mỗi PR có hai loại phiên: phiên PR (Fable) viết code tới khi CI xanh rồi báo `ready`; sau đó một
  phiên review mới (`--agent code-reviewer`, Opus) review độc lập. PASS thì script tự kiểm 3 check
  bắt buộc và merge; FAIL thì phiên PR chạy lại kèm kết luận review, tối đa 2 vòng review.
- CI trên `develop` đỏ (sau một lần merge): không mở việc mới, không release. Chỉ lượt CI xanh mới
  gỡ cờ (`gh` lỗi hay lượt bị huỷ thì giữ nguyên). Nếu lúc đó không còn phiên nào chạy, script dừng
  và ghi `develop-ci` vào "Việc cần Lam"; sửa xong thì chạy lại.
- Phiên im lặng quá 30 phút (`--stall-minutes`, trừ lúc chờ quota reset) bị coi là treo và dừng.
  Việc thất bại được thử lại 1 lần (tổng 2 lần chạy); lần thử lại nhận kèm lý do và phần cuối log
  lần trước.
- `--auto-approve-harness`: nếu S0.4 merge xanh và log không có lệnh bị từ chối ngoài dự kiến, coi
  như harness đã duyệt và chạy tiếp từ PR B. Bỏ cờ này nếu muốn tự xem `docs/harness-check.md` trước.
- Thiếu secret cloud: làm hết việc không cần cloud (S0.4, B, 2, 3, 4, 5, 6, 12) rồi dừng ở `probe`.
- Hết quota: chạy tới khi hết. Limit 5 giờ: phiên tự chờ; nếu phiên thoát vì limit thì script ngủ tới giờ reset rồi
  `--resume` đúng phiên đó. Limit tuần (reset xa hơn 6 giờ) thì dừng và báo; thêm `--hold-at <phần trăm>` (số nguyên 1–100) để thôi mở việc mới khi quota 5 giờ hoặc 7 ngày chạm ngưỡng đó; `--hold-at-90` tương đương `--hold-at 90` (chừa lại 10% quota).
- Release `develop → main` tự động sau B, 7, 11, 14 khi CI xanh.

**Dừng:** tạo file `.claude/state/STOP`. Việc đang chạy làm nốt rồi script dừng.

**Sáng ra xem:** `logs/run-<ngày>.md` (việc cần Lam, kết quả từng việc, tiến độ, danh sách lệnh bị
hỏi/bị từ chối). Log chi tiết từng việc: `logs/pr-<id>.jsonl`.

**Báo cáo ngày qua email:** mỗi phiên kết thúc ghi một dòng (lượt, phút, USD, lệnh bị từ chối,
subagent) vào `logs/orchestrator-metrics.csv`. Lúc 22h (`--daily-hour`), nếu orchestrator đang chạy,
nó viết `logs/daily-<ngày>.md` cho **24 giờ tính tới lúc gửi** (phiên xong sau 22h vào mail hôm sau;
`daily --date` thì lấy đúng ngày lịch), gồm: PR đã merge, thất bại/review FAIL kèm lý do, chi phí theo việc, đề
xuất luật từ lệnh bị từ chối lặp lại, việc cần Lam. Rồi gửi mail, mỗi ngày một lần; chỉ đánh dấu
"đã gửi" khi mail đi thật, nên đặt biến SMTP muộn trong tối thì `daily` vẫn gửi được.

- Bật mail: tạo Gmail App password, rồi đặt biến môi trường của Windows (mở terminal mới sau đó):
  `setx REPOPULSE_SMTP_USER "<gmail>"`, `setx REPOPULSE_SMTP_APP_PASSWORD "<app password>"`,
  tuỳ chọn `setx REPOPULSE_REPORT_TO "<người nhận>"`. Thiếu thì chỉ ghi file.
- Khi orchestrator không chạy lúc 22h, đặt Task Scheduler gọi lệnh `daily` (không gửi trùng):
  `schtasks /Create /SC DAILY /ST 22:05 /TN RepoPulseDaily /TR "python D:\Desktop\RepoPulse\scripts\orchestrate.py daily"`
- Đề xuất luật chỉ là đề xuất: Lam duyệt rồi tự thêm vào `.claude/rules/` (đường dẫn được bảo vệ).

```
python scripts/orchestrate.py daily [--date 2026-11-11] [--no-send] [--force]
python scripts/orchestrate.py report         # tiến độ theo PR đã merge
python scripts/orchestrate.py quota          # quota gần nhất
python scripts/orchestrate.py clean --dry-run
```

## Mở phiên tay (khi cần sửa harness hoặc xem lại)

```
claude --permission-mode auto --settings .claude/auto-mode.json --model fable --effort high
```

> Tiếp tục RepoPulse: chạy `python scripts/orchestrate.py report`, đọc `logs/` mới nhất và
> `docs/plan.md`, đối chiếu `gh pr list` và `git worktree list`, rồi báo việc còn lại.
