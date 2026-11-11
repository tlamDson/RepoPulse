# RepoPulse · Việc cần chủ repo làm

Agent không làm thay được các việc dưới đây. Không dán token hay mật khẩu vào chat hoặc vào file trong repo.

## Trước khi chạy giai đoạn 1 (khoảng 30 phút)

| # | Việc | Ghi chú | Xong |
|---|---|---|---|
| 1 | Duyệt PR harness `chore/claude-harness` | Đọc danh sách allow, ask, deny trong `.claude/settings.json` trước khi merge | ☐ |
| 2 | Đăng ký Databricks Free Edition | Không cần thẻ. Tạo một personal access token trong User Settings → Developer | ☐ |
| 3 | Đăng ký MotherDuck Lite | Dự phòng cho trường hợp Databricks không điều khiển được từ ngoài. Không cần thẻ | ☐ |
| 4 | Đăng ký Tableau Public, cài Tableau Desktop Public Edition | Dùng ở PR dashboard | ☐ |
| 5 | Thêm secret vào repo | Lệnh ở dưới | ☐ |
| 6 | Xem `docs/harness-check.md` rồi cho chạy tiếp | Cổng giữa S0 và giai đoạn 1 | ☐ |

Thêm secret (chạy trong thư mục repo; mỗi lệnh sẽ hỏi giá trị, không hiện lên màn hình):

```
gh secret set DATABRICKS_HOST
gh secret set DATABRICKS_TOKEN
gh secret set MOTHERDUCK_TOKEN
```

## Trong lúc chạy

| Việc | Khi nào |
|---|---|
| Đăng ký Snowflake trial rồi thêm secret `SNOWFLAKE_*` | Khi agent báo tới PR 13. Đồng hồ 30 ngày tính ngay lúc đăng ký |
| Publish dashboard lên Tableau Public | Sau PR 11, khoảng 30–60 phút, làm một lần |
| Thêm secret chỉ đọc tới nguồn event thật | Tuỳ chọn, nếu muốn có event thật của một app |

## Giai đoạn 2 (trên máy)

Bật Docker Desktop khi tới từng đợt. Mỗi đợt ước 5–8 GB RAM; đợt đầu đo RAM thật trước.
