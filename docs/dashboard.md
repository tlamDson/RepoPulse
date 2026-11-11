# Dashboard: trang tĩnh GitHub Pages + CSV cho Tableau

Trang công khai `https://tlamdson.github.io/RepoPulse/` hiển thị 6 mart Q1–Q6 và `runs.csv`. Trang
là HTML + CSS + JS thuần trong `site/` (không bộ build, không npm, không CDN, không font/analytics
ngoài). Dữ liệu là GH Archive (dữ liệu thật); trang hiện **không** có dữ liệu mô phỏng nào, phần nào
sau này lấy từ luồng event app mô phỏng phải gắn nhãn "simulated" (và ghi vào
`meta.json.simulated_sections`).

| Thành phần | File |
|---|---|
| Exporter (CLI) | `repopulse/export_site.py` (`python -m repopulse.export_site`) |
| Trang | `site/index.html`, `site/style.css`, `site/app.js` |
| Workflow | `.github/workflows/pages.yml` |
| Test | `tests/test_export_site.py`, `tests/test_site_static.py`, `tests/test_pages_workflow.py` |

## Trang đọc gì

Chỉ đọc schema **public** (`main_public`, vai `public` trong `transform/governance.yml`,
`docs/governance.md`), không bao giờ đọc `main_gold`, và chỉ đọc các cột của hợp đồng dưới đây (cột
thừa trong bảng không được xuất).

`python -m repopulse.export_site --db <db> --runs runs.csv --out <dir>` ghi `<dir>/`:

| File | Nội dung |
|---|---|
| `index.html`, `style.css`, `app.js` | bản sao `site/` |
| `data/<mart>.json` | `{mart, query, columns, rows}`; dòng sắp theo khoá của mart, NaN → `null`, ngày ISO |
| `data/runs.json` | `{columns, rows}`; giá trị giữ nguyên chuỗi trong `runs.csv`, ô trống → `null` |
| `data/meta.json` | `generated_at` (UTC), `git_sha`, `schema`, `source` (GH Archive, `real_data: true`), `simulated_sections` (rỗng), `latest_data_date` (ngày lớn nhất của Q1 `period = 'day'`), số dòng mỗi mart |
| `tableau/<mart>.csv`, `tableau/runs.csv` | CSV cho Tableau Public; `runs.csv` chép nguyên byte |

Mọi số trên trang đến từ các file này; HTML không có số viết tay (test kiểm). JS dựng DOM bằng
`textContent`/`createElementNS`, không `innerHTML` (tên repo là dữ liệu ngoài).

Mỗi phần Q1–Q6 có biểu đồ SVG + bảng số + link CSV:

| Phần | Biểu đồ |
|---|---|
| Q1 | đường: người đóng góp hoạt động mỗi ngày theo category (bảng = cùng giá trị; dòng theo repo/tuần ở CSV) |
| Q2 | đường: tỉ lệ giữ lại W+4 và W+12 theo cohort |
| Q3 | thanh ngang nhóm: PR mở / PR merge, các repo nhiều PR mở nhất |
| Q4 | thanh ngang nhóm: giờ phản hồi đầu tiên (trung vị) cho issue / PR |
| Q5 | thanh ngang: tỉ phần của người đóng góp nhiều nhất (bus factor) |
| Q6 | thanh ngang: điểm sức khoẻ, đầu bảng xếp hạng |
| Runs | thanh ngang `total_seconds` mỗi dòng `runs.csv` (nhãn variant · data_date; chỉ so dòng cùng ngày dữ liệu) |

Biểu đồ theo skill dataviz: màu categorical theo thứ tự slot cố định (sáng/tối qua
`prefers-color-scheme`), legend khi có ≥ 2 series, chữ dùng màu chữ, tooltip khi hover/focus, bảng
số luôn có. Biểu đồ vẽ lại theo chiều rộng khung (`viewBox` = chiều rộng thật), màn hình hẹp đưa tên
repo lên trên thanh; bảng rộng cuộn ngang trong khung riêng, trang không cuộn ngang. Mart 0 dòng
hoặc file lỗi: phần đó hiện thông báo, phần khác vẫn chạy.

## Hợp đồng cột (build thất bại khi lệch)

`export_site.MART_COLUMNS` = cột của 6 mart trong `transform/models/public/_public.yml` (test giữ hai
chỗ bằng nhau). Build **thất bại** (exit 1, thông báo nêu tên) khi:

- schema không tồn tại;
- thiếu một mart (`missing mart(s) in schema main_public: <mart>`);
- mart thiếu một cột (`missing column(s): <mart>.<cột>`);
- `runs.csv` không có hoặc header khác `repopulse.runs_log.COLUMNS`;
- file database local không có (không tạo file rỗng), `--generated-at` thiếu múi giờ, `--git-sha` sai.

Mọi thứ được đọc và kiểm trước khi ghi; output dựng trong thư mục tạm rồi thay `--out` một lần. Lỗi
thì `--out` giữ nguyên như trước. `--out` đã có file mà không phải export cũ (không có
`data/meta.json`) thì từ chối ghi đè.

Thêm cột vào mart public: sửa `_public.yml` và `MART_COLUMNS` cùng lúc, rồi `app.js` nếu muốn vẽ.

## Build và xem thử local

```bash
python -m dbt.cli.main build --project-dir transform --profiles-dir transform --target ci
python -m repopulse.export_site --db ci.duckdb --runs runs.csv --out _site
python -m http.server 8000 --bind 127.0.0.1 --directory _site
```

Mở `http://127.0.0.1:8000/`. Phải qua `http.server` (trang dùng `fetch`, mở `file://` thì trình duyệt
chặn). `_site/` đã gitignore. Tham số khác: `--schema` (mặc định `main_public`), `--site-dir`
(mặc định `site/`), `--generated-at <ISO có múi giờ>` (test dùng để output tất định), `--git-sha`.

Database local mở chế độ chỉ đọc. Với `--db md:repopulse`, DuckDB tự đọc `MOTHERDUCK_TOKEN` từ môi
trường; exporter không đọc, không in token.

## Workflow deploy (`.github/workflows/pages.yml`)

| Trigger | Job | Làm gì |
|---|---|---|
| `workflow_run` của `Pipeline (scheduled)`, `completed`, nhánh `main` | `pages-build` → `pages-deploy` | Chỉ khi lần chạy pipeline `success`, không phải `pull_request` (smoke của PR không deploy), trên `main`. Export từ `md:repopulse` (schema `main_public`), `upload-pages-artifact`, rồi `configure-pages` + `deploy-pages` |
| `workflow_dispatch` (trên `main`) | như trên | Dựng lại tay |
| `pull_request` (lọc `paths:` `site/**`, exporter, workflow, `transform/models/public/**`) | `pages-preview` | `dbt build --target ci` trên seed fixture, export từ `ci.duckdb`, upload `_site/` làm artifact thường `pages-preview-<số PR>`. Không deploy, không secret |

- `workflow_run` chỉ chạy khi file đã có trên `main` (sau release), và GitHub chạy bản trên `main`:
  checkout lấy nhánh mặc định, không lấy code của lần chạy kích hoạt.
- Secret: chỉ `MOTHERDUCK_TOKEN` (đọc database `repopulse`), chỉ vào `env:` của bước kiểm secret và
  bước export; thiếu thì job dừng với thông báo rõ.
- Quyền: mặc định `contents: read`; chỉ `pages-deploy` có `pages: write` + `id-token: write`, môi
  trường `github-pages`. Deploy xếp hàng trong nhóm `pages-deploy`, không huỷ lần đang chạy.
- DuckDB khoá cùng bản với `pipeline.yml` (MotherDuck từ chối client mới hơn).
- Đường dẫn trong trang đều tương đối (Pages phục vụ dưới `/RepoPulse/`).

`runs.csv` trên trang là bản đã commit trên `main`: dòng của pipeline nằm ở artifact và chỉ lên
trang sau khi được gộp bằng `benchmarks/record_run.py` và merge (`docs/pipeline.md`).

## Tableau Public (chủ repo làm tay)

1. Tải CSV từ trang (link "Download CSV (Tableau)" mỗi phần) hoặc artifact `pages-preview-*`/build
   local `_site/tableau/`.
2. Tableau Public → Connect → Text file, mỗi mart một nguồn.
3. Ghi nguồn là GH Archive (dữ liệu thật) kèm ngày dữ liệu mới nhất (`data/meta.json`).
4. Publish lên Tableau Public, thêm link vào README nếu muốn.

Tài liệu này không chứa số đo; số đo chỉ lấy từ `runs.csv`.
