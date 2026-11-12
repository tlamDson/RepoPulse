# SQL agent chỉ đọc schema `public` (AI2)

Agent nhận câu hỏi tiếng Anh, gọi một LLM qua API OpenAI-compatible (`base_url` và model là cấu
hình, ví dụ `llama-server` chạy model local), lấy SQL trong câu trả lời rồi chạy trên file DuckDB do
dbt build. Agent đóng vai `public` của `docs/governance.md`: **chỉ** đọc schema `public`
(`main_public` trên DuckDB), không ghi gì. Mã ở `repopulse/sql_agent.py`, bộ câu hỏi đánh giá ở
`repopulse/sql_agent_eval.toml`, test ở `tests/test_sql_agent.py` và
`tests/test_sql_agent_eval.py`.

Tài liệu này không có số đo nào. CI và mọi test chỉ dùng LLM giả lập (kịch bản trả lời cố định,
hoặc server HTTP giả trên `127.0.0.1`), nên kết quả test **không phải** số đo độ chính xác của
model nào. Số đo thật (độ chính xác, thời gian mỗi câu) do việc AI3 ghi vào `runs.csv` bằng lệnh
`eval` dưới đây.

## Ranh giới chỉ đọc (từng lớp)

Các lớp không tin nhau: lớp sau vẫn chặn khi lớp trước sót.

1. **Guard** (`Guard.check`) dùng parser của chính DuckDB, không dùng regex từ khoá:
   `extract_statements` đếm câu lệnh và lấy loại; `json_serialize_sql` cho cây cú pháp, duyệt đệ
   quy mọi dict/list (không phụ thuộc vị trí khoá, để chạy được trên cả DuckDB 1.4 và 2.0).
   Fail closed: parse lỗi, cây không serialize được, hay gặp mục FROM lạ đều bị từ chối.
   - Đúng **một** câu lệnh loại `SELECT` (kể cả `WITH … SELECT`). `SHOW`/`DESCRIBE`/`SUMMARIZE`
     (DuckDB coi là SELECT trên `SHOW_REF`) và `PIVOT` dạng câu lệnh cũng bị từ chối.
   - Mọi bảng phải là quan hệ của schema public, danh sách đọc từ `information_schema` của chính
     database lúc dựng guard. Nhận tên không kèm schema (khi kết nối phân giải tên trần về
     `main_public`, hoặc là CTE **trong phạm vi**) và `main_public.<bảng>` (có thể kèm catalog của
     database). Từ chối `main_gold`, `main_staging`, `main`, `information_schema`, `pg_catalog`,
     view hệ thống (`duckdb_tables`, `pg_tables`), bảng không tồn tại, đường dẫn file
     (`FROM 'x.csv'`), dù nằm trong subquery ở WHERE, scalar subquery, CTE, nhánh UNION, JOIN hay
     LATERAL. CTE chỉ có hiệu lực trong query định nghĩa nó: tên CTE ở subquery không che được bảng
     thật bên ngoài.
   - Table function bị từ chối hết (`read_csv`, `read_parquet`, `read_json`, `read_text`, `glob`,
     `query`, `duckdb_*()`, `pragma_*`…), trừ `range`, `generate_series`, `unnest`.
   - Hàm lộ cấu hình/secret bị từ chối: `current_setting` (trên MotherDuck
     `current_setting('motherduck_token')` lộ token), `getenv`, `which_secret`, mọi `duckdb_*`,
     `pragma_*`.
   - Tên so không phân biệt hoa thường, có hay không ngoặc kép. Chuỗi `'drop table x'`, comment
     chứa `delete`, cột `updated_at` không bị chặn nhầm (test chứng minh guard không phải regex).
2. **Kết nối** (`open_readonly`): chỉ file DuckDB local, `read_only=True`,
   `enable_external_access=false` (không đọc/ghi file, không mạng), tắt autoinstall/autoload
   extension, tắt replacement scan của Python, `search_path` = schema public, rồi
   `lock_configuration=true` (không `SET` lại được). Test chạy thẳng `DROP`, `INSERT`, `CREATE`,
   `read_csv`, `COPY … TO`, `SET enable_external_access`, `LOAD` trên kết nối (bỏ qua guard): tất
   cả lỗi, file database không đổi (so sha256).
3. **Giới hạn dòng** (`run_query`): fetch tối đa `max_rows + 1` dòng, trả `max_rows` dòng và cờ
   `truncated`, bất kể LLM viết `LIMIT` gì. Mặc định 200 (`--max-rows`).

Target MotherDuck (`md:`, `motherduck:`) bị từ chối với lỗi rõ ràng: bản này chỉ hỗ trợ file local.

## Cấu hình LLM

| Biến môi trường | Cờ CLI | Mặc định |
|---|---|---|
| `REPOPULSE_LLM_BASE_URL` | `--base-url` | `http://127.0.0.1:8080/v1` (cổng mặc định của `llama-server`) |
| `REPOPULSE_LLM_MODEL` | `--model` | không có: thiếu thì lỗi cấu hình (exit 2) |
| `REPOPULSE_LLM_API_KEY` | — | không gửi header `Authorization` |

Cờ đè biến môi trường. Module không đọc file `.env`; key không bao giờ xuất hiện trong thông báo lỗi,
`repr` của client hay report. Client gửi `POST {base_url}/chat/completions` với
`{"model", "messages", "temperature": 0}` và đọc `choices[0].message.content`; lỗi HTTP, lỗi kết
nối, JSON sai dạng thành `LLMError`. `extract_sql` bỏ khối `<think>…</think>` (model Qwen suy
luận), lấy khối ```` ```sql ```` cuối cùng (không có thì lấy cả câu trả lời), bỏ `;` cuối.

## Chạy với `llama-server`

```bash
llama-server -m <model>.gguf --port 8080          # server OpenAI-compatible trên 127.0.0.1:8080
python -m dbt.cli.main build --project-dir transform --profiles-dir transform --target ci   # ghi ci.duckdb
python -m repopulse.sql_agent ask --db ci.duckdb --model <tên model> "Which 3 repositories have the highest health score?"
```

`ask` in SQL, bảng kết quả (tab), số lần thử. Exit 0 khi trả lời được, 1 khi hết lượt thử mà không
có SQL hợp lệ, 2 khi lỗi cấu hình, không gọi được LLM hoặc không mở được database.

Vòng lặp (`ask`): prompt hệ thống gồm luật (một câu SELECT, dialect DuckDB, chỉ bảng liệt kê) và mô
tả schema (bảng + cột + kiểu của **chỉ** `main_public`, do code đọc từ `information_schema`, không
qua LLM). SQL bị guard chặn hoặc DuckDB báo lỗi thì lỗi được gửi lại cho LLM để sửa, tối đa
`--max-attempts` lần (mặc định 3).

## Bộ đánh giá

```bash
python -m repopulse.sql_agent eval --db ci.duckdb --model <tên model> --report out/sql-agent-eval.json \
    --out out/sql-agent-row.csv --git-sha <sha>
python benchmarks/record_run.py out/sql-agent-row.csv   # gộp vào runs.csv (việc AI3)
```

- `repopulse/sql_agent_eval.toml`: mỗi `[[question]]` có `id`, `question` (tiếng Anh, nói rõ cột cần
  trả), `sql` (đáp án chuẩn trên `main_public`), `ordered` (mặc định `false`). 16 câu, phủ 6 mart
  Q1–Q6, `fct_events`, `dim_repo`, `dim_user`; từ lọc một bảng, top-N có thứ tự, gộp, tới join
  fact–dim.
- **Execution accuracy**: đáp án là kết quả của SQL chuẩn chạy trên **cùng database lúc đánh giá**
  (qua cùng guard), không gõ cứng giá trị vì dữ liệu đổi theo giờ. Giới hạn dòng của agent được
  nâng lên bằng số dòng đáp án, để riêng giới hạn không làm câu trả lời đúng thành sai.
- **Chấm** (`score`): đúng khi có một ánh xạ đơn ánh từ cột đáp án sang cột kết quả mà các dòng
  chiếu ra bằng nhau. Bỏ qua tên cột, cho phép thừa cột và đổi thứ tự cột; so đa tập khi
  `ordered = false`, so dãy khi `ordered = true`; số so với dung sai tuyệt đối `1e-6`
  (`1 == 1.0`, `Decimal` so với `float`), `NULL == NULL`, bool không phải số. Sai giá trị,
  thiếu/thừa dòng, sai số lần lặp dòng, sai thứ tự khi `ordered`, kết quả bị cắt (`truncated`) hay
  agent lỗi đều tính sai.
- Report JSON: từng câu `{id, question, correct, sql, error, attempts, seconds}`, tổng
  `{questions, correct, accuracy, total_seconds, p50_seconds}`, kèm `model`, `base_url` (không có
  key). `eval` exit 0 khi chạy trọn bộ: độ chính xác là số đo, không phải pass/fail.
- **LLM không gọi được thì phép đo vô hiệu**: exit 2, không ghi report, không ghi dòng CSV (không
  tính thành "sai hết").
- Dòng `runs.csv` (`--out`, cần `--git-sha`): `variant` = `--variant` hoặc `sql-agent-<slug model>`
  (chữ thường, ký tự ngoài `[a-z0-9_-]` thành `-`, ví dụ `Qwen3.6-35B-A3B` →
  `sql-agent-qwen3-6-35b-a3b`), `data_date` = `max(event_date)` của `main_public.fct_events`,
  `total_seconds` = tổng thời gian các câu, `row_count` = số câu, `notes` =
  `accuracy=<đúng>/<tổng>; p50_question_s=<giây>; model=<model>`. File một dòng cùng dạng các
  benchmark khác, gộp bằng `benchmarks/record_run.py`.

## Giới hạn đã biết

- Chỉ file DuckDB local; chưa hỗ trợ MotherDuck (kết nối `md:` không có `read_only` theo file và
  có `current_setting('motherduck_token')`, cần thiết kế riêng).
- Chưa có timeout cho query chạy lâu (DuckDB có `conn.interrupt()`); giới hạn dòng chỉ giới hạn kết
  quả trả về, không giới hạn công tính.
- Guard từ chối cả những thứ vô hại nằm ngoài danh sách (ví dụ table function khác ba hàm sinh dãy,
  `DESCRIBE`): fail closed là có chủ ý.
- Mô tả schema chỉ có tên cột và kiểu, chưa có mô tả cột của dbt (`persist_docs` chưa bật).
- Bộ câu hỏi nhỏ, chấm theo kết quả: câu trả lời đúng bằng may mắn (cùng kết quả trên dữ liệu hiện
  tại) vẫn tính đúng.
