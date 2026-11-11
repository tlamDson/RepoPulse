# simkit — dữ liệu event mô phỏng (simulated) có seed và lỗi biết trước

`simkit/` sinh event của app lập kế hoạch du lịch **mô phỏng** (simulated) theo catalog
`eventspec/` (`docs/eventspec.md`), rồi chèn một số lượng lỗi dữ liệu biết trước. Không có người
dùng thật; đây **không** phải dữ liệu GH Archive. Mọi event mang `simulated: true`, manifest cũng
có `"simulated": true`, thư mục ra mặc định là `data/simulated/` (không commit, đã có trong
`.gitignore`). Email mô phỏng chỉ dùng domain dành riêng `example.com` / `example.org`.

Người dùng sau: PR 5 (bronze/silver: khử trùng, cửa sổ đến muộn, băm PII, quarantine) và PR 8
(chạy bộ lỗi chèn qua pipeline; manifest là "đáp án" để đối chiếu).

## API

```python
from simkit import simulate, inject_faults, to_jsonl, bot_users, is_late, arrival_delay

sim = simulate(seed=42, users=100, days=7, start=date(2026, 11, 1))  # -> Simulation
sim.users  # tuple[SimUser(user_id, persona, email, signed_up_at)]
sim.events  # list[dict], sắp theo occurred_at, mọi event qua eventspec.validate

result = inject_faults(sim.events, {"duplicate": 5, "bot": 2}, seed=42)  # -> FaultResult
result.events  # list[dict] đã chèn lỗi (input không bị sửa)
result.faults  # tuple[FaultRecord(fault, indices, event_ids, detail)]
result.counts()  # {"duplicate": 5, "null": 0, ..., "bot": 2}
result.manifest()  # dict JSON được, xem "Manifest"
```

Hằng số công khai: `FAULT_TYPES`, `LATE_THRESHOLD` (1 giờ), `MAX_NORMAL_LATENCY` (5 giây),
`BOT_EVENTS_PER_MINUTE` (30), `PERSONAS`.

## Tái lập

Mọi ngẫu nhiên đi từ một `random.Random(seed)` riêng (generator) và một `random.Random(seed)`
riêng (chèn lỗi); không dùng `random` toàn cục, giờ hệ thống hay uuid ngẫu nhiên. `event_id` là
UUID dựng từ bit của rng. Cùng tham số + cùng seed ra cùng byte khi ghi JSONL (`to_jsonl`: một
object mỗi dòng, `sort_keys=True`, không khoảng trắng, UTF-8, xuống dòng `\n`); khác seed ra khác.

## Dữ liệu sạch

- **Persona** (`simkit/personas.py`):

  | persona | tỷ lệ | phiên/ngày (Poisson, sau ngày đăng ký) | event hành động/phiên | trộn trip_created : itinerary_generated : trip_shared |
  |---|---|---|---|---|
  | `browser` (lướt qua) | 50% | 0.3 | 0–1 | 3 : 1 : 0 |
  | `planner` (lập kế hoạch) | 35% | 1.0 | 1–4 | 3 : 4 : 1 |
  | `power` (dùng nhiều) | 15% | 3.0 | 3–8 | 2 : 3 : 3 |

- Mỗi người dùng đăng ký vào một ngày ngẫu nhiên trong khoảng; ngày đó có ít nhất một phiên.
  Phiên đầu tiên: `session_started` rồi `user_signed_up` (cùng `session_id`, vì envelope bắt buộc
  `session_id`); mọi event khác của người đó xảy ra sau đăng ký. `user_signed_up` có cả v1 và v2.
- Mỗi phiên bắt đầu bằng đúng một `session_started`; các event trong phiên cách nhau 5–120 giây,
  nên người dùng sạch có tối đa 12 event trong một phút. Các phiên trong ngày nằm ở các khung giờ
  riêng, không chồng nhau, và nằm trọn trong `[start, start + days)` (UTC).
- `itinerary_generated` và `trip_shared` luôn trỏ tới một `trip_id` do chính người đó tạo trước;
  chưa có chuyến thì sinh `trip_created`. `start_date` >= ngày tạo, `day_count` khớp độ dài chuyến.
- `received_at` = `occurred_at` + 0,2–5 giây (`MAX_NORMAL_LATENCY`), timestamp UTC độ chính xác
  mili giây (`2026-11-01T08:15:03.120Z`).

## Sáu loại lỗi và đơn vị đếm

Một event mang tối đa một loại lỗi; event không bị chọn giữ nguyên và vẫn hợp lệ. Mục tiêu được
chọn trong các event **đủ điều kiện**: qua `validate` và không phải bản trùng của event trước.
Các trường không bao giờ bị sửa: `event_id`, `event_name`, `event_version` (trừ `schema_drift`),
`simulated` — nên event lỗi vẫn mang `simulated: true`.

| loại | một lỗi là | quan sát độc lập bằng |
|---|---|---|
| `duplicate` | một bản sao y hệt (cùng `event_id`) của một event sạch, luôn đứng sau bản gốc; được neo tối đa 20 event của input sau bản gốc, trong output có thể xa hơn vì event late/bot chen vào | `eventspec.find_duplicates` trả đúng N, `index` = vị trí bản sao |
| `null` | một event có một trường (ngoài 4 trường trên) thành `null` | `validate` có `field '<f>' must be of type <t>, got null` |
| `wrong_type` | một event có một trường đổi sang kiểu JSON khác (chuỗi → số nguyên; số → chuỗi số; boolean → `"true"`/`"false"`) | `validate` có `field '<f>' must be of type <t>, got <u>` |
| `late` | một event có `received_at - occurred_at` > `LATE_THRESHOLD` (1 giờ; thêm 1 phút–72 giờ), vẫn hợp lệ schema; được dời tới vị trí nó "đến" trong luồng | `is_late(event)` / `arrival_delay(event)` |
| `schema_drift` | một event có trường lạ (`utm_source`, `ab_bucket`, `referrer`, `client_build`) **hoặc** `event_version` = version lớn nhất đã khai + 1 | `validate` có `unexpected field '<f>'` hoặc `event_version N is not declared ...` |
| `bot` | **một người dùng bot** (user_id mới cùng dạng `u-NNNNN`, số kế tiếp sau người dùng lớn nhất của input; `session_id`/`trip_id`/`itinerary_id` cũng cùng dạng với dữ liệu sạch, nên chỉ nhận ra bot qua hành vi): một phiên với 40–80 event hợp lệ cách nhau 0,1–0,7 giây, cả loạt nằm trong dưới 60 giây | `bot_users(events)`: người dùng có > `BOT_EVENTS_PER_MINUTE` (30) event trong một cửa sổ 60 giây bất kỳ |

Lỗi `null`/`wrong_type`/`schema_drift` làm event sai schema (PR 5 đưa vào quarantine). `late` và
`bot` hợp lệ schema, chỉ lộ qua thời gian đến / tốc độ. Bản sao `duplicate` và event bot không
đến muộn.

Ràng buộc: count thiếu = 0; tất cả bằng 0 thì output bằng đúng input. Tên loại lạ, count âm hay
không phải số nguyên, tổng lỗi một-event (`duplicate` + `null` + `wrong_type` + `late` +
`schema_drift`) lớn hơn số event đủ điều kiện, hay `bot` > 0 mà input không có event nào → ném
`ValueError` nêu rõ lý do (không chèn thiếu).

Gọi `inject_faults` **một lần trên dữ liệu sạch**. Gọi lần hai trên output đã chèn lỗi không được
hỗ trợ: event `late` và event bot của lần trước vẫn đủ điều kiện, nên lần sau có thể chèn chồng lỗi
lên chúng và số đếm của hai manifest không còn cộng lại được. Cần nhiều loại lỗi thì đưa hết vào
một `counts`.

## Manifest

`result.manifest()` (CLI thêm `params`):

```json
{
  "simulated": true,
  "params": {"seed": 42, "users": 100, "days": 7, "start": "2026-11-01"},
  "late_threshold_seconds": 3600.0,
  "bot_events_per_minute": 30,
  "counts": {"duplicate": 5, "null": 5, "wrong_type": 5, "late": 5, "schema_drift": 5, "bot": 2},
  "faults": [
    {"fault": "wrong_type", "indices": [298], "event_ids": ["dffa28ae-..."],
     "detail": {"field": "start_date", "value": 29431, "expected_type": "string",
                "actual_type": "integer"}}
  ]
}
```

- `indices`: vị trí (từ 0) trong output; `event_ids`: cùng thứ tự. Lỗi một-event có đúng một
  phần tử; `bot` liệt kê mọi event của bot đó.
- `detail` theo loại: `duplicate` → `original_index`; `null` → `field`, `value` (`null`),
  `expected_type`; `wrong_type` → thêm `actual_type`; `late` → `field` (`received_at`),
  `delay_seconds`; `schema_drift` → `kind` (`unexpected_field` | `undeclared_version`), `field`,
  `value` (và `declared`); `bot` → `user_id`, `events`, `span_seconds`.
- Manifest không chép lại giá trị gốc của trường bị sửa (tránh mang email vào manifest).
- `faults` sắp theo vị trí đầu tiên trong output.

## Lệnh

```bash
python -m simkit.generate                                  # 100 người dùng, 7 ngày từ 2026-11-01, seed 42, không lỗi
python -m simkit.generate --seed 7 --users 200 --days 3 --start 2026-11-01 \
    --duplicate 5 --null 5 --wrong-type 5 --late 5 --schema-drift 5 --bot 2 \
    --out data/simulated
```

Ghi `<out>/events.jsonl` và `<out>/manifest.json`. Tham số sai (ví dụ yêu cầu nhiều lỗi hơn số
event) thì thoát mã 2 kèm thông báo.
