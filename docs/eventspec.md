# eventspec — catalog event của app mô phỏng (simulated)

`eventspec/` mô tả event của một app lập kế hoạch du lịch **mô phỏng** (simulated). Không có
người dùng thật; dữ liệu do `simkit` (PR 3) sinh theo catalog này. Đây **không** phải dữ liệu GH
Archive.

## Định dạng catalog

`eventspec/catalog/`:

- `_envelope.yaml` — các trường chung của mọi event: `event_id` (tuỳ chọn), `event_name`,
  `event_version`, `occurred_at` (date-time có offset), `received_at` (tuỳ chọn), `user_id`,
  `session_id`, `simulated` (bắt buộc, chỉ nhận `true`). Phải khai `event_id`, `event_name`,
  `event_version`.
- `<event>.yaml` — một event mỗi file, tên file trùng `name`. Khoá: `name` (snake_case),
  `description`, `dedup_fields`, `versions`. Mỗi version: `version` (số nguyên >= 1) và `fields`.

Mỗi trường: `type` (`string` | `integer` | `number` | `boolean`), `required` và `pii` (bắt buộc
ghi rõ `true`/`false`), tuỳ chọn `enum`, `format` (`date-time` | `date` | `email`, chỉ cho
`string`), `description`. Khoá lạ bị từ chối. Trường của event không được trùng tên trường
envelope. `null` không hợp lệ ở bất kỳ trường nào; trường tuỳ chọn thì bỏ hẳn khỏi event.

`load_catalog()` kiểm toàn bộ khi đọc và ném `CatalogError` nêu file và trường sai.

## Quy tắc version

- Đổi schema (thêm, bỏ, đổi trường) thì **thêm version mới**, không sửa version đã có. Version
  liệt kê tăng dần, không trùng; hai version có trường giống hệt nhau bị từ chối.
- Validator nhận **mọi version đã khai**; `event_version` chưa khai bị từ chối.
- Ví dụ: `user_signed_up` v2 thay `country` (chữ tự do) bằng `country_code` và thêm
  `marketing_opt_in` bắt buộc.
- Schema sinh ra được commit ở `eventspec/schemas/<event>.v<n>.schema.json`; test
  `test_committed_schemas_match_catalog` đỏ khi catalog lệch file đã commit. Sửa catalog xong chạy
  `python -m eventspec.generate`. Diff làm đổi file của version cũ là dấu hiệu quên tăng version.

## JSON Schema và `validate`

- Draft 2020-12, `additionalProperties: false` (trường lạ bị từ chối, để bắt đổi schema ngầm),
  `event_name`/`event_version` là `const`, trường PII có annotation `x-pii: true`.
- `validate(event) -> list[str]`: rỗng là hợp lệ; không bao giờ ném exception. Thông báo có dạng
  `missing required field 'email'`, `field 'traveler_count' must be of type integer, got string`,
  `field 'occurred_at' is not a valid date-time`, `unexpected field 'utm_source'`,
  `unknown event_name 'x'`, `event_version 3 is not declared for event 'user_signed_up' (declared: 1, 2)`.
- Kiểm `format` bằng checker riêng (date-time bắt buộc có offset), không phụ thuộc gói
  `rfc3339-validator`.

## Khoá chống trùng

`dedup_key(event)`:

1. `event_id` là chuỗi khác rỗng → `event_id:<event_id>`.
2. Ngược lại → `sha256:<hex>` của JSON
   `{"event_name": ..., "fields": {<dedup_fields>: giá trị}}` với `sort_keys=True`,
   `separators=(",", ":")`, `ensure_ascii=False`, mã hoá UTF-8. Thứ tự key của dict không ảnh
   hưởng. `event_version` không nằm trong khoá: cùng hành động gửi lại ở version mới vẫn là trùng.
   Thiếu trường định danh thì ném `EventSpecError`.

`dedup_fields` phải là trường bắt buộc ở mọi version. `find_duplicates(events)` giữ event đầu,
trả `Duplicate(index, first_index, key)` cho các bản sau.

## PII

Trường có `pii: true` (hiện là `user_signed_up.email`, `trip_shared.recipient_email`) phải được
băm trước silver (PR 5). Lấy danh sách bằng `pii_fields(event_name, version)`.

## Lệnh

```bash
python -m eventspec.generate               # ghi lại eventspec/schemas/
python -m eventspec.generate --out <dir>   # ghi ra thư mục khác
```
