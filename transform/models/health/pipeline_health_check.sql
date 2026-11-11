-- Model giữ chỗ để job `dbt build` trong CI chạy được từ ngày đầu.
-- PR `feature/dbt-star-schema` sẽ thay bằng các model thật.
select 1 as check_id, 'ok' as status
