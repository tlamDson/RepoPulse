## Summary

- <thay đổi chính>
- <lý do>

## RED

<!-- Dán output thật của lần chạy test ĐỎ, trước khi có code. Commit đầu của PR chỉ chứa test. -->

```
```

## Test plan

- [ ] Test viết trước, đã thấy đỏ thật (assertion fail hoặc import error đúng nghĩa)
- [ ] `ruff check .` và `ruff format --check .` sạch
- [ ] `pytest` xanh
- [ ] `dbt build` trên DuckDB xanh (nếu PR chạm `transform/`)
- [ ] Không commit `.env`, token hay dữ liệu thô
- [ ] Số đo mới (nếu có) nằm trong `runs.csv`, không gõ tay vào README
