# TDD (Red → Green → Refactor) — bắt buộc cho mọi feature/fix

> Nguồn duy nhất cho quy trình test của RepoPulse. Stack và cạm bẫy nằm ở
> `.claude/rules/tech-defaults.md` — file này chỉ nói **quy trình**.

1. **RED** — viết test fail trước, chưa viết implementation. **Commit đầu tiên của PR chỉ chứa test.** Chạy test, xác nhận đỏ thật (assertion fail hoặc `ImportError`/`ModuleNotFoundError` đúng nghĩa của module chưa có, **không phải lỗi cấu hình** như thiếu fixture, sai đường dẫn, thiếu dependency). Chép output đỏ vào mục `## RED` của mô tả PR.
2. **GREEN** — code tối thiểu để pass, không thêm logic chưa có test bao phủ.
3. **REFACTOR** — cải thiện code, chạy lại toàn bộ test, không được break.

```bash
python -m pytest                                   # toàn bộ test Python
python -m pytest tests/test_<module>.py -x         # một file
python -m pytest -k <tên_test>                     # một case
python -m dbt.cli.main build --project-dir transform --profiles-dir transform --target ci   # dbt trên DuckDB
```

Luôn dùng dạng `python -m` (trên máy local `pytest`/`ruff`/`dbt` không có trong PATH; `python -m` còn bảo
đảm import đúng package của worktree hiện tại). `pyproject.toml` đã đặt `addopts = "-q"`: đừng thêm
`-q` nữa (thành `-qq` sẽ mất dòng tổng kết `N passed` cần chép làm bằng chứng).

**Package first-party mới** (`eventspec`, `simkit`, `benchmarks`...): khi commit chỉ có test import một
package chưa tồn tại, ruff (isort) xếp nó vào nhóm bên thứ ba và báo `I001`. Thêm tên package vào
`[tool.ruff.lint.isort] known-first-party` trong `pyproject.toml` **ngay trong commit test** và ghi
vào mục `## Giả định` — đây là cấu hình lint, không vi phạm "commit đầu chỉ có test".

## Vị trí test

- **Python** (`repopulse/`, `eventspec/`, `simkit/`...): `tests/test_<module>.py`, fixture dùng chung ở `tests/conftest.py`, dữ liệu mẫu nhỏ ở `tests/fixtures/`.
- **PySpark**: test chạy với `SparkSession` local (`master("local[1]")`), fixture `spark` scope `session` trong `conftest.py`. Dữ liệu vài chục dòng, không đọc file GH Archive thật.
- **dbt**: generic test và singular test trong `transform/models/**/schema.yml` và `transform/tests/`; unit test của dbt (`unit_tests:`) cho logic biến đổi; `contract: {enforced: true}` cho model gold. Test dbt "đỏ" nghĩa là `python -m dbt.cli.main build … --target ci` thất bại đúng chỗ.

## Quy ước

- **Không bao giờ gọi dịch vụ thật trong unit test** (Databricks, MotherDuck, `data.gharchive.org`, GitHub API). Dùng fixture nhỏ trong `tests/fixtures/` hoặc `monkeypatch`/`unittest.mock`.
- Dữ liệu ngẫu nhiên phải có seed cố định; test kiểm "cùng seed ra cùng dữ liệu".
- Test idempotent: chạy lại cùng đầu vào (cùng ngày, giờ) không sinh bản ghi trùng.
- Test PII: khẳng định không còn email/giá trị thô ở tầng sau bước băm.
- Bug tìm thấy khi viết test mà không thuộc phạm vi task → viết test khẳng định hành vi **hiện tại** kèm comment `# [BUG]`, không tự sửa. Ghi vào `.claude/rules/tech-defaults.md` mục "Nợ kỹ thuật" theo format 3 phần ở `workflow.md`.

Test độc lập (không phụ thuộc thứ tự chạy), assertion cụ thể theo hành vi, không `assert x` mơ hồ. Không `pytest.mark.skip` mà không có lý do. Không commit code mới thiếu test.
