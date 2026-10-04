# K4-Track02-Day17 — Report cá nhân

**Họ tên / MSSV:** Phạm Minh Cương / 2A202602825<br>
**Repo:** https://github.com/mcnb2005/K4-Track02-Day17-PhamMinhCuong-2A202602825-DataPipelineEngineering<br>
**Commit code và checksum:** `35c33771a1058ed952c206c8d8a869900aa53e09`<br>
**AI đã dùng và phạm vi hỗ trợ:** OpenAI Codex hỗ trợ đọc đề, chẩn đoán, chỉnh mã, chạy kiểm thử và soạn REPORT; output được sinh trực tiếp từ repo local.<br>
**Nguồn tham khảo khác (nếu có):** README và tài liệu trong repo đề bài.

## 1. Ba lỗi

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | Baseline chỉ có 8/18 contract pass; `silver_tickets` có 24 hàng cho 12 ticket và T-91 trả về ba trạng thái. | Checksum feature là `c50b8851affe`, khác full recompute `8630e04a61d1`; u05 ngày 08-12 chỉ có `(2 events, 1 click, 0 down)` thay vì `(5, 3, 1)`; `LOOKBACK_DAYS=0 < 3`. | T-97 còn hai hàng sống ở Silver, một hàng trong snapshot mới nhất và hai chunks trong RAG; dữ liệu cá nhân vẫn còn sau yêu cầu xoá. |
| **Nguyên nhân gốc** | Mỗi batch dùng `INSERT`, chỉ dedup trong batch mà không upsert giữa các batch; replay batch cũ cũng có thể ghi trạng thái cũ. | Gold chỉ overwrite partition ngày đang chạy, trong khi Bronze đo được P99 lateness là 3 ngày; event đến ngày 08-15 nhưng có event time 08-12 không được tính lại. | Debezium delete có `after=null`; staging chỉ đọc `ticket_id` từ `after`, nên loại mất change `op='d'` trước khi vào Silver. |
| **Cách sửa** | `pipeline/silver.py`: đổi sang `MERGE` theo `ticket_id`; chỉ update khi `s._lsn > t._lsn`, insert khi chưa có khóa. | `pipeline/config.py`: đặt `LOOKBACK_DAYS=3` theo `ceil(P99)`; logic Gold sẵn có sẽ overwrite cửa sổ `[day-3, day]` theo event time. | `pipeline/staging.py`: lấy khóa bằng `coalesce(after.ticket_id, before.ticket_id, key.ticket_id)`; các cột lấy từ `after` nên delete tạo tombstone không còn PII. |
| **Khái niệm trên slide** | Silver “một hàng = một thực thể”, keyed MERGE, LSN guard và idempotency. | Event time khác ingest time; đo lateness từ Bronze và recompute partition theo lookback. | CDC log-based, Debezium envelope, tombstone và “xoá phải lan” xuống Gold. |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: **PASS** — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: **PARITY**

## 3. Lựa chọn công cụ / kỹ thuật

- **MERGE / overwrite-partition:** Silver là trạng thái theo thực thể nên MERGE theo khóa+LSN; Gold là aggregate theo ngày nên overwrite cửa sổ để hấp thụ late data và vẫn idempotent.
- **Tombstone:** giữ khóa+LSN để replay cũ không làm ticket “sống lại”, nhưng xóa PII khỏi dữ liệu downstream.
- **Snapshot bất biến:** dựng as-of từ Bronze để tái lập/audit; thay đổi mới tạo version mới.
- **DuckDB / dbt:** dữ liệu nhỏ, local nên DuckDB đủ nhanh và zero-key; dbt thêm contract, test, merge/microbatch, trong khi Spark tạo chi phí không cần thiết.

## 4. Hai câu hỏi suy ngẫm

1. Sau yêu cầu xoá, ghi revocation manifest, chặn snapshot cũ, tạo version đã purge và xóa index/cache liên quan. Dữ liệu thật nên mã hóa theo chủ thể để crypto-shred; audit chỉ giữ ID/hash tối thiểu. Quyền xoá ưu tiên hơn khả năng tái lập nội dung PII.
2. Đặt PII gate ở Bronze→Silver. Ngoài regex, dùng NER/DLP cho tên, địa chỉ, định danh; bản ghi không chắc chắn được quarantine/token hóa. Đo precision/recall trên tập có nhãn, tỷ lệ quarantine và số leak qua scan Silver/Gold, với SLO leak bằng 0.

## 5. Output thực tế

```text
> .\.venv\Scripts\python.exe -m scripts.verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

> .\.venv\Scripts\python.exe -m pytest
..................................                                       [100%]
34 passed in 2.41s

> .\.venv\Scripts\python.exe -m scripts.rerun_check
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

> .\.venv\Scripts\python.exe main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

> ..\.venv\Scripts\dbt.exe build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17
Running with dbt=1.12.5
Registered adapter: duckdb=1.11.0
Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 8.96 seconds (8.96s).
Completed successfully
Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

> .\.venv\Scripts\python.exe -m scripts.parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```
