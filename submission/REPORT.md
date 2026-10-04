# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Nguyễn Quang Hữu / 2A202602756  
**Repo:** K4-Track02-Day17-NguyenQuangHuu-2A202602756-DataPipelineEngineering  
**Commit bài nộp:** fbf74549564cf474f0b59b1452db9dd4b07cd131  
**AI đã dùng và phạm vi hỗ trợ:** Antigravity AI (hỗ trợ trace lỗi qua pytest/verify, gợi ý cú pháp MERGE DuckDB và đối chiếu parity với dbt)  
**Nguồn tham khảo khác:** Slide bài giảng Ngày 17, docs DuckDB và dbt microbatch  

## 1. Ba lỗi

Mỗi lỗi 4 dòng. Triệu chứng = thứ bạn *thấy* đầu tiên (check nào fail, số nào lạ,
checksum nào lệch) — không phải cách sửa.

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | Check `silver_tickets has exactly one row per ticket_id` báo lỗi (thấy 24 dòng cho 12 ticket); ticket T-91 có tới 3 dòng trạng thái khác nhau. | Check `gold_feature_daily` lệch checksum với full recompute (`c50b8851affe != 8630e04a61d1`); user u05 ngày 12/08 chỉ đếm được 2 event thay vì 5. | Check xóa T-97 fail (T-97 vẫn còn 2 dòng chưa xóa ở Silver); T-97 vẫn lọt vào training snapshot ngày 16/08 và RAG chunks. |
| **Nguyên nhân gốc** | Code cũ dùng `INSERT INTO silver_tickets` nên mỗi ngày chạy lại là chèn thêm dòng mới, không cập nhật theo khóa `ticket_id`. | `LOOKBACK_DAYS = 0` nên pipeline chỉ tính đúng ngày hôm đó, bỏ quên các event của u05 xảy ra ngày 12/08 nhưng ngày 15/08 mới tới Kafka do mất mạng. | `ticket_changes_sql` chỉ đọc `ticket_id` từ trường `after`. Khi bản ghi là CDC delete (`op = 'd'`), `after` bị null làm mất `ticket_id` nên bị filter bỏ qua. |
| **Cách sửa** (file, vài dòng) | Sửa `pipeline/silver.py`: đổi `INSERT` thành `MERGE INTO silver_tickets`, cập nhật khi `s._lsn > t._lsn` và insert nếu chưa có. | Sửa `pipeline/config.py`: đổi `LOOKBACK_DAYS = 3` (bằng P99 đo từ Bronze) để tính lùi lại 3 ngày theo `event_time`. | Sửa `pipeline/staging.py`: lấy `ticket_id` bằng `coalesce(after, before, key)`; ghi tombstone (`is_deleted=True`, xóa PII) và chặn replay bằng `_lsn`. |
| **Khái niệm trên slide** | Silver có khoá: Ghi MERGE theo khoá chính, dùng thứ tự LSN để replay batch cũ không đè mất dữ liệu mới. | Data về muộn: Tính feature theo Event Time (ngày xảy ra sự kiện); đặt lookback window bao phủ P99 đo từ Bronze. | CDC log-based & Xoá phải lan: Parse đúng phong bì Debezium delete (`before` có dữ liệu, `after` null), truyền hiệu ứng xóa tới Gold. |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: PASS — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: PARITY

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: Silver quản lý thực thể nên cần update từng dòng theo khóa và LSN; còn Gold tổng hợp theo ngày nên ghi đè cả partition vừa nhanh vừa tránh trùng dòng.
- Tombstone thay vì xoá hẳn hàng trong Silver: Giữ lại tombstone có LSN mới nhất để khi chạy lại (replay) các batch cũ trong quá khứ thì bản ghi đã xóa không bị hồi sinh lại.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: Đảm bảo tính tái lập (reproducibility) của dữ liệu train tại đúng thời điểm đó và tránh rò rỉ dữ liệu tương lai (data leakage).
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: Dữ liệu vài triệu dòng chạy in-process bằng DuckDB cực nhanh, gọn nhẹ, không tốn tài nguyên cài đặt và quản lý cụm Spark cồng kềnh.

## 4. Hai câu hỏi suy ngẫm

1. **Snapshot `v2026-08-12`..`v2026-08-14` vẫn chứa văn bản của T-97 (đã bị xoá ngày 08-15). "Snapshot bất biến" và "quyền được xoá dữ liệu" mâu thuẫn — bạn xử lý thế nào?**  
Trong thực tế (như luật GDPR hay Nghị định 13), quyền xóa dữ liệu của người dùng luôn được ưu tiên hơn tính bất biến nội bộ. Cách xử lý thực tế:
   - Với snapshot cũ: Đánh dấu cách ly (quarantine), không cho phép đưa vào pipeline huấn luyện mô hình mới nữa, chỉ lưu trữ mã hóa riêng để phục vụ kiểm toán nếu luật yêu cầu.
   - Về kỹ thuật: Nên áp dụng *cryptographic erasure* (mã hóa dữ liệu kèm khóa riêng của user; khi user yêu cầu xóa thì hủy khóa đó, dữ liệu cũ thành chuỗi vô nghĩa) hoặc lên lịch train lại mô hình từ snapshot sạch mới nhất.

2. **Regex che được email và số điện thoại, nhưng tên "Nguyễn Văn An" vẫn còn. Bạn sẽ đặt chốt PII nào, ở tầng nào, và đo nó ra sao?**  
   - Chốt PII: Dùng mô hình NER (như PhoBERT-NER) chuyên nhận diện tên người (PER) và địa chỉ tiếng Việt, kết hợp đối soát với bảng danh sách khách hàng có sẵn.
   - Vị trí: Đặt ngay cổng chuyển đổi từ Bronze sang Silver (bước Staging/Cleansing), tuyệt đối không để tên chưa che lọt vào Silver cho data scientist hay phân tích viên đọc được.
   - Cách đo: Tạo một tập dữ liệu mẫu chuẩn (khoảng 1.000 mẫu đã gán nhãn tay), tính chỉ số Recall và Precision. Tiêu chuẩn là Recall phải đạt từ 99.9% trở lên (chấp nhận che nhầm hơn là bỏ sót), đồng thời cài contract check tự động quét Silver hằng ngày để báo động nếu lọt tên người.

## 5. Output (dán nguyên văn)

```text
$ python -m scripts.verify
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

$ python -m pytest
..................................                                       [100%]
34 passed in 3.28s

$ python -m scripts.rerun_check
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

$ python main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

$ Push-Location dbt_project; dbt build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17; Pop-Location
04:54:50  Running with dbt=1.12.5
04:54:51  Registered adapter: duckdb=1.11.0
04:54:52  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
04:54:52  
04:54:52  Concurrency: 1 threads (target='dev')
04:54:52  
04:54:52  1 of 19 START sql view model main.stg_events ................................... [RUN]
04:54:52  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.15s]
04:54:52  2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
04:54:52  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.05s]
04:54:52  3 of 19 START sql incremental model main.silver_events ......................... [RUN]
04:54:53  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.25s]
04:54:53  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
04:54:53  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.24s]
04:54:53  8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
04:54:53  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.31s]
04:54:53  5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
04:54:53  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.07s]
04:54:53  6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
04:54:53  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.03s]
04:54:53  7 of 19 START test unique_silver_events_event_id ............................... [RUN]
04:54:53  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.04s]
04:54:53  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
04:54:53  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.05s]
04:54:53  10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
04:54:53  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.05s]
04:54:53  11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
04:54:53  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.05s]
04:54:53  12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
04:54:53  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.03s]
04:54:53  13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
04:54:53  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.03s]
04:54:53  14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
04:54:53  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.03s]
04:54:53  15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
04:54:54  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.04s]
04:54:54  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
04:54:54  Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
04:54:54  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.07s]
04:54:54  Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
04:54:54  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.06s]
04:54:54  Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
04:54:54  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.05s]
04:54:54  Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
04:54:54  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.05s]
04:54:54  Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
04:54:54  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.06s]
04:54:54  Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
04:54:54  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.05s]
04:54:54  Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
04:54:54  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.06s]
04:54:54  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.47s]
04:54:54  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
04:54:54  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.05s]
04:54:54  18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
04:54:54  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.02s]
04:54:54  19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
04:54:54  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.03s]
04:54:54  
04:54:54  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 2.34 seconds (2.34s).
04:54:54  
04:54:54  Completed successfully
04:54:54  
04:54:54  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

$ python -m scripts.parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```

### Bằng chứng Bonus B1 (LLM step có cache và quarantine)

```text
$ python -m scripts.bonus_llm
=== bonus: LLM labelling of 11 live tickets ===
  cost estimate before running: ~484 tokens = $0.0010 per full run
  [OK ] first run labels every live ticket
  [OK ] re-run with same model + prompt makes 0 LLM calls
  [OK ] every Gold label is bug / billing / other
  [OK ] off-schema answers go to llm_label_quarantine
  [OK ] new prompt version re-labels on purpose
  [OK ] labels carry their prompt version
BONUS PASS
```

### Bằng chứng Bonus B2 (Kiến trúc pipeline AI thực tế)
Tài liệu thiết kế chi tiết (> 1.700 từ) nộp tại: [`bonus/DESIGN.md`](../bonus/DESIGN.md).
