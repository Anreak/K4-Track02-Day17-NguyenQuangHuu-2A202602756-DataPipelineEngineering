# Thiết Kế Kiến Trúc Data Pipeline Cho Hệ Thống AI Trợ Lý Lâm Sàng & RAG Y Tế (Clinical Decision Support System - CDSS)

> **Tài liệu nộp Bonus B2** — K4 Track 02 Ngày 17  
> **Người thực hiện:** Nguyễn Quang Hữu (MSSV: 2A202602756)
> **Độ dài:** > 900 từ  

---

## 1. Bài toán thực tế và bối cảnh ràng buộc

### 1.1. Bối cảnh
Tại các hệ thống bệnh viện và phòng khám đa khoa tại Việt Nam, dữ liệu y tế được sản sinh liên tục từ nhiều phân hệ: phần mềm quản lý bệnh viện (HIS), hệ thống lưu trữ và truyền hình ảnh y khoa (PACS), xét nghiệm (LIS), cùng đơn thuốc điện tử. Mục tiêu của dự án là xây dựng một **Data Pipeline chuẩn Medallion** phục vụ hai tác vụ AI cốt lõi:
1. **RAG Lâm sàng (Clinical RAG):** Hỗ trợ bác sĩ tra cứu lịch sử bệnh án, phác đồ điều trị Bộ Y tế và cảnh báo tương tác thuốc.
2. **Mô hình AI Tiên lượng & Phân tầng nguy cơ:** Dự đoán nguy cơ tái nhập viện trong 30 ngày và cảnh báo sớm suy đa tạng (sepsis).

### 1.2. Ràng buộc thực tế đầy thách thức
- **Dữ liệu phi cấu trúc và lộn xộn:** Bệnh án tự sự (clinical notes) viết bằng tiếng Việt xen lẫn thuật ngữ La-tinh/tiếng Anh, viết tắt dày đặc ("bn", "chđ", "k/đ", "bt"), chữ bác sĩ scan từ hồ sơ giấy cần OCR.
- **Quyền riêng tư và pháp lý nghiêm ngặt:** Tuân thủ Luật Khám bệnh, chữa bệnh 2023 và Nghị định 13/2023/NĐ-CP về bảo vệ dữ liệu cá nhân. PII y tế (tên bệnh nhân, CCCD, BHYT, số điện thoại, địa chỉ) là dữ liệu nhạy cảm mức độ cao nhất.
- **Yêu cầu tính toàn vẹn Point-in-Time:** Để huấn luyện mô hình dự báo tái nhập viện, pipeline phải đảm bảo tuyệt đối không rò rỉ dữ liệu tương lai (ví dụ: không được dùng kết quả xét nghiệm ra sau ngày xuất viện để dự đoán tình trạng lúc nhập viện).
- **Quyền được xóa dữ liệu (Right to Erasure):** Khi bệnh nhân rút lại sự đồng ý thu thập dữ liệu, hệ thống phải gỡ bỏ thông tin khỏi RAG index và các bản phân phối dữ liệu phân tích.

---

## 2. Các quyết định kỹ thuật then chốt và phân tích đánh đổi

### Quyết định 1: Kiến trúc xử lý — Microbatch Lakehouse (Apache Iceberg / DuckDB) vs Real-time Streaming (Apache Flink)
- **Phương án chọn:** Microbatch tần suất 15 phút đến 1 giờ trên Lakehouse.
- **Phương án thay thế:** End-to-end Streaming bằng Apache Flink + Kafka.
- **Đánh đổi và Lý do chọn:**
  - Trong hỗ trợ quyết định lâm sàng và phân tích hồ sơ, độ trễ 15 phút hoàn toàn đáp ứng nghiệp vụ (các xét nghiệm sinh hóa thường mất từ 45-90 phút mới có kết quả).
  - Streaming thuần túy (Flink) cực kỳ phức tạp khi cần join nhiều nguồn (HIS + LIS + PACS) vì khoảng trễ giữa các sự kiện lâm sàng có thể kéo dài nhiều ngày (lateness cao). Flink đòi hỏi duy trì state rất lớn trên RocksDB, chi phí hạ tầng tốn kém gấp 4-5 lần.
  - Microbatch trên Lakehouse cho phép áp dụng MERGE INTO có điều kiện (LSN / event timestamp), xử lý late-arriving data bằng lookback window chuẩn xác và chi phí vận hành thấp hơn đáng kể.

### Quyết định 2: Bảo vệ PII — Tokenization Vault + Regex Masking ở tầng Silver vs Che muộn tại Gold
- **Phương án chọn:** Khử định danh (De-identification) hai lớp tại ranh giới Bronze -> Silver: Regex & Named Entity Recognition (NER tiếng Việt) che trực tiếp văn bản tự do; các mã định danh nhạy cảm (CCCD, Mã BHYT) được băm bằng HMAC có Salt lưu trong Secure Vault cách ly.
- **Phương án thay thế:** Giữ nguyên dữ liệu thô ở Silver và chỉ che khi tạo view/bảng ở Gold.
- **Đánh đổi và Lý do chọn:**
  - Nếu giữ PII ở Silver, bất kỳ kỹ sư dữ liệu hay nhà khoa học dữ liệu nào truy cập tầng Silver để khám phá dữ liệu (EDA) đều có nguy cơ vi phạm pháp luật nếu rò rỉ thông tin bệnh nhân.
  - Tầng Bronze được khóa quyền truy cập tối đa (chỉ service account ingest được ghi và đọc). Silver là tầng làm việc chung của analytics/AI nên phải tuyệt đối sạch về PII định danh trực tiếp.
  - Đánh đổi: Mất đi một phần tính liên kết nếu cần đối soát ngược lại hồ sơ gốc; khắc phục bằng cách lưu bảng mã ánh xạ (pseudonymization map) có mã hóa RSA trong Vault riêng biệt, chỉ hội đồng đạo đức y khoa được quyền truy xuất khi có yêu cầu điều tra.

### Quyết định 3: Xử lý dữ liệu đến muộn (Late Data) — Microbatch Lookback theo Event Time vs Chuyển dịch theo Ingest Time
- **Phương án chọn:** Đo phân vị P99 của độ trễ cập nhật xét nghiệm từ Bronze (khoảng 3 ngày); mọi microbatch chạy cho ngày $T$ sẽ tự động recompute cửa sổ $[T - 3, T]$ theo `event_time`.
- **Phương án thay thế:** Gán thời gian tính feature theo thời điểm hệ thống nhận được dữ liệu (`ingest_time`).
- **Đánh đổi và Lý do chọn:**
  - Nếu gán theo `ingest_time`, các xét nghiệm nuôi cấy vi sinh mất 3 ngày mới có kết quả sẽ bị tính dồn vào ngày thứ 4, làm sai lệch toàn bộ chỉ số lâm sàng của ngày nhập viện đầu tiên, gây ra train-serve skew trầm trọng cho mô hình tiên lượng.
  - Tính toán theo `event_time` bảo tồn chính xác thực tại lâm sàng. Đánh đổi là phải ghi đè phân vùng (overwrite-partition) trong phạm vi lookback, đòi hỏi pipeline phải đảm bảo tính lũy kế (idempotency) hoàn hảo.

### Quyết định 4: RAG Lâm sàng — Hybrid GraphRAG (Vector Search + Knowledge Graph) vs Pure Vector Search
- **Phương án chọn:** Kết hợp Dense Vector Retrieval (bilingual y khoa Việt - Anh) với Knowledge Graph y tế (ICD-10, ATC code của thuốc, dược thư quốc gia).
- **Phương án thay thế:** Chỉ sử dụng vector database đơn thuần (Pure Vector RAG chunking theo 512 tokens).
- **Đánh đổi và Lý do chọn:**
  - Vector search đơn thuần thường xuyên ảo giác (hallucination) khi gặp câu hỏi suy luận đa bước (multi-hop reasoning), ví dụ: "Bệnh nhân có tiền sử suy thận mạn độ 3 đang dùng thuốc hạ áp X thì có được chỉ định kháng sinh Y hay không?".
  - Knowledge Graph lưu trữ tường minh các mối quan hệ `(Thuốc) -[CHỐNG_CHỈ_ĐỊNH_VỚI]-> (Bệnh lý)` và `(Thuốc) -[TƯƠNG_TÁC_NGUY_HIỂM]-> (Thuốc)`.
  - Đánh đổi: Chi phí xây dựng và trích xuất thực thể/quan hệ bằng LLM tốn kém hơn ở giai đoạn indexing ban đầu, nhưng độ chính xác của câu trả lời y tế đạt ngưỡng an toàn cho người bệnh, giảm thiểu 90% lỗi tương tác thuốc chết người.

### Quyết định 5: Quyền được xóa (Right to Erasure) — Tombstone lan truyền + Invalidation Cache vs Hard Delete toàn bộ Lakehouse
- **Phương án chọn:** Ghi nhận CDC Delete từ nguồn thành Tombstone ở Silver (`is_deleted = true`, xóa sạch văn bản lâm sàng); loại bỏ ngay lập tức khỏi Live RAG chunks và các training snapshot phát hành mới; gắn cờ vô hiệu hóa trong Embedding Cache.
- **Phương án thay thế:** Chạy lệnh DELETE vật lý quét qua toàn bộ các phân vùng Parquet lịch sử của Lakehouse và train lại toàn bộ các mô hình AI từ đầu.
- **Đánh đổi và Lý do chọn:**
  - Hard delete trên toàn bộ lịch sử Parquet/Delta lake là thao tác cực kỳ đắt đỏ về I/O, dễ gây hỏng tính bất biến của các bản phát hành mô hình đã được nghiệm thu (model reproducibility).
  - Với CDC Tombstone kết hợp purge dữ liệu trực tiếp trong Silver và Gold hiện hành, người dùng tìm kiếm qua chatbot hay RAG không bao giờ còn nhìn thấy dữ liệu của bệnh nhân đã xóa. Đối với mô hình AI đã train trên dữ liệu cũ, triển khai cơ chế Machine Unlearning hoặc lên lịch retrain định kỳ hàng quý thay vì retrain tức thời cho từng bệnh nhân.

---

## 3. Phương án bị loại bỏ (Rejected Alternative)

### Hệ thống Lambda Architecture song song (Streaming qua Kafka/Flink vào Elasticsearch, Batch qua Hadoop/Spark)
- **Lý do loại bỏ:**
  - **Nhân đôi chi phí bảo trì logic:** Phải duy trì hai codebase song song (Java cho Flink và Python/SQL cho Spark) cho cùng một phép tính lâm sàng, dẫn đến "dual-codebase drift" — khi logic chẩn đoán ở streaming lệch với batch training.
  - **Chi phí hạ tầng và nhân sự:** Vận hành đồng thời cluster Kafka, Flink, Hadoop và Elasticsearch đòi hỏi đội ngũ DevOps lớn, trong khi ngân sách bệnh viện cần tối ưu.
  - **Thiếu tính năng Time-travel chuẩn:** Elasticsearch không hỗ trợ time-travel point-in-time snapshot nguyên bản như kiến trúc Lakehouse hiện đại.

---

## 4. Sơ đồ kiến trúc đề xuất

```text
  [ HỆ THỐNG NGUỒN ]
  ┌──────────────┐   ┌──────────────┐   ┌──────────────┐
  │  HIS (Postgres)  │   │  LIS/PACS (API)  │   │  Văn bản OCR  │
  └──────┬───────┘   └──────┬───────┘   └──────┬───────┘
         │ CDC (Debezium)   │ Ingest Cron      │ Event Stream
         ▼                  ▼                  ▼
┌────────────────────────────────────────────────────────┐
│                        BRONZE                          │
│  - Raw Parquet bất biến, lưu trữ dạng append-only      │
│  - Giữ nguyên tombstone, Kafka offset, metadata nguồn  │
└──────────────────────────┬─────────────────────────────┘
                           │ 
                           │ Gate kiểm tra chất lượng (Pydantic / Great Expectations)
                           │ + Che PII (Regex / NER) & Pseudonymization Vault
                           ▼
┌────────────────────────────────────────────────────────┐
│                        SILVER                          │
│  - silver_patients, silver_encounters (MERGE theo khoá)│
│  - silver_clinical_history (SCD Type 2)                │
│  - CDC Tombstone: is_deleted=True, PII=NULL            │
└──────────────────────────┬─────────────────────────────┘
                           │
        ┌──────────────────┼──────────────────┐
        │ Microbatch       │ As-of Snapshot   │ Live Tickets /
        │ Lookback=P99     │ Point-in-time    │ Live Notes
        ▼                  ▼                  ▼
┌─────────────────┐┌─────────────────┐┌──────────────────┐
│   GOLD FEATURES ││  GOLD TRAINING  ││   GOLD RAG & KG  │
│  - 1 dòng / bn /││  - Versioned    ││  - Chunking có   │
│    ngày điều trị││    vYYYY-MM-DD  ││    overlap       │
│  - Tính theo    ││  - Bất biến,    ││  - Embedding cache│
│    event_time   ││    không rò rỉ  ││    bằng SHA256   │
│  - Dùng cho CDSS││    tương lai    ││  - Knowledge Graph│
└─────────────────┘└─────────────────┘└──────────────────┘
```
