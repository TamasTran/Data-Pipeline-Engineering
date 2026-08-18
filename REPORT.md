# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Trần Tuấn Anh  **Lớp:** AICB-P2T2  **Ngày:** 2026-08-18

---

## 0 · Kết quả `make verify`

<details>
<summary>Dán nguyên output ba lần chạy vào đây</summary>

```
  BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     ✓ ok              12,480      12,480   ✓
  gold_feature_daily    ✓ ok               9,100       9,100   ✓
  gold_doc_chunks       ✓ ok              31,200      31,200   ✓
  quarantine_tickets    ✓ ok                 312         312   ✓

  CHECKSUM từng lượt
  ──────────────────────────────────────────────────────────────────────────
  gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
  gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
  gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
  quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

  KIỂM TRA KHÁC
  ──────────────────────────────────────────────────────────────────────────
  dbt test                                    ✓ 11/11 pass
  silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
  quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
  gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt**

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Mỗi lần chạy `make pipeline`, số hàng của `gold_training_set` tăng lên — dù dữ liệu đầu vào không đổi. Chạy lần thứ hai, số hàng gấp đôi. |
| **Nguyên nhân** | Incremental model không khai báo `unique_key` và `incremental_strategy`. Khi thiếu `unique_key`, dbt không biết dòng nào là "cùng một dòng", nên nó sinh ra câu `INSERT INTO ... SELECT ...` thuần tuý thay vì `MERGE`. Chạy lại cùng một partition ngày, toàn bộ ticket trong partition đó bị **ghi thêm** (append) thay vì **ghi đè** (upsert) — mỗi lượt chạy lại nhân đôi số hàng của partition đó. Đồng thời, nguồn CDC có bản ghi `op = 'u'`: ticket được tạo ngày D1 rồi update ngày D2 sẽ lọt qua điều kiện lọc theo `_ingested_at` ở **hai** partition khác nhau trong cùng một lượt chạy — chiến lược `delete+insert` theo partition ngày không đủ, phải dùng `merge` theo khoá `ticket_id`. Ngoài ra, DAG Airflow đặt `catchup=True` nên khi người trực bấm Clear Task, Airflow tự schedule chạy bù mọi ngày trong quá khứ, kích hoạt lỗi nhiều lần; và thiếu `max_active_runs` nên nhiều run có thể ghi đồng thời vào cùng bảng. |
| **Cách khắc phục** | **(1)** `dbt/models/gold/gold_training_set.sql`: thêm `unique_key = 'ticket_id'` và `incremental_strategy = 'merge'` vào `config()`. **(2)** `dags/ai_training_pipeline.py`: đổi `catchup=False` và thêm `max_active_runs=1`. |
| **Bằng chứng** | trước: số hàng tăng sau mỗi lượt chạy · sau: 12,480 hàng · checksum 3 lượt: `8dd7c98653` (ổn định) |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` chỉ có 8,645 hàng thay vì 9,100 (14 ngày × 650 customer). Các cặp (event_date, customer_id) bị thiếu tập trung ở các ngày cũ. |
| **P99 độ trễ đo được** | **~3 ngày** |
| **Lookback đã chọn** | 3 ngày — vì P99 ≈ 3 ngày, đủ bao phủ gần như toàn bộ dữ liệu tới trễ mà không tốn chi phí quét lại quá nhiều ở mỗi lượt chạy. |
| **Nguyên nhân** | Điều kiện lọc incremental `WHERE event_date > (SELECT max(event_date) FROM target)` chỉ xử lý những `event_date` **lớn hơn** ngày lớn nhất đã có trong bảng đích. Khi dữ liệu tới muộn (ví dụ event xảy ra ngày 08-12 nhưng `_ingested_at` là 08-15), vào lượt chạy ngày 08-15 thì `max(event_date)` đã là 08-14 → event 08-12 không thoả `> 08-14` → bị bỏ qua vĩnh viễn. Các ngày sau cũng tương tự vì `max(event_date)` chỉ tăng, không bao giờ giảm. Tóm lại: **điều kiện lọc chỉ nhìn về phía trước, không bao giờ quay lại xử lý dữ liệu tới trễ.** |
| **Cách khắc phục** | `dbt/models/gold/gold_feature_daily.sql`: **(1)** Đổi điều kiện lọc thành `WHERE event_date >= (SELECT max(event_date) FROM target) - interval 3 day` để lùi lookback window 3 ngày (theo P99). **(2)** Thêm `unique_key = ['event_date', 'customer_id']` và `incremental_strategy = 'merge'` vào `config()` để khi cùng một cặp (event_date, customer_id) được tính lại ở nhiều lượt chạy, kết quả mới **thay thế** kết quả cũ thay vì cộng dồn. |
| **Bằng chứng** | trước: 8,645 hàng · sau: 9,100 hàng (ổn định, checksum `3db448685c`) |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> Dùng P99 (~3 ngày) thay vì max (có thể rất lớn) vì lookback window ảnh hưởng **chi phí ở mọi lượt chạy sau này**: mỗi ngày lùi thêm, dbt phải quét và merge lại toàn bộ dữ liệu của ngày đó — chi phí tích luỹ qua hàng trăm lần chạy. P99 bao phủ 99% dữ liệu trễ với chi phí hợp lý (3 ngày quét lại). Dùng max sẽ bao phủ 100% nhưng nếu max = 7–10 ngày thì mỗi lượt chạy tốn gấp 2–3 lần, và phần lớn chi phí đó là quét lại dữ liệu đã đúng. 1% còn lại có thể xử lý bằng backfill thủ công khi cần.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Cột `priority` trong `silver_tickets` chứa nhiều giá trị NULL và các giá trị ngoài contract (0, 5, -1). Đồng thời, từ ngày 08-10 team backend đổi format từ số sang nhãn chuỗi (urgent, high, medium, low) nhưng pipeline không xử lý được sự thay đổi này. |
| **Nguyên nhân** | Macro `normalize_priority` dùng `try_cast(priority_raw as integer)` sai theo **hai hướng ngược nhau**: (1) nó biến các nhãn chuỗi hợp lệ (urgent, high, medium, low) thành NULL — vứt bỏ một nửa dữ liệu tốt chỉ vì source đổi format; (2) nó lại chấp nhận các giá trị số không hợp lệ (0, 5, -1) vì chúng đúng là integer — dù contract chỉ cho phép 1..4. Thêm vào đó, `silver_tickets.sql` xếp hạng (row_number) trước rồi mới lọc, nên ticket có bản ghi mới nhất bị hỏng sẽ biến mất hoàn toàn khỏi Silver thay vì giữ lại trạng thái hợp lệ từ lần cập nhật trước. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | *Nhóm 1:* `'1','2','3','4'` — số hợp lệ, giữ nguyên. *Nhóm 2:* `'urgent','high','medium','low'` — schema evolution, map `urgent→1, high→2, medium→3, low→4`. *Nhóm 3:* `'P1','unknown','0','5','-1','',NULL` — dữ liệu hỏng, trả về NULL → đưa vào quarantine. |
| **Cách khắc phục** | **(a)** `dbt/macros/normalize_priority.sql`: thay `try_cast` bằng khối CASE xử lý 3 nhóm. **(b)** `dbt/models/silver/silver_tickets.sql`: lọc bỏ bản ghi có priority NULL **trước** khi xếp hạng (filter-before-rank), để ticket có bản ghi mới nhất hỏng vẫn giữ trạng thái hợp lệ từ lần trước. **(c)** `dbt/models/silver/quarantine_tickets.sql`: thay `WHERE false` bằng `WHERE normalize_priority(priority_raw) IS NULL`. **(d)** `dbt/models/silver/schema.yml`: đổi `enforced: true` và thêm test `not_null` + `accepted_values: [1, 2, 3, 4]` cho cột priority. |
| **Bằng chứng** | `quarantine_tickets` = 312 hàng · `dbt test` 11/11 pass · `silver_tickets` giữ đủ 12,480 ticket |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để pipeline dừng khi gặp bản ghi lỗi?

> **Chặn ở Silver, không phải Bronze.** Bronze giữ vai trò "bản sao trung thành" của dữ liệu nguồn — nếu Bronze từ chối row lỗi thì khi cần điều tra sự cố sau này (ví dụ: source gửi sai từ bao giờ? bao nhiêu row bị ảnh hưởng? pattern lỗi là gì?) ta không còn bằng chứng gốc để truy vấn. Bronze nên ghi tất cả, Silver mới là nơi áp contract.
>
> **Không để pipeline dừng** vì 312 bản ghi lỗi chỉ chiếm ~2% tổng số bản ghi CDC, trong khi hơn 130,000 event và 31,200 chunk hoàn toàn bình thường đang chờ được phục vụ. Dừng cả pipeline vì một tỷ lệ lỗi nhỏ là đánh đổi sai: người dùng cuối mất toàn bộ dữ liệu mới chỉ vì một nhúm bản ghi hỏng. Thay vào đó, tách riêng bản ghi lỗi vào quarantine để pipeline chạy tiếp, đồng thời tạo hàng đợi cho người trực xử lý.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | không làm |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Kiểm tra incremental model có khai báo `unique_key` và `incremental_strategy` không — thiếu hai tham số này thì mọi lần chạy lại đều nhân bản dữ liệu. Đồng thời kiểm tra DAG có `catchup=False` và `max_active_runs=1` để tránh ghi đồng thời. |
| 2 | Đo phân bố độ trễ (latency) của dữ liệu từ nguồn đến kho, và kiểm tra điều kiện lọc incremental có lookback window đủ rộng để bắt dữ liệu tới trễ. Nếu chỉ nhìn về phía trước (`> max`), mọi bản ghi muộn sẽ bị bỏ qua vĩnh viễn. |
| 3 | Kiểm tra xem pipeline có xử lý được schema evolution không (source đổi format), và có tách riêng dữ liệu lỗi vào quarantine thay vì vứt bỏ hoặc dừng pipeline. Quan trọng: lọc bản ghi hỏng trước khi xếp hạng, để không vô tình xoá cả entity chỉ vì bản ghi mới nhất bị hỏng. |
