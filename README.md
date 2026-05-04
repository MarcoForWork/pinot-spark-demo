# pinot-spark-demo

Pipeline phân tích dữ liệu taxi NYC sử dụng Apache Spark (ETL) + Apache Pinot (OLAP queries).

---

## Yêu cầu

- [Docker Desktop](https://www.docker.com/products/docker-desktop/)
- Python 3.8+
- File CSV dataset (xem hướng dẫn bên dưới)

---

## Cấu trúc thư mục

```
pinot-spark-demo/
├── data/
│   └── yellow_tripdata_2016-03.csv   ← file dataset (tự thêm vào, không có trong repo)
├── pinot-config/
│   ├── schema.json
│   ├── table.json
│   └── ingestion-job.yml
├── spark-jobs/
│   └── etl.py
├── docker-compose.yml
├── run.py
└── .gitignore
```

---

## Cách tạo folder và thêm dataset

**Bước 1 — Tạo folder `data`:**

```bash
mkdir data
```

**Bước 2 — Tải dataset:**

1. Truy cập: https://www.kaggle.com/datasets/elemento/nyc-yellow-taxi-trip-data
2. Đăng nhập Kaggle (hoặc tạo tài khoản miễn phí)
3. Click nút **Download** để tải file ZIP về máy
4. Giải nén, lấy file `yellow_tripdata_2016-03.csv`
5. Đặt file vào folder `data/`:

```
pinot-spark-demo/
└── data/
    └── yellow_tripdata_2016-03.csv   ✅
```

---

## Cách chạy

```bash
python run.py
```

Pipeline sẽ tự động chạy 8 bước:

| Bước | Mô tả                                              |
| ---- | -------------------------------------------------- |
| 1/8  | Kiểm tra môi trường (Docker, dataset, config)      |
| 2/8  | Khởi động cụm Docker (Zookeeper, Pinot, Spark)     |
| 3/8  | Chờ Broker & Server đăng ký, tag vào DefaultTenant |
| 4/8  | Đăng ký Schema lên Pinot                           |
| 5/8  | Đăng ký Table Config lên Pinot                     |
| 6/8  | Chạy Spark ETL (có thể mất 5–10 phút)              |
| 7/8  | Nạp dữ liệu Parquet vào Pinot                      |
| 8/8  | Kiểm tra kết quả                                   |

---

## Sau khi chạy xong

|                     | URL                         |
| ------------------- | --------------------------- |
| Pinot UI            | http://localhost:9000       |
| Pinot Query Console | http://localhost:9000/query |
| Spark UI            | http://localhost:8080       |

---

## Dừng hệ thống

```bash
docker compose down
```
