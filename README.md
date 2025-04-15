# Dự án ETL (Extract, Transform, Load)

Dự án này thực hiện quá trình trích xuất dữ liệu từ eBay API và database, sau đó biến đổi và tải vào MinIO (S3-compatible storage)

## Cấu trúc dự án

```
ETL/
├── dags/           # Chứa các DAG của Apache Airflow
├── data/           # Thư mục lưu trữ dữ liệu
├── docker/         # Cấu hình Docker
├── docker-requierments/ # Yêu cầu cho Docker
├── logs/           # Thư mục lưu log
├── plugins/        # Các plugin tùy chỉnh
├── scripts/        # Các script tiện ích
├── src/            # Mã nguồn chính
├── test/           # Thư mục chứa các test
├── .env            # File cấu hình môi trường
├── docker-compose.yml # Cấu hình Docker Compose
└── requirements.txt # Các thư viện Python cần thiết

## Yêu cầu hệ thống

- Python 3.x
- Docker và Docker Compose

## Cài đặt

1. Clone repository:
```bash
https://github.com/pnquang260805/E-Commerce-ETL.git
cd ./E-Commerce-ETL
```

2. Cài đặt các thư viện cần thiết:
```bash
pip install -r requirements.txt
```

3. Cấu hình môi trường:
Tạo file .env với các biến môi trường:
+ EBAY_TOKEN: token của eBay API
+ AIRFLOW_UID: ID người dùng Airflow (mặc định: 50000)
+ AIRFLOW_GID: ID nhóm Airflow (mặc định: 50000)
+ _AIRFLOW_WWW_USER_USERNAME: Tên người dùng Airflow
+ _AIRFLOW_WWW_USER_PASSWORD: Mật khẩu người dùng Airflow

4. Khởi động các service bằng Docker:
```bash
docker-compose up -d
```

## Sử dụng

1. Truy cập Airflow UI:
- Mở trình duyệt và truy cập: `http://localhost:8080`
- Đăng nhập với tài khoản và mật khẩu đã đặt ở file .env: ```_AIRFLOW_WWW_USER_USERNAME``` và ```_AIRFLOW_WWW_USER_PASSWORD```

2. Kích hoạt DAG:
- Tìm DAG cần chạy trong danh sách
- Bật DAG bằng nút toggle
- Chạy DAG bằng nút "Trigger DAG"