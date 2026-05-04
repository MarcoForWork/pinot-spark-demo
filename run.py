#!/usr/bin/env python3
import subprocess, sys, time, json, urllib.request, urllib.error
from pathlib import Path

DATASET_FILE     = Path("data/yellow_tripdata_2016-03.csv")
PINOT_CONTROLLER = "http://localhost:9000"

def check_env():
    print("[1/8] Kiểm tra môi trường...")
    if subprocess.run(["docker", "info"], capture_output=True).returncode != 0:
        sys.exit("Docker chưa chạy!")
    if not DATASET_FILE.exists():
        sys.exit(f"Không tìm thấy dataset: {DATASET_FILE}")
    for f in ["pinot-config/schema.json", "pinot-config/table.json", "pinot-config/ingestion-job.yml"]:
        if not Path(f).exists():
            sys.exit(f"Thiếu file config: {f}")
    print(f"OK — Dataset: {DATASET_FILE} ({DATASET_FILE.stat().st_size / 1024**3:.2f} GB)")

def docker_up():
    print("[2/8] Khởi động Docker...")
    subprocess.run(["docker", "compose", "down", "--remove-orphans"], capture_output=True)
    if subprocess.run(["docker", "compose", "up", "-d"]).returncode != 0:
        sys.exit("Không thể khởi động Docker Compose!")
    print("OK — Các container đã chạy.")

def tag_instances():
    print("[3/8] Chờ Broker & Server đăng ký...")
    broker = server = None
    for i in range(24):
        try:
            with urllib.request.urlopen(f"{PINOT_CONTROLLER}/instances", timeout=5) as r:
                instances = json.loads(r.read()).get("instances", [])
                broker = next((x for x in instances if x.startswith("Broker_")), None)
                server = next((x for x in instances if x.startswith("Server_")), None)
                if broker and server:
                    break
        except Exception:
            pass
        print(f"  Đang chờ... ({(i+1)*5}s)", end="\r")
        time.sleep(5)
    else:
        sys.exit("Không tìm thấy Broker/Server sau 2 phút!")

    for instance, tag in [(broker, "DefaultTenant_BROKER"), (server, "DefaultTenant_OFFLINE")]:
        req = urllib.request.Request(
            f"{PINOT_CONTROLLER}/instances/{instance}/updateTags?tags={tag}",
            data=b"", method="PUT"
        )
        with urllib.request.urlopen(req, timeout=10):
            print(f"OK — Tagged: {instance}")

def post_config(step, label, path, url):
    print(f"[{step}] {label}...")
    req = urllib.request.Request(url, data=Path(path).read_bytes(),
                                 headers={"Content-Type": "application/json"}, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            print(f"OK — {r.read().decode().strip()}")
    except urllib.error.HTTPError as e:
        resp = e.read().decode()
        if "already exists" in resp.lower():
            print("OK — Đã tồn tại, bỏ qua.")
        else:
            sys.exit(f"Thất bại ({e.code}): {resp}")

def spark_etl():
    print("[6/8] Chạy Spark ETL (có thể mất 5–10 phút)...")
    result = subprocess.run([
        "docker", "exec", "spark-master",
        "/opt/spark/bin/spark-submit",
        "--master", "local[*]", "--driver-memory", "4g", "--executor-memory", "4g",
        "/opt/spark/jobs/etl.py"
    ])
    if result.returncode != 0:
        sys.exit("Spark ETL thất bại! Xem log: docker logs spark-master")
    print("OK — Parquet đã lưu vào data/nyc_taxi_clean/")

def ingestion():
    print("[7/8] Nạp dữ liệu vào Pinot...")
    result = subprocess.run([
        "docker", "exec", "pinot-controller",
        "/opt/pinot/bin/pinot-admin.sh", "LaunchDataIngestionJob",
        "-jobSpecFile", "/opt/pinot/config/ingestion-job.yml"
    ])
    if result.returncode != 0:
        sys.exit("Ingestion Job thất bại!")
    print("OK — Segment đã được đẩy vào Pinot.")

def verify():
    print("[8/8] Kiểm tra kết quả...")
    time.sleep(5)
    try:
        with urllib.request.urlopen(f"{PINOT_CONTROLLER}/tables/nyc_taxi/size", timeout=5) as r:
            data = json.loads(r.read())
            size_mb = data.get("reportedSizeInBytes", 0) / 1024 / 1024
            print(f"OK — Kích thước: {size_mb:.1f} MB")
    except Exception:
        print("Không lấy được thông tin — kiểm tra tại localhost:9000")

def main():
    print("=" * 55)
    print("   NYC Taxi Pipeline — Spark + Pinot")
    print("=" * 55)

    check_env()
    docker_up()
    tag_instances()
    post_config("4/8", "Đăng ký Schema", "pinot-config/schema.json", f"{PINOT_CONTROLLER}/schemas")
    post_config("5/8", "Đăng ký Table",  "pinot-config/table.json",  f"{PINOT_CONTROLLER}/tables")
    spark_etl()
    ingestion()
    verify()

    print("\n  Pipeline hoàn thành!")
    print(f"  Pinot UI  : http://localhost:9000")
    print(f"  Dừng hệ thống: docker compose down\n")

if __name__ == "__main__":
    main()