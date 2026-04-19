# Pipeline KG Nhà Hàng

Dự án này xây dựng Knowledge Graph (KG) nhà hàng từ các nguồn CSV/JSON, xuất file đồ thị dạng CSV và import vào Neo4j.

## 1) Cấu trúc dự án

Tập tin/thư mục chính:

- `augment_and_build_kg.py`: Tạo 14 bảng KG và graph CSV từ dữ liệu nguồn.
- `load_kg_to_neo4j.py`: Import graph CSV (`nodes.csv`, `edges.csv`) vào Neo4j.
- `run_kg_pipeline.ps1`: Pipeline 1 lệnh (build -> bật Neo4j -> import -> verify).
- `docker-compose.neo4j.yml`: Dịch vụ Docker cho Neo4j.
- `start-neo4j-kg.ps1`: Script hỗ trợ chạy compose stack.
- `neo4j_visualize.cypher`: Mẫu truy vấn Cypher để trực quan hóa.

Dữ liệu đầu vào (thư mục gốc):

- `store_from_top5_json.csv`
- `menu_item_from_top5_json.csv`
- `be_google_maps_unique.csv`
- `store_feedback_crawled.csv`
- `user_scenarios_1.csv`
- `foody_hust_output/foody_hust_places_from_store_csv.csv`

Dữ liệu đầu ra được tạo:

- `kg_tables_all/`
  - 14 file CSV bảng trung gian (store, category, review, user, location, ...)
- `kg_tables_all/kg_graph/`
  - `nodes.csv`
  - `edges.csv`
  - `triples.csv`

## 2) Điều kiện cần

- Python 3.10+
- Gói Python: `neo4j`
- Docker Desktop (hoặc Docker Engine) đang chạy
- PowerShell (Windows)

Nếu chưa cài dependency Python:

```powershell
pip install neo4j
```

## 3) Chạy nhanh (khuyến nghị)

Chạy từ thư mục gốc của project:

```powershell
powershell -ExecutionPolicy Bypass -File .\run_kg_pipeline.ps1
```

Script sẽ làm:

1. Kiểm tra `python` và `docker`
2. Build KG CSV bằng `augment_and_build_kg.py`
3. Khởi động container Neo4j (`kg-neo4j`)
4. Import KG vào Neo4j bằng `load_kg_to_neo4j.py`
5. Kiểm tra số lượng node/relationship

Tùy chọn hay dùng:

```powershell
# Bỏ qua bước build, chỉ import file graph hiện có
powershell -ExecutionPolicy Bypass -File .\run_kg_pipeline.ps1 -SkipBuild

# Xóa dữ liệu cũ trước khi import
powershell -ExecutionPolicy Bypass -File .\run_kg_pipeline.ps1 -Wipe

# Dùng mật khẩu Neo4j khác
powershell -ExecutionPolicy Bypass -File .\run_kg_pipeline.ps1 -Neo4jPassword your_password

# Bỏ qua bước verify số lượng
powershell -ExecutionPolicy Bypass -File .\run_kg_pipeline.ps1 -SkipVerify
```

## 4) Chạy thủ công từng bước

### Bước 1: Tạo KG tables và graph CSV

```powershell
python augment_and_build_kg.py
```

Dự kiến sẽ thấy log tương tự:

- `Wrote 14 KG tables from source files to kg_tables_all`
- `Wrote KG graph with ... nodes and ... edges to kg_tables_all\kg_graph`

### Bước 2: Khởi động Neo4j

Nên chỉ chạy service Neo4j để tránh lỗi container importer:

```powershell
docker compose -f docker-compose.neo4j.yml up -d neo4j
```

Kiểm tra container:

```powershell
docker ps --filter "name=kg-neo4j"
```

### Bước 3: Import graph vào Neo4j

```powershell
python load_kg_to_neo4j.py --password neo4j123
```

Nếu cần xóa dữ liệu cũ:

```powershell
python load_kg_to_neo4j.py --password neo4j123 --wipe
```

### Bước 4: Kiểm tra số lượng dữ liệu

```powershell
docker exec kg-neo4j cypher-shell -u neo4j -p neo4j123 "MATCH (n) RETURN count(n) AS nodes;"
docker exec kg-neo4j cypher-shell -u neo4j -p neo4j123 "MATCH ()-[r]->() RETURN count(r) AS rels;"
```

## 5) Mở Neo4j Browser

- URL: `http://localhost:7474`
- User: `neo4j`
- Password: (mặc định theo compose/script, thường là `neo4j123`)

Truy vấn mẫu:

```cypher
MATCH (n)-[r]->(m)
RETURN n, r, m
LIMIT 100;
```

## 6) Lỗi thường gặp

### A) Thiếu `--password`

Nguyên nhân: `load_kg_to_neo4j.py` bắt buộc có tham số password.

Khắc phục:

```powershell
python load_kg_to_neo4j.py --password neo4j123
```

### B) `Connection refused localhost:7687`

Nguyên nhân: Neo4j chưa được khởi động.

Khắc phục:

```powershell
docker compose -f docker-compose.neo4j.yml up -d neo4j
```

Sau đó chạy lại lệnh import.

### C) Compose chạy importer và lỗi (`exec: "--uri" not found`)

Nguyên nhân: stack đầy đủ có service `importer` nhưng command không khớp.

Khắc phục: chỉ chạy service `neo4j`:

```powershell
docker compose -f docker-compose.neo4j.yml up -d neo4j
```

## 7) Lệnh đã được xác thực

Lệnh sau đã được chạy thành công trong workspace này:

```powershell
powershell -ExecutionPolicy Bypass -File .\run_kg_pipeline.ps1 -SkipBuild
```

Lệnh trên đã import KG và verify thành công.
