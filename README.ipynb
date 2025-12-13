from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import time
import uuid

cassandra_hosts = ["cassandra-db"]   # hoặc ["127.0.0.1"] nếu bạn map cổng ra host
keyspace = "mykeyspace"
table = "mytable"

# message bạn muốn lưu (dùng result từ Kafka nếu có)
msg_received = "test_kafka_to_cassandra"

# Thử kết nối nhiều lần (Cassandra khởi lâu)
max_retries = 12
retry_delay = 5  # seconds

cluster = None
for attempt in range(1, max_retries+1):
    try:
        cluster = Cluster(contact_points=cassandra_hosts, port=9042)
        session = cluster.connect()  # connect không kèm keyspace
        print(f"✅ Connected to Cassandra (attempt {attempt})")
        break
    except Exception as e:
        print(f"⚠️ Không kết nối được tới Cassandra (attempt {attempt}/{max_retries}): {e}")
        time.sleep(retry_delay)

if cluster is None:
    raise SystemExit("❌ Không thể kết nối tới Cassandra sau nhiều lần thử. Kiểm tra container/logs và thử lại.")

# Tạo keyspace nếu chưa có
create_ks_cql = f"""
CREATE KEYSPACE IF NOT EXISTS {keyspace}
WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': '1'}}
"""
session.execute(create_ks_cql)
print(f"✅ Keyspace '{keyspace}' đã sẵn sàng (hoặc đã tồn tại).")

# Set keyspace cho session
session.set_keyspace(keyspace)

# Tạo table nếu chưa có
create_table_cql = f"""
CREATE TABLE IF NOT EXISTS {table} (
    id text PRIMARY KEY,
    value text
)
"""
session.execute(create_table_cql)
print(f"✅ Table '{table}' đã sẵn sàng (hoặc đã tồn tại).")

# Chèn message
try:
    unique_id = str(uuid.uuid4())
    insert_cql = f"INSERT INTO {table} (id, value) VALUES (%s, %s)"
    session.execute(insert_cql, (unique_id, msg_received))
    print("✅ Cassandra: Message test ghi thành công vào table.")
except Exception as e:
    print("❌ Cassandra: Lỗi khi ghi:", e)

# Đóng kết nối
cluster.shutdown()
