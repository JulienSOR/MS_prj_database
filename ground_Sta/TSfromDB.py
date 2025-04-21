import psycopg2
import pandas as pd

# กำหนดค่าการเชื่อมต่อ
conn = psycopg2.connect(
    host="192.168.12.135",
    dbname="ground_sta",
    user="hydro",
    password="Hydr0@123",
    port="5432"
)

# ดึงข้อมูลจาก daily_wl
query_wl = "SELECT * FROM daily_wl;"
df_wl = pd.read_sql(query_wl, conn)

# ดึงข้อมูลจาก daily_q
query_q = "SELECT * FROM daily_q;"
df_q = pd.read_sql(query_q, conn)

# ปิดการเชื่อมต่อ
conn.close()

# แสดงตัวอย่าง
print("ข้อมูลระดับน้ำ (df_wl):", df_wl.shape)
print("ข้อมูลปริมาณน้ำ (df_q):", df_q.shape)
