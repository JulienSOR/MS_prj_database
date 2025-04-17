import psycopg2
import json
from sqlalchemy import create_engine
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from psycopg2.extras import Json
import pandas as pd
class FloodMap:
    def flood1day(self,json_file):
        # 1. นำเข้า JSON
        with open(json_file, 'r', encoding='utf-8') as file:
            json_data = json.load(file)

        print(json_data)

        # 2. เชื่อมต่อ PostgreSQL และสร้าง Database
        DB_NAME = "flood1day"
        DB_USER = "postgres"
        DB_PASSWORD = "112130"
        DB_HOST = "localhost"
        DB_PORT = "5432"
        # เชื่อมต่อ PostgreSQL (ใช้ postgres ก่อนเพื่อสร้าง database)
        conn = psycopg2.connect(
            dbname="postgres",
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cur = conn.cursor()

        # ลบฐานข้อมูลถ้ามีอยู่แล้ว และสร้างใหม่
        cur.execute(f"SELECT 1 FROM pg_database WHERE datname='{DB_NAME}'")
        if cur.fetchone():
            cur.execute(f"DROP DATABASE {DB_NAME}")
            print(f"🗑️ ลบฐานข้อมูล '{DB_NAME}' สำเร็จ!")

        cur.execute(f"CREATE DATABASE {DB_NAME}")
        print(f"✅ สร้างฐานข้อมูล '{DB_NAME}' สำเร็จ!")

        cur.close()
        conn.close()

        # 3. เชื่อมต่อไปยังฐานข้อมูลใหม่
        DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(DATABASE_URL)

        # 4. สร้างตารางในฐานข้อมูล (ถ้ายังไม่มี)
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS flood_raw_json (
                id SERIAL PRIMARY KEY,
                data JSONB
            )
        """)
        conn.commit()

        # 5. JSON to PostgreSQL
        cur.execute("INSERT INTO flood_raw_json (data) VALUES (%s)", [Json(json_data)])
        conn.commit()

        cur.close()
        conn.close()
        print("✅ ตาราง 'flood_raw_json' ถูกสร้างสำเร็จ!")

    def flood3days(self,json_file):
        pass
    def flood7days(self,json_file):
        pass
    def flood30days(self,json_file):
        pass
    def get_data(self):
        # กำหนดค่าการเชื่อมต่อ PostgreSQL
        DB_NAME = "flood1day"
        DB_USER = "postgres"
        DB_PASSWORD = "112130"
        DB_HOST = "localhost"
        DB_PORT = "5432"

        # เชื่อมต่อฐานข้อมูล
        DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(DATABASE_URL)

        # อ่านข้อมูลจากตาราง `rain_data`
        query = "SELECT * FROM flood_raw_json"
        df = pd.read_sql(query, engine)

        print("✅ ดึงข้อมูลจาก PostgreSQL สำเร็จ!")
        print(df['data'])  # แสดงข้อมูลทั้งหมด

        return df  # สามารถนำ DataFrame นี้ไปใช้งานต่อได้


