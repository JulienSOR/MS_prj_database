import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import time
class TSRain:
    def rHII(self,file):
        # 1. DataFrame ข้อมูลฝน
        df = pd.read_excel(file)  # อ่านข้อมูลจาก Excel
        print(df)

        # 2. เชื่อมต่อ PostgreSQL และสร้าง Database
        DB_NAME = "rhiigsta"
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
            CREATE TABLE IF NOT EXISTS rain_data (
                id SERIAL PRIMARY KEY,
                datetime VARCHAR(100),
                ait003 FLOAT
            )
        """)
        conn.commit()
        cur.close()
        conn.close()
        print("✅ ตาราง 'rain_data' ถูกสร้างสำเร็จ!")

        # 5. นำ DataFrame ไปใส่ Table
        df.to_sql("rain_data", engine, if_exists="replace", index=False)
        print("✅ DataFrame ถูกนำเข้าสู่ PostgreSQL สำเร็จ!")

    def rRID(self,file):
        pass
    def rTMD(self,file):
        pass
    def rDWR(self,file):
        pass

    def get_data(self):
        # กำหนดค่าการเชื่อมต่อ PostgreSQL
        DB_NAME = "rhiigsta"
        DB_USER = "postgres"
        DB_PASSWORD = "112130"
        DB_HOST = "localhost"
        DB_PORT = "5432"

        # เชื่อมต่อฐานข้อมูล
        DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(DATABASE_URL)

        # อ่านข้อมูลจากตาราง `rain_data`
        query = "SELECT * FROM rain_data"
        df = pd.read_sql(query, engine)

        print("✅ ดึงข้อมูลจาก PostgreSQL สำเร็จ!")
        print(df)  # แสดงข้อมูลทั้งหมด

        return df  # สามารถนำ DataFrame นี้ไปใช้งานต่อได้

