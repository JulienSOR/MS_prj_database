from ftplib import FTP
import numpy as np
from netCDF4 import Dataset
from io import BytesIO
from datetime import datetime, timedelta
import psycopg2
import time

class SatelliteRain:
    def __init__(self, lat_max, lat_min, long_max, long_min, name_sat):
        # พิกัด pixel index
        self.lat_max = lat_max
        self.lat_min = lat_min
        self.long_max = long_max
        self.long_min = long_min
        self.X1 = int((90 - round(lat_max, 1)) * 10)
        self.X2 = int((90 - round(lat_min, 1)) * 10)
        self.Y1 = int((180 + round(long_max, 1)) * 10)
        self.Y2 = int((180 + round(long_min, 1)) * 10)

        # เวลา + ชื่อไฟล์
        self.file_time,self.year,self.month,self.day = self.time_lag()
        self.file_name = f"gsmap_now_rain.{self.file_time}.nc"

        # FTP
        self.name_sat = name_sat
        self.ftp_host = "hokusai.eorc.jaxa.jp"
        self.username = "rainmap"
        self.password = "Niskur+1404"
        self.remote_dir = f"/now/netcdf/{self.year}/{self.month}/{self.day}"

        # database PostgreSQL
        self.PG_HOST = "localhost"
        self.PG_DB = "gsmap_db"
        self.PG_USER = "postgres"
        self.PG_PASSWORD = "112130"
        self.PG_PORT = "5432"

    def download_gsmap_data(self, max_retries=10, wait_sec=60):
        print(f"โหลดไฟล์ {self.file_name} ...")
        for attempt in range(1, max_retries + 1):
            print(f"🔁 Attempt {attempt}/{max_retries} | Trying to get {self.file_name}")

            # FTP Connect
            ftp = FTP(self.ftp_host)
            ftp.login(self.username, self.password)
            ftp.cwd(self.remote_dir)
            file_list = ftp.nlst()

            if self.file_name in file_list:
                # Success!
                buffer = BytesIO()
                ftp.retrbinary(f"RETR {self.file_name}", buffer.write)
                ftp.quit()

                buffer.seek(0)
                print(f"โหลดไฟล์ {self.file_name} เข้า RAM สำเร็จ!")
                dataset = Dataset(self.file_name, mode='r', memory=buffer.read())
                return dataset

            ftp.quit()
            print(f"ยังไม่เจอ {self.file_name} บน FTP | รอ {wait_sec} วินาทีแล้วลองใหม่...")
            time.sleep(wait_sec)

        # ครบทุกครั้งแล้ว ยังไม่เจอ
        print("ไม่พบไฟล์ GSMaP แม้จะลอง 10 ครั้ง")
        raise FileNotFoundError(f"ไม่พบไฟล์ {self.file_name} บน FTP ภายใน {max_retries} นาที")

    def crop_gsmap_data(self,dataset):
        # ใส่ safety check:
        x1, x2 = sorted([self.X1, self.X2])
        y1, y2 = sorted([self.Y1, self.Y2])
        # เช็คคอลัมป์ของข้อมูล
        if 'hourlyPrecipRate' in dataset.variables:
            rain_var = 'hourlyPrecipRate'
        elif 'hourlyPrecipRateGC' in dataset.variables:
            rain_var = 'hourlyPrecipRateGC'
        else:
            print(f"Error: Could not find rainfall variable in {self.file_name}")
            return
        rain_data = dataset.variables[rain_var][:]

        if rain_data.ndim == 3:
            rain_data = rain_data[0]  # First time step if applicable
        rain_data = rain_data[::-1]  # Flip if needed
        cropped_data = np.ma.filled(rain_data[x1:x2, y1:y2].astype(float), fill_value=np.nan)
        return cropped_data

    def insert_to_db(self, cropped_data):

        # 1. Connect to default DB to create new DB (optional)
        conn = psycopg2.connect(host=self.PG_HOST, dbname="postgres", user=self.PG_USER, password=self.PG_PASSWORD, port=self.PG_PORT)
        conn.autocommit = True
        cur = conn.cursor()

        # 2. Create database (if not exist)
        cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{self.PG_DB}'")
        exists = cur.fetchone()
        if not exists:
            cur.execute(f"CREATE DATABASE {self.PG_DB}")
            print(f"✅ Created database: {self.PG_DB}")
        cur.close()
        conn.close()

        # 3. Connect to newly created DB to create table
        conn = psycopg2.connect(host=self.PG_HOST, dbname=self.PG_DB, user=self.PG_USER, password=self.PG_PASSWORD, port=self.PG_PORT)
        cur = conn.cursor()

        cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.name_sat}_rainfall (
                    id SERIAL PRIMARY KEY,
                    file_name TEXT,
                    date DATE,
                    time TEXT,
                    lat DOUBLE PRECISION,
                    lon DOUBLE PRECISION,
                    rainfall_mmhr DOUBLE PRECISION
                );
            """)
        conn.commit()
        cur.close()
        conn.close()
        print(f"✅ PostgreSQL table '{self.name_sat}_rainfall' ready.")

        # 4. สร้าง lat/lon ตามพื้นที่ crop
        height, width = cropped_data.shape
        lat_vals = np.linspace(round(self.lat_max, 1), round(self.lat_min, 1), height, endpoint=False)
        lon_vals = np.linspace(round(self.long_min, 1), round(self.long_max, 1), width, endpoint=False)
        # 5. เชื่อมต่อ PostgreSQL
        try:
            conn = psycopg2.connect(
                host=self.PG_HOST,
                dbname=self.PG_DB,
                user=self.PG_USER,
                password=self.PG_PASSWORD,
                port=self.PG_PORT
            )
            cur = conn.cursor()
        except Exception as e:
            print(f"❌ ไม่สามารถเชื่อมต่อ PostgreSQL ได้: {e}")
            return

        # 6. สร้างรายการข้อมูลฝนแต่ละพิกเซล
        rows = []
        for y in range(height):
            for x in range(width):
                rainfall = cropped_data[y, x]
                if not np.isnan(rainfall):
                    rows.append((
                        self.file_name,
                        f"{self.year}-{self.month}-{self.day}",
                        self.file_time[-4:],  # HHMM
                        round(float(lat_vals[y]),2),
                        round(float(lon_vals[x]),2),
                        float(rainfall)
                    ))
        # 7. Insert แบบ bulk
        if rows:
            try:
                args_str = ",".join(cur.mogrify("(%s,%s,%s,%s,%s,%s)", row).decode("utf-8") for row in rows)
                cur.execute(f"INSERT INTO {self.name_sat}_rainfall (file_name, date, time, lat, lon, rainfall_mmhr) VALUES " + args_str)
                conn.commit()
                print(f"✅ Inserted {len(rows)} rows into PostgreSQL!")
            except Exception as e:
                print(f"❌ เกิดข้อผิดพลาดขณะ insert: {e}")
                conn.rollback()
        else:
            print("☁️ ไม่มีฝนในช่วงเวลานี้ (ทุกค่าคือ NaN)")
        cur.close()
        conn.close()
    def time_lag(self):
        now = datetime.utcnow() - timedelta(hours=1)
        # ถ้า (now.hour - 8) < 0 ให้ถอยหลังไป 1 วัน
        rounded_minute = "00" if now.minute < 30 else "30"
        year_str = now.year
        month_str = f"{now.month:02d}"
        day_str = f"{now.day:02d}"
        hour_str = f"{now.hour:02d}"
        date_str = now.strftime("%Y%m%d")
        file_time = f"{date_str}.{hour_str}{rounded_minute}"
        return file_time,year_str,month_str,day_str