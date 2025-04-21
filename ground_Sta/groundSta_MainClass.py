import pandas as pd
import psycopg2
import os
class GroundSta:
    def __init__(self,path_data):
        # input data
        self.path_data = path_data
        self.folder_agencies = ['rid', 'dwr']

        # database PostgreSQL
        self.PG_HOST = "192.168.12.135"
        self.PG_DB = "ground_sta"
        self.PG_USER = "hydro"
        self.PG_PASSWORD = "Hydr0@123"
        self.PG_PORT = "5432"

        # 1.เข้าถึงทุกหน่วยงาน
        for agency in self.folder_agencies:
            if agency == 'rid':
                df_wl_rid,df_q_rid = self.rid_getdata(agency)
            elif agency == 'dwr':
                df_wl_dwr = self.dwr_getdata(agency)

        # 2.รวม dataframe
        df_wl_combined = pd.concat([df_wl_rid,df_wl_dwr], ignore_index=True)
        df_q_combined = pd.concat([df_q_rid], ignore_index=True)

        # 3.นำเข้า db
        self.insert_wl_to_db(df_wl_combined)
        self.insert_q_to_db(df_q_combined)
    def rid_getdata(self,agency):
        path_agency = os.path.join(self.path_data,agency)
        path_file = os.path.join(path_agency,'2.3สถานีวัดน้ำท่าจากศูนย์อุทกวิทยา(waterlevel).csv')
        data = pd.read_csv(path_file)
        df = pd.DataFrame(data) # ข้อมูลดิบจาก rid
        # จัดรูปข้อมูล
        # 1. water level
        df_wl = df.copy()
        df_wl['agency'] = 'RID'
        df_wl = df_wl.drop(columns=['flow_rate','discharge'])
        df_wl = df_wl.rename(columns={'tele_station_oldcode':'sta_code'
            ,'station_name':'sta_name'
            ,'tele_station_lat':'sta_lat'
            ,'tele_station_long':'sta_long'
            ,'waterlevel_datetime':'date_time'})
        # 2. discharge
        df_q = df.copy()
        df_q['agency'] = 'RID'
        df_q = df_q.drop(columns=['waterlevel_m','waterlevel_msl'])
        df_q = df_q.rename(columns={'tele_station_oldcode': 'sta_code'
            , 'station_name': 'sta_name'
            , 'tele_station_lat': 'sta_lat'
            , 'tele_station_long': 'sta_long'
            , 'waterlevel_datetime': 'date_time'})
        return df_wl,df_q

    def dwr_getdata(self,agency):
        path_agency = os.path.join(self.path_data,agency)
        # 1.water level
        path_files = os.path.join(path_agency,'waterlevel')
        dfs_wl = []
        for name_file in os.listdir(path_files):
            path_file = os.path.join(path_files, name_file)
            try:
                data = pd.read_csv(path_file)
                df_wl = pd.DataFrame(data)
                df_wl['agency'] = 'DWR'
                df_wl = df_wl.rename(columns={'tele_station_oldcode':'sta_code'
            ,'station_name':'sta_name'
            ,'tele_station_lat':'sta_lat'
            ,'tele_station_long':'sta_long'
            ,'waterlevel_datetime':'date_time'})
                dfs_wl.append(df_wl)
            except Exception as e:
                print(f"⚠️ อ่านไฟล์ {name_file} ไม่ได้: {e}")
        df_wl_combined = pd.concat(dfs_wl, ignore_index=True)
        return df_wl_combined

    def insert_wl_to_db(self, df):

        # 1. Connect to default DB to create new DB (optional)
        conn = psycopg2.connect(host=self.PG_HOST, dbname="postgres", user=self.PG_USER, password=self.PG_PASSWORD,
                                port=self.PG_PORT)
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
        conn = psycopg2.connect(host=self.PG_HOST, dbname=self.PG_DB, user=self.PG_USER, password=self.PG_PASSWORD,
                                port=self.PG_PORT)
        cur = conn.cursor()

        cur.execute(f"""
                        CREATE TABLE IF NOT EXISTS daily_wl (
                            id SERIAL PRIMARY KEY,
                            sta_code TEXT,
                            sta_name TEXT,
                            sta_lat DOUBLE PRECISION,
                            sta_long DOUBLE PRECISION,
                            area_name TEXT,
                            tambon_name TEXT,
                            amphoe_name TEXT,
                            province_name TEXT,
                            agency_name TEXT,
                            date_time TEXT,
                            waterlevel_m DOUBLE PRECISION,
                            waterlevel_msl DOUBLE PRECISION,
                            agency TEXT,
                            UNIQUE(sta_code, date_time)
                        );
                    """)
        conn.commit()
        cur.close()
        conn.close()
        print("✅ PostgreSQL table 'daily_wl' ready.")

        # 4. Insert ข้อมูล
        conn = psycopg2.connect(
            host=self.PG_HOST,
            dbname=self.PG_DB,
            user=self.PG_USER,
            password=self.PG_PASSWORD,
            port=self.PG_PORT
        )
        cur = conn.cursor()
        rows = df.values.tolist()

        args_str = ",".join(
            cur.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", row).decode("utf-8") for row in rows)
        cur.execute(
            f"""
            INSERT INTO daily_wl (
                sta_code, sta_name, sta_lat, sta_long, area_name, tambon_name, amphoe_name,
                province_name, agency_name, date_time, waterlevel_m, waterlevel_msl, agency
            )
            VALUES {args_str}
            ON CONFLICT (sta_code, date_time) DO NOTHING;
            """
        )
        conn.commit()
        print(f"✅ Inserted {len(rows)} rows into daily_wl!")

        cur.close()
        conn.close()

    def insert_q_to_db(self, df):

        # 1. Connect to default DB to create new DB (optional)
        conn = psycopg2.connect(host=self.PG_HOST, dbname="postgres", user=self.PG_USER, password=self.PG_PASSWORD,
                                port=self.PG_PORT)
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
        conn = psycopg2.connect(host=self.PG_HOST, dbname=self.PG_DB, user=self.PG_USER, password=self.PG_PASSWORD,
                                port=self.PG_PORT)
        cur = conn.cursor()

        cur.execute(f"""
                        CREATE TABLE IF NOT EXISTS daily_q (
                            id SERIAL PRIMARY KEY,
                            sta_code TEXT,
                            sta_name TEXT,
                            sta_lat DOUBLE PRECISION,
                            sta_long DOUBLE PRECISION,
                            area_name TEXT,
                            tambon_name TEXT,
                            amphoe_name TEXT,
                            province_name TEXT,
                            agency_name TEXT,
                            date_time TEXT,
                            flow_rate DOUBLE PRECISION,
                            discharge DOUBLE PRECISION,
                            agency TEXT,
                            UNIQUE(sta_code, date_time)
                        );
                    """)
        conn.commit()
        cur.close()
        conn.close()
        print("✅ PostgreSQL table 'daily_q' ready.")

        # 4. Insert ข้อมูล
        conn = psycopg2.connect(
            host=self.PG_HOST,
            dbname=self.PG_DB,
            user=self.PG_USER,
            password=self.PG_PASSWORD,
            port=self.PG_PORT
        )
        cur = conn.cursor()
        rows = df.values.tolist()

        args_str = ",".join(
            cur.mogrify("(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", row).decode("utf-8") for row in rows)
        cur.execute(
            f"""
            INSERT INTO daily_q (
                sta_code, sta_name, sta_lat, sta_long, area_name, tambon_name, amphoe_name,
                province_name, agency_name, date_time, flow_rate, discharge, agency
            )
            VALUES {args_str}
            ON CONFLICT (sta_code, date_time) DO NOTHING;
            """
        )
        conn.commit()
        print(f"✅ Inserted {len(rows)} rows into daily_q!")

        cur.close()
        conn.close()