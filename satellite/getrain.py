import psycopg2
import pandas as pd

def get_rainfall_data(date_str, lat_val, lon_val):
    """
    ดึงข้อมูลฝนจาก PostgreSQL โดยใช้วันที่ + lat + lon เป็นเงื่อนไข

    Args:
        date_str (str): วันที่รูปแบบ 'YYYY-MM-DD'
        lat_val (float): ค่าละติจูด (เช่น 20.15)
        lon_val (float): ค่าลองจิจูด (เช่น 99.85)

    Returns:
        pd.DataFrame: ตารางข้อมูลฝน (rainfall_mmhr) รายชั่วโมง
    """
    # เชื่อมต่อ PostgreSQL
    conn = psycopg2.connect(
        host="192.168.12.135",
        dbname="gsmap_db",
        user="hydro",
        password="Hydr0@123",
        port="5432"
    )

    query = f"""
        SELECT date, time, lat, lon, rainfall_mmhr
        FROM now_rainfall
        WHERE date = %s AND lat = %s AND lon = %s
        ORDER BY time;
    """

    df = pd.read_sql(query, conn, params=(date_str, lat_val, lon_val))
    conn.close()
    return df

df = get_rainfall_data("2024-09-10", 20.3, 99.7)
print(df)
