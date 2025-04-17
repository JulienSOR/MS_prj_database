import os
import zipfile
import geopandas as gpd
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

# === CONFIGURATION ===
KMZ_FOLDER = "8-5 GE DEM and Contour"  # path ที่เก็บไฟล์ .kmz
TEMP_KML_FOLDER = "temp_kml"    # โฟลเดอร์สำหรับเก็บ .kml ชั่วคราว
DB_NAME = "gisdem"
DB_USER = "postgres"
DB_PASSWORD = "112130"
DB_HOST = "localhost"
DB_PORT = 5432

# สร้าง connection string
postgres_url = URL.create(
    drivername="postgresql+psycopg2",
    username=DB_USER,
    password=DB_PASSWORD,
    host=DB_HOST,
    port=DB_PORT,
    database=DB_NAME
)
engine = create_engine(postgres_url)

# === STEP 1: ตรวจสอบหรือสร้าง Database (Optional) ===
def create_database_if_not_exists():
    conn = psycopg2.connect(
        dbname="postgres", user=DB_USER, password=DB_PASSWORD,
        host=DB_HOST, port=DB_PORT
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{DB_NAME}';")
    exists = cur.fetchone()
    if not exists:
        cur.execute(f"CREATE DATABASE {DB_NAME};")
        print(f"✅ Database '{DB_NAME}' created.")
    else:
        print(f"✅ Database '{DB_NAME}' already exists.")
    cur.close()
    conn.close()

# === STEP 2: แตกไฟล์ .kmz และอ่าน .kml ===
def extract_kmz(kmz_path, extract_to):
    with zipfile.ZipFile(kmz_path, 'r') as zf:
        zf.extractall(extract_to)
    kml_files = [f for f in os.listdir(extract_to) if f.endswith(".kml")]
    if not kml_files:
        raise Exception(f"No .kml found in {kmz_path}")
    return os.path.join(extract_to, kml_files[0])

# === STEP 3: โหลด .kml เข้า PostGIS ===
def import_kml_to_postgis(kml_path, table_name):
    print(f"📄 Reading {kml_path}")
    gdf = gpd.read_file(kml_path, driver='KML')
    gdf = gdf.to_crs("EPSG:4326")  # Ensure CRS is WGS84
    gdf.to_postgis(
        name=table_name,
        con=engine,
        if_exists="replace",  # หรือใช้ 'append' ถ้าต้องการรวม
        index=False
    )
    print(f"✅ Imported to table: {table_name}")
def enable_postgis_extension():
    conn = psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
        host=DB_HOST, port=DB_PORT
    )
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("CREATE EXTENSION IF NOT EXISTS postgis;")
    print("✅ PostGIS extension enabled.")
    cur.close()
    conn.close()


# === MAIN WORKFLOW ===
create_database_if_not_exists()
enable_postgis_extension()
os.makedirs(TEMP_KML_FOLDER, exist_ok=True)

for file in os.listdir(KMZ_FOLDER):
    if file.endswith(".kmz"):
        kmz_path = os.path.join(KMZ_FOLDER, file)
        name = os.path.splitext(file)[0].lower().replace(" ", "_")
        table_name = f"kml_{name}"

        # ล้าง temp folder ก่อนใช้
        for f in os.listdir(TEMP_KML_FOLDER):
            os.remove(os.path.join(TEMP_KML_FOLDER, f))

        kml_path = extract_kmz(kmz_path, TEMP_KML_FOLDER)
        import_kml_to_postgis(kml_path, table_name)
