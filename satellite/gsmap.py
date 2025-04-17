from ftplib import FTP
import os
import h5py
import numpy as np
import matplotlib.pyplot as plt
import netCDF4 as nc
import matplotlib.patheffects as path_effects
import psycopg2
import pandas as pd
# Site Coordinates (WGS84)
LAT_MAX, LAT_MIN = 20.70, 20.00
LON_MAX, LON_MIN = 100.00, 99.30

# Calculate pixel indices
X1 = int((90 - round(LAT_MAX, 1)) * 10)
X2 = int((90 - round(LAT_MIN, 1)) * 10)
Y1 = int((180 + round(LON_MIN, 1)) * 10)
Y2 = int((180 + round(LON_MAX, 1)) * 10)

# Connection Postgresql config
PG_HOST = "localhost"
PG_DB = "gsmap_db"
PG_USER = "postgres"
PG_PASSWORD = "112130"
PG_PORT = "5432"

def create_database_and_table():
    """Create PostgreSQL database and rainfall table if they don't exist."""
    # Connect to default DB to create new DB (optional)
    conn = psycopg2.connect(host=PG_HOST, dbname="postgres", user=PG_USER, password=PG_PASSWORD, port=PG_PORT)
    conn.autocommit = True
    cur = conn.cursor()

    # Create database (if not exist)
    cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{PG_DB}'")
    exists = cur.fetchone()
    if not exists:
        cur.execute(f"CREATE DATABASE {PG_DB}")
        print(f"✅ Created database: {PG_DB}")
    cur.close()
    conn.close()

    # Connect to newly created DB to create table
    conn = psycopg2.connect(host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD, port=PG_PORT)
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS rainfall (
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
    print("✅ PostgreSQL table 'rainfall' ready.")
def insert_rainfall_data(file_path, data, lat_min, lat_max, lon_min, lon_max):
    """Insert cropped rainfall data into PostgreSQL."""
    conn = psycopg2.connect(host=PG_HOST, dbname=PG_DB, user=PG_USER, password=PG_PASSWORD, port=PG_PORT)
    cur = conn.cursor()

    file_name = os.path.basename(file_path)
    name, date_str, time_str, _ = file_name.split('.')
    date_fmt = f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:]}"  # YYYY-MM-DD

    height, width = data.shape
    lat_vals = np.linspace(lat_min, lat_max, height, endpoint=False)
    lon_vals = np.linspace(lon_min, lon_max, width, endpoint=False)

    rows = []
    for y in range(height):
        for x in range(width):
            rainfall = data[y, x]
            if not np.isnan(rainfall):
                rows.append((file_name, date_fmt, time_str, float(lat_vals[y]), float(lon_vals[x]), float(rainfall)))

    args_str = ",".join(cur.mogrify("(%s,%s,%s,%s,%s,%s)", row).decode("utf-8") for row in rows)
    cur.execute("INSERT INTO rainfall (file_name, date, time, lat, lon, rainfall_mmhr) VALUES " + args_str)
    conn.commit()
    cur.close()
    conn.close()
    print(len(rows))
    print(f"✅ Inserted {len(rows)} pixels into PostgreSQL for {file_name}")
def download_gsmap_data(ftp_host, username, password, remote_dir, local_dir, year_range, month_range, day_range,
                        file_ext):
    """Downloads GSMaP data and saves locally."""
    ftp = FTP(ftp_host)
    ftp.login(username, password)
    ftp.cwd(remote_dir)

    os.makedirs(local_dir, exist_ok=True)

    for year in year_range:
        year_path = os.path.join(local_dir, str(year))
        os.makedirs(year_path, exist_ok=True)

        for month in month_range:
            month_str = f"{month:02d}"
            month_path = os.path.join(year_path, month_str)
            os.makedirs(month_path, exist_ok=True)

            for day in day_range:
                day_str = f"{day:02d}"
                day_path = os.path.join(month_path, day_str)
                os.makedirs(day_path, exist_ok=True)

                ftp_dir = f"{remote_dir}/{year}/{month_str}/{day_str}"
                try:
                    ftp.cwd(ftp_dir)
                    files = ftp.nlst()
                    for file in files:
                        if file.endswith(file_ext):
                            local_file_path = os.path.join(day_path, file)
                            if not os.path.exists(local_file_path):
                                with open(local_file_path, 'wb') as f:
                                    ftp.retrbinary(f'RETR {file}', f.write)
                                print(f'Downloaded: {file}')
                            else:
                                print(f'Skipped (already exists): {file}')
                except:
                    print(f"Directory {ftp_dir} not found on FTP server.")

    ftp.quit()
    print("Download complete.")
def crop_gsmap_data(input_file, output_file):
    """Crops the GSMaP data and saves it as a NumPy array."""
    with nc.Dataset(input_file, 'r') as dataset:

        if 'hourlyPrecipRate' in dataset.variables:
            rain_var = 'hourlyPrecipRate'
        elif 'hourlyPrecipRateGC' in dataset.variables:
            rain_var = 'hourlyPrecipRateGC'
        else:
            print(f"Error: Could not find rainfall variable in {input_file}")
            return

        rain_data = dataset.variables[rain_var][:]
        if rain_data.ndim == 3:
            rain_data = rain_data[0]  # First time step if applicable

        rain_data = rain_data[::-1]  # Flip if needed

        cropped_data = np.ma.filled(rain_data[X1:X2, Y1:Y2].astype(float), fill_value=np.nan)

        np.save(output_file, cropped_data)  # ✅ Ensure correct NumPy save format
        print(f"Cropped data saved: {output_file}")
def show_cropped_data(cropped_file):
    """Displays the cropped GSMaP data as an image with values, grid, and color bar."""
    if not cropped_file.endswith('.npy'):
        print(f"Error: {cropped_file} is not a valid .npy file.")
        return

    data = np.load(cropped_file)  # Load the NumPy file correctly
    if data is None or data.size == 0:
        print(f"Error: Empty or invalid data in {cropped_file}")
        return

    plt.figure(figsize=(8, 6), dpi=150)
    cmap = plt.get_cmap('jet')

    # Display image
    plt.imshow(data, cmap=cmap, origin='upper',
               extent=[LON_MIN, LON_MAX, LAT_MIN, LAT_MAX], vmin=0, vmax=25)
    cbar = plt.colorbar(label='Rainfall (mm/hr)')
    cbar.set_ticks([0, 1, 2, 3, 5, 10, 15, 20, 25])

    # Display pixel values
    height, width = data.shape
    x_ticks = np.linspace(LON_MIN, LON_MAX, width, endpoint=False)
    y_ticks = np.linspace(LAT_MIN, LAT_MAX, height, endpoint=False)

    for y in range(height):
        for x in range(width):
            text_x = x_ticks[x] + (x_ticks[1] - x_ticks[0]) / 2
            text_y = y_ticks[y] + (y_ticks[1] - y_ticks[0]) / 2
            text_value = f'{data[y, x]:.2f}'

            # White outline buffer
            txt = plt.text(text_x, text_y, text_value, ha='center', va='center',
                           fontsize=8, color='red', path_effects=[
                    path_effects.Stroke(linewidth=2, foreground='white'),
                    path_effects.Normal()
                ])

    file_name = os.path.basename(cropped_file)
    name, date, time, _ = file_name.split('.')[:4]

    plt.title(f'Image Matrix Size: {height}x{width}\n{name} {date} {time}')
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    plt.show()


# %% Configuration
ftp_host = "hokusai.eorc.jaxa.jp"
username = "rainmap"
password = "Niskur+1404"
remote_dir = "/now/netcdf"  # Use netCDF directory
local_dir = "D:/OneDrive/00_WORKS/HII/test/GSMaP_Data"
year_range = range(2024, 2025)  # Modify as needed
month_range = range(9, 10)  # January only
day_range = range(10, 11)  # 1st January only
file_ext = ".nc"  # Change to .h5 for HDF5 files

# Step 1: Download GSMaP data
download_gsmap_data(ftp_host, username, password
                    , remote_dir, local_dir, year_range
                    , month_range, day_range, file_ext)
create_database_and_table() # Create DB in postgresql
# # # test function
# # Step 2: Crop the downloaded product
#input_nc = r"D:\OneDrive\00_WORKS\HII\test\GSMaP_Data\2024\09\10\gsmap_now_rain.20240910.2000.nc"
#output_npy = r"D:\OneDrive\00_WORKS\HII\test\GSMaP_Data\2024\09\10\gsmap_now_rain.20240910.2000.cropped.npy"
#crop_gsmap_data(input_nc, output_npy)
# # Step 3: Show the cropped product
#show_cropped_data(output_npy)

# %%
for year in year_range:
    for month in month_range:
        month_str = f"{month:02d}"
        for day in day_range:
            day_str = f"{day:02d}"
            path = f"D:/OneDrive/00_WORKS/HII/test/GSMaP_Data/{year}/{month_str}/{day_str}"
            files = os.listdir(path)

            for file in files:
                # Ensure only NetCDF files are processed
                if not file.endswith(".nc"):
                    print(f"Skipping non-NetCDF file: {file}")
                    continue

                    # Ensure path is correctly formatted
                input_path = os.path.normpath(os.path.join(path, file))

                name, date, time, _ = file.split('.')
                # =============================================================================
                #     input_name = input_name.split('.')
                #     name = input_name[0]
                #     date = input_name[1]
                #     time = input_name[2]
                # =============================================================================

                output_path = os.path.normpath(os.path.join(path, f'{name}.{date}.{time}.cropped.npy'))

                try:
                    # Step 2: Crop the downloaded product
                    crop_gsmap_data(input_path, output_path)

                    if os.path.exists(output_path):
                        # Step 3: Show the cropped product
                        #show_cropped_data(output_path)

                        # Step 4: Insert to PostgreSQL
                        # Step 4: Insert to PostgreSQL
                        cropped_array = np.load(output_path)
                        insert_rainfall_data(output_path, cropped_array, LAT_MIN, LAT_MAX, LON_MIN, LON_MAX)

                except Exception as e:
                    print(f"Error processing {input_path}: {e}")


