from now_MainClass import SatelliteRain_now_realtime

# 1. input data
# Site Coordinates (WGS84)
LAT_MAX, LAT_MIN = 20.70, 20.00
LON_MAX, LON_MIN = 100.00, 99.30

# name of satellite
name_sat = "now"

# 2. run
rain = SatelliteRain_now_realtime(LAT_MAX, LAT_MIN, LON_MAX, LON_MIN, name_sat)
try:
    dataset = rain.download_gsmap_data()
    cropped = rain.crop_gsmap_data(dataset)
    rain.insert_to_db(cropped)
    print('#'*100)
except FileNotFoundError as e:
    print(f"ดึงข้อมูลของไฟล์ {rain.file_name} ไม่สำเร็จ: {e}")
    print('#' * 100)
