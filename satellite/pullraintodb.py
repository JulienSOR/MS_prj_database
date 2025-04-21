from now_MainClass import SatelliteRain_now
import calendar

# 1. input data
# Site Coordinates (WGS84)
LAT_MAX, LAT_MIN = 20.70, 20.00
LON_MAX, LON_MIN = 100.00, 99.30

# name of satellite
name_sat = "now"

# time
years = [2023]
#months = list(range(4,5,1))
months = list(range(10,13,1))
hours = [f"{i:02}" for i in range(0,24,1)]
#hours = [f"{i:02}" for i in range(1,2,1)]
minutes = ["00","30"]

# 2. run
rain = SatelliteRain_now(LAT_MAX, LAT_MIN, LON_MAX, LON_MIN, name_sat)
for year in years:
    for month in months:
        num_days = calendar.monthrange(year, month)  # ได้จำนวนวันของเดือนนั้น
        for day in range(1, num_days[1]+1): # ตามจำนวนวัน
        #for day in range(1, 20):
            for hour in hours:
                for minute in minutes:
                    day_str = f"{day:02d}"
                    month_str = f"{month:02d}"
                    year_str = str(year)
                    file_name = rain.setnamefile_bytime(year_str, month_str, day_str, hour, minute)
                    try:
                        dataset = rain.download_gsmap_data(file_name)
                        if dataset:
                            cropped = rain.crop_gsmap_data(dataset, file_name)
                            rain.insert_to_db(cropped, file_name)
                        else:
                            print(f"❌ ไม่พบไฟล์: {file_name}")
                    except Exception as e:
                        print(f"⚠️ ดึงข้อมูล {file_name} ไม่สำเร็จ: {e}")
                    print("#" * 80)