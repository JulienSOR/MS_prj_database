import requests
import json
import pandas as pd
# ส่งคำขอ GET ไปยัง API
url = "https://wea.hii.or.th:3005/api/v1/b2bdb8f8-0627-11f0-aabc-4d43e47cc2f5?username=ku_mae_sai_2025&password=fcdde146cc2bf1fbc4faf6119877aee2"
response = requests.get(url)

file = 'ดาวเทียมสสน/tele30.xlsx'
# เช็คว่า API ตอบกลับมาด้วยสถานะ 200 OK
if response.status_code == 200:
    with pd.ExcelWriter(file) as writer:
        # แปลงข้อมูลจาก JSON เป็น Python dictionary
        data = response.json()
        l_data = data['data']
        df = pd.DataFrame(l_data)
        df.to_excel(writer)
    #print(df)


    # ใช้ json.dumps() เพื่อแสดงข้อมูลในรูปแบบที่อ่านง่าย
    #print(json.dumps(data, indent=4))  # จะแสดงข้อมูลในรูปแบบที่สวยงาม
else:
    print("Failed to retrieve data. Status code:", response.status_code)
