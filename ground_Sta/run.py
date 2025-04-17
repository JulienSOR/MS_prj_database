from rain import TSRain

file = r"C:\ms\ground_Sta\RAIN\HII\HII_Rain_AIT003_60m.xlsx"
ts = TSRain()
ts.rHII(file)
ts.get_data()
