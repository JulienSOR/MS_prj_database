from floodmap import FloodMap

file = r"C:\ms\flood_map\S1A_IW_GRDH_1SDV_20241022_0616.json"

fm = FloodMap()
fm.flood1day(file)
#fm.get_data()