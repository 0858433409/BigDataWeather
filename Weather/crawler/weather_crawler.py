import requests
from datetime import datetime


def get_coordinates_global(city_name):
    """
    NÂNG CẤP: Tìm tọa độ bất kỳ thành phố nào trên thế giới
    """
    try:
        # API tìm kiếm địa danh miễn phí của Open-Meteo
        url = f"https://geocoding-api.open-meteo.com/v1/search?name={city_name}&count=1&language=en&format=json"
        res = requests.get(url).json()

        if "results" in res and len(res["results"]) > 0:
            data = res["results"][0]
            print(f"🌍 Tìm thấy: {data['name']}, {data['country']} ({data['latitude']}, {data['longitude']})")
            return data['latitude'], data['longitude'], data['name']
        else:
            return None, None, None
    except:
        return None, None, None


def crawl_weather_history(city_input):
    # 1. Tìm tọa độ trước
    lat, lon, real_name = get_coordinates_global(city_input)

    if not lat:
        print(f"❌ Không tìm thấy thành phố: {city_input}")
        return []

    print(f"--- 🌤️ CRAWL DATA: {real_name} ---")

    # 2. Lấy dữ liệu lịch sử
    start_date = "2020-01-01"
    end_date = datetime.now().strftime("%Y-%m-%d")

    url = f"https://archive-api.open-meteo.com/v1/archive?latitude={lat}&longitude={lon}&start_date={start_date}&end_date={end_date}&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"

    try:
        response = requests.get(url)
        data = response.json()

        hourly = data['hourly']
        times = hourly['time']
        temps = hourly['temperature_2m']
        humidities = hourly['relative_humidity_2m']
        winds = hourly['wind_speed_10m']

        weather_data = []

        for i in range(len(times)):
            t_str = times[i].replace("T", " ") + ":00"
            # Lưu tên chuẩn của thành phố (ví dụ nhập 'hanoi' -> lưu 'Hanoi')
            record = (real_name, t_str, temps[i], humidities[i], winds[i])
            weather_data.append(record)

        print(f"✅ Lấy thành công {len(weather_data)} dòng cho {real_name}.")
        return weather_data

    except Exception as e:
        print(f"❌ Lỗi API: {e}")
        return []