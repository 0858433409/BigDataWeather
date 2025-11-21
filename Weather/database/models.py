from .db_connect import get_db_connection

def get_all_weather(cursor):
    # Lấy 100 dòng mới nhất để hiển thị demo
    cursor.execute("SELECT city, date_time, temperature, humidity, wind_speed FROM weather ORDER BY date_time DESC LIMIT 100")
    return cursor.fetchall()

def delete_all_weather(cursor):
    cursor.execute("TRUNCATE TABLE weather")

def search_weather(cursor, city):
    city_term = f"%{city}%"
    sql = "SELECT city, date_time, temperature, humidity, wind_speed FROM weather WHERE city LIKE %s ORDER BY date_time DESC LIMIT 100"
    cursor.execute(sql, (city_term,))
    return cursor.fetchall()

# Giữ mấy hàm cũ tên dummy để tránh lỗi import app.py (nếu lười sửa app.py nhiều)
def get_all_songs(c): return []
def delete_all_songs(c): pass
def search_songs(c, q): return []