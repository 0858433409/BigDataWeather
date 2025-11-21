import csv
import os
from datetime import datetime
from database.db_connect import get_db_connection


def save_weather_to_csv(weather_data):
    folder = "datalake"
    if not os.path.exists(folder):
        os.makedirs(folder)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{folder}/weather_batch_{timestamp}.csv"

    try:
        with open(filename, mode='w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['City', 'Time', 'Temperature', 'Humidity', 'Wind Speed'])
            writer.writerows(weather_data)
        print(f"✅ [Data Lake] Đã lưu CSV: {filename}")
        return filename
    except Exception as e:
        print(f"❌ Lỗi lưu CSV: {e}")
        return "Error"


def save_weather_to_db(weather_data):
    conn = get_db_connection()
    if not conn: return

    cursor = conn.cursor()
    query = """
        INSERT INTO weather (city, date_time, temperature, humidity, wind_speed)
        VALUES (%s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        temperature=VALUES(temperature), humidity=VALUES(humidity)
    """

    try:
        # Batch insert (Insert 1000 dòng một lúc cho nhanh)
        batch_size = 1000
        for i in range(0, len(weather_data), batch_size):
            batch = weather_data[i:i + batch_size]
            cursor.executemany(query, batch)
            conn.commit()

        print(f"✅ [Database] Đã lưu {len(weather_data)} dòng vào DB.")
    except Exception as e:
        print(f"❌ Lỗi lưu DB: {e}")
    finally:
        cursor.close()
        conn.close()