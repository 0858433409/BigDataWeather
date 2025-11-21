from flask import Flask, request, render_template, jsonify, flash, redirect, url_for
from concurrent.futures import ThreadPoolExecutor
import json

# Import các module của dự án
from crawler.weather_crawler import crawl_weather_history
from store.weather_store import save_weather_to_db, save_weather_to_csv
from database.db_connect import get_db_connection
from database.models import get_all_weather, delete_all_weather

app = Flask(__name__)
app.secret_key = 'weather_secret_key_pro'  # Key bảo mật cho flash message

# Tạo pool xử lý đa luồng (để web không bị treo khi crawl)
executor = ThreadPoolExecutor(max_workers=2)

# Biến toàn cục lưu trạng thái crawl
crawl_status = {
    "is_running": False,
    "message": "Hệ thống sẵn sàng",
    "count": 0
}

def background_task(city_name):
    """Hàm chạy ngầm để crawl dữ liệu"""
    global crawl_status
    crawl_status["is_running"] = True
    crawl_status["message"] = f"Đang kết nối vệ tinh để tải dữ liệu: {city_name}..."

    try:
        # 1. Gọi Crawler
        data = crawl_weather_history(city_name)

        if data:
            # 2. Lưu vào Data Lake (CSV)
            save_weather_to_csv(data)

            # 3. Lưu vào Database
            save_weather_to_db(data)

            crawl_status["message"] = f"Thành công! Đã nhập kho {len(data)} bản ghi cho {city_name}."
            crawl_status["count"] = len(data)
        else:
            crawl_status["message"] = f"Không tìm thấy dữ liệu cho thành phố: {city_name}"

    except Exception as e:
        crawl_status["message"] = f"Lỗi hệ thống: {str(e)}"
    finally:
        crawl_status["is_running"] = False

@app.route("/", methods=["GET", "POST"])
def index():
    """Hàm xử lý trang chủ (View Function)"""
    global crawl_status

    # --- XỬ LÝ POST (Khi người dùng bấm nút) ---
    if request.method == "POST":
        action = request.form.get("action")

        if action == "crawl":
            # Lấy tên thành phố từ ô nhập liệu
            city_input = request.form.get("city_input", "").strip()

            if city_input:
                if not crawl_status["is_running"]:
                    # Đẩy việc crawl ra chạy ngầm
                    executor.submit(background_task, city_input)
                    flash(f"Đang khởi động quy trình thu thập cho: {city_input}...", "info")
                else:
                    flash("Hệ thống đang bận xử lý tác vụ khác. Vui lòng chờ!", "warning")
            else:
                flash("Vui lòng nhập tên thành phố!", "error")

            return redirect(url_for('index'))

        elif action == "delete":
            # Xóa dữ liệu
            try:
                conn = get_db_connection()
                delete_all_weather(conn.cursor())
                conn.commit()
                conn.close()
                flash("Đã xóa sạch kho dữ liệu (Reset Database).", "success")
            except Exception as e:
                flash(f"Lỗi khi xóa: {e}", "error")
            return redirect(url_for('index'))

    # --- XỬ LÝ GET (Hiển thị dữ liệu & Biểu đồ) ---
    conn = get_db_connection()
    if not conn:
        return "Lỗi kết nối Database! Hãy kiểm tra XAMPP/MySQL."

    cursor = conn.cursor()

    # 1. Lấy dữ liệu bảng (100 dòng mới nhất)
    weather_data = get_all_weather(cursor)

    # 2. Lấy dữ liệu vẽ biểu đồ (Lấy 500 mốc thời gian gần nhất)
    # Query này lấy 2 cột: [0]date_time và [1]temperature
    cursor.execute("SELECT date_time, temperature FROM weather ORDER BY date_time DESC LIMIT 500")
    chart_rows = cursor.fetchall()
    conn.close()

    # Xử lý dữ liệu cho Chart.js
    # Đảo ngược list để biểu đồ chạy từ trái qua phải (Quá khứ -> Hiện tại)
    chart_rows = list(chart_rows)  # Chuyển tuple sang list
    chart_rows.reverse()

    # --- ĐOẠN SỬA LỖI QUAN TRỌNG ---
    # row[0] là cột date_time
    # row[1] là cột temperature
    labels = [str(row[0]) for row in chart_rows]  # Trục X: Thời gian (Sửa thành index 0)
    values = [row[1] for row in chart_rows]       # Trục Y: Nhiệt độ (Sửa thành index 1)

    return render_template(
        "index.html",
        weather=weather_data,
        status=crawl_status,
        # Chuyển list Python sang chuỗi JSON để JavaScript đọc được
        chart_labels=json.dumps(labels),
        chart_values=json.dumps(values)
    )

@app.route("/status")
def status():
    """API trả về trạng thái hiện tại (cho AJAX gọi)"""
    return jsonify(crawl_status)

if __name__ == "__main__":
    app.run(debug=True)