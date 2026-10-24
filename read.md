# ==============================================================================
# QUY TRÌNH CHẠY HỆ THỐNG REAL-TIME BIG DATA PIPELINE TRÊN UBUNTU
# ==============================================================================

# --- BƯỚC 1: Clone dự án & Di chuyển vào thư mục ---
git clone https://github.com/hai12354/bigdata.git && cd bigdata

# --- BƯỚC 2: Tạo file .env chứa API Key ---
cat << 'EOF' > .env
GROQ_API_KEY=gsk_your_groq_api_key_here
GEMINI_KEY=your_gemini_api_key_here
EOF

# --- BƯỚC 3: Bật các dịch vụ Docker (Kafka, Postgres, Airflow) ---
docker compose up -d

# --- BƯỚC 4: Cài đặt gói venv và Java cho Ubuntu ---
sudo apt update && sudo apt install -y python3-venv python3.10-venv openjdk-11-jdk

# --- BƯỚC 5: Tạo môi trường ảo & Cài đặt thư viện Python ---
python3 -m venv venv && source venv/bin/activate && pip install --upgrade pip && pip install -r requirements.txt

# --- BƯỚC 6: SỬA LỖI HỆ THỐNG TRƯỚC KHI CHẠY (BẮT BUỘC) ---
# 6.1 Nâng cấp Streamlit lên bản mới nhất (Sửa lỗi: AttributeError: 'streamlit' has no attribute 'fragment')
pip install --upgrade streamlit

# 6.2 Sửa đường dẫn Checkpoint từ Windows (D:\...) sang Linux (/tmp/...) trong file PySpark
sed -i 's|D:\\\\bigData\\\\checkpoints\\\\news_analytics|/tmp/checkpoints/news_analytics|g' spark_jobs/spark_streaming_ai.py
sed -i 's|D:/bigData/checkpoints/news_analytics|/tmp/checkpoints/news_analytics|g' spark_jobs/spark_streaming_ai.py

# --- BƯỚC 7: Tạo thư mục Checkpoint, đặt SPARK_HOME & Khởi chạy PySpark Streaming (Terminal 1) ---
mkdir -p /tmp/checkpoints/news_analytics
export SPARK_HOME=$(pwd)/venv/lib/python3.10/site-packages/pyspark
export PATH=$SPARK_HOME/bin:$PATH
python3 spark_jobs/spark_streaming_ai.py

# ==============================================================================
# HƯỚNG DẪN KÍCH HOẠT PRODUCER, MỞ STREAMLIT UI & RESET HỆ THỐNG
# ==============================================================================

# --- BƯỚC 8: Mở Terminal 2 chạy Producer bơm dữ liệu vào Kafka ---
# (Lưu ý: Có thể dùng nút Trigger trên Airflow UI http://localhost:8081 thay vì chạy lệnh này)
cd ~/bigdata 
source venv/bin/activate  
python3 producer_2apis.py

# --- BƯỚC 9: Mở Terminal 3 chạy Streamlit Dashboard UI ---
cd ~/bigdata 
source venv/bin/activate  
streamlit run app/app.py

# --- BƯỚC 10: Lệnh Reset / Xóa sạch 100% dữ liệu để chạy lại từ đầu (khi cần) ---

 docker compose down -v   
 rm -rf /tmp/checkpoints/news_analytics/*