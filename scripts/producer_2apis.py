import json
import time
import requests
import random
from kafka import KafkaProducer
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def get_random_user():
    try:
        res = requests.get('https://randomuser.me/api/', timeout=3, verify=False).json()
        user = res['results'][0]
        return f"{user['name']['first']} {user['name']['last']}"
    except Exception:
        return "Nguoidung_AnDanh"

def get_latest_news():
    try:
        res = requests.get('https://dummyjson.com/posts', timeout=3, verify=False).json()
        posts = res.get('posts', [])
        if posts:
            random_post = random.choice(posts)
            return random_post.get('title', 'Tin tuc moi'), random_post.get('body', 'Noi dung chi tiet...')
        return "Cong nghe AI va Big Data", "Ung dung AI vao xu ly luong du lieu lon..."
    except Exception as e:
        return "Cong nghe AI va Big Data", "Ung dung AI vao xu ly luong du lieu lon..."

def produce_single_message(producer):
    author = get_random_user()
    title, content = get_latest_news()
    
    payload = {
        "author": author,
        "news_title": title,
        "news_content": content,
        "timestamp": int(time.time())
    }
    
    producer.send('bigdata_topic', payload)
    producer.flush()
    print(f"[KAFKA SUCCESS] {time.strftime('%H:%M:%S')} | Author: {author}")

def run_producer(continuous=True, max_messages=1):
    # Khởi tạo danh sách tất cả các Host Kafka có thể kết nối
    bootstrap_list = [
        'kafka1:29092', 'kafka2:29093', 'kafka3:29094',  # Tên host nội bộ Docker
        'kafka:29092',                                   # Fallback cho container 1 broker
        'localhost:9092', 'localhost:9093', 'localhost:9094' # Cho máy thật Windows
    ]
    
    producer = None
    for host in bootstrap_list:
        try:
            producer = KafkaProducer(
                bootstrap_servers=[host],
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                acks=1,
                retries=2,
                request_timeout_ms=3000
            )
            print(f"[PRODUCER KẾT NỐI THÀNH CÔNG THÔNG QUA]: {host}")
            break
        except Exception:
            continue

    if not producer:
        print("[PRODUCER LỖI] Không thể kết nối tới bất kỳ Kafka Broker nào!")
        return
    
    SLEEP_TIME = 3 # Thời gian nghỉ giữa các lần gửi message, tránh spam quá nhanh
    print(f"[PRODUCER START] Đang đẩy dữ liệu lên Kafka...")
    
    if continuous:
        while True:
            try:
                produce_single_message(producer)
            except Exception as e:
                print(f"[PRODUCER ERROR] {e}")
            time.sleep(SLEEP_TIME)
    else:
        for _ in range(max_messages):
            try:
                produce_single_message(producer)
            except Exception as e:
                print(f"[PRODUCER ERROR] {e}")
            time.sleep(SLEEP_TIME)

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == 'once':
        run_producer(continuous=False, max_messages=1)
    else:
        run_producer(continuous=True)