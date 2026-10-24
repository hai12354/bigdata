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
    # Tự động nhận diện host Kafka (trong Docker hay ngoài máy thật)
    try:
        producer = KafkaProducer(
            bootstrap_servers=['kafka:29092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks=1,
            retries=2
        )
    except Exception:
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            acks=1,
            retries=2
        )
    
    SLEEP_TIME = 2
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