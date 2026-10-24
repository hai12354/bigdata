import os
import sys
import json
import re
import requests
import urllib3
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, from_json, lit
from pyspark.sql.types import StringType, StructType, StructField, LongType, TimestampType

# 1. Tắt cảnh báo InsecureRequestWarning khi verify=False
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 2. Loại bỏ biến môi trường CA Bundle bị lỗi trên Windows
os.environ.pop("REQUESTS_CA_BUNDLE", None)
os.environ.pop("CURL_CA_BUNDLE", None)

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] = os.environ["HADOOP_HOME"] + r"\bin;" + os.environ.get("PATH", "")

load_dotenv()

GROQ_KEY = os.getenv("GROQ_API_KEY", "")
GEMINI_KEY = os.getenv("GEMINI_API_KEY", "")

def analyze_sentiment_ai(text_content, groq_key, gemini_key):
    if not text_content or not str(text_content).strip():
        return "Trung lập"
        
    text_str = str(text_content).strip()[:150]
    prompt = (
        "Classify the sentiment of the following news post into exactly ONE word: "
        "'Tích cực', 'Tiêu cực', or 'Trung lập'. "
        "Do not write anything else.\n"
        f"Text: {text_str}"
    )

    # 1. Gọi Groq REST API (Giữ nguyên model openai/gpt-oss-20b)
    if groq_key and len(groq_key) > 5:
        try:
            url = "https://api.groq.com/openai/v1/chat/completions"
            headers = {
                "Authorization": f"Bearer {groq_key}",
                "Content-Type": "application/json; charset=utf-8"
            }
            payload = {
                "model": "openai/gpt-oss-20b",
                "messages": [
                    {"role": "system", "content": "You are a sentiment analyzer. Answer only with one word: Tích cực, Tiêu cực, or Trung lập."},
                    {"role": "user", "content": prompt}
                ],
                "temperature": 0.0
            }
            res = requests.post(
                url, 
                headers=headers, 
                data=json.dumps(payload, ensure_ascii=False).encode('utf-8'), 
                timeout=2.5,
                verify=False
            )
            
            if res.status_code == 200:
                raw_text = res.json()['choices'][0]['message']['content'].strip()
                clean_text = raw_text.lower()
                
                # In log an toàn chống lỗi charmap bằng sys.stdout.buffer
                try:
                    sys.stdout.buffer.write(f"[AI GROQ RAW]: {raw_text}\n".encode('utf-8'))
                    sys.stdout.buffer.flush()
                except Exception:
                    print(f"[AI GROQ RAW]: {raw_text.encode('ascii', 'ignore').decode('ascii')}")
                
                if any(kw in clean_text for kw in ["tích cực", "positive", "pos"]):
                    return "Tích cực"
                elif any(kw in clean_text for kw in ["tiêu cực", "negative", "neg"]):
                    return "Tiêu cực"
                elif any(kw in clean_text for kw in ["trung lập", "neutral", "neu"]):
                    return "Trung lập"
            else:
                print(f"[GROQ HTTP ERROR]: {res.status_code}")
        except Exception as e:
            print(f"[GROQ EXCEPTION]: {e}")

    # 2. Fallback Gemini REST API nếu Groq lỗi
    if gemini_key and len(gemini_key) > 5:
        try:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={gemini_key}"
            payload = {"contents": [{"parts": [{"text": prompt}]}]}
            res = requests.post(url, json=payload, timeout=2.5, verify=False)
            if res.status_code == 200:
                raw_text = res.json()['candidates'][0]['content']['parts'][0]['text'].strip()
                clean_text = raw_text.lower()
                
                try:
                    sys.stdout.buffer.write(f"[AI GEMINI RAW]: {raw_text}\n".encode('utf-8'))
                    sys.stdout.buffer.flush()
                except Exception:
                    pass
                
                if any(kw in clean_text for kw in ["tích cực", "positive"]):
                    return "Tích cực"
                elif any(kw in clean_text for kw in ["tiêu cực", "negative"]):
                    return "Tiêu cực"
                elif any(kw in clean_text for kw in ["trung lập", "neutral"]):
                    return "Trung lập"
        except Exception:
            pass

    return "Trung lập"

ai_sentiment_udf = udf(analyze_sentiment_ai, StringType())

spark = SparkSession.builder \
    .appName("SparkKafkaStreamingPostgresAI") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0,org.postgresql:postgresql:42.5.4") \
    .config("spark.sql.session.timeZone", "Asia/Ho_Chi_Minh") \
    .config("spark.driver.extraJavaOptions", "-Duser.timezone=Asia/Ho_Chi_Minh") \
    .config("spark.executor.extraJavaOptions", "-Duser.timezone=Asia/Ho_Chi_Minh") \
    .config("spark.network.timeout", "300s") \
    .config("spark.executor.heartbeatInterval", "60s") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

json_schema = StructType([
    StructField("author", StringType()),
    StructField("news_title", StringType()),
    StructField("news_content", StringType()),
    StructField("timestamp", LongType())
])

kafka_raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "bigdata_topic") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", 2) \
    .option("failOnDataLoss", "false") \
    .load()

parsed_df = kafka_raw_df \
    .selectExpr("CAST(value AS STRING) as json_str") \
    .select(from_json(col("json_str"), json_schema).alias("data")) \
    .select("data.*")

enriched_df = parsed_df \
    .withColumn("created_at", col("timestamp").cast(TimestampType())) \
    .drop("timestamp") \
    .withColumn("sentiment", ai_sentiment_udf(col("news_content"), lit(GROQ_KEY), lit(GEMINI_KEY)))

def write_to_postgres(df, epoch_id):
    if df.rdd.isEmpty():
        return
        
    jdbc_url = "jdbc:postgresql://localhost:5432/bigdata_db?stringtype=unspecified"
    try:
        df.write \
          .format("jdbc") \
          .option("url", jdbc_url) \
          .option("dbtable", "news_analytics") \
          .option("user", "postgres") \
          .option("password", "postgrespassword") \
          .option("driver", "org.postgresql.Driver") \
          .mode("append") \
          .save()
        print(f"[SPARK BATCH SUCCESS] Batch ID: {epoch_id} | Đã ghi dữ liệu vào PostgreSQL!")
    except Exception as e:
        print(f"[LỖI GHI DB] Batch ID {epoch_id}: {e}")

checkpoint_path = r"D:\bigData\checkpoints\news_analytics"

query = enriched_df.writeStream \
    .option("checkpointLocation", checkpoint_path) \
    .trigger(processingTime='2 seconds') \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

print("[SPARK STREAMING] Đang lắng nghe Kafka và đẩy dữ liệu vào PostgreSQL real-time...")
query.awaitTermination()