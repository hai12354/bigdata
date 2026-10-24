import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import time

st.set_page_config(
    page_title="Big Data News Analytics",
    page_icon="📊",
    layout="wide"
)

@st.cache_resource
def init_connection():
    DATABASE_URL = "postgresql://postgres:postgrespassword@localhost:5432/bigdata_db"
    return create_engine(DATABASE_URL, pool_size=10, max_overflow=10, pool_pre_ping=True)

engine = init_connection()

def load_data():
    query = """
    SELECT author, news_title, news_content, sentiment, created_at 
    FROM news_analytics 
    ORDER BY created_at DESC 
    LIMIT 20;
    """
    try:
        with engine.connect() as conn:
            df = pd.read_sql_query(query, conn)
            
            # Chuẩn hóa dữ liệu sentiment để loại bỏ khoảng trắng ẩn/xuống dòng
            if not df.empty and 'sentiment' in df.columns:
                df['sentiment'] = df['sentiment'].astype(str).str.strip()
            return df
    except Exception:
        return pd.DataFrame()

st.title("📊 BẢNG ĐIỀU KHIỂN DỮ LIỆU TIN TỨC REAL-TIME & AI ANALYTICS")
st.caption("⚡ Dữ liệu real-time từ Kafka -> PySpark Streaming -> PostgreSQL")
st.markdown("---")

# Tần suất tự động làm mới 2 giây
@st.fragment(run_every=2)
def render_dashboard():
    df = load_data()
    current_time = time.strftime("%H:%M:%S")
    st.markdown(f"🕒 *Giao diện cập nhật thời gian thực lúc: **{current_time}***")

    if df.empty:
        st.info("Đang chờ dữ liệu mới từ Spark Streaming...")
    else:
        # Tách thành 4 cột metric để theo dõi toàn bộ trạng thái
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Tổng số tin (Top 20)", len(df))
            
        with col2:
            pos_count = len(df[df['sentiment'] == 'Tích cực'])
            st.metric("Tin Tích cực 🟢", pos_count)
            
        with col3:
            neg_count = len(df[df['sentiment'] == 'Tiêu cực'])
            st.metric("Tin Tiêu cực 🔴", neg_count)
            
        with col4:
            neu_count = len(df[df['sentiment'] == 'Trung lập'])
            st.metric("Tin Trung lập ⚪", neu_count)

        st.markdown("---")
        st.subheader("📰 Danh sách tin tức mới nhất")
        
        # Tô màu thẻ sentiment để dễ phân biệt trực quan
        def highlight_sentiment(val):
            if val == 'Tích cực':
                return 'background-color: #d4edda; color: #155724; font-weight: bold;'
            elif val == 'Tiêu cực':
                return 'background-color: #f8d7da; color: #721c24; font-weight: bold;'
            return 'background-color: #e2e3e5; color: #383d41;'

        st.dataframe(
            df[['author', 'news_title', 'news_content', 'sentiment', 'created_at']].style.applymap(
                highlight_sentiment, subset=['sentiment']
            ),
            use_container_width=True,
            hide_index=True
        )

render_dashboard()