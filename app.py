"""
📊 LUMINA CAPITAL - 투자 성향 맞춤형 자산관리
================================
Streamlit 기반 대시보드 웹앱

페이지 구성:
  🏠 메인 대시보드  - 시장 개요 및 거래량 상위 종목
  📋 투자 성향 설문  - 11문항 기반 5단계 성향 분류
  ⭐ 맞춤 종목 추천  - 성향별 추천 종목 리스트 및 차트
  📰 종목 뉴스      - 추천 종목 관련 최신 뉴스
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import matplotlib.pyplot as plt
import matplotlib
import seaborn as sns
import os
import glob
from datetime import datetime

from analyzer import (
    SURVEY_QUESTIONS, classify_investor_type, score_stocks,
    get_top_recommendations, generate_analysis_summary,
    TYPE_DESCRIPTIONS, WEIGHT_PROFILES,
    generate_analysis_signals, generate_newsletter,
)

# ── 한글 폰트 설정 (matplotlib) ──
matplotlib.rcParams['font.family'] = 'Malgun Gothic'
matplotlib.rcParams['axes.unicode_minus'] = False

# ── 페이지 설정 ──
st.set_page_config(
    page_title="LUMINA CAPITAL | 당신을 위한 투자의 길잡이",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ============================================================
# 0. 세션 매니지먼트 (30분 자동 로그아웃)
# ============================================================
import time

# SESSION_TIMEOUT_SECONDS = 1800 # 30분 기능을 제거합니다.

# 세션 복구 및 상태 관리
if 'logged_in' not in st.session_state:
    st.session_state['logged_in'] = False
if 'username' not in st.session_state:
    st.session_state['username'] = ""
if 'current_page' not in st.session_state:
    st.session_state['current_page'] = "🏠 메인 대시보드"

# 자동 로그아웃 기능을 제거했습니다.

# ============================================================
# 1. 데이터 로드
# ============================================================
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
OUT_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'out_data')

def check_db_port(host="25.4.53.12", port=3306, timeout=1.5):
    """DB 서버 포트가 열려있는지 소켓으로 빠르게 확인합니다."""
    import socket
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False

def run_outbound_sync():
    """DB에서 로컬로 데이터를 동기화하는 outbound/run_all.py 스크립트를 실행합니다."""
    import subprocess
    import sys
    import os
    
    # 1. 포트 체크 먼저 수행 (속도 개선 핵심)
    if not check_db_port():
        st.warning("⚠️ DB 서버에 연결할 수 없어 로컬 데이터를 사용합니다.")
        return False

    script_path = os.path.join(os.path.dirname(__file__), 'outbound', 'run_all.py')
    if os.path.exists(script_path):
        try:
            # 동기화 시작 토스트 알림
            st.toast("🔄 DB 데이터를 로컬로 동기화 중입니다...", icon="🔃")
            result = subprocess.run([sys.executable, script_path], 
                                  capture_output=True, 
                                  text=True, 
                                  check=True)
            st.toast("✅ DB 동기화 완료!", icon="✨")
            return True
        except Exception as e:
            st.error(f"DB 동기화 중 오류 발생: {e}")
            return False
    return False

def run_full_system_sync():
    """웹 수집 -> DB 반영 -> 로컬 동기화의 전체 파이프라인을 실행합니다."""
    import subprocess
    import sys
    import os
    from scraper import run_full_pipeline

    try:
        # 단일 진행 바/상태창 사용
        with st.status("🚀 전체 시스템 데이터 동기화 시작...", expanded=True) as status:
            # 1. 웹 스크래핑 (2~4분 소요)
            st.write("1️⃣ 네이버 증권에서 최신 데이터 수집 중 (scraper.py)...")
            run_full_pipeline()
            
            # 2. DB 업로드 (C~G)
            st.write("2️⃣ 수집된 데이터를 DB에 반영 중 (database_script/)...")
            scripts = [
                'C_stocks_table.py', 'D_price_snapshots_table.py', 
                'E_analysis_signals.py', 'F_recommendations.py', 'G_newsletters.py',
                'H_stock_fundamentals.py', 'I_investor_trends.py'
            ]
            script_dir = os.path.join(os.path.dirname(__file__), 'database_script')
            for script_name in scripts:
                script_path = os.path.join(script_dir, script_name)
                if os.path.exists(script_path):
                    st.write(f"   -> {script_name} 실행 중...")
                    subprocess.run([sys.executable, script_path], check=True, capture_output=True)
            
            # 3. 로컬 JSON 동기화 (Outbound)
            st.write("3️⃣ DB에서 로컬 앱용 데이터 추출 중 (outbound/)...")
            run_outbound_sync()
            
            status.update(label="✅ 모든 데이터 동기화가 완료되었습니다!", state="complete")
            st.toast("✨ 시스템 전체 동기화 성공!", icon="🎊")
            return True
    except Exception as e:
        st.error(f"❌ 전체 동기화 중 오류 발생: {e}")
        return False

def ensure_data_exists():
    """
    데이터가 아예 없는 최초 구동 시에만 전체 파이프라인을 실행합니다.
    """
    # JSON 파일 존재 여부로 체크 (실제 앱이 쓰는 데이터)
    json_file = os.path.join(OUT_DATA_DIR, 'stocks_export.json')
    
    if not os.path.exists(json_file):
        with st.container():
            st.info("👋 처음 오셨군요! 앱 구동에 필요한 기초 데이터를 수집하고 동기화합니다.")
            if st.button("🚀 데이터 초기화 및 수집 시작"):
                run_full_system_sync()
                st.rerun()
            st.stop()

@st.cache_data(ttl=300)
def load_latest_data():
    """out_data/ 디렉토리에서 최종 백업된 JSON 데이터를 로드합니다."""
    import json
    import os
    
    out_dir = OUT_DATA_DIR
    
    stock_df = pd.DataFrame()
    signals_df = pd.DataFrame()
    news_df = pd.DataFrame()
    hist_df = pd.DataFrame()
    recs_df = pd.DataFrame()
    newsletters_df = pd.DataFrame()
    user_types_df = pd.DataFrame()

    # 1. 시세/거래량 JSON 로드 (C_export_stocks.py 결과물)
    stock_json_path = os.path.join(out_dir, 'stocks_export.json')
    if os.path.exists(stock_json_path):
        try:
            with open(stock_json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if 'stocks' in data:
                    stock_df = pd.DataFrame(data['stocks'])
                    st.session_state['data_file'] = "stocks_export.json"
        except Exception as e:
            print(f"Failed to load stocks JSON: {e}")

    # 2. 분석 시그널 JSON 로드 (E_export_analysis_signals.py 결과물)
    signal_json_path = os.path.join(out_dir, 'analysis_signals_export.json')
    if os.path.exists(signal_json_path):
        try:
            with open(signal_json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if 'analysis_signals' in data:
                    signals_df = pd.DataFrame(data['analysis_signals'])
        except Exception as e:
            print(f"Failed to load analysis signals JSON: {e}")

    # 3. 추천 종목 JSON 로드 (F_export_recommendations.py 결과물)
    recs_json_path = os.path.join(out_dir, 'recommendations_export.json')
    if os.path.exists(recs_json_path):
        try:
            with open(recs_json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if 'recommendations' in data:
                    recs_df = pd.DataFrame(data['recommendations'])
        except Exception as e:
            print(f"Failed to load recommendations JSON: {e}")

    # 4. 뉴스레터 JSON 로드 (G_export_newsletters.py 결과물)
    newsletters_json_path = os.path.join(out_dir, 'newsletters_export.json')
    if os.path.exists(newsletters_json_path):
        try:
            with open(newsletters_json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if 'newsletters' in data:
                    newsletters_df = pd.DataFrame(data['newsletters'])
        except Exception as e:
            print(f"Failed to load newsletters JSON: {e}")

    # 5. 사용자 성향 정보 로드 (B_export_user_type.py 결과물)
    user_type_json_path = os.path.join(out_dir, 'user_type_export.json')
    if os.path.exists(user_type_json_path):
        try:
            with open(user_type_json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if 'user_type' in data:
                    user_types_df = pd.DataFrame(data['user_type'])
        except Exception as e:
            print(f"Failed to load user_type JSON: {e}")

    # 6. 뉴스 및 과거 시세 (기존 CSV 백업 방식 유지)
    import glob
    news_files = sorted(glob.glob(os.path.join(DATA_DIR, 'stock_news_*.csv')))
    hist_files = sorted(glob.glob(os.path.join(DATA_DIR, 'historical_*.csv')))
    if news_files:
        news_df = pd.read_csv(news_files[-1])
    if hist_files:
        hist_df = pd.read_csv(hist_files[-1])

    # signals가 없으면 실시간 생성 (Fallback)
    if signals_df.empty and not stock_df.empty:
        signals_df = generate_analysis_signals(stock_df, '1D')

    # 텍스트 컬럼에 "None", "NONE", "N/A" 등이 포함된 행 자체를 완전히 삭제 (발표용 요구사항)
    for df_name, df_tmp in {'stock': stock_df, 'signals': signals_df, 'recs': recs_df, 'newsletters': newsletters_df}.items():
        if not df_tmp.empty:
            for col in df_tmp.select_dtypes(include=['object']):
                df_tmp[col] = df_tmp[col].replace(['None', 'NONE', 'N/A', 'NaN', 'nan', ''], pd.NA)
            df_tmp.dropna(inplace=True)

    return stock_df, news_df, hist_df, signals_df, recs_df, newsletters_df, user_types_df


# ============================================================
# CSS 스타일
# ============================================================
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@300;400;500;700&display=swap');

    /* 전체 배경 — 부드럽고 고급스러운 베이지/크림 다크톤 */
    .stApp {
        background: linear-gradient(160deg, #2b2622 0%, #302b28 50%, #26221f 100%);
        font-family: 'Noto Sans KR', sans-serif;
    }

    /* 상단 헤더 (Deploy 창 등) 투명 및 아이콘 색상 변경 */
    [data-testid="stHeader"] {
        background-color: transparent !important;
    }
    [data-testid="stHeader"] * {
        color: #a89f91 !important;
    }

    /* 사이드바 */
    [data-testid="stSidebar"] {
        background: rgba(38, 34, 31, 0.98);
        border-right: 1px solid rgba(220, 185, 140, 0.15);
    }

    /* 메트릭 카드 및 내부 텍스트 */
    [data-testid="stMetric"] {
        background: rgba(55, 50, 46, 0.7);
        border: 1px solid rgba(220, 185, 140, 0.25);
        border-radius: 12px;
        padding: 16px;
        box-shadow: 0 4px 15px rgba(0, 0, 0, 0.1);
    }
    [data-testid="stMetricValue"] > div {
        color: #f2ece4 !important;
        font-weight: 700 !important;
    }
    [data-testid="stMetricLabel"] > div {
        color: #dcb98c !important;
        font-weight: 600 !important;
    }
    /* 한국 시장 metric delta: 상승=빨강, 하락=파랑 덮어씌움 (color만 변경, fill은 건드리지 않아 화살표 유지) */
    /* 하락(Down) → 파랑 */
    [data-testid="stMetricDelta"]:has([data-testid="stMetricDeltaIcon-Down"]),
    [data-testid="stMetricDelta"]:has([data-testid="stMetricDeltaIcon-Down"]) * {
        color: #3b82f6 !important;
    }
    /* 상승(Up) → 빨강 */
    [data-testid="stMetricDelta"]:has([data-testid="stMetricDeltaIcon-Up"]),
    [data-testid="stMetricDelta"]:has([data-testid="stMetricDeltaIcon-Up"]) * {
        color: #f85149 !important;
    }

    /* 헤더 */
    h1 {
        color: #dcb98c !important;
        font-weight: 800 !important;
        font-family: 'Noto Sans KR', sans-serif !important;
    }

    h2, h3 {
        color: #f2ece4 !important;
        font-weight: 600 !important;
    }

    /* 팝업(모달/다이얼로그) 타이틀 색상 보정 (흰 배경일 때 보이게끔 검정색 적용) */
    div[role="dialog"] h2 {
        color: #000000 !important;
    }

    /* 성향 결과 카드 */
    .investor-card {
        background: rgba(55, 50, 46, 0.85);
        border: 1px solid rgba(220, 185, 140, 0.3);
        border-radius: 16px;
        padding: 24px;
        margin: 16px 0;
        box-shadow: 0 6px 20px rgba(0, 0, 0, 0.15);
    }

    .investor-card h2 {
        margin: 0 0 12px 0;
        font-size: 28px;
        color: #dcb98c !important;
    }

    .investor-card p {
        color: #e5dac9;
        line-height: 1.7;
    }

    /* 종목 카드 */
    .stock-card {
        background: rgba(50, 45, 41, 0.8);
        border: 1px solid rgba(220, 185, 140, 0.2);
        border-radius: 12px;
        padding: 16px;
        margin: 8px 0;
        transition: all 0.2s ease-in-out;
    }

    .stock-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 25px rgba(220, 185, 140, 0.15);
        border-color: rgba(220, 185, 140, 0.5);
    }

    /* 점수 배지 */
    .score-badge {
        display: inline-block;
        background: linear-gradient(135deg, #a67c52, #c19b76);
        color: white;
        padding: 4px 12px;
        border-radius: 20px;
        font-weight: 700;
        font-size: 14px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.2);
    }

    /* 탭 */
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }

    .stTabs [data-baseweb="tab"] {
        background: rgba(55, 50, 46, 0.7);
        border-radius: 8px;
        color: #a89f91;
        padding: 8px 24px;
        font-weight: 500;
    }
    /*  탭 하단의 빨간색 강조 선(인디케이터) 제거 */
    [data-baseweb="tab-highlight"] {
        display: none !important;
    }

    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #a67c52, #c19b76) !important;
        color: white !important;
    }

    /* 위젯 라벨 (셀렉트박스, 라디오, 체크박스 등) */
    .stSelectbox label, .stRadio label, .stMultiSelect label, 
    .stNumberInput label, .stTextInput label, .stSlider label {
        color: #dcb98c !important;
        font-weight: 600 !important;
    }

    /* 일반 텍스트 — 밝게 유지 */
    p, span, li {
        color: #f0e8dc !important;
    }

    /* Streamlit markdown 본문 */
    .stMarkdown p, .stMarkdown span {
        color: #f0e8dc !important;
    }

    /* 입력 위젯 내부 값 텍스트 */
    input, textarea {
        color: #f0e8dc !important;
        background-color: rgba(55, 50, 46, 0.9) !important;
    }

    /* 라디오/체크박스 옵션 글씨 */
    .stRadio div[role="radiogroup"] label p,
    .stCheckbox label p {
        color: #f0e8dc !important;
        font-size: 14px;
    }
      
    /* 드롭다운이 펼쳐졌을 때 각 항목의 글자색 변경 */
    div[data-baseweb="popover"] li {
        color: #000000 !important; /* 글자색을 검정으로 강제 */
        background-color: transparent !important;
    }
    /* 2. 이미 선택되어 박스에 표시되는 글자색 (가독성 확보) */
    div[data-baseweb="select"] > div:first-child {
        color: #ffffff !important; /* 이 부분은 배경이 어두우면 흰색, 밝으면 검정으로 조절하세요 */
    }
        

    /* 드롭다운 (셀렉트박스) 내부 텍스트 및 팝업창 스타일 (신규 Streamlit UI 대응 포함) */
    .stSelectbox div[data-baseweb="select"] > div {
        background-color: rgba(55, 50, 46, 0.9) !important;
        color: #f0e8dc !important;
    }
    
    /* 기존 listbox 타겟팅 유지 */
    div[role="listbox"] {
        background-color: #302b28 !important;
        border: 1px solid rgba(220, 185, 140, 0.3) !important;
        border-radius: 8px !important;
    }
    
    div[role="listbox"] ul {
        background-color: #302b28 !important;
    }
    
    div[role="listbox"] ul li {
        color: #f0e8dc !important;
        background-color: transparent !important;
    }
    
    div[role="listbox"] ul li:hover {
        background-color: rgba(220, 185, 140, 0.2) !important;
        color: #dcb98c !important;
    }

    /* 최신 버전을 위한 Popover 타겟팅 (배경색 강제) */
    div[data-baseweb="popover"] > div {
        background-color: #302b28 !important;
    }
    div[data-baseweb="popover"] ul {
        background-color: #302b28 !important;
    }
    
    div[data-baseweb="popover"] ul li, div[data-baseweb="popover"] span {
        color: #f0e8dc !important;
        background-color: transparent !important;
    }
    
    div[data-baseweb="popover"] ul li:hover {
        background-color: rgba(220, 185, 140, 0.2) !important;
        color: #dcb98c !important;
    }

    /* 데이터프레임 / 테이블 */
    table {
        color: #f0e8dc !important;
    }
    
    th, td {
        border-bottom: 1px solid rgba(220, 185, 140, 0.2) !important;
        color: #f0e8dc !important;
    }
    
    th {
        color: #e8c87e !important;
        font-weight: 700 !important;
        background-color: rgba(55, 50, 46, 0.7) !important;
    }

    /* 추천 이유 테이블 셀 */
    .reason-table {
        width: 100%;
        border-collapse: collapse;
        font-size: 14px;
    }
    .reason-table th {
        background: rgba(166, 124, 82, 0.3) !important;
        color: #e8c87e !important;
        padding: 10px 12px;
        text-align: left;
    }
    .reason-table td {
        padding: 9px 12px;
        color: #f0e8dc !important;
        border-bottom: 1px solid rgba(220,185,140,0.15) !important;
    }
    .reason-table tr:hover td {
        background: rgba(220, 185, 140, 0.08);
    }

    /* 버튼 및 폼 제출 버튼 */
    .stButton > button, [data-testid="stFormSubmitButton"] > button {
        background: linear-gradient(135deg, #a67c52, #c19b76) !important;
        color: white !important;
        border: none !important;
        border-radius: 8px !important;
        font-weight: 600 !important;
        box-shadow: 0 4px 10px rgba(0,0,0,0.2) !important;
    }
    .stButton > button:hover, [data-testid="stFormSubmitButton"] > button:hover {
        background: linear-gradient(135deg, #c19b76, #dcb98c) !important;
        box-shadow: 0 4px 15px rgba(220, 185, 140, 0.3) !important;
        color: white !important;
    }

    /* 익스팬더 (펼쳐보기) 스타일 수정 — 열렸을 때도 다크 유지 */
    [data-testid="stExpander"] {
        background-color: rgba(55, 50, 46, 0.7) !important;
        border: 1px solid rgba(220, 185, 140, 0.25) !important;
        border-radius: 12px !important;
    }
    [data-testid="stExpander"] > details {
        background-color: rgba(55, 50, 46, 0.7) !important;
    }
    [data-testid="stExpander"] > details > div {
        background-color: rgba(50, 45, 41, 0.9) !important;
        border-radius: 0 0 12px 12px !important;
    }
    [data-testid="stExpander"] summary {
        color: #dcb98c !important;
        font-weight: 600 !important;
        background-color: rgba(55, 50, 46, 0.7) !important;
    }
    [data-testid="stExpander"] summary:hover {
        color: #f2ece4 !important;
    }
    /* streamlit 버전별 펼쳐진 내용 영역 */
    .streamlit-expanderContent {
        background-color: rgba(50, 45, 41, 0.95) !important;
        border-radius: 0 0 12px 12px !important;
    }
    .streamlit-expanderContent p,
    .streamlit-expanderContent span,
    .streamlit-expanderContent td,
    .streamlit-expanderContent th {
        color: #f0e8dc !important;
    }

    /* 경고/정보/성공/에러 박스 텍스트 가독성 */
    [data-testid="stAlert"] {
        background: rgba(55, 50, 46, 0.9) !important;
    }
    [data-testid="stAlert"] p, [data-testid="stAlert"] span, [data-testid="stAlert"] div {
        color: #f0e8dc !important;
    }
</style>
""", unsafe_allow_html=True)


# ============================================================
# 🔐 사용자 인증 시스템 (Phase 10)
# ============================================================
import json
import bcrypt as _bcrypt  # passlib 대신 raw bcrypt 사용 (backend 호환 문제 해결)
import os

USERS_DB_FILE = os.path.join(DATA_DIR, 'users_db.csv')
USER_TYPE_DB_FILE = os.path.join(DATA_DIR, 'user_type_db.csv')

def init_user_type_table():
    pass # 파일 기반 관리로 변경되었으므로 별도의 초기화 불필요

def load_users():
    import json
    
    # JSON 확인 부분을 제거하고 로컬 CSV (작업본)만 확인

    if os.path.exists(USERS_DB_FILE):
        try:
            df = pd.read_csv(USERS_DB_FILE)
            fallback_dict = {}
            for _, row in df.iterrows():
                fallback_dict[str(row["user_id"])] = {
                    "user_password": str(row.get("user_password", "")),
                    "user_email": str(row.get("user_email", ""))
                }
            return fallback_dict
        except Exception as e:
            pass
    return {}

def save_users(users_dict):
    new_users = []
    for uid, udata in users_dict.items():
        new_users.append({
            "user_id": uid,
            "user_password": udata.get("user_password", ""),
            "user_email": udata.get("user_email", "")
        })
    df = pd.DataFrame(new_users)
    df.to_csv(USERS_DB_FILE, index=False, encoding='utf-8-sig')

def save_user_profile(user_id, type_id, user_check=0):
    try:
        if os.path.exists(USER_TYPE_DB_FILE):
            df = pd.read_csv(USER_TYPE_DB_FILE)
            user_type_list = df.to_dict('records')
        else:
            user_type_list = []
    except:
        user_type_list = []
        
    type_names = {1: "안정형", 2: "안정추구형", 3: "위험중립형", 4: "적극투자형", 5: "공격투자형"}
    found = False
    for ut in user_type_list:
        if str(ut.get("user_id")) == str(user_id):
            ut["type_id"] = type_id
            ut["type_name"] = type_names.get(type_id, "Unknown Profile")
            ut["description"] = f"User has been profiled as {ut['type_name']}."
            ut["user_check"] = user_check
            found = True
            break
            
    if not found:
        user_type_list.append({
            "user_id": user_id,
            "type_id": type_id,
            "type_name": type_names.get(type_id, "Unknown Profile"),
            "description": f"User has been profiled as {type_names.get(type_id, 'Unknown Profile')}.",
            "user_check": user_check
        })
        
    df = pd.DataFrame(user_type_list)
    df.to_csv(USER_TYPE_DB_FILE, index=False, encoding='utf-8-sig')

if 'user_type_init' not in st.session_state:
    init_user_type_table()
    st.session_state['user_type_init'] = True

# bcrypt는 최대 72바이트 제한 → raw bcrypt로 안전하게 처리
def _safe_hash(password: str) -> str:
    pw_bytes = password.encode('utf-8')[:72]
    return _bcrypt.hashpw(pw_bytes, _bcrypt.gensalt()).decode('utf-8')

def _safe_verify(password: str, hashed: str) -> bool:
    pw_bytes = password.encode('utf-8')[:72]
    return _bcrypt.checkpw(pw_bytes, hashed.encode('utf-8'))

# ============================================================
# 사이드바 네비게이션 & 로그인 폼
# ============================================================
with st.sidebar:
    # ── 로고 이미지 삽입 ──
    logo_path = os.path.join(os.path.dirname(__file__), 'assets', 'logo.jpg')
    if os.path.exists(logo_path):
        import base64
        with open(logo_path, "rb") as image_file:
            encoded_string = base64.b64encode(image_file.read()).decode()
        # 마크만 동그랗게 자르고(누끼) 크기 축소 + 확대(클릭) 방지 HTML 구성
        st.markdown(
            f'''
            <div style="text-align: center; margin-top: 10px; margin-bottom: 20px;">
                <img src="data:image/jpeg;base64,{encoded_string}" 
                     style="width: 140px; height: 140px; border-radius: 50%; object-fit: cover; 
                            box-shadow: 0 4px 15px rgba(220,185,140,0.2); pointer-events: none;">
                <h2 style="color: #dcb98c; margin-top: 15px; margin-bottom: 5px; font-weight: 800; font-size: 22px; letter-spacing: 1px;">LUMINA CAPITAL</h2>
                <p style="color: #a89f91; font-size: 13px; margin: 0; font-weight: 500; letter-spacing: 0.5px;">당신을 위한 투자의 길잡이</p>
            </div>
            ''', 
            unsafe_allow_html=True
        )
    else:
        st.markdown("## 📊 LUMINA CAPITAL")
    st.markdown("---")
    
    # 로그인 폼 구성
    if not st.session_state['logged_in']:
        st.markdown("### 🔑 로그인")
        with st.form("login_form"):
            login_id = st.text_input("아이디", key="login_id")
            login_pw = st.text_input("비밀번호", type="password", key="login_pw")
            submitted = st.form_submit_button("로그인", use_container_width=True)
            
            if submitted:
                users = load_users()
                if login_id in users:
                    user_data = users[login_id]
                    if isinstance(user_data, str):
                        hashed_pw = user_data
                    else:
                        hashed_pw = user_data.get("user_password", "")
                        
                    if _safe_verify(login_pw, hashed_pw):
                        st.session_state['logged_in'] = True
                        st.session_state['username'] = login_id
                        t = time.time()
                        st.session_state['last_active'] = t
                        st.query_params["login_token"] = login_id
                        st.query_params["last_active"] = str(t)
                        
                        st.success("로그인 성공!")
                        st.rerun()
                    else:
                        st.error("아이디 또는 비밀번호가 틀렸습니다.")
                else:
                    st.error("아이디 또는 비밀번호가 틀렸습니다.")
                
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("📝 회원가입 하기", use_container_width=True):
            st.session_state['current_page'] = "📝 회원가입"
            st.session_state['_from_signup_btn'] = True # 메뉴 덮어쓰기 1회 방지
            if 'menu_radio' in st.session_state:
                del st.session_state['menu_radio']
            st.rerun()
    else:
        st.success(f"👋 환영합니다, **{st.session_state['username']}**님!")
        
        if st.button("로그아웃", use_container_width=True):
            st.session_state['logged_in'] = False
            st.session_state['username'] = ""
            st.query_params.clear()
            st.rerun()
            
    st.markdown("---")

    import streamlit as st
    from streamlit_option_menu import option_menu

    if 'current_page' not in st.session_state:
        st.session_state['current_page'] = "🏠 메인 대시보드"
    # --- 사이드바 메뉴 섹션 ---
    with st.sidebar:
        
        menu_options = ["🏠 메인 대시보드", "📋 투자 성향 설문", "⭐ 맞춤 종목 추천",
                        "📈 분석 신호", "📰 종목 뉴스", "📧 뉴스레터"]
        
        # 아이콘 설정
        menu_icons = ["house", "clipboard-check", "star", "graph-up", "newspaper", "envelope"]

        # option_menu 생성 (빨간 선 제거)
        selected = option_menu(
            menu_title=None,
            options=menu_options,
            icons=menu_icons,
            menu_icon="cast",
            default_index=menu_options.index(st.session_state['current_page']) if st.session_state['current_page'] in menu_options else 0,
            styles={
                "container": {
                    "padding": "0!important", 
                    "background-color": "transparent" # 컨테이너 배경 투명화
                },
                "icon": {"color": "#dcb98c", "font-size": "18px"}, 
                "nav-link": {
                    "font-size": "16px", 
                    "text-align": "left", 
                    "margin": "0px", 
                    "color": "#ffffff",
                    "background-color": "transparent", # 기본 배경을 투명하게 설정 (흰색 제거)
                    "transition": "0.2s",
                    "--hover-color": "rgba(255, 255, 255, 0.1)"
                },
                "nav-link-selected": {
                    "background-color": "#BA996B",      # 선택된 탭 배경색 (원하시는 올리브색)
                    "color": "#ffffff", 
                    "font-weight": "600",
                    "border-left": "none"
                },
            }
        )

        # 페이지 전환 로직
        # - 회원가입 버튼 직후 1회: 옵션메뉴 기본값으로 덮어쓰지 않음
        # - 그 외: 사용자가 메뉴 클릭 시 해당 페이지로 전환
        skip_overwrite = st.session_state.pop('_from_signup_btn', False)
        if not skip_overwrite and st.session_state['current_page'] != selected:
            st.session_state['current_page'] = selected
            st.rerun()

    # 최종 페이지 상태 저장
    page = st.session_state['current_page']

    st.markdown("---")
    
    if st.button("🔄 데이터 새로고침", use_container_width=True, help="DB 서버에서 최신 정제 데이터를 다시 가져옵니다."):
        st.cache_data.clear()
        st.rerun()

    # with st.expander("🛠️ 시스템 관리"):
    #     if st.button("📥 전체 시스템 리프레시", use_container_width=True, help="Web 스크래핑부터 DB 반영까지 전체 과정을 재실행합니다."):
    #         run_full_system_sync()
    #         st.cache_data.clear()
    #         st.rerun()

    # 데이터 파일 정보
    if 'data_file' in st.session_state:
        st.caption(f"📁 {st.session_state['data_file']}")

    st.markdown("---")
    st.markdown(
        "<div style='color:#888; font-size:12px; text-align:center;'>"
        "LUMINA CAPITAL 알고리즘 기반<br>"
        "투자 성향 5단계 분류<br>"
        "© 2026 Stock Recommender"
        "</div>",
        unsafe_allow_html=True,
    )


# ============================================================
# 📌 데이터 로드 & DB 동기화
# ============================================================
# 1. 세션당 최초 1회 DB에서 로컬로 데이터 동기화 수행 (사이드바 메뉴 로드 전 실행)
if 'last_sync_time' not in st.session_state:
    run_outbound_sync()
    st.session_state['last_sync_time'] = time.time()

# 2. 로컬 데이터가 아예 없는 경우 스크래핑 (최초 실행용)
ensure_data_exists()

# 3. 로컬 JSON 데이터 로드 (캐싱 지원)
stock_df, news_df, hist_df, signals_df, recs_df, newsletters_df, user_types_df = load_latest_data()


# ============================================================
# 📝 회원가입 전용 페이지
# ============================================================
if page == "📝 회원가입":
    st.markdown("# 📝 회원가입")
    st.markdown("LUMINA CAPITAL의 모든 프리미엄 자산관리 기능을 이용하시려면 회원가입을 진행해주세요.")
    st.markdown("---")
    
    col1, col2 = st.columns([1, 1])
    with col1:
        with st.form("signup_form_main"):
            new_id = st.text_input("아이디 (4자리 이상)")
            new_email = st.text_input("이메일 주소")
            new_pw = st.text_input("비밀번호 (4자리 이상)", type="password")
            new_pw_check = st.text_input("비밀번호 확인", type="password")
            
            submitted = st.form_submit_button("회원가입", use_container_width=True)
            
            if submitted:
                users = load_users()
                if new_id in users:
                    st.error("이미 존재하는 아이디입니다.")
                elif new_pw != new_pw_check:
                    st.error("비밀번호가 일치하지 않습니다.")
                elif len(new_id) < 4 or len(new_pw) < 4:
                    st.error("아이디와 비밀번호는 4자리 이상이어야 합니다.")
                elif not new_email or "@" not in new_email:
                    st.error("유효한 이메일 주소를 입력해주세요.")
                else:
                    users[new_id] = {
                        "user_password": _safe_hash(new_pw),
                        "user_email": new_email
                    }
                    save_users(users)
                    
                    # 회원가입 및 DB 스크립트 실행 결과를 팝업으로 명확히 보여주기
                    @st.dialog("회원가입 성공!")
                    def show_signup_result():
                        st.success("✅ 회원가입이 완료되었습니다!")
                        
                        with st.status("외부 DB 서버(A_users_table.py) 연동 중...", expanded=True) as status:
                            try:
                                import subprocess
                                import sys
                                script_path = os.path.join(os.path.dirname(__file__), 'database_script', 'A_users_table.py')
                                
                                # 백그라운드 스크립트 실행. 서버 연동 시간을 극적으로 줄임.
                                subprocess.Popen([sys.executable, script_path])
                                
                                st.write("🌐 DB 동기화 백그라운드 스케줄링 완료")
                                status.update(label="DB 연동 완료 (백그라운드)", state="complete")
                                    
                            except subprocess.TimeoutExpired:
                                st.write("⚠️ DB 서버 응답이 너무 늦습니다. (타임아웃)")
                                status.update(label="DB 연동 타임아웃 (로컬 접속은 가능)", state="error")
                            except Exception as e:
                                st.write(f"⚠️ 예기치 않은 오류: {e}")
                                status.update(label="DB 연동 중 오류 발생", state="error")
                        
                        st.info("이제 왼쪽 메뉴에서 로그인을 진행해주세요.")
                        if st.button("로그인하러가기", use_container_width=True):
                            st.session_state['current_page'] = "🏠 메인 대시보드"
                            st.rerun()

                    show_signup_result()

# ============================================================
# 🏠 메인 대시보드
# ============================================================
elif page == "🏠 메인 대시보드":
    st.markdown("# 🏠 시장 개요 대시보드")

    # ── 초보자 용어 설명 ──
    with st.expander("📖 처음 오셨나요? 주요 용어 설명 보기"):
        st.markdown("""
        | 용어 | 뜻 | 쉬운 설명 |
        |------|----|-----------|
        | **PER** | 주가수익비율 | 낮을수록 '저평가' 가능성. 보통 10~20이 적정 |
        | **PBR** | 주가순자산비율 | 1 미만이면 회사 자산보다 주가가 낮음 (저평가) |
        | **외국인 순매수** | 외국인 투자자 매수-매도 | (+)면 외국인이 사는 중, (-)면 파는 중 |
        | **기관 순매수** | 연기금·펀드 등 매수-매도 | 기관이 사면 일반적으로 긍정 신호 |
        | **거래대금** | 하루 총 거래 금액 | 클수록 많은 사람이 관심 갖는 종목 |
        | **등락률** | 전날 대비 가격 변화 | (+)는 상승, (-)는 하락 |
        | **🟢 BUY** | 매수 신호 | 여러 지표가 상승 가능성을 보임 |
        | **🟡 HOLD** | 보유 신호 | 추세가 불분명, 지켜보는 것 권장 |
        | **🔴 SELL** | 매도 신호 | 하락 지표가 나타남, 주의 필요 |
        """)
        st.info("⚠️ 본 서비스는 **투자 참고용**입니다. 실제 투자 결정은 전문가 상담을 권장합니다.")

    if stock_df.empty:
        st.warning(
            "⚠️ 데이터가 없습니다. 먼저 `python scraper.py`를 실행하여 "
            "데이터를 수집해 주세요."
        )
        st.code("python scraper.py", language="bash")
        st.stop()

    # ── 주요 종목 실시간 시세 (Top 50 Quick Glance) ──
    st.markdown("### 🏆 당일 거래량 상위 50종목 현재가")
    
    top50_df = stock_df.sort_values(by='거래량', ascending=False).head(50)
    
    if not top50_df.empty:
        # 화면을 너무 길게 차지하지 않도록 Expander 안에 넣기
        with st.expander("👀 종목 리스트 펼쳐보기 (Top 50)", expanded=True):
            # 5열 그리드로 배치
            cols = st.columns(5)
            for i, row in enumerate(top50_df.itertuples()):
                col_idx = i % 5
                price = f"{row.현재가:,}"
                change = f"{row.등락률}"
                
                
                cols[col_idx].metric(
                    label=row.종목명, 
                    value=price, 
                    delta=change,
                    delta_color="inverse"
                )
    else:
        st.info("수집된 데이터가 없습니다.")
        
    st.markdown("---")

    st.markdown("### 📈 오늘의 증시 (KOSPI / KOSDAQ)")
    # 데이터 로드 (indices_df가 로드되었다고 가정)
    # ── 1. 데이터 정의 및 더미 데이터 생성 로직 ──
    import numpy as np
    from datetime import datetime, timedelta

    # indices_df가 로드되지 않았거나 비어있는 경우 더미 데이터 생성
    if 'indices_df' not in locals() or indices_df.empty:
        # 그래프 모양 확인을 위한 100일치 가상 데이터 생성
        test_dates = [(datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(100)]
        test_dates.reverse()
        
        # 실제 지수와 유사한 랜덤 흐름 생성
        np.random.seed(42) # 동일한 그래프 모양 유지를 위해 시드 고정
        kp_sample = np.linspace(2450, 2580, 100) + np.random.normal(0, 15, 100)
        kd_sample = np.linspace(810, 870, 100) + np.random.normal(0, 8, 100)
        
        df_kp = pd.DataFrame({'Date': test_dates, 'Close': kp_sample, '시장': 'KOSPI'})
        df_kd = pd.DataFrame({'Date': test_dates, 'Close': kd_sample, '시장': 'KOSDAQ'})
        
        st.caption("✨ 현재 레이아웃 확인을 위한 **샘플 데이터**를 표시 중입니다. (실제 데이터 없음)")
    else:
        # 실제 데이터가 존재하는 경우 필터링
        df_kp = indices_df[indices_df['시장'] == 'KOSPI']
        df_kd = indices_df[indices_df['시장'] == 'KOSDAQ']
    
    # ── 2. 레이아웃 분리 (2개의 컬럼 생성) ──
    col_chart1, col_chart2 = st.columns(2)

    # 공통 레이아웃 설정 함수
    def get_layout(title_text, color):
        return dict(
            template='plotly_dark',
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            height=300, # 분리된 만큼 높이를 조금 줄임
            margin=dict(l=10, r=10, t=40, b=10),
            hovermode='x unified',
            title=dict(text=title_text, font=dict(color=color, size=18)),
            xaxis=dict(showgrid=False, tickfont=dict(color='#888')),
            yaxis=dict(
                showgrid=True,
                gridcolor='rgba(255,255,255,0.05)',
                tickfont=dict(color=color),
                zeroline=False
            )
        )

    # ── 3. 코스피 그래프 (좌측) ──
    with col_chart1:
        fig_kp = go.Figure()
        fig_kp.add_trace(go.Scatter(
            x=df_kp['Date'], y=df_kp['Close'],
            name='KOSPI',
            line=dict(color='#dcb98c', width=2),
            fill='tozeroy',
            fillcolor='rgba(220, 185, 140, 0.1)'
        ))
        fig_kp.update_layout(get_layout("코스피(KOSPI)", "#dcb98c"))
        st.plotly_chart(fig_kp, use_container_width=True)

    # ── 4. 코스닥 그래프 (우측) ──
    with col_chart2:
        fig_kd = go.Figure()
        fig_kd.add_trace(go.Scatter(
            x=df_kd['Date'], y=df_kd['Close'],
            name='KOSDAQ',
            line=dict(color='#f2ece4', width=2),
            fill='tozeroy',
            fillcolor='rgba(242, 236, 228, 0.05)'
        ))
        fig_kd.update_layout(get_layout("코스닥(KOSDAQ)", "#f2ece4"))
        st.plotly_chart(fig_kd, use_container_width=True)

    # ── 4. 지수 요약 메트릭 ──
    index_metrics_container = st.container()

    with index_metrics_container:
        # 1. 이 컨테이너 바로 다음에 오는 메트릭들만 가로로 배치하는 CSS
        # nth-child를 사용하여 지수 그래프 바로 아래의 메트릭 섹션만 정밀 타겟팅합니다.
        st.markdown("""
            <style>
            /* 상자 자체의 여백 최소화 및 테두리 설정 */
            [data-testid="stVerticalBlock"] > div:has(div#index-area-marker) + div [data-testid="stMetric"] {
                padding: 5px 0px !important; 
                border: 1px solid rgba(220, 185, 140, 0.3) !important;
                border-radius: 10px !important;
                text-align: center !important;
            }

            /* 내부 요소를 가로 한 줄로 세우고 전체 중앙 정렬 */
            [data-testid="stVerticalBlock"] > div:has(div#index-area-marker) + div [data-testid="stMetric"] > div {
                display: flex !important;
                flex-direction: row !important;
                justify-content: center !important; /* 모든 요소를 가로 중앙으로 */
                align-items: baseline !important;    /* 글자 아래선 맞춤 */
                gap: 10px !important;                /* 요소 간 간격 */
                width: 100% !important;
            }

            /* 항목 이름(KOSPI) 스타일 */
            [data-testid="stVerticalBlock"] > div:has(div#index-area-marker) + div [data-testid="stMetricLabel"] {
                margin-bottom: 0 !important;
                min-width: fit-content !important;
            }
            
            [data-testid="stVerticalBlock"] > div:has(div#index-area-marker) + div [data-testid="stMetricLabel"] > div {
                font-size: 14px !important;
                font-weight: 600 !important;
                color: #dcb98c !important;
            }

            /* 지수 숫자(Value) 확대 */
            [data-testid="stVerticalBlock"] > div:has(div#index-area-marker) + div [data-testid="stMetricValue"] > div {
                font-size: 30px !important; 
                font-weight: 800 !important;
                line-height: 1 !important;
            }

            /* 변동폭(Delta) 중앙 정렬을 위해 마진 해제 */
            [data-testid="stVerticalBlock"] > div:has(div#index-area-marker) + div [data-testid="stMetricDelta"] {
                margin-top: 0 !important;
                margin-left: 0 !important; /* 오른쪽 밀착 해제 */
                display: flex !important;
                align-items: center !important;
            }
            
            [data-testid="stMetricDelta"] svg {
                display: none !important; /* 화살표가 너무 크면 숨기거나 조정 가능 */
            }
            </style>
            <div id="index-area-marker"></div>
        """, unsafe_allow_html=True)

        # 2. 실제 메트릭 배치
        idx_col1, idx_col2 = st.columns(2)
        
        kp_last = df_kp.iloc[-1]['Close']
        kp_delta = kp_last - df_kp.iloc[-2]['Close']
        kd_last = df_kd.iloc[-1]['Close']
        kd_delta = kd_last - df_kd.iloc[-2]['Close']

        with idx_col1:
            st.metric("KOSPI", f"{kp_last:,.2f}", f"{kp_delta:+.2f}")

        with idx_col2:
            st.metric("KOSDAQ", f"{kd_last:,.2f}", f"{kd_delta:+.2f}")

    st.markdown("---")

    # ── 요약 통계 ──
    summary = generate_analysis_summary(stock_df)

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("📈 총 종목 수", summary.get('총 종목 수', 0))
    with col2:
        st.metric("🔴 상승", summary.get('상승 종목 수', 0))
    with col3:
        st.metric("🔵 하락", summary.get('하락 종목 수', 0))
    with col4:
        avg_pct = summary.get('평균 등락률(%)', 0)
        st.metric("📊 평균 등락률", f"{avg_pct}%")

    st.markdown("---")

    # ── 시장별 탭 ──
    tab1, tab2, tab3, tab4 = st.tabs(["📊 거래량 차트", "🔥 외국인/기관 매매", "📋 전체 데이터", "⏱️ 실시간 분석 (RTD)"])

    with tab1:
        st.markdown("### 거래량 상위 종목")
        
        market_filter = st.selectbox(
            "시장 선택", ["전체", "KOSPI", "KOSDAQ"], key="market_filter_vol"
        )
        # 1. 시장 필터링 적용
        if market_filter == "전체":
            filtered_df = stock_df.copy()
        else:
            filtered_df = stock_df[stock_df['시장'] == market_filter].copy()

        # 2. 거래량 기준으로 내림차순 정렬
        filtered_df = filtered_df.sort_values(by='거래량', ascending=False)

        # 3. 정렬된 데이터에서 상위 20개 추출
        top20 = filtered_df.head(20)

        col_left, col_pie1 = st.columns([1.5, 1])
        with col_left:
            if not top20.empty:
                # 막대 그래프 (Bar Chart)
                fig = px.bar(
                    top20,
                    x='종목명',
                    y='거래량',
                    color='시장',
                    # 전체 선택 시 두 시장이 모두 보일 수 있도록 카테고리별 색상 지정
                    color_discrete_map={'KOSPI': '#dcb98c', 'KOSDAQ': "#4a3728"},
                    title=f'거래량 상위 종목 ({market_filter})',
                    template='plotly_dark',
                    # 범례 제목(시장) 표시 설정
                    #labels={'시장': '시장 구분'}
                )
            
                # X축 순서가 거래량 순으로 유지되도록 설정
                fig.update_layout(
                    # 타이틀 색상 변경
                    title={
                    'font': {'color': "#ffffff", 'size': 20}
                    },
                    # 각 색상별 어떤 시장인지 표시
                    showlegend=True,
                    legend=dict(
                        title_text='시장',
                        font=dict(size=14, color="white"), # 텍스트 크기를 키우고 흰색으로 고정
                        orientation="v", # 세로로 나열
                        yanchor="top",
                        y=0.99,
                        xanchor="left",
                        x=1.02 # 차트 오른쪽에 범례 표시
                    ),
                    #xaxis={'categoryorder':'total descending'},
                    xaxis_tickangle=-45,
                    xaxis=dict(
                        {'categoryorder':'total descending'},
                        title_font=dict(color="#ffffff"),   # 축 이름 색상
                        tickfont=dict(color="#ffffff")   # 축 숫자 색상
                    ),
                    yaxis=dict(
                        title_font=dict(color="#ffffff"),  # 축 이름 색상
                        tickfont=dict(color="#ffffff")    # 축 숫자 색상
                    ),
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font=dict(color="#ffffff"),
                    height=550
                )
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("거래량 데이터가 없습니다.")

        

        with col_pie1:
            # 1. 상승/하락 그룹 나누기 로직 (등락률 숫자가 있다고 가정)
            def get_signal_label(row):
                if row['등락률(숫자)'] > 0: return '상승 종목'
                elif row['등락률(숫자)'] < 0: return '하락 종목'
                else: return '보합'

            # 상위 50개 혹은 전체 데이터를 대상으로 비중 계산
            analysis_df = filtered_df.copy()
            analysis_df['구분'] = analysis_df.apply(get_signal_label, axis=1)
            
            # 그룹별 거래량 합계
            vol_dist = analysis_df.groupby('구분')['거래량'].sum().reset_index()

            # 2. 도넛 차트 생성
            fig_pie = px.pie(
                vol_dist, 
                values='거래량', 
                names='구분',
                hole=0.5,
                color='구분',
                color_discrete_map={'상승 종목': '#f85149', '하락 종목': '#3b82f6', '보합': '#8b949e'},  # 한국 시장: 상승=빨강, 하락=파랑
                title=f"🔥 {market_filter} 거래량 수급 비중 (상승 vs 하락)"
            )
            
            fig_pie.update_traces(textposition='inside', textinfo='percent+label',textfont=dict(size=16, family="Arial", color="black"),insidetextfont=dict(weight='bold'))
            fig_pie.update_layout(
                title={
                    'font': {'color': "#ffffff", 'size': 20}
                },
                paper_bgcolor='rgba(0,0,0,0)',
                plot_bgcolor='rgba(0,0,0,0)',
                font=dict(color="#ffffff"),
                showlegend=False,
                margin=dict(t=50, b=0, l=0, r=0),
                height=350
            )
            st.plotly_chart(fig_pie, use_container_width=True)
            st.markdown("#### 💡 수급 비중 인사이트")
            # 간단한 로직으로 시장 해석 제공
            up_vol = vol_dist[vol_dist['구분'] == '상승 종목']['거래량'].sum()
            total_vol = vol_dist['거래량'].sum()
            up_ratio = (up_vol / total_vol) * 100 if total_vol > 0 else 0

            if up_ratio > 60:
                st.success(f"**강세장:** 현재 거래량의 {up_ratio:.1f}%가 상승 종목에 쏠려 있습니다. 매수세가 매우 강력합니다.")
            elif up_ratio < 40:
                st.error(f"**약세장:** 현재 거래량의 {100-up_ratio:.1f}%가 하락 종목에서 발생하고 있습니다. 패닉 셀링에 주의하세요.")
            else:
                st.info(f"**혼조세:** 상승/하락 종목의 거래량 비중이 팽팽합니다. 방향성이 결정될 때까지 관망이 필요합니다.")
            
            st.caption("※ 이 차트는 종목 수가 아닌, 실제 '거래된 대금/물량'의 비중을 나타냅니다.")   

    with tab2:
        st.markdown("### 외국인/기관 매매 동향")

        if '외국인_순매수량' in stock_df.columns and '기관_순매수량' in stock_df.columns:
            inv_df = stock_df[['종목명', '외국인_순매수량', '기관_순매수량']].dropna()

            if not inv_df.empty:
                # 상위 N개만 표시 (가독성 목적)
                top_n_display = st.slider("표시할 종목 수 (외국인 순매수 기준)", 10, 50, 20)
                inv_df_top = inv_df.sort_values('외국인_순매수량', ascending=False).head(top_n_display)

                fig2 = go.Figure()
                fig2.add_trace(go.Bar(
                    x=inv_df_top['종목명'],
                    y=inv_df_top['외국인_순매수량'],
                    name='외국인',
                    marker_color='#dcb98c',
                ))
                fig2.add_trace(go.Bar(
                    x=inv_df_top['종목명'],
                    y=inv_df_top['기관_순매수량'],
                    name='기관',
                    marker_color="#3f3122",
                ))
                fig2.update_layout(
                    title={
                        'text': f'외국인/기관 순매수량 비교 (상위 {top_n_display}종목)',
                        'font': {'color': "#ffffff", 'size': 20}
                    },
                    barmode='group',
                    template='plotly_dark',
                    xaxis=dict(
                    title_font=dict(color="#ffffff"),  # 축 이름 색상
                    tickfont=dict(color="#ffffff")    # 축 숫자 색상
                    ),
                    yaxis=dict(
                    title_font=dict(color="#ffffff"),  # 축 이름 색상
                    tickfont=dict(color="#ffffff")    # 축 숫자 색상
                    ),
                    showlegend=True,
                    legend=dict(
                        font=dict(size=14, color="white"), # 텍스트 크기를 키우고 흰색으로 고정
                        orientation="v", # 세로로 나열
                        yanchor="top",
                        y=0.99,
                        xanchor="left",
                        x=1.02 # 차트 오른쪽에 범례 표시
                    ),
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font=dict(color='#f2ece4'),
                    xaxis_tickangle=-45,
                    height=500,
                )
                st.plotly_chart(fig2, use_container_width=True)

                # Seaborn 히트맵 (matplotlib)
                st.markdown("### 투자 지표 상관관계 히트맵")
                numeric_cols = ['현재가', '거래량', '거래대금', 'PER', 'PBR',
                                '외국인_순매수량', '기관_순매수량']
                available_cols = [c for c in numeric_cols if c in stock_df.columns]

                if len(available_cols) >= 3:
                    corr_data = stock_df[available_cols].apply(
                        pd.to_numeric, errors='coerce'
                    ).corr()

                    fig_heat, ax = plt.subplots(figsize=(10, 6))
                    fig_heat.patch.set_facecolor('#2b2622')
                    ax.set_facecolor('#2b2622')
                    sns.heatmap(
                        corr_data, annot=True, cmap='YlOrBr', fmt='.2f',
                        ax=ax, linewidths=0.5,
                        annot_kws={'color': '#f2ece4', 'fontsize': 9},
                        cbar_kws={'label': '상관계수'},
                    )
                    # --- 글자 뒤집힘/회전 방지 설정 ---
                    # x축 레이블을 가로(0도)로 설정
                    ax.set_xticklabels(ax.get_xticklabels(), rotation=0, color='#f2ece4')
                    # y축 레이블을 가로(0도)로 설정 (기본은 보통 90도 돌아가 있음)
                    ax.set_yticklabels(ax.get_yticklabels(), rotation=0, color='#f2ece4')
                    ax.tick_params(colors='#f2ece4')
                    ax.xaxis.label.set_color('#f2ece4')
                    ax.yaxis.label.set_color('#f2ece4')
                    plt.title('투자 지표 상관관계', color='#dcb98c', fontsize=14)
                    plt.tight_layout()
                    st.pyplot(fig_heat)
                    plt.close()
        else:
            st.info("외국인/기관 매매 데이터가 없습니다.")

    with tab3:
        st.markdown("### 전체 종목 데이터")

        # 필터 옵션
        col_a, col_b = st.columns(2)
        with col_a:
            market_filter2 = st.selectbox(
                "시장", ["전체", "KOSPI", "KOSDAQ"], key="market_filter_all"
            )
        with col_b:
            sort_col = st.selectbox(
                "정렬 기준", ['거래량', '현재가', '등락률(숫자)', '거래대금'],
                key="sort_col"
            )

        display_df = stock_df.copy()
        if market_filter2 != "전체":
            display_df = display_df[display_df['시장'] == market_filter2]

        if sort_col in display_df.columns:
            display_df = display_df.sort_values(sort_col, ascending=False)

        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            height=600,
        )


    with tab4:
        st.markdown("### ⏱️ 시간대별 실시간 모멘텀 (RTD)")
        st.info("💡 매 1시간 정각마다 누적되는 데이터를 비교하여, 가장 거래량이 가파르게 상승한 종목을 스캔합니다.")
        
        try:
            from rtd_analyzer import load_realtime_market_data, analyze_volume_surge
            rtd_df = load_realtime_market_data()
            surge_df = analyze_volume_surge(rtd_df)
            
            if not surge_df.empty:
                col1, col2 = st.columns([1, 1])
                
                with col1:
                    st.markdown("#### 🚀 시간당 거래량 급증 TOP 10")
                    fig_surge, ax_surge = plt.subplots(figsize=(8, 5))
                    fig_surge.patch.set_facecolor('#2b2622')
                    ax_surge.set_facecolor('#2b2622')
                    sns.barplot(
                        x='시간당_순거래량', y='종목명', data=surge_df, 
                        palette='YlOrBr_r', ax=ax_surge
                    )
                    ax_surge.tick_params(colors='#f2ece4')
                    ax_surge.xaxis.label.set_color('#f2ece4')
                    ax_surge.yaxis.label.set_color('#f2ece4')
                    plt.title('직전 시간 대비 거래량 순증가 TOP 10', color='#dcb98c', fontsize=12)
                    plt.tight_layout()
                    st.pyplot(fig_surge)
                    plt.close()
                    
                with col2:
                    st.markdown("#### 🎯 현재가 대비 거래대금 분포")
                    latest_time = rtd_df['수집시간'].max()
                    latest_df = rtd_df[rtd_df['수집시간'] == latest_time]
                    
                    fig_scatter, ax_scatter = plt.subplots(figsize=(8, 5))
                    fig_scatter.patch.set_facecolor('#2b2622')
                    ax_scatter.set_facecolor('#2b2622')
                    ax_scatter.scatter(
                        latest_df['현재가'], latest_df['거래대금'], 
                        c='#dcb98c', alpha=0.6, edgecolors='none'
                    )
                    ax_scatter.tick_params(colors='#f2ece4')
                    ax_scatter.xaxis.label.set_color('#f2ece4')
                    ax_scatter.yaxis.label.set_color('#f2ece4')
                    plt.xlabel("현재가 (원)", color='#f2ece4')
                    plt.ylabel("거래대금", color='#f2ece4')
                    plt.title(f'가격대별 거래대금 분산 ({pd.to_datetime(latest_time).strftime("%H:%M")} 기준)', color='#dcb98c', fontsize=12)
                    plt.tight_layout()
                    st.pyplot(fig_scatter)
                    plt.close()
            else:
                st.warning("⚠️ 아직 2개 이상의 시간대 데이터가 누적되지 않아 실시간 비교를 할 수 없습니다. (매 정각 수집기 대기 중)")
        except Exception as e:
            st.error(f"실시간 분석 모듈 로딩 중 오류 발생: {e}") 

# ============================================================
# 📋 투자 성향 설문
# ============================================================
elif page == "📋 투자 성향 설문":
    st.markdown("# 📋 투자 성향 진단")
    st.markdown(
        "> 한양증권 투자성향진단 기준 **11문항**으로 구성된 설문입니다.\n"
        "> 솔직하게 답변해 주시면 **5단계 투자 성향**을 분류해 드립니다."
    )
    st.markdown("---")

    # ── 설문 폼 ──
    answers = {}
    with st.form("survey_form"):
        for i, q in enumerate(SURVEY_QUESTIONS):
            st.markdown(f"### {i+1}. {q['question']}")
            options = [opt[0] for opt in q['options']]
            selected = st.radio(
                f"Q{i+1}",
                range(len(options)),
                format_func=lambda idx, opts=options: f"{'①②③④⑤⑥'[idx]} {opts[idx]}",
                key=f"q_{q['id']}",
                label_visibility="collapsed",
            )
            answers[q['id']] = selected
            st.markdown("")

        st.markdown("### 📧 뉴스레터 구독")
        newsletter_opt = st.radio(
            "이메일로 뉴스레터 구독 받으시겠습니까?",
            options=["예", "아니오"],
            horizontal=True,
            key="newsletter_subscribe"
        )
        st.markdown("")

        submitted = st.form_submit_button(
            "🔍 투자 성향 진단하기",
            use_container_width=True,
        )

    if submitted:
        # 뉴스레터 구독 여부 세션 저장
        st.session_state['newsletter_subscribed'] = (newsletter_opt == "예")

        investor_type, total_score = classify_investor_type(answers)
        st.session_state['investor_type'] = investor_type
        st.session_state['survey_score'] = total_score
        st.session_state['survey_answers'] = answers
        
        # 로그인 되어있다면 유저별 투자 성향(user_profile) DB에 업데이트
        if st.session_state.get('logged_in'):
            user_id = st.session_state.get('username')
            if user_id:
                type_id_map = {
                    '안정형': 1,
                    '안정추구형': 2,
                    '위험중립형': 3,
                    '적극투자형': 4,
                    '공격투자형': 5
                }
                type_id = type_id_map.get(investor_type)
                if type_id:
                    user_check_val = 1 if st.session_state.get('newsletter_subscribed') else 0
                    save_user_profile(user_id, type_id, user_check=user_check_val)
                    st.toast(f"✅ {user_id}님의 투자 성향({investor_type})이 로컬에 저장되었습니다!")
                    
                    # 투자 성향 외부 DB 최신화 스크립트 실행 (B_users_type_table.py)
                    with st.status("📊 외부 DB 서버(B_users_type_table.py) 연동 중...", expanded=True) as status:
                        try:
                            import subprocess
                            import sys
                            import os
                            script_path = os.path.join(os.path.dirname(__file__), 'database_script', 'B_users_type_table.py')
                            res = subprocess.run([sys.executable, script_path], capture_output=True, text=True, timeout=30)
                            
                            if res.returncode == 0:
                                st.write("🌐 투자성향 DB 테이블 최신화 성공")
                                status.update(label="DB 연동 완료", state="complete")
                            else:
                                st.write("⚠️ DB 서버 연결에 실패했거나 지연되었습니다.")
                                status.update(label="DB 연동 실패 (로컬 저장은 완료)", state="error")
                        except subprocess.TimeoutExpired:
                            st.write("⚠️ DB 서버 응답이 너무 늦습니다. (타임아웃)")
                            status.update(label="DB 연동 타임아웃 (로컬 저장은 완료)", state="error")
                        except Exception as e:
                            st.write(f"⚠️ 예기치 않은 오류: {e}")
                            status.update(label="DB 연동 중 오류 발생", state="error")
                            
        # 설문 완료 후 결과 페이지(맞춤 종목 추천)로 자동 강제 이동
        import time 
        time.sleep(1) # 유저가 토스트 메시지/상태창을 볼 아주 잠깐의 여유 제공
        
        # 라디오 버튼 UI 동기화를 위해 session_state 처리
        st.session_state['current_page'] = "⭐ 맞춤 종목 추천"
        if 'menu_radio' in st.session_state:
            del st.session_state['menu_radio']
        st.rerun()

        type_info = TYPE_DESCRIPTIONS[investor_type]

        st.markdown("---")
        st.markdown(
            f"""
            <div class="investor-card">
                <h2>{type_info['emoji']} 당신의 투자 성향: {type_info['title']}</h2>
                <p>{type_info['desc']}</p>
                <p style="color:{type_info['color']}; font-weight:700; font-size:16px;">
                    💡 추천 전략: {type_info['strategy']}
                </p>
                <p style="color:#888; font-size:14px;">총점: {total_score}점</p>
            </div>
            """,
            unsafe_allow_html=True,
        )

        # 5단계 프로그레스
        types_order = ['안정형', '안정추구형', '위험중립형', '적극투자형', '공격투자형']
        current_idx = types_order.index(investor_type)

        st.markdown("### 투자 성향 스케일")
        cols = st.columns(5)
        for i, t in enumerate(types_order):
            info = TYPE_DESCRIPTIONS[t]
            with cols[i]:
                if i == current_idx:
                    st.markdown(
                        f"<div style='text-align:center; padding:12px; "
                        f"background:linear-gradient(135deg, {info['color']}33, {info['color']}66); "
                        f"border:2px solid {info['color']}; border-radius:12px;'>"
                        f"<span style='font-size:24px;'>{info['emoji']}</span><br>"
                        f"<span style='color:white; font-weight:700;'>{t}</span>"
                        f"</div>",
                        unsafe_allow_html=True,
                    )
                else:
                    st.markdown(
                        f"<div style='text-align:center; padding:12px; "
                        f"background:rgba(255,255,255,0.03); "
                        f"border:1px solid rgba(255,255,255,0.1); border-radius:12px;'>"
                        f"<span style='font-size:24px;'>{info['emoji']}</span><br>"
                        f"<span style='color:#888;'>{t}</span>"
                        f"</div>",
                        unsafe_allow_html=True,
                    )

        st.markdown("")
        st.info("👈 좌측 메뉴에서 **⭐ 맞춤 종목 추천**을 선택하시면 추천 결과를 확인할 수 있습니다.")


# ============================================================
# ⭐ 맞춤 종목 추천
# ============================================================
elif page == "⭐ 맞춤 종목 추천":
    st.markdown("# ⭐ 맞춤 종목 추천")
    # 로그인 체크
    if not st.session_state.get('logged_in', False):
        @st.dialog("로그인 안내")
        def show_login_dialog():
            st.warning("⚠️ 맞춤 종목 추천 서비스는 로그인이 필요합니다.")
            st.info("좌측 사이드바에서 로그인 후 이용해 주세요.")
            if st.button("홈으로 돌아가기", use_container_width=True):
                st.session_state['current_page'] = "🏠 메인 대시보드"
                st.rerun()
                
        show_login_dialog()
        st.stop()

    if stock_df.empty:
        st.warning("⚠️ 주식 데이터가 없습니다. 먼저 `python scraper.py`를 실행해 주세요.")
        st.stop()

    # ── 투자 성향 확인 (DB 연동 기반) ──
    # 세션에 투자 성향이 없어도 DB에 기록이 있다면 불러오기
    if 'investor_type' not in st.session_state and st.session_state.get('logged_in'):
        import os, pandas as pd
        
        type_db = os.path.join(DATA_DIR, 'user_type_db.csv')
        if os.path.exists(type_db):
            try:
                tdf = pd.read_csv(type_db)
                user_match = tdf[tdf['user_id'].astype(str) == str(st.session_state['username'])]
                if not user_match.empty:
                    # DB에서 찾아온 성향 이름 저장
                    st.session_state['investor_type'] = user_match.iloc[-1]['type_name']
            except Exception as e:
                pass
                
    if 'investor_type' not in st.session_state:
        st.info("📋 먼저 **투자 성향 설문**을 완료해 주세요.")

        # 임시 선택 옵션
        st.markdown("---")
        st.markdown("### 또는 투자 성향을 직접 선택하세요")
        investor_type = st.selectbox(
            "투자 성향 선택",
            ['안정형', '안정추구형', '위험중립형', '적극투자형', '공격투자형'],
            index=2,
        )
    else:
        investor_type = st.session_state['investor_type']
        type_info = TYPE_DESCRIPTIONS[investor_type]
        st.markdown(
            f"**{type_info['emoji']} 현재 투자 성향: {type_info['title']}** — "
            f"_{type_info['strategy']}_"
        )

    st.markdown("---")

    # ── 추천 개수 설정 ──
    col1, col2 = st.columns([1, 3])
    with col1:
        top_n = st.slider("추천 종목 수", 3, 20, 10)
    with col2:
        market_sel = st.selectbox(
            "시장 필터", ["전체", "KOSPI", "KOSDAQ"], key="rec_market"
        )

    # ── 데이터 필터링 ──
    filtered_df = stock_df.copy()
    if market_sel != "전체":
        filtered_df = filtered_df[filtered_df['시장'] == market_sel]

    # 발표용 요건: 시가총액 높은 상위 100개 종목 내에서만 추천
    if not filtered_df.empty and '시가총액(억)' in filtered_df.columns:
        filtered_df['시가총액(억)'] = pd.to_numeric(filtered_df['시가총액(억)'], errors='coerce')
        filtered_df = filtered_df.sort_values(by='시가총액(억)', ascending=False).head(100)

    # ── 추천 종목 계산 ──
    # DB 추천 데이터 중 현재 사용자의 성향과 일치하는 것 필터링 정렬
    recommendations = pd.DataFrame()
    if not recs_df.empty:
        recs_display = recs_df.copy()
        if '현재가' in recs_display.columns:
            recs_display['현재가'] = pd.to_numeric(recs_display['현재가'], errors='coerce')
            recs_display = recs_display[recs_display['현재가'] > 0]
            
        if not filtered_df.empty and '종목코드' in filtered_df.columns:
            top_tickers = filtered_df['종목코드'].astype(str).tolist()
            recs_display = recs_display[recs_display['종목코드'].astype(str).isin(top_tickers)]
            
        recommendations = recs_display.sort_values(by='추천점수', ascending=False).head(top_n)

    # DB 필터를 거친 후 종목 수가 부족하거나 데이터가 없으면 즉시 실시간 연산 수행 (보조 수단)
    if len(recommendations) < top_n:
        recommendations = get_top_recommendations(filtered_df, investor_type, top_n)

    if recommendations.empty:
        st.warning("추천 가능한 종목이 없습니다.")
        st.stop()

    # ── 추천 결과 표시 ──
    st.markdown(f"### 🏆 {investor_type} 성향 추천 TOP {len(recommendations)}")

    # 상위 3개 하이라이트
    top3_cols = st.columns(min(3, len(recommendations)))
    for i, col in enumerate(top3_cols):
        if i < len(recommendations):
            row = recommendations.iloc[i]
            with col:
                medals = ['🥇', '🥈', '🥉']
                medal = medals[i] if i < 3 else ''
                # 한국 시장: 상승=빨강, 하락=파랑
                change_color = '#f85149' if row.get('전일비', 0) > 0 else '#3b82f6'
                st.markdown(
                    f"""
                    <div class="stock-card">
                        <div style="display:flex; justify-content:space-between; align-items:center;">
                            <span style="font-size:18px; font-weight:700; color:#e6edf3;">
                                {medal} {row['종목명']}
                            </span>
                            <span class="score-badge">{row.get('추천점수', 0):.1f}점</span>
                        </div>
                        <div style="margin-top:8px; color:#8b949e;">
                            현재가: <strong style="color:white;">{row['현재가']:,}원</strong>
                            <span style="color:{change_color}; margin-left:8px;">
                                {row.get('등락률', 'N/A')}
                            </span>
                        </div>
                        <div style="margin-top:4px; color:#8b949e; font-size:13px;">
                            {row.get('추천이유', '')}
                        </div>
                        <div style="margin-top:4px; color:#6e7681; font-size:12px;">
                            거래량: {row['거래량']:,} | {row['시장']}
                        </div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )

    st.markdown("")

    # ── 추천 점수 차트 ──
    tab_a, tab_b, tab_c, tab_d = st.tabs(["📊 추천 점수 차트", "📈 캔들스틱 차트", "📈 종목 비교", "📋 상세 데이터"])

    with tab_a:
        fig_score = px.bar(
            recommendations,
            x='종목명',
            y='추천점수',
            color='추천점수',
            color_continuous_scale='Viridis',
            title=f'{investor_type} 성향 추천 종목 점수',
            template='plotly_dark',
            text='추천점수',
        )
        fig_score.update_traces(texttemplate='%{text:.1f}', textposition='outside')
        fig_score.update_layout(
            xaxis_tickangle=-45,
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(color='#e0e0ff'),
            height=500,
            showlegend=False,
        )
        st.plotly_chart(fig_score, use_container_width=True)

        # ── 추천 이유 설명 테이블 ──
        st.markdown("### 📋 추천 이유 상세 설명")

        reason_rows = ""
        for i, row in recommendations.iterrows():
            rsi_val   = row.get('RSI', None)
            macd_hist = row.get('MACD_Hist', None)
            golden    = row.get('골든크로스', None)
            sentiment = row.get('sentiment_score', None)

            rsi_txt = "-"
            if rsi_val is not None:
                rsi_color = '#3fb950' if rsi_val < 30 else ('#f85149' if rsi_val > 70 else '#ccc')
                rsi_txt = f"<span style='color:{rsi_color}'>RSI {rsi_val:.0f}</span>"

            macd_txt = "-"
            if macd_hist is not None:
                mc = '#f85149' if macd_hist > 0 else '#3b82f6'  # 한국 시장: 상승=빨강, 하락=파랑
                ml = '▲상승' if macd_hist > 0 else '▼하락'
                macd_txt = f"<span style='color:{mc}'>{ml}</span>"

            golden_txt = "<span style='color:#dcb98c'>⭐발생</span>" if golden == 1 else "<span style='color:#555'>-</span>"

            sent_txt = "-"
            if sentiment is not None:
                sc = '#3fb950' if sentiment > 20 else ('#f85149' if sentiment < -20 else '#ccc')
                sl = '긍정' if sentiment > 20 else ('부정' if sentiment < -20 else '중립')
                sent_txt = f"<span style='color:{sc}'>{sl}({sentiment:+.0f})</span>"

            reason = row.get('추천이유', '-')
            name   = row.get('종목명', '')
            score  = row.get('추천점수', 0)
            # 줄바꿈 없이 한 줄로 이어붙임 → 마크다운 코드블록 방지
            reason_rows += (f"<tr>"
                f"<td style='font-weight:700;color:#dcb98c'>#{i+1}</td>"
                f"<td style='font-weight:600;color:#f0e8dc'>{name}</td>"
                f"<td style='text-align:center;font-weight:700;color:#c19b76'>{score:.1f}</td>"
                f"<td style='color:#ccc;font-size:13px'>{reason}</td>"
                f"</tr>")

        # unsafe_allow_html=True + HTML 앞 공백 없애야 마크다운 코드블록 오파싱 방지
        table_html = (
            "<table class='reason-table'>"
            "<thead><tr>"
            "<th>순위</th><th>종목명</th><th>점수</th>"
            "<th>추천이유</th>"
            "</tr></thead>"
            f"<tbody>{reason_rows}</tbody>"
            "</table>"
        )
        st.markdown(table_html, unsafe_allow_html=True)
        st.markdown("&nbsp;", unsafe_allow_html=True)

        # 레이더 차트 (상위 5개 종목)
        if len(recommendations) >= 3:
            st.markdown("### 📡 상위 종목 레이더 차트")
            radar_metrics = ['거래량', '현재가', '거래대금']
            if 'PER' in recommendations.columns:
                radar_metrics.append('PER')
            if '외국인_순매수량' in recommendations.columns:
                radar_metrics.append('외국인_순매수량')

            available_radar = [m for m in radar_metrics if m in recommendations.columns]
            if len(available_radar) >= 3:
                top5_rec = recommendations.head(5)
                fig_radar = go.Figure()

                for _, row in top5_rec.iterrows():
                    values = []
                    for col in available_radar:
                        val = pd.to_numeric(row.get(col, 0), errors='coerce')
                        values.append(val if pd.notna(val) else 0)

                    # 정규화
                    max_val = max(abs(v) for v in values) if values else 1
                    if max_val == 0:
                        max_val = 1
                    normalized = [v / max_val * 100 for v in values]

                    fig_radar.add_trace(go.Scatterpolar(
                        r=normalized + [normalized[0]],
                        theta=available_radar + [available_radar[0]],
                        name=row['종목명'],
                        fill='toself',
                        opacity=0.5,
                    ))

                fig_radar.update_layout(
                    polar=dict(bgcolor='rgba(0,0,0,0)'),
                    template='plotly_dark',
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font=dict(color='#e0e0ff'),
                    height=500,
                    title='상위 종목 비교 레이더',
                )
                st.plotly_chart(fig_radar, use_container_width=True)

    with tab_b:
        st.markdown("### 📈 개별 종목 캔들스틱 차트")

        if hist_df.empty:
            st.info("⏳ 과거 시세 데이터가 없습니다. `python scraper.py`를 실행하면 pykrx로 5일캡 데이터를 수집합니다.")
        else:
            # 추천 종목 중 선택
            rec_tickers = recommendations['종목코드'].tolist() if '종목코드' in recommendations.columns else []
            rec_names = recommendations['종목명'].tolist() if '종목명' in recommendations.columns else []

            available_tickers = [t for t in rec_tickers if t in hist_df['종목코드'].values]
            if available_tickers:
                ticker_name_map = dict(zip(rec_tickers, rec_names))
                display_options = [f"{ticker_name_map.get(t, t)} ({t})" for t in available_tickers]

                selected_display = st.selectbox("종목 선택", display_options, key="candle_stock")
                selected_ticker = available_tickers[display_options.index(selected_display)]

                stock_hist = hist_df[hist_df['종목코드'] == selected_ticker].sort_values('날짜')

                if not stock_hist.empty:
                    # 캔들스틱 & 거래량 통합 차트 생성 (전문 트레이딩 차트 스타일화)
                    from plotly.subplots import make_subplots
                    
                    fig_candle = make_subplots(
                        rows=2, cols=1, 
                        shared_xaxes=True, 
                        vertical_spacing=0.03, 
                        row_heights=[0.75, 0.25]
                    )
                    
                    # 한국 시장 표준 상승(빨강) / 하락(파랑) 적용
                    up_color = '#ef4444'
                    down_color = '#3b82f6'
                    
                    # 캔들스틱 (오버레이 및 색상 조정)
                    fig_candle.add_trace(go.Candlestick(
                        x=stock_hist['날짜'],
                        open=stock_hist['시가'],
                        high=stock_hist['고가'],
                        low=stock_hist['저가'],
                        close=stock_hist['종가'],
                        increasing_line_color=up_color,
                        decreasing_line_color=down_color,
                        increasing_fillcolor=up_color,
                        decreasing_fillcolor=down_color,
                        name='시세'
                    ), row=1, col=1)
                    
                    # 거래량 바 (상승/하락 색상 자동 맞춤)
                    vol_colors = [up_color if row['종가'] >= row['시가'] else down_color for _, row in stock_hist.iterrows()]
                    fig_candle.add_trace(go.Bar(
                        x=stock_hist['날짜'],
                        y=stock_hist['거래량'],
                        marker_color=vol_colors,
                        name='거래량',
                        opacity=0.8
                    ), row=2, col=1)
                    
                    # 레이아웃 프로페셔널 다듬기
                    fig_candle.update_layout(
                        title=dict(
                            text=f"<b>{ticker_name_map.get(selected_ticker, selected_ticker)}</b> 정밀 시세 & 거래량 분해",
                            font=dict(color='#e6edf3', size=18)
                        ),
                        template='plotly_dark',
                        plot_bgcolor='#1e1e1e',
                        paper_bgcolor='rgba(0,0,0,0)',
                        font=dict(color='#e6edf3', size=13),
                        height=550,
                        margin=dict(l=50, r=40, t=60, b=40),
                        showlegend=False,
                        xaxis_rangeslider_visible=False,
                        hovermode='x unified'
                    )
                    
                    # 우측 축 및 그리드 라인 설정으로 고급스러움 연출
                    fig_candle.update_xaxes(showgrid=True, gridwidth=1, gridcolor='#333333', row=1, col=1)
                    fig_candle.update_xaxes(showgrid=True, gridwidth=1, gridcolor='#333333', row=2, col=1)
                    fig_candle.update_yaxes(showgrid=True, gridwidth=1, gridcolor='#333333', side='right', row=1, col=1)
                    fig_candle.update_yaxes(showgrid=True, gridwidth=1, gridcolor='#333333', side='right', row=2, col=1)

                    st.plotly_chart(fig_candle, use_container_width=True)

                    # --- 전문가 분석 코멘트 추가 ---
                    rec_row = recommendations[recommendations['종목코드'].astype(str) == selected_ticker]
                    if not rec_row.empty:
                        expert_score = rec_row.iloc[0]['추천점수']
                        expert_reason = rec_row.iloc[0]['추천이유']
                        
                        st.markdown(
                            f"""
                            <div style="background-color:rgba(30, 41, 59, 0.6); border-left: 5px solid #dcb98c; padding:15px; border-radius:8px; margin-top:20px; font-family:'Pretendard', sans-serif;">
                                <h4 style="margin-top:0; color:#e2e8f0; font-weight:600; font-size:16px;">
                                    💡 퀀트 분석가(Lumina AI)의 정밀 진단 
                                </h4>
                                <p style="color:#94a3b8; font-size:14px; line-height:1.6; margin-bottom:0;">
                                    <strong style="color:#fcd34d;">종합 퀀트 스코어 {expert_score:.1f}점</strong>을 획득하였습니다. <br/>
                                    <strong>{expert_reason}</strong> 등 다방면의 재무/수급/기술적 지표가 복합적으로 우수한 상태를 가리키고 있습니다.<br/>
                                    해당 종목의 최근 수급 및 변동성 브레이크아웃(Breakout) 패턴을 고려할 때, <strong>우상향 랠리 가능성</strong>에 무게를 두는 전략이 유효합니다.
                                </p>
                            </div>
                            """,
                            unsafe_allow_html=True
                        )
            else:
                st.info("추천 종목의 과거 시세 데이터가 없습니다.")

    with tab_c:
        st.markdown("### 추천 종목 등락률 비교")
        if '등락률(숫자)' in recommendations.columns:
            fig_change = px.bar(
                recommendations,
                x='종목명',
                y='등락률(숫자)',
                color='등락률(숫자)',
                color_continuous_scale='RdBu',  # 한국 시장: 상승=빨강, 하락=파랑
                color_continuous_midpoint=0,
                title='추천 종목 등락률',
                template='plotly_dark',
            )
            fig_change.update_layout(
                xaxis_tickangle=-45,
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font=dict(color='#e0e0ff'),
                height=450,
            )
            st.plotly_chart(fig_change, use_container_width=True)

        # (PER/PBR 그래프 삭제됨)
    with tab_d:
        st.markdown("### 추천 종목 상세 데이터")
        display_cols = [
            '종목명', '시장', '현재가', '등락률', '거래량', '거래대금',
            'PER', 'PBR', '배당수익률', '외국인_순매수량', '기관_순매수량',
            '추천점수', '추천이유'
        ]
        avail_cols = [c for c in display_cols if c in recommendations.columns]
        st.dataframe(
            recommendations[avail_cols],
            use_container_width=True,
            hide_index=True,
            height=500,
        )


# ============================================================
# 📰 종목 뉴스
# ============================================================
elif page == "📰 종목 뉴스":
    st.markdown("# 📰 종목 관련 뉴스")

    if news_df.empty:
        st.warning(
            "⚠️ 뉴스 데이터가 없습니다. `python scraper.py`를 실행하여 "
            "뉴스를 수집해 주세요."
        )
        st.stop()

    # 종목별 필터
    if '종목명' in news_df.columns:
        stock_names = ['전체'] + sorted(news_df['종목명'].dropna().unique().tolist())
        selected_stock = st.selectbox("종목 선택", stock_names)

        if selected_stock != '전체':
            display_news = news_df[news_df['종목명'] == selected_stock]
        else:
            display_news = news_df
    else:
        display_news = news_df

    # 뉴스 카드형 표시
    for _, row in display_news.iterrows():
        stock_name = row.get('종목명', row.get('종목코드', ''))
        title = row.get('제목', row.get('뉴스제목', ''))
        date = row.get('날짜', row.get('뉴스날짜', row.get('수집시간', '')))
        source = row.get('출처', row.get('뉴스출처', ''))

        st.markdown(
            f"""
            <div class="stock-card">
                <div style="display:flex; justify-content:space-between;">
                    <span style="color:#58a6ff; font-weight:700;">{stock_name}</span>
                    <span style="color:#8b949e; font-size:13px;">{date}</span>
                </div>
                <div style="margin-top:8px; color:#e6edf3; font-size:15px;">
                    📰 {title}
                </div>
                <div style="margin-top:4px; color:#6e7681; font-size:12px;">
                    {source}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )


# ============================================================
# 📈 분석 신호 (BUY / HOLD / SELL)
# ============================================================
elif page == "📈 분석 신호":
    st.markdown("# 📈 종목별 분석 신호")

    if signals_df.empty:
        st.warning("⚠️ 분석 신호 데이터가 없습니다. `python scraper.py`를 실행해 주세요.")
        st.stop()

    # 시가총액 50위까지만 필터링하고 그 중 20개만 표시 (발표용 요구사항)
    if not stock_df.empty and '시가총액(억)' in stock_df.columns and '종목코드' in stock_df.columns:
        stock_df['시가총액(억)'] = pd.to_numeric(stock_df['시가총액(억)'], errors='coerce')
        top50_tickers = stock_df.sort_values(by='시가총액(억)', ascending=False).head(50)['종목코드'].astype(str).tolist()
        signals_df = signals_df[signals_df['ticker'].astype(str).isin(top50_tickers)].head(20)

    # 종목명 매핑
    if not stock_df.empty and '종목코드' in stock_df.columns:
        name_map = dict(zip(stock_df['종목코드'].astype(str), stock_df['종목명']))
        signals_df['종목명'] = signals_df['ticker'].astype(str).map(name_map).fillna(signals_df['ticker'])
    else:
        signals_df['종목명'] = signals_df['ticker']

    # 신호 요약 카드
    col1, col2, col3, col4 = st.columns(4)
    buy_cnt = (signals_df['signal'] == 'BUY').sum()
    hold_cnt = (signals_df['signal'] == 'HOLD').sum()
    sell_cnt = (signals_df['signal'] == 'SELL').sum()
    with col1:
        st.metric("📊 총 분석", f"{len(signals_df)}개")
    with col2:
        st.metric("🟢 매수(BUY)", f"{buy_cnt}개")
    with col3:
        st.metric("🟡 보유(HOLD)", f"{hold_cnt}개")
    with col4:
        st.metric("🔴 매도(SELL)", f"{sell_cnt}개")

    st.markdown("---")

    # 신호 필터
    signal_filter = st.selectbox("신호 필터", ['전체', '매수', '보유', '매도'], key='sig_filter')
    
    filter_map = {'매수': 'BUY', '보유': 'HOLD', '매도': 'SELL'}
    if signal_filter == '전체':
        display_signals = signals_df
    else:
        display_signals = signals_df[signals_df['signal'] == filter_map[signal_filter]]

    # 추세 점수 바 차트
    # 1. 데이터프레임의 값을 한글로 치환
    display_signals['signal'] = display_signals['signal'].replace({'BUY': '매수', 'HOLD': '보유', 'SELL': '매도'})

    # 2. 컬러 맵도 한글 키값으로 변경
    color_map = {'매수': '#3fb950', '보유': '#d29922', '매도': '#f85149'}

    fig_sig = px.bar(
        display_signals,
        x='종목명',
        y='trend_score',
        color='signal',
        color_discrete_map=color_map,
        labels={
            'BUY': '매수',      # 'BUY'를 '매수 신호'로 변경
            'HOLD': '보유',     # 'HOLD'를 '보유 신호'로 변경
            'SELL': '매도'      # 'SELL'를 '매도 신호'로 변경
        },
        title='종목별 점수 분포 및 매매 신호',
        template='plotly_dark',
        text='trend_score',
    )
    fig_sig.update_traces(texttemplate='%{text:.0f}', textposition='outside')
    fig_sig.update_layout(
        title={
                'font': {'color': "#ffffff", 'size': 20}
                },
        # 각 색상별 어떤 시장인지 표시
        showlegend=True,
        legend=dict(
            title_text='신호',
            font=dict(size=14, color="white"), # 텍스트 크기를 키우고 흰색으로 고정
            orientation="v", # 세로로 나열
            yanchor="top",
            y=0.99,
            xanchor="left",
            x=1.02 # 차트 오른쪽에 범례 표시
            ),
        #xaxis={'categoryorder':'total descending'},
        xaxis_tickangle=-45,
        xaxis=dict(
            {'categoryorder':'total descending'},
            title_font=dict(color="#ffffff",size=18),   # 축 이름 색상
            tickfont=dict(color="#ffffff")   # 축 숫자 색상
            ),
        yaxis=dict(
            title_text='추세 점수',
            title_font=dict(color="#ffffff",size=18),  # 축 이름 색상
            tickfont=dict(color="#ffffff")    # 축 숫자 색상
            ),
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color="#ffffff"),
        height=500,
    )
    # 기준선 추가
    fig_sig.add_hline(y=60, line_dash='dash', line_color='#3fb950')
    fig_sig.add_hline(y=40, line_dash='dash', line_color='#f85149')

    # 범례에만 나타나게 하는 가짜 데이터 추가 (중요: x, y에 아무것도 넣지 않음)
    fig_sig.add_scatter(
        x=[None], 
        y=[None],
        mode='lines',
        line=dict(color='#3fb950', dash='dash'),
        name='매수 기준 (60)',
        showlegend=True
    )

    fig_sig.add_scatter(
        x=[None], 
        y=[None],
        mode='lines',
        line=dict(color='#f85149', dash='dash'),
        name='매도 기준 (40)',
        showlegend=True
    )
    st.plotly_chart(fig_sig, use_container_width=True)

    # 신호 분포 파이 차트
    col_a, col_b = st.columns(2)
    with col_a:
        fig_pie = px.pie(
            signals_df, names='signal',
            color='signal',
            color_discrete_map=color_map,
            title='신호 분포',
            template='plotly_dark',
        )
        fig_pie.update_layout(
            plot_bgcolor='rgba(0,0,0,0)',
            paper_bgcolor='rgba(0,0,0,0)',
            font=dict(color='#e6edf3'),
        )
        st.plotly_chart(fig_pie, use_container_width=True)

    with col_b:
        st.markdown("### 📊 추세 점수 범위 설명")
        st.markdown("""
        | 점수 | 신호 | 의미 |
        |------|------|------|
        | **≥ 60** | 🟢 **매수** | 등락률 + 거래량 + 외국인/기관 추세 양호 |
        | **40~59** | 🟡 **보유** | 동향 혼재, 관망 유지 |
        | **< 40** | 🔴 **매도** | 하락 추세 또는 외국인/기관 순매도 |
        """)
        st.markdown("""
        **추세 점수 산출:**
        - 등락률 (40%) + 거래량 (20%) + 외국인 (20%) + 기관 (20%)
        """)

    # 신호별 종목 카드
    st.markdown("---")
    st.markdown("### 종목별 신호 카드")
    for _, row in display_signals.iterrows():
        sig = row['signal']
        sig_emoji = '🟢' if sig == '매수' else '🟡' if sig == '보유' else '🔴'
        sig_color = color_map.get(sig, "#8b949e") # Fallback color instead of raising KeyError
        st.markdown(
            f"""
            <div class="stock-card">
                <div style="display:flex; justify-content:space-between; align-items:center;">
                    <span style="font-size:16px; font-weight:700; color:#e6edf3;">
                        {row['종목명']}
                    </span>
                    <span style="background:{sig_color}; color:white; padding:4px 14px;
                           border-radius:20px; font-weight:700; font-size:14px;">
                        {sig_emoji} {sig}
                    </span>
                </div>
                <div style="margin-top:8px;">
                    <span style="color:#8b949e;">추세 점수:</span>
                    <strong style="color:white; font-size:18px; margin-left:4px;">
                        {row['trend_score']:.1f}
                    </strong>
                    <span style="color:#6e7681; margin-left:12px;">
                        기간: {row.get('window', '1D')}
                    </span>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )


# ============================================================
# 📧 뉴스레터 미리보기
# ============================================================
elif page == "📧 뉴스레터":
    st.markdown("# 📧 투자 뉴스레터 미리보기")
    
    # 로그인 체크
    if not st.session_state['logged_in']:
        @st.dialog("로그인 안내")
        def show_login_dialog():
            st.warning("⚠️ 뉴스레터 구독 및 열람은 로그인이 필요합니다.")
            st.info("좌측 사이드바에서 로그인 후 이용해 주세요.")
            if st.button("홈으로 돌아가기", key="newsletter_login_home_btn"):
                st.session_state['current_page'] = "🏠 메인 대시보드"
                if 'menu_radio' in st.session_state:
                    del st.session_state['menu_radio']
                st.rerun()
        show_login_dialog()
        st.stop()

    if stock_df.empty:
        st.warning("⚠️ 데이터가 없습니다.")
        st.stop()

    # ── [신규 추가] 뉴스레터 심야/아침(00:00 ~ 08:59) 비활성화 ──
    current_hour = datetime.now().hour
    if 0 <= current_hour < 9:
        st.info("🌙 **현재는 정규장 개장 전입니다.**\n\n전일의 낡은 뉴스레터를 삭제(초기화)했습니다. 오늘의 새로운 맞춤 뉴스레터는 데이터 정비 후 **오전 9시 이후**부터 발행됩니다!")
        st.stop()

    # 성향 선택
    inv_type = st.selectbox(
        "투자 성향 선택",
        ['안정형', '안정추구형', '위험중립형', '적극투자형', '공격투자형'],
        index=2,
        key='newsletter_type'
    )

    type_info = TYPE_DESCRIPTIONS[inv_type]
    st.markdown(
        f"**{type_info['emoji']} {type_info['title']}** — _{type_info['strategy']}_"
    )

    # 뉴스레터 생성 (DB 데이터 우선 사용)
    if not newsletters_df.empty:
        # DB에서 현재 성향에 맞는 뉴스레터 찾기 (type_id 매칭 등)
        # 여기서는 가장 최근 것을 가져옴
        newsletter = newsletters_df.iloc[-1].to_dict()
    else:
        scored = score_stocks(stock_df, inv_type)
        newsletter = generate_newsletter(
            stock_df=stock_df,
            scored_df=scored,
            signals_df=signals_df,
            investor_type=inv_type,
            user_id=1,
            news_df=news_df,
        )

    st.markdown("---")
    st.markdown(f"### {newsletter['title']}")

    # 뉴스레터 본문 표시
    st.markdown(
        f"""
        <div style="background:rgba(22,27,34,0.9); border:1px solid rgba(255,255,255,0.1);
             border-radius:12px; padding:24px; font-family:monospace;
             white-space:pre-wrap; color:#c9d1d9; line-height:1.8; font-size:14px;">
{newsletter['content']}
        </div>
        """,
        unsafe_allow_html=True,
    )

    # 다운로드 버튼
    st.download_button(
        label="💾 뉴스레터 다운로드 (.txt)",
        data=newsletter['content'],
        file_name=f"newsletter_{inv_type}_{datetime.now().strftime('%Y%m%d')}.txt",
        mime='text/plain',
    )
