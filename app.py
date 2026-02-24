"""
📊 투자 성향별 주식 추천 시스템
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
    page_title="📊 투자 성향별 주식 추천 시스템",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ============================================================
# 0. 데이터 로드
# ============================================================
DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')


@st.cache_data(ttl=300)
def load_latest_data():
    """data/ 디렉토리에서 최신 CSV 파일을 로드합니다."""
    stock_files = sorted(glob.glob(os.path.join(DATA_DIR, 'stock_data_*.csv')))
    news_files = sorted(glob.glob(os.path.join(DATA_DIR, 'stock_news_*.csv')))
    hist_files = sorted(glob.glob(os.path.join(DATA_DIR, 'historical_*.csv')))
    signal_files = sorted(glob.glob(os.path.join(DATA_DIR, 'analysis_signals_*.csv')))

    # data/ 폴더에 없으면 프로젝트 루트에서도 탐색
    if not stock_files:
        root_dir = os.path.dirname(os.path.abspath(__file__))
        stock_files = sorted(glob.glob(os.path.join(root_dir, 'stock_data_*.csv')))
        news_files = sorted(glob.glob(os.path.join(root_dir, 'stock_news_*.csv')))

    stock_df = pd.DataFrame()
    news_df = pd.DataFrame()
    hist_df = pd.DataFrame()
    signals_df = pd.DataFrame()

    if stock_files:
        stock_df = pd.read_csv(stock_files[-1])
        st.session_state['data_file'] = os.path.basename(stock_files[-1])
    if news_files:
        news_df = pd.read_csv(news_files[-1])
    if hist_files:
        hist_df = pd.read_csv(hist_files[-1])
    if signal_files:
        signals_df = pd.read_csv(signal_files[-1])

    # signals가 없으면 실시간 생성
    if signals_df.empty and not stock_df.empty:
        signals_df = generate_analysis_signals(stock_df, '1D')

    return stock_df, news_df, hist_df, signals_df


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

    /* 일반 텍스트 밝게 조정 (오류 방지를 위해 div 제외) */
    p, span {
        color: #f2ece4 !important;
    }

    /* 드롭다운 (셀렉트박스) 내부 텍스트 및 팝업창 스타일 */
    .stSelectbox div[data-baseweb="select"] > div {
        background-color: rgba(55, 50, 46, 0.9) !important;
        color: #f2ece4 !important;
    }
    
    div[role="listbox"] {
        background-color: #302b28 !important;
        border: 1px solid rgba(220, 185, 140, 0.3) !important;
        border-radius: 8px !important;
    }
    
    div[role="listbox"] ul li {
        color: #f2ece4 !important;
        background-color: transparent !important;
    }
    
    div[role="listbox"] ul li:hover {
        background-color: rgba(220, 185, 140, 0.2) !important;
        color: #dcb98c !important;
    }


    table {
        color: #f2ece4 !important;
    }
    
    th, td {
        border-bottom: 1px solid rgba(220, 185, 140, 0.2) !important;
    }
    
    th {
        color: #dcb98c !important;
        font-weight: 700 !important;
    }

    /* 버튼 */
    .stButton > button {
        background: linear-gradient(135deg, #a67c52, #c19b76);
        color: white;
        border: none;
        border-radius: 8px;
        font-weight: 600;
        box-shadow: 0 4px 10px rgba(0,0,0,0.2);
    }
    .stButton > button:hover {
        background: linear-gradient(135deg, #c19b76, #dcb98c);
        box-shadow: 0 4px 15px rgba(220, 185, 140, 0.3);
    }

    /* 익스팬더 (펼쳐보기) 스타일 수정 */
    [data-testid="stExpander"] {
        background-color: rgba(55, 50, 46, 0.7) !important;
        border: 1px solid rgba(220, 185, 140, 0.25) !important;
        border-radius: 12px !important;
    }
    [data-testid="stExpander"] summary {
        color: #dcb98c !important;
        font-weight: 600 !important;
    }
    [data-testid="stExpander"] summary:hover {
        color: #f2ece4 !important;
    }

    /* 경고/정보 박스 */
    .stWarning, .stInfo {
        background: rgba(55, 50, 46, 0.9) !important;
        border-left-color: #dcb98c !important;
    }
</style>
""", unsafe_allow_html=True)


# ============================================================
# 🔐 사용자 인증 시스템 (Phase 10)
# ============================================================
import json
import bcrypt as _bcrypt  # passlib 대신 raw bcrypt 사용 (backend 호환 문제 해결)
import os

USERS_DB_FILE = os.path.join(DATA_DIR, 'users_db.json')

def load_users():
    if not os.path.exists(USERS_DB_FILE):
        return {}
    with open(USERS_DB_FILE, 'r', encoding='utf-8') as f:
        return json.load(f)

def save_users(users):
    with open(USERS_DB_FILE, 'w', encoding='utf-8') as f:
        json.dump(users, f, indent=4)

# bcrypt는 최대 72바이트 제한 → raw bcrypt로 안전하게 처리
def _safe_hash(password: str) -> str:
    pw_bytes = password.encode('utf-8')[:72]
    return _bcrypt.hashpw(pw_bytes, _bcrypt.gensalt()).decode('utf-8')

def _safe_verify(password: str, hashed: str) -> bool:
    pw_bytes = password.encode('utf-8')[:72]
    return _bcrypt.checkpw(pw_bytes, hashed.encode('utf-8'))

if 'logged_in' not in st.session_state:
    st.session_state['logged_in'] = False
if 'username' not in st.session_state:
    st.session_state['username'] = ""

# ============================================================
# 사이드바 네비게이션 & 로그인 폼
# ============================================================
with st.sidebar:
    st.markdown("## 📊 주식 추천 시스템")
    st.markdown("---")
    
    # 로그인 폼 구성
    if not st.session_state['logged_in']:
        tab_login, tab_signup = st.tabs(["🔑 로그인", "📝 회원가입"])
        
        with tab_login:
            login_id = st.text_input("아이디", key="login_id")
            login_pw = st.text_input("비밀번호", type="password", key="login_pw")
            if st.button("로그인", use_container_width=True):
                users = load_users()
                if login_id in users:
                    user_data = users[login_id]
                    # 구버전 호환성 (단순 string 해시 비밀번호인 경우)
                    if isinstance(user_data, str):
                        hashed_pw = user_data
                    else:
                        hashed_pw = user_data.get("user_password", "")
                        
                    if _safe_verify(login_pw, hashed_pw):
                        st.session_state['logged_in'] = True
                        st.session_state['username'] = login_id
                        st.success("로그인 성공!")
                        st.rerun()
                    else:
                        st.error("아이디 또는 비밀번호가 틀렸습니다.")
                else:
                    st.error("아이디 또는 비밀번호가 틀렸습니다.")
                    
        with tab_signup:
            new_id = st.text_input("새 아이디", key="new_id")
            new_email = st.text_input("이메일 주소", key="new_email")
            new_pw = st.text_input("새 비밀번호", type="password", key="new_pw")
            new_pw_check = st.text_input("비밀번호 확인", type="password", key="new_pw_chk")
            
            if st.button("가입하기", use_container_width=True):
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
                        "user_email": new_email,
                        "type_id": "미정" # 설문 전 기본값
                    }
                    save_users(users)
                    st.success("회원가입이 완료되었습니다! 로그인해주세요.")
    else:
        st.success(f"👋 환영합니다, **{st.session_state['username']}**님!")
        if st.button("로그아웃", use_container_width=True):
            st.session_state['logged_in'] = False
            st.session_state['username'] = ""
            st.rerun()
            
    st.markdown("---")

    page = st.radio(
        "메뉴 선택",
        ["🏠 메인 대시보드", "📋 투자 성향 설문", "⭐ 맞춤 종목 추천",
         "📈 분석 신호", "📰 종목 뉴스", "📧 뉴스레터"],
        label_visibility="collapsed",
    )

    st.markdown("---")
    
    if st.button("🔄 데이터 새로고침", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    # 데이터 파일 정보
    if 'data_file' in st.session_state:
        st.caption(f"📁 {st.session_state['data_file']}")

    st.markdown("---")
    st.markdown(
        "<div style='color:#888; font-size:12px; text-align:center;'>"
        "네이버 증권 데이터 기반<br>"
        "투자 성향 5단계 분류<br>"
        "© 2026 Stock Recommender"
        "</div>",
        unsafe_allow_html=True,
    )


# ============================================================
# 📌 데이터 로드
# ============================================================
stock_df, news_df, hist_df, signals_df = load_latest_data()


# ============================================================
# 🏠 메인 대시보드
# ============================================================
if page == "🏠 메인 대시보드":
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
                
                # 순위 표시 추가 (1~50)
                rank = i + 1
                label_with_rank = f"{rank}. {row.종목명}"
                
                cols[col_idx].metric(
                    label=label_with_rank, 
                    value=price, 
                    delta=change,
                    delta_color="normal"
                )
    else:
        st.info("수집된 데이터가 없습니다.")
        
    st.markdown("---")

    # ── 요약 통계 ──
    summary = generate_analysis_summary(stock_df)

    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("📈 총 종목 수", summary.get('총 종목 수', 0))
    with col2:
        st.metric("🟢 상승", summary.get('상승 종목 수', 0))
    with col3:
        st.metric("🔴 하락", summary.get('하락 종목 수', 0))
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
        filtered = stock_df if market_filter == "전체" else stock_df[stock_df['시장'] == market_filter]
        top20 = filtered.head(20)

        if not top20.empty:
            fig = px.bar(
                top20,
                x='종목명',
                y='거래량',
                color='시장',
                color_discrete_map={'KOSPI': '#dcb98c', 'KOSDAQ': '#8a735c'},
                title='거래량 상위 종목',
                template='plotly_dark',
            )
            fig.update_layout(
                xaxis_tickangle=-45,
                plot_bgcolor='rgba(0,0,0,0)',
                paper_bgcolor='rgba(0,0,0,0)',
                font=dict(color='#f2ece4'),
                height=500,
            )
            st.plotly_chart(fig, use_container_width=True)

            # 등락률 산점도
            if '등락률(숫자)' in top20.columns:
                fig2 = px.scatter(
                    top20,
                    x='거래량',
                    y='등락률(숫자)',
                    size='거래대금',
                    color='시장',
                    hover_name='종목명',
                    color_discrete_map={'KOSPI': '#667eea', 'KOSDAQ': '#764ba2'},
                    title='거래량 vs 등락률 (버블 크기 = 거래대금)',
                    template='plotly_dark',
                )
                fig2.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font=dict(color='#e0e0ff'),
                    height=500,
                )
                st.plotly_chart(fig2, use_container_width=True)

    with tab2:
        st.markdown("### 외국인/기관 매매 동향")

        if '외국인_순매수량' in stock_df.columns and '기관_순매수량' in stock_df.columns:
            inv_df = stock_df[['종목명', '외국인_순매수량', '기관_순매수량']].dropna()

            if not inv_df.empty:
                # 상위 N개만 표시 (가독성 목적)
                top_n_display = st.slider("표시할 종목 수 (외국인 순매수 기준)", 10, 50, 20)
                inv_df_top = inv_df.sort_values('외국인_순매수량', ascending=False).head(top_n_display)

                fig3 = go.Figure()
                fig3.add_trace(go.Bar(
                    x=inv_df_top['종목명'],
                    y=inv_df_top['외국인_순매수량'],
                    name='외국인',
                    marker_color='#dcb98c',
                ))
                fig3.add_trace(go.Bar(
                    x=inv_df_top['종목명'],
                    y=inv_df_top['기관_순매수량'],
                    name='기관',
                    marker_color='#8a735c',
                ))
                fig3.update_layout(
                    title=f'외국인/기관 순매수량 비교 (상위 {top_n_display}종목)',
                    barmode='group',
                    template='plotly_dark',
                    plot_bgcolor='rgba(0,0,0,0)',
                    paper_bgcolor='rgba(0,0,0,0)',
                    font=dict(color='#f2ece4'),
                    xaxis_tickangle=-45,
                    height=500,
                )
                st.plotly_chart(fig3, use_container_width=True)

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

        submitted = st.form_submit_button(
            "🔍 투자 성향 진단하기",
            use_container_width=True,
        )

    if submitted:
        investor_type, total_score = classify_investor_type(answers)
        st.session_state['investor_type'] = investor_type
        st.session_state['survey_score'] = total_score
        st.session_state['survey_answers'] = answers
        
        # 로그인 되어있다면 유저 DB에 투자 성향(type_id) 업데이트
        if st.session_state.get('logged_in'):
            user_id = st.session_state.get('username')
            if user_id:
                users = load_users()
                if user_id in users and isinstance(users[user_id], dict):
                    users[user_id]['type_id'] = investor_type
                    save_users(users)
                    st.toast(f"✅ {user_id}님의 투자 성향({investor_type})이 저장되었습니다!")

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
    if not st.session_state['logged_in']:
        st.warning("⚠️ 맞춤 종목 추천 서비스는 로그인이 필요합니다. 좌측 메뉴에서 로그인해주세요.")
        st.stop()

    if stock_df.empty:
        st.warning("⚠️ 주식 데이터가 없습니다. 먼저 `python scraper.py`를 실행해 주세요.")
        st.stop()

    # ── 투자 성향 확인 ──
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

    # ── [Phase 1] 기술적 지표 계산 ──
    tech_df = pd.DataFrame()
    if not hist_df.empty:
        try:
            from analyzer import calculate_technical_indicators
            tech_df = calculate_technical_indicators(hist_df)
        except Exception as e:
            st.caption(f"⚠️ 기술적 지표 계산 실패: {e}")

    # ── [Phase 4] 뉴스 감성 분석 ──
    sentiment_df = pd.DataFrame()
    if not news_df.empty:
        try:
            from analyzer import analyze_news_sentiment
            sentiment_df = analyze_news_sentiment(news_df)
        except Exception as e:
            st.caption(f"⚠️ 감성 분석 실패: {e}")

    # ── [Phase 2] pykrx 재무지표 (캐시 처리로 반복 호출 방지) ──
    @st.cache_data(ttl=3600, show_spinner=False)
    def load_fundamentals(tickers_tuple):
        try:
            from scraper import scrape_fundamentals
            return scrape_fundamentals(list(tickers_tuple))
        except Exception:
            return pd.DataFrame()

    fund_df = pd.DataFrame()
    if '종목코드' in filtered_df.columns:
        tickers_tuple = tuple(filtered_df['종목코드'].tolist())
        with st.spinner("📊 재무지표 불러오는 중..."):
            fund_df = load_fundamentals(tickers_tuple)
        if not fund_df.empty:
            filtered_df = filtered_df.merge(fund_df, on='종목코드', how='left')

    # ── [Phase 3] 증권사 목표주가 (캐시 처리) ──
    @st.cache_data(ttl=3600, show_spinner=False)
    def load_analyst():
        try:
            from scraper import scrape_analyst_opinion
            name_map = dict(zip(stock_df['종목코드'], stock_df['종목명'])) if '종목코드' in stock_df.columns else {}
            return scrape_analyst_opinion({v: k for k, v in name_map.items()})
        except Exception:
            return pd.DataFrame()

    analyst_df = load_analyst()

    # ── 추천 종목 계산 (모든 지표 통합) ──
    from analyzer import score_stocks
    scored = score_stocks(
        filtered_df, investor_type,
        tech_df=tech_df if not tech_df.empty else None,
        sentiment_df=sentiment_df if not sentiment_df.empty else None,
        analyst_df=analyst_df if not analyst_df.empty else None,
    )
    recommendations = scored.head(top_n)

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
                change_color = '#3fb950' if row.get('전일비', 0) > 0 else '#f85149'

                # 기술적 지표 배지 생성
                rsi_val   = row.get('RSI', None)
                macd_hist = row.get('MACD_Hist', None)
                golden    = row.get('골든크로스', None)
                sentiment = row.get('sentiment_score', None)

                badges = ""
                if rsi_val is not None:
                    rsi_color = '#3fb950' if rsi_val < 30 else ('#f85149' if rsi_val > 70 else '#8b949e')
                    badges += f"<span style='background:{rsi_color}22; color:{rsi_color}; padding:2px 8px; border-radius:10px; font-size:11px; margin-right:4px;'>RSI {rsi_val:.0f}</span>"
                if macd_hist is not None:
                    m_color = '#3fb950' if macd_hist > 0 else '#f85149'
                    m_label = '▲MACD' if macd_hist > 0 else '▼MACD'
                    badges += f"<span style='background:{m_color}22; color:{m_color}; padding:2px 8px; border-radius:10px; font-size:11px; margin-right:4px;'>{m_label}</span>"
                if golden == 1:
                    badges += "<span style='background:#dcb98c22; color:#dcb98c; padding:2px 8px; border-radius:10px; font-size:11px; margin-right:4px;'>⭐골든크로스</span>"
                if sentiment is not None and sentiment > 20:
                    badges += "<span style='background:#3fb95022; color:#3fb950; padding:2px 8px; border-radius:10px; font-size:11px;'>😀긍정뉴스</span>"
                elif sentiment is not None and sentiment < -20:
                    badges += "<span style='background:#f8514922; color:#f85149; padding:2px 8px; border-radius:10px; font-size:11px;'>😟부정뉴스</span>"

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
                        <div style="margin-top:6px;">{badges}</div>
                        <div style="margin-top:6px; color:#8b949e; font-size:13px;">
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
                    # 캔들스틱 차트
                    fig_candle = go.Figure(data=[go.Candlestick(
                        x=stock_hist['날짜'],
                        open=stock_hist['시가'],
                        high=stock_hist['고가'],
                        low=stock_hist['저가'],
                        close=stock_hist['종가'],
                        increasing_line_color='#3fb950',
                        decreasing_line_color='#f85149',
                    )])
                    fig_candle.update_layout(
                        title=f"{ticker_name_map.get(selected_ticker, selected_ticker)} 5일 캔들스틱",
                        template='plotly_dark',
                        plot_bgcolor='rgba(0,0,0,0)',
                        paper_bgcolor='rgba(0,0,0,0)',
                        font=dict(color='#e6edf3'),
                        height=450,
                        xaxis_rangeslider_visible=False,
                    )
                    st.plotly_chart(fig_candle, use_container_width=True)

                    # 거래량 차트
                    fig_vol = px.bar(
                        stock_hist, x='날짜', y='거래량',
                        title=f"{ticker_name_map.get(selected_ticker, '')} 거래량 추이",
                        template='plotly_dark',
                        color_discrete_sequence=['#58a6ff'],
                    )
                    fig_vol.update_layout(
                        plot_bgcolor='rgba(0,0,0,0)',
                        paper_bgcolor='rgba(0,0,0,0)',
                        font=dict(color='#e6edf3'),
                        height=300,
                    )
                    st.plotly_chart(fig_vol, use_container_width=True)
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
                color_continuous_scale='RdYlGn',
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

        # PER / PBR 분포 (Seaborn)
        if 'PER' in recommendations.columns and 'PBR' in recommendations.columns:
            st.markdown("### PER / PBR 분포")
            fig_pp, axes = plt.subplots(1, 2, figsize=(14, 5))
            fig_pp.patch.set_facecolor('#1a1a2e')

            for ax in axes:
                ax.set_facecolor('#1a1a2e')
                ax.tick_params(colors='white')
                ax.xaxis.label.set_color('white')
                ax.yaxis.label.set_color('white')

            per_data = pd.to_numeric(recommendations['PER'], errors='coerce').dropna()
            pbr_data = pd.to_numeric(recommendations['PBR'], errors='coerce').dropna()

            if not per_data.empty:
                sns.histplot(per_data, kde=True, ax=axes[0], color='#667eea')
                axes[0].set_title('PER 분포', color='white', fontsize=13)
                axes[0].set_xlabel('PER')

            if not pbr_data.empty:
                sns.histplot(pbr_data, kde=True, ax=axes[1], color='#764ba2')
                axes[1].set_title('PBR 분포', color='white', fontsize=13)
                axes[1].set_xlabel('PBR')

            plt.tight_layout()
            st.pyplot(fig_pp)
            plt.close()

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
    signal_filter = st.selectbox("신호 필터", ['전체', 'BUY', 'HOLD', 'SELL'], key='sig_filter')
    display_signals = signals_df if signal_filter == '전체' else signals_df[signals_df['signal'] == signal_filter]

    # 추세 점수 바 차트
    color_map = {'BUY': '#3fb950', 'HOLD': '#d29922', 'SELL': '#f85149'}
    display_signals = display_signals.sort_values('trend_score', ascending=False)

    fig_sig = px.bar(
        display_signals,
        x='종목명',
        y='trend_score',
        color='signal',
        color_discrete_map=color_map,
        title='종목별 추세 점수 및 매매 신호',
        template='plotly_dark',
        text='trend_score',
    )
    fig_sig.update_traces(texttemplate='%{text:.0f}', textposition='outside')
    fig_sig.update_layout(
        xaxis_tickangle=-45,
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(color='#e6edf3'),
        height=500,
    )
    # 기준선 추가
    fig_sig.add_hline(y=60, line_dash='dash', line_color='#3fb950',
                      annotation_text='BUY 기준(60)', annotation_position='top left')
    fig_sig.add_hline(y=40, line_dash='dash', line_color='#f85149',
                      annotation_text='SELL 기준(40)', annotation_position='bottom left')
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
        | **≥ 60** | 🟢 **BUY** | 등락률 + 거래량 + 외국인/기관 추세 양호 |
        | **40~59** | 🟡 **HOLD** | 동향 혼재, 관망 유지 |
        | **< 40** | 🔴 **SELL** | 하락 추세 또는 외국인/기관 순매도 |
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
        sig_emoji = '🟢' if sig == 'BUY' else '🟡' if sig == 'HOLD' else '🔴'
        sig_color = color_map[sig]
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
        st.warning("⚠️ 뉴스레터 구독 및 열람은 로그인이 필요합니다. 좌측 메뉴에서 로그인해주세요.")
        st.stop()

    if stock_df.empty:
        st.warning("⚠️ 데이터가 없습니다.")
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

    # 뉴스레터 생성
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
