# 📊 투자 성향별 주식 추천 웹앱

> 네이버 금융 데이터 기반 | Streamlit 대시보드 | 투자 성향 맞춤 추천

---

## 🎯 프로젝트 목표

- 네이버 증권 웹스크래핑으로 거래량·외국인/기관 매매 데이터 수집
- 한양증권 기준 11문항 설문 → **5단계 투자 성향 분류**
- 성향별 맞춤 종목 추천 및 인터랙티브 시각화
- Streamlit 기반 대시보드 웹앱
- 회원가입/로그인 기반 개인화 서비스

---

## 🛠️ 기술 스택

| 분류 | 기술 |
|------|------|
| 웹 스크래핑 | `requests`, `BeautifulSoup4`, `pykrx` |
| 데이터 분석 | `pandas`, `numpy` |
| 시각화 | `plotly`, `matplotlib`, `seaborn` |
| 웹앱 | `streamlit` |
| 데이터 저장 | `pymysql`, `sqlalchemy` / CSV fallback |
| 인증 | `bcrypt` |
| 자동화 | `apscheduler` |
| 환경 관리 | `uv`, `python-dotenv` |

---

## 📁 프로젝트 구조

```
miniproject_1/
├── app.py              # Streamlit 웹앱 (메인)
├── scraper.py          # 네이버 증권 스크래핑 모듈
├── analyzer.py         # 투자 성향 분석 & 추천 엔진
├── rtd_analyzer.py     # 실시간 거래량 모멘텀 분석
├── scheduler_job.py    # APScheduler 자동 수집기
├── fetch_inv.py        # 외국인/기관 순매수 수집
├── config.py           # 환경 설정 관리
├── db_manager.py       # DB 연동 (CSV fallback 포함)
├── schema.sql          # MySQL 스키마 정의
├── CHANGELOG.md        # 개발 변경 이력
├── .env.example        # 환경 변수 템플릿
├── pyproject.toml      # 의존성 관리 (uv)
└── data/               # 로컬 데이터 저장
    ├── stock_data_YYYYMMDD.csv
    ├── stock_market_data_fallback.csv
    └── users_db.json   # 사용자 계정 정보
```

---

## 🚀 설치 및 실행

```bash
# 1. 의존성 설치
uv sync

# 2. 데이터 수집 (최초 1회)
uv run python scraper.py

# 3. Streamlit 앱 실행
uv run streamlit run app.py

# 4. 자동 수집 스케줄러 실행 (별도 터미널)
uv run python scheduler_job.py
```

> 💡 `scheduler_job.py`는 평일 **09:00 ~ 15:00 매 정각**에 자동으로 시세를 수집합니다.

---

## 🗂️ 데이터 파이프라인

```
데이터 수집 (scraper.py / scheduler_job.py)
    ├── [requests+BS4] KOSPI/KOSDAQ 거래량 상위 100종목
    ├── [requests+BS4] 외국인/기관 순매수 데이터
    └── [pykrx] 7일치 과거 시세 (OHLCV)
         ↓
데이터 정제 & CSV / MySQL 저장 (db_manager.py)
         ↓
투자 성향 분석 (analyzer.py)
    ├── 11문항 설문 → 5단계 성향 분류
    ├── trend_score 산출 (등락률 40% + 거래량 20% + 외국인 20% + 기관 20%)
    └── BUY / HOLD / SELL 신호 자동 생성
         ↓
실시간 분석 (rtd_analyzer.py)
    ├── 직전 시간 대비 거래량 급증 TOP 10 (seaborn)
    └── 현재가 vs 거래대금 산점도 (matplotlib)
         ↓
Streamlit 대시보드 (app.py)
    ├── 🏠 시장 개요 (Top 50 시세 + 거래량 차트)
    ├── ⏱️ 실시간 분석 RTD
    ├── 📋 투자 성향 설문
    ├── ⭐ 맞춤 종목 추천 [🔐 로그인 필요]
    ├── 📈 분석 신호 (BUY/HOLD/SELL)
    ├── 📰 종목 뉴스
    └── 📧 뉴스레터 [🔐 로그인 필요]
```

---

## 👤 투자 성향 5단계

| 성향 | 설명 | 추천 전략 |
|------|------|-----------| 
| 🛡️ 안정형 | 원금 손실 미허용 | 고배당 대형 우량주 |
| 🔒 안정추구형 | 안정적 투자, 일부 변동 허용 | 배당+가치주, 기관 매수 |
| ⚖️ 위험중립형 | 일정 손실 감수 | 성장주, 외국인 매수 |
| 🚀 적극투자형 | 높은 수익 추구 | 모멘텀, 고변동성 |
| 🔥 공격투자형 | 시장 초과 수익 추구 | 테마주, 최고 거래량 |

---

## 🔐 회원 인증 시스템

회원가입 시 **아이디 / 이메일 / 비밀번호**를 입력하여 계정을 생성합니다.

**사용자 데이터 구조 (`data/users_db.json`)**

```json
{
  "userid": {
    "user_password": "$2b$12$...",
    "user_email": "user@example.com",
    "type_id": "위험중립형"
  }
}
```

| 항목 | 내용 |
|------|------|
| 비밀번호 암호화 | `bcrypt` (`hashpw` / `checkpw`) |
| 성향 저장 | 설문 완료 시 `type_id` 자동 업데이트 |
| 접근 제어 | 추천/뉴스레터 페이지 미로그인 차단 |

---

## 🗄️ DB 스키마 (MySQL)

```sql
-- 시간별 시세 데이터 (scheduler_job.py가 매 정각 INSERT)
CREATE TABLE stock_market_data (
    종목코드   VARCHAR(10),
    종목명     VARCHAR(50),
    시장       VARCHAR(10),
    현재가     BIGINT,
    전일비     BIGINT,
    등락률     VARCHAR(10),
    등락률_num FLOAT,
    거래량     BIGINT,
    거래대금   BIGINT,
    수집시간   DATETIME
);

-- 사용자 계정
CREATE TABLE users (
    user_id       VARCHAR(50) PRIMARY KEY,
    user_password VARCHAR(200),
    user_email    VARCHAR(100),
    type_id       VARCHAR(20) DEFAULT '미정'
);
```

> DB 미연결 시 `data/` 폴더에 CSV로 자동 백업됩니다.

---

## 📦 주요 의존성

```toml
dependencies = [
    "streamlit", "pykrx", "pandas", "numpy",
    "plotly", "seaborn", "matplotlib",
    "requests", "beautifulsoup4",
    "sqlalchemy", "pymysql",
    "apscheduler", "bcrypt", "python-dotenv"
]
```

---

## 🐛 주요 버그 수정 이력

| 버그 | 원인 | 해결 |
|------|------|------|
| 셀렉트박스 드롭다운 텍스트 안보임 | CSS 포탈 컨테이너 미처리 | `div[role="listbox"]` 별도 스타일 적용 |
| 외국인 데이터 병합 오류 | 종목코드 int/str 타입 불일치 | `zfill(6)` 정규화 후 병합 |
| bcrypt 72바이트 에러 | `passlib` + `bcrypt>=4` 호환 문제 | raw `bcrypt` 직접 사용 전환 |
| Expander 배경 밝음 | CSS 미지정 | `[data-testid="stExpander"]` 다크 스타일 추가 |
| `SyntaxError` in app.py | replace 오류로 두 문장 합쳐짐 | 개행 복구 |

---

*© 2026 1팀 미니프로젝트 — 주식 추천 시스템*
