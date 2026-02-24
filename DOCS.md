# 📊 주식 흐름 시장 분석 & 투자 성향별 종목 추천 시스템

## 📌 목차
1. [프로젝트 기획 및 주제 정의](#1-프로젝트-기획-및-주제-정의)
2. [데이터 수집 및 정제](#2-데이터-수집-및-정제)
3. [데이터 분석 및 시각화](#3-데이터-분석-및-시각화)
4. [대시보드(웹앱) 구현](#4-대시보드웹앱-구현)
5. [프로젝트 구조 및 실행 방법](#5-프로젝트-구조-및-실행-방법)

---

## 1. 프로젝트 기획 및 주제 정의

### 1-1. 주제 선정의 타당성

주식 시장에서 개인 투자자들은 종종 본인의 투자 성향을 정확히 파악하지 못한 채 투자에 임하고 있습니다.
이로 인해 과도한 위험 노출이나 지나치게 보수적인 투자로 자산 성장의 기회를 놓치는 경우가 발생합니다.

**본 프로젝트는 이러한 문제를 해결하기 위해:**
- 네이버 증권에서 **실시간 거래 데이터**를 수집
- 한양증권 투자성향진단 기준 **11문항 설문으로 5단계 투자 성향**을 분류
- 성향에 맞는 **맞춤형 종목을 추천**하는 웹앱을 개발했습니다

### 1-2. 문제 정의

| 문제 | 설명 |
|------|------|
| **정보 비대칭** | 개인 투자자는 외국인/기관 매매 동향을 실시간으로 파악하기 어려움 |
| **성향 미파악** | 자신의 투자 성향을 모른 채 투자하여 부적절한 종목 선택 |
| **데이터 접근성** | 증권사 API 없이도 활용할 수 있는 데이터 수집 방법 필요 |

### 1-3. 해결 방안

```
웹 스크래핑(F12 개발자도구 분석) → 데이터 정제 → 성향 분석 → 맞춤 종목 추천
```

---

## 2. 데이터 수집 및 정제

### 2-1. 웹스크래핑 구현 (scraper.py)

증권 API를 사용하지 않고, 네이버 증권 사이트를 **F12 개발자도구로 분석**하여 직접 스크래핑합니다.

#### ✅ requests + BeautifulSoup 활용

| 수집 대상 | URL | 설명 |
|-----------|-----|------|
| 거래량 상위 종목 | `sise_quant.naver` | KOSPI/KOSDAQ 각 20개 (총 40개) |
| 종목 상세 정보 | `item/main.naver` | PER, PBR, 배당수익률, 52주 최고/최저, 시가총액 |

```python
# requests + BeautifulSoup 사용 예시 (scraper.py)
def scrape_top_volume(market="KOSPI", limit=20, session=None):
    """네이버 금융 거래량 상위 종목 스크래핑"""
    session = session or create_session()  # 재시도 로직 포함
    url = f"https://finance.naver.com/sise/sise_quant.naver?sosok={sosok}"
    resp = session.get(url, timeout=10)
    resp.encoding = 'euc-kr'
    soup = BeautifulSoup(resp.text, 'html.parser')
    rows = soup.select('table.type_2 tr')
    # ... 데이터 파싱 및 정제
```

**코드 품질 포인트:**
- `requests.Session` + `Retry` 어댑터로 네트워크 장애 시 **3회 자동 재시도**
- `logging` 모듈을 사용한 체계적 로그 관리 (`print` 미사용)
- 에러 발생 시 구체적 예외 처리 (bare `except` 미사용)

#### ✅ Selenium 활용

| 수집 대상 | URL | 설명 |
|-----------|-----|------|
| 외국인/기관 매매 동향 | `item/frgn.naver` | 순매수량, 외국인 보유비율 |
| 종목별 최신 뉴스 | `item/news_news.naver` | 제목, 날짜, 출처 |

```python
# Selenium 사용 예시 (scraper.py)
def scrape_investor_trend(ticker, driver=None):
    """Selenium으로 외국인/기관 매매 동향 수집"""
    driver.get(f"https://finance.naver.com/item/frgn.naver?code={ticker}")
    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, 'table.type2'))
    )
    # ... 동적 페이지 데이터 추출
```

**Selenium이 필요한 이유:**
- 외국인/기관 매매 데이터는 JavaScript 동적 렌더링으로 생성
- `requests`로는 접근 불가 → `Selenium` headless Chrome 사용

### 2-2. 데이터 구조화 및 정제 노력

#### 수집 데이터 정제 과정

```
원본 데이터 (HTML)
    ↓ clean_number()  — 콤마 제거, 음수 부호 보존, 정수 변환
    ↓ clean_float()   — 소수점 추출, 실수 변환
    ↓ parse_change_pct() — 등락률 부호 파싱 (+2.35% → 2.35)
    ↓ merge_and_clean() — 3개 DataFrame 병합, 타입 변환, 파생 지표 계산
    ↓
정제된 DataFrame (20개 컬럼)
```

#### 정제 주요 처리

| 처리 항목 | 내용 |
|-----------|------|
| 결측치 처리 | `fillna(0)`, `setdefault(None)`, 결측 현황 로그 출력 |
| 타입 변환 | 정수형 (현재가, 거래량), 실수형 (PER, PBR, 배당수익률) |
| 파생 지표 | `52주변동폭(%)` = (최고-최저)/최저×100 |
| 등락률 숫자형 | 시각화용 `등락률(숫자)` 컬럼 추가 |

### 2-3. DB 스키마 설계 (schema.sql)

MySQL 8.0 기반 7개 테이블 설계:

| 테이블 | 역할 | 주요 컬럼 |
|--------|------|-----------|
| **(A) users** | 사용자 | user_id, username, email |
| **(B) user_profile** | 투자 성향 | risk_level (5단계 ENUM), survey_score |
| **(C) stocks** | 종목 마스터 | ticker(PK), name, market |
| **(D) price_snapshots** | 시세 스냅샷 | ticker, price, volume, trade_value |
| **(E) analysis_signals** | 분석 신호 | trend_score, signal (BUY/HOLD/SELL) |
| **(F) recommendations** | 추천 결과 | user_id, ticker, score, reason |
| **(G) newsletters** | 뉴스레터 | title, content |

---

## 3. 데이터 분석 및 시각화

### 3-1. pandas를 활용한 분석 (analyzer.py)

#### 투자 성향 5단계 분류 알고리즘

한양증권 투자성향진단 기준 **11문항** 설문을 구현하고, 점수 합산 비율로 5단계를 분류합니다.

| 성향 | 점수 비율 | 설명 |
|------|-----------|------|
| 🛡️ 안정형 | ≤25% | 원금 보존 최우선, 예·적금 수준 수익 기대 |
| 🔒 안정추구형 | ≤40% | 안정적 투자, 일부 변동성 허용 |
| ⚖️ 위험중립형 | ≤60% | 예·적금 이상 수익 기대, 일정 손실 감수 |
| 🚀 적극투자형 | ≤80% | 높은 수익 추구, 위험 감내 |
| 🔥 공격투자형 | >80% | 시장 초과 수익 추구, 손실 적극 수용 |

#### 성향별 가중치 스코어링

각 성향별로 다른 가중치를 적용하여 종목 추천 점수를 산출합니다:

```
안정형:    배당수익률(30%) + 시가총액(25%) + 낮은변동폭(25%) + PBR(10%) + 기관매수(10%)
안정추구형: 배당수익률(20%) + 시가총액(20%) + 기관매수(20%) + PER적정(20%) + 낮은변동폭(20%)
위험중립형: 외국인매수(25%) + PER적정(20%) + 거래량(20%) + 시가총액(15%) + 등락률(20%)
적극투자형: 거래량(30%) + 등락률(25%) + 외국인매수(20%) + 변동폭(15%) + PER(10%)
공격투자형: 거래량(35%) + 등락률(30%) + 변동폭(20%) + 외국인매수(15%)
```

#### 분석 신호 생성 (BUY / HOLD / SELL)

수집된 데이터를 바탕으로 각 종목의 **추세 점수**를 계산하고 매매 신호를 생성합니다:

```
추세 점수 = 등락률(40%) + 거래량(20%) + 외국인순매수(20%) + 기관순매수(20%)

≥ 60점 → 🟢 BUY (매수)
≥ 40점 → 🟡 HOLD (보유)
< 40점 → 🔴 SELL (매도)
```

#### 도출된 인사이트 예시

- **거래량 급등 + 외국인 순매수** 종목은 단기 상승 가능성 높음
- **높은 배당수익률 + 낮은 변동폭** 종목은 안정형 투자자에게 적합
- **기관/외국인 동시 순매수** 종목은 전 성향 투자자에게 신뢰도 높은 종목

### 3-2. seaborn/matplotlib 시각화

| 차트 | 라이브러리 | 활용 목적 |
|------|------------|-----------|
| 투자 지표 상관관계 히트맵 | `seaborn` | PER/PBR/거래량/외국인매매 간 상관성 분석 |
| PER 분포 히스토그램 | `seaborn` | 추천 종목의 PER 분포 확인 |
| PBR 분포 히스토그램 | `seaborn` | 추천 종목의 저평가 여부 판단 |

### 3-3. plotly 인터랙티브 시각화

| 차트 | 설명 |
|------|------|
| 거래량 바 차트 | KOSPI/KOSDAQ 구분 색상, 마우스 오버 상세 정보 |
| 거래량 vs 등락률 버블 차트 | 버블 크기 = 거래대금, 인터랙티브 탐색 |
| 외국인/기관 매매 비교 차트 | 종목별 Group Bar 차트 |
| 추천 점수 차트 | Viridis 색상, 종목 비교 |
| 상위 종목 레이더 차트 | 다차원 지표 비교 (거래량, PER, 외국인매수 등) |
| 등락률 비교 차트 | RdYlGn 색 스케일, 상승/하락 직관적 표시 |

---

## 4. 대시보드(웹앱) 구현

### 4-1. Streamlit 4페이지 구성 (app.py)

```
📊 주식 추천 시스템
├── 🏠 메인 대시보드      — 시장 개요, 거래량 차트, 외국인/기관 매매, 히트맵
├── 📋 투자 성향 설문      — 11문항, 5단계 분류, 결과 스케일 표시
├── ⭐ 맞춤 종목 추천      — 성향별 TOP N, 점수 차트, 레이더, PER/PBR
└── 📰 종목 뉴스          — 종목 필터, 카드형 뉴스 표시
```

### 4-2. 구성의 일관성

- **사이드바 네비게이션**: 4개 페이지 라디오 버튼 + 데이터 새로고침 버튼
- **일관된 색상 테마**: glassmorphism 다크 테마 (그라데이션 배경, 반투명 카드)
- **통일된 차트 스타일**: plotly_dark 템플릿, 동일한 색상 팔레트 (`#667eea`, `#764ba2`)

### 4-3. 사용자 인터페이스(UI) 직관성

| 기능 | UI 요소 |
|------|---------|
| 시장 필터 | `st.selectbox` (전체/KOSPI/KOSDAQ) |
| 정렬 기준 선택 | `st.selectbox` (거래량/현재가/등락률) |
| 추천 수 조절 | `st.slider` (3~20개) |
| 성향 결과 표시 | 5단계 스케일 시각화 + 이모지 |
| 추천 종목 카드 | 🥇🥈🥉 TOP 3 하이라이트 카드 |

### 4-4. 결과 시각화 연동

설문 결과 → 성향 분류 → 추천 종목 → 차트 연동:

```
설문 11문항 응답
    ↓ classify_investor_type()
투자 성향 분류 (5단계)
    ↓ score_stocks()
종목별 추천 점수 계산
    ↓ get_top_recommendations()
TOP N 추천 종목 + 추천 이유
    ↓ plotly / seaborn
인터랙티브 시각화 차트 렌더링
```

---

## 5. 프로젝트 구조 및 실행 방법

### 5-1. 파일 구조

```
miniproject_1/
├── app.py              # Streamlit 웹앱 (메인 대시보드)
├── scraper.py          # 네이버 증권 스크래핑 모듈
│                         ├ requests + BS4: 거래량/종목상세
│                         └ Selenium: 외국인·기관/뉴스
├── analyzer.py         # 투자 성향 분석 & 추천 엔진
│                         ├ 11문항 설문 → 5단계 분류
│                         ├ 성향별 가중치 스코어링
│                         ├ BUY/HOLD/SELL 신호 생성
│                         └ 뉴스레터 자동 생성
├── config.py           # 환경 설정 (python-dotenv)
├── db_manager.py       # DB 연동 초안 (MySQL + CSV fallback)
├── schema.sql          # MySQL 스키마 (7개 테이블)
├── .env.example        # 환경 변수 템플릿
├── pyproject.toml      # uv 의존성 관리 (29개 패키지)
└── data/               # CSV 데이터 저장
    ├── stock_data_YYYYMMDD.csv
    ├── price_snapshots_YYYYMMDD.csv
    ├── analysis_signals_YYYYMMDD.csv
    ├── recommendations_YYYYMMDD.csv
    └── newsletters_YYYYMMDD.csv
```

### 5-2. 기술 스택

| 분류 | 기술 | 용도 |
|------|------|------|
| 웹 스크래핑 | `requests` + `BeautifulSoup` | 시세, 종목 정보 수집 |
| 동적 스크래핑 | `Selenium` | 외국인/기관 매매, 뉴스 수집 |
| 데이터 분석 | `pandas`, `numpy` | 데이터 정제, 스코어링 계산 |
| 정적 시각화 | `matplotlib`, `seaborn` | 히트맵, 분포 차트 |
| 인터랙티브 시각화 | `plotly` | 바 차트, 버블, 레이더 |
| 웹앱 | `streamlit` | 4페이지 대시보드 |
| DB 연동 | `pymysql`, `sqlalchemy` | MySQL 초안 |
| 환경 관리 | `uv`, `python-dotenv` | 가상환경, 환경변수 |

### 5-3. 실행 방법

```bash
# 1. 의존성 설치
uv sync

# 2. 데이터 수집 (스크래핑 → 분석 → 저장 전체 파이프라인)
uv run python scraper.py

# 3. Streamlit 웹앱 실행
uv run streamlit run app.py
```

### 5-4. 데이터 파이프라인

```
┌─────────────────────────────────────────────────────────┐
│                    scraper.py 실행                       │
├──────────┬──────────┬──────────┬─────────────────────────┤
│ STEP 1-2 │ STEP 3   │ STEP 4   │ 수집 도구               │
│ BS4      │ Selenium │ Selenium │                         │
│ 거래량   │ 외국인   │ 뉴스     │                         │
│ 종목상세 │ 기관매매 │          │                         │
├──────────┴──────────┴──────────┤                         │
│ STEP 5: 데이터 통합 & 정제     │ merge_and_clean()       │
├────────────────────────────────┤                         │
│ STEP 6: 분석 신호 생성         │ → (E) analysis_signals  │
│ STEP 7: 추천 데이터 생성       │ → (F) recommendations   │
│ STEP 8: 뉴스레터 생성          │ → (G) newsletters       │
├────────────────────────────────┤                         │
│ STEP 9: DB + CSV 저장          │ → (C)~(G) 전체 테이블   │
└────────────────────────────────┘
```

---

## 부록: 코드 품질

| 항목 | 적용 내용 |
|------|-----------|
| **재사용성** | 모듈 분리 (scraper / analyzer / db_manager / config) |
| **에러 처리** | 구체적 예외 처리, 네트워크 재시도, graceful fallback |
| **로깅** | `logging` 모듈 레벨별 로그 (print 미사용) |
| **환경 변수** | `.env` + `python-dotenv`로 민감 정보 보호 |
| **의존성 관리** | `uv` + `pyproject.toml`로 버전 고정 |
| **문서화** | 모든 함수에 docstring, 타입 힌트 |
