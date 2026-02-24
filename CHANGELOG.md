# 📊 주식 추천 시스템 — 개발 변경 내역 (CHANGELOG)

> **Branch:** `feature/stock-app` → `upstream/devbr`
> **날짜:** 2026-02-24
> **작성자:** 1팀

---

## 🧭 프로젝트 개요

Streamlit 기반 주식 추천 시스템. 네이버 금융에서 KOSPI/KOSDAQ 거래량 상위 종목을 수집하고, 투자 성향에 맞는 맞춤 종목 추천 및 뉴스레터를 제공한다.

---

## ✅ Phase 1 — 스크래퍼 기초 구현 (`scraper.py`)

- 네이버 금융 거래량 상위 종목 스크래핑 (KOSPI/KOSDAQ)
- `requests` + `BeautifulSoup4` 기반 파싱
- 종목코드, 종목명, 현재가, 등락률, 거래량, 거래대금 수집

---

## ✅ Phase 2 — 데이터 확장 및 품질 개선

- 수집 대상 확대: **KOSPI 100 + KOSDAQ 100 → 총 200종목**
- `pykrx` 연동: **7일치 과거 시세 데이터** 수집
- 외국인/기관 순매수 데이터 수집 모듈 신설 (`fetch_inv.py`)
- 종목코드 타입 불일치 문제 해결 (zfill 패딩 처리)
- 세션 관리 및 에러 핸들링 강화

---

## ✅ Phase 3 — 분석 모듈 구현 (`analyzer.py`)

- 등락률 · 거래량 · 외국인/기관 순매수 기반 **추세 점수(trend_score)** 산출
- BUY / HOLD / SELL 신호 자동 분류
- 투자 성향 5단계 분류 알고리즘 (안정형 → 공격투자형)
- 한양증권 기준 11문항 설문 기반 성향 진단 로직

---

## ✅ Phase 4 — Streamlit 앱 구축 (`app.py`)

- 사이드바 네비게이션 (6페이지 구성)
- 메인 대시보드: 요약 통계, 거래량 차트, 외국인/기관 탭
- 투자 성향 설문 폼 및 11문항 UI
- 맞춤 종목 추천 (투자 성향 기반 필터링 + 스코어링)
- 분석 신호 페이지 (BUY/HOLD/SELL 바 차트 + 파이 차트)
- 캔들스틱 차트 및 종목 뉴스 페이지

---

## ✅ Phase 5 — UI/UX 테마 고도화

- **웜(Warm) 다크 테마** 전면 적용 (Cream + Beige + Gold + Dark Brown)
- 폰트: `Noto Sans KR` (Google Fonts)
- Streamlit Metric/Expander/Selectbox/Tab 전용 CSS 다크 오버라이드
- 드롭다운 메뉴 배경/텍스트 가독성 문제 해결
- Plotly 차트 warm palette 적용

---

## ✅ Phase 6 — DB 연동 설계 (`db_manager.py`)

- SQLAlchemy 기반 MySQL 연결 엔진 구현
- `save_to_db()` / `load_from_db()` 유틸리티 함수
- DB 미연결 시 **CSV 자동 백업 fallback** 처리
- 정의된 테이블 스키마:
  - `stock_market_data` — 시간별 시세 누적
  - `users` — 사용자 계정 정보

---

## ✅ Phase 7 — 환경 설정 및 보안

- `.env` 파일로 DB 자격증명 분리 관리
- `.gitignore`에 `.env`, `data/` 추가
- `pyproject.toml` 의존성 정비 (`uv` 패키지 매니저 사용)

---

## ✅ Phase 8 — 데이터 수집 자동화 (`scheduler_job.py`)

- `APScheduler` 기반 스케줄러 구현
- 평일 **09:00 ~ 15:00 매 정각** 자동 수집
- KOSPI/KOSDAQ 200종목 시세 → `stock_market_data` 테이블 INSERT
- DB 연결 실패 시 `data/stock_market_data_fallback.csv` 자동 저장

---

## ✅ Phase 9 — 실시간 데이터 분석 시각화 (`rtd_analyzer.py`)

- 당일 시간대별 데이터 로드 함수 (DB 우선, 실패 시 CSV)
- **직전 시간 대비 거래량 급증 TOP 10** 분석 (`analyze_volume_surge`)
- Streamlit 탭 추가: **"⏱️ 실시간 분석 (RTD)"**
  - `seaborn` 수평 바 차트 (급증 모멘텀 TOP 10)
  - `matplotlib` 산점도 (현재가 vs 거래대금 분포)

---

## ✅ Phase 10 — 회원 인증 시스템

### 10-1. 주요 종목 시세 패널
- 메인 대시보드에 **당일 거래량 상위 50종목** 현재가 표시
- 5열 그리드 + 순위 번호(1~50) + 등락률 delta 표시
- `[data-testid="stExpander"]` 다크 CSS 적용

### 10-2. 사용자 인증 구현

**저장 구조 (`data/users_db.json`)**
```json
{
  "userid": {
    "user_password": "$2b$12$...",
    "user_email": "user@example.com",
    "type_id": "위험중립형"
  }
}
```

**구현 항목**
| 항목 | 내용 |
|------|------|
| 회원가입 | 아이디 + 이메일 + 비밀번호 입력, 이메일 형식 검증 |
| 비밀번호 암호화 | `bcrypt` 라이브러리 직접 사용 (`hashpw` / `checkpw`) |
| 로그인 | Session State 관리, 구버전 해시 호환성 유지 |
| 로그아웃 | 세션 초기화 |
| 접근 제어 | "⭐ 맞춤 종목 추천" / "📧 뉴스레터" 미로그인 차단 |
| 성향 저장 | 설문 완료 시 `type_id` 자동 업데이트 + toast 알림 |

---

## 📁 신규 파일 목록

| 파일 | 역할 |
|------|------|
| `scheduler_job.py` | APScheduler 기반 시세 자동 수집기 |
| `rtd_analyzer.py` | 실시간 거래량 모멘텀 분석 |
| `fetch_inv.py` | 외국인/기관 순매수 데이터 수집 |
| `data/users_db.json` | 사용자 계정 JSON 저장소 |

## 📦 주요 의존성 추가

```
apscheduler
seaborn
matplotlib
bcrypt
passlib
cryptography
```

---

## 🐛 주요 버그 수정

| 버그 | 원인 | 해결 |
|------|------|------|
| 셀렉트박스 드롭다운 텍스트 안보임 | CSS 포탈 컨테이너 미처리 | `div[role="listbox"]` 별도 스타일 적용 |
| 외국인 데이터 병합 오류 | 종목코드 int/str 타입 불일치 | `zfill(6)` 정규화 후 병합 |
| bcrypt 72바이트 에러 | `passlib` + `bcrypt>=4` 호환 문제 | raw `bcrypt` 직접 사용 전환 |
| Streamlit Expander 배경 밝음 | CSS 미지정 | `[data-testid="stExpander"]` 다크 스타일 추가 |
| app.py SyntaxError line 370 | replace 누락으로 두 문장 한 줄 합쳐짐 | 개행 복구 |

---

*© 2026 1팀 미니프로젝트 — 주식 추천 시스템*
