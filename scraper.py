"""
네이버 증권 주식 데이터 스크래핑 & 정제
=======================================
채점 기준 충족을 위한 수집 도구:
  ① requests + BeautifulSoup : 거래량 상위 종목 시세 + 종목 기본 정보
  ② Selenium                 : 투자자별 매매 동향 + 종목 뉴스
  ③ 데이터 정제/구조화       : 결측치 처리, 타입 변환, CSV 저장
"""

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
import time
import re
import os
import logging

# Selenium
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException

# ============================================================
# 로깅 설정
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# ============================================================
# 공통 유틸리티
# ============================================================
HEADERS = {
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/120.0.0.0 Safari/537.36'
}

DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')
os.makedirs(DATA_DIR, exist_ok=True)


def create_session(retries=3, backoff=0.5):
    """재시도 로직이 포함된 requests.Session을 생성합니다."""
    session = requests.Session()
    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.headers.update(HEADERS)
    return session


def clean_number(text):
    """
    텍스트에서 숫자를 추출하여 정수로 변환합니다.
    음수 부호를 지원합니다.
    """
    if not text:
        return 0
    text = str(text).strip()
    # 음수 부호 보존
    is_negative = text.startswith('-') or '▼' in text or '하락' in text
    nums = re.sub(r'[^\d]', '', text)
    if not nums:
        return 0
    result = int(nums)
    return -result if is_negative else result


def clean_float(text):
    """텍스트에서 실수(소수점 포함)를 추출합니다."""
    if not text:
        return None
    text = text.strip().replace(',', '')
    match = re.search(r'[-+]?\d+\.?\d*', text)
    return float(match.group()) if match else None


def safe_text(element, default=""):
    """BeautifulSoup 요소에서 안전하게 텍스트를 추출합니다."""
    return element.text.strip() if element else default


def parse_change_pct(pct_text):
    """
    등락률 문자열을 파싱하여 float로 변환합니다.
    예: '+2.35%' -> 2.35, '-1.05%' -> -1.05
    """
    if not pct_text:
        return 0.0
    pct_text = pct_text.strip().replace('%', '')
    try:
        return float(pct_text)
    except ValueError:
        return 0.0


# ============================================================
# 1. [requests + BeautifulSoup] 거래량 상위 종목 시세 수집
# ============================================================
def scrape_top_volume(market="KOSPI", limit=20, session=None):
    """
    네이버 금융에서 거래량 상위 종목 시세를 스크래핑합니다.

    Args:
        market: "KOSPI" 또는 "KOSDAQ"
        limit:  수집할 종목 수
        session: requests.Session (없으면 새로 생성)
    Returns:
        pandas DataFrame
    """
    if session is None:
        session = create_session()

    sosok = "0" if market == "KOSPI" else "1"
    base_url = f"https://finance.naver.com/sise/sise_quant.naver?sosok={sosok}"

    stocks = []
    page = 1

    logger.info(f"[수집] {market} 거래량 상위 {limit}개 종목 수집 시작")

    while len(stocks) < limit:
        url = f"{base_url}&page={page}"
        try:
            resp = session.get(url, timeout=10)
            resp.encoding = 'euc-kr'
            soup = BeautifulSoup(resp.text, 'html.parser')
        except requests.RequestException as e:
            logger.error(f"[네트워크 오류] {market} page={page}: {e}")
            break

        rows = soup.select('table.type_2 tr')
        found = False

        for row in rows:
            cols = row.select('td')
            if len(cols) < 10 or not cols[1].text.strip():
                continue

            found = True
            try:
                name = cols[1].text.strip()
                link = cols[1].find('a')
                if not link or 'href' not in link.attrs:
                    continue
                code = link['href'].split('code=')[-1]

                # ── 데이터 정제 ──
                price      = clean_number(cols[2].text)   # 현재가
                change_val = clean_number(cols[3].text)    # 전일비 (절대값)
                change_pct = cols[4].text.strip()          # 등락률

                volume     = clean_number(cols[5].text)    # 거래량
                trade_val  = clean_number(cols[6].text)    # 거래대금(백만원)

                # 등락률의 부호를 기준으로 전일비 부호 결정
                pct_value = parse_change_pct(change_pct)
                if pct_value < 0:
                    change_val = -abs(change_val)
                elif pct_value == 0:
                    change_val = 0

                stocks.append({
                    '종목코드': code,
                    '종목명':   name,
                    '시장':     market,
                    '현재가':   price,
                    '전일비':   change_val,
                    '등락률':   change_pct,
                    '거래량':   volume,
                    '거래대금': trade_val,
                    '수집시간': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                })

                if len(stocks) >= limit:
                    break

            except (ValueError, TypeError, AttributeError) as e:
                logger.warning(f"  [정제 오류] {e}")
                continue

        if not found:
            break
        page += 1
        time.sleep(0.3)

    df = pd.DataFrame(stocks)
    logger.info(f"  -> {market} {len(df)}개 종목 수집 완료")
    return df


# ============================================================
# 2. [requests + BeautifulSoup] 종목별 기본 정보 수집
#    (시가총액, PER, PBR, 배당수익률, 52주 최고/최저)
# ============================================================
def scrape_stock_detail(ticker, session=None):
    """
    네이버 금융 종목 메인 페이지에서 투자 지표를 스크래핑합니다.

    Args:
        ticker: 종목코드 (예: "005930")
        session: requests.Session
    Returns:
        dict - 종목 기본 정보
    """
    if session is None:
        session = create_session()

    url = f"https://finance.naver.com/item/main.naver?code={ticker}"

    try:
        resp = session.get(url, timeout=10)
        resp.encoding = 'euc-kr'
        soup = BeautifulSoup(resp.text, 'html.parser')
    except requests.RequestException as e:
        logger.error(f"[상세 수집 네트워크 오류] {ticker}: {e}")
        return _default_detail(ticker)

    info = {'종목코드': ticker}

    try:
        # ── 시가총액 ──
        market_cap_tag = soup.select_one('#_market_sum')
        if market_cap_tag:
            raw = market_cap_tag.text.strip().replace('\n', '').replace('\t', '')
            info['시가총액(억)'] = clean_number(raw)

        # ── PER / PBR / 배당수익률 ── (table.per_table)
        table = soup.select_one('table.per_table')
        if table:
            em_tags = table.select('em')
            # per_table 구조: PER, EPS, PBR, BPS 순서
            if len(em_tags) >= 1:
                info['PER'] = clean_float(em_tags[0].text)
            if len(em_tags) >= 3:
                info['PBR'] = clean_float(em_tags[2].text)

        # ── per_table이 없으면 tb_type1에서 찾기 ──
        table2 = soup.select('table.tb_type1.tb_num')
        for t in table2:
            rows = t.select('tr')
            for row in rows:
                th = safe_text(row.select_one('th'))
                td = safe_text(row.select_one('td'))
                if 'PER' in th and 'PER' not in info:
                    info['PER'] = clean_float(td)
                elif 'PBR' in th and 'PBR' not in info:
                    info['PBR'] = clean_float(td)
                elif '배당수익률' in th:
                    info['배당수익률'] = clean_float(td)

        # ── 52주 최고/최저 ──
        high_low = soup.select('div.tab_con1 table td span')
        nums = []
        for span in high_low:
            val = clean_number(span.text)
            if val > 0:
                nums.append(val)
        if len(nums) >= 2:
            info['52주최고'] = nums[0]
            info['52주최저'] = nums[1]

        # ── 별도 em 태그에서 PER/PBR 찾기 (fallback) ──
        all_em = soup.select('#corp_group2 em')
        if len(all_em) >= 4 and 'PER' not in info:
            info['PER'] = clean_float(all_em[0].text)
        if len(all_em) >= 4 and 'PBR' not in info:
            info['PBR'] = clean_float(all_em[2].text)

    except Exception as e:
        logger.error(f"  [상세 수집 파싱 오류] {ticker}: {e}")

    # ── 결측치 기본값 설정 ──
    for key in ['시가총액(억)', 'PER', 'PBR', '배당수익률', '52주최고', '52주최저']:
        info.setdefault(key, None)

    return info


def _default_detail(ticker):
    """상세 정보 수집 실패 시 기본값 반환"""
    return {
        '종목코드': ticker,
        '시가총액(억)': None, 'PER': None, 'PBR': None,
        '배당수익률': None, '52주최고': None, '52주최저': None,
    }


def scrape_all_details(tickers, session=None, delay=0.3):
    """
    여러 종목의 기본 정보를 일괄 수집합니다.

    Args:
        tickers: 종목코드 리스트
        session: requests.Session
        delay: 요청 간 대기 시간(초)
    Returns:
        pandas DataFrame
    """
    if session is None:
        session = create_session()

    logger.info(f"[수집] 종목 상세 정보 수집 중... ({len(tickers)}개)")
    details = []
    for i, ticker in enumerate(tickers):
        detail = scrape_stock_detail(ticker, session=session)
        details.append(detail)
        if (i + 1) % 10 == 0:
            logger.info(f"  -> {i+1}/{len(tickers)} 완료")
        time.sleep(delay)

    df = pd.DataFrame(details)
    logger.info(f"  -> 상세 정보 {len(df)}개 종목 수집 완료")
    return df


# ============================================================
# 3. [Selenium] 투자자별 매매 동향 (외국인/기관 순매수)
# ============================================================
def create_driver():
    """Selenium Chrome 드라이버를 headless 모드로 생성합니다."""
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--log-level=3')
    options.add_argument(f'user-agent={HEADERS["user-agent"]}')
    return webdriver.Chrome(options=options)


def scrape_investor_trend(ticker, driver=None):
    """
    Selenium으로 종목의 외국인/기관 매매 동향을 수집합니다.

    Args:
        ticker: 종목코드
        driver: 재사용할 Selenium 드라이버 (없으면 새로 생성)
    Returns:
        dict - 외국인/기관 순매수 데이터
    """
    own_driver = driver is None
    if own_driver:
        driver = create_driver()

    result = {
        '종목코드': ticker,
        '외국인_순매수량': None,
        '기관_순매수량': None,
        '외국인_보유비율': None,
    }

    try:
        url = f"https://finance.naver.com/item/frgn.naver?code={ticker}"
        driver.get(url)

        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'table.type2'))
        )

        # ── 외국인/기관 순매수 (최근 1일) ──
        rows = driver.find_elements(By.CSS_SELECTOR, 'table.type2 tr')
        for row in rows:
            tds = row.find_elements(By.TAG_NAME, 'td')
            if len(tds) >= 9:
                try:
                    # 외국인 순매수
                    foreign_text = tds[5].text.strip()
                    result['외국인_순매수량'] = clean_number(foreign_text)

                    # 기관 순매수
                    inst_text = tds[6].text.strip()
                    result['기관_순매수량'] = clean_number(inst_text)
                except (ValueError, IndexError) as e:
                    logger.warning(f"  [매매 데이터 파싱 오류] {ticker}: {e}")
                break

        # ── 외국인 보유비율 ──
        try:
            spans = driver.find_elements(By.CSS_SELECTOR, '.foreign span')
            for span in spans:
                text = span.text.strip()
                val = clean_float(text)
                if val is not None and 0 < val < 100:
                    result['외국인_보유비율'] = val
                    break
        except (TimeoutException, WebDriverException) as e:
            logger.warning(f"  [외국인 보유비율 수집 실패] {ticker}: {e}")

    except TimeoutException:
        logger.error(f"  [투자자 동향 타임아웃] {ticker}")
    except WebDriverException as e:
        logger.error(f"  [투자자 동향 Selenium 오류] {ticker}: {e}")
    except Exception as e:
        logger.error(f"  [투자자 동향 오류] {ticker}: {e}")
    finally:
        if own_driver:
            driver.quit()

    return result


def scrape_all_investor_trends(tickers, delay=1):
    """
    여러 종목의 투자자 매매 동향을 Selenium으로 일괄 수집합니다.

    Args:
        tickers: 종목코드 리스트
        delay: 요청 간 대기 시간(초)
    Returns:
        pandas DataFrame
    """
    logger.info(f"[수집] 투자자 매매 동향 수집 중... ({len(tickers)}개, Selenium)")
    driver = create_driver()
    trends = []

    try:
        for i, ticker in enumerate(tickers):
            trend = scrape_investor_trend(ticker, driver=driver)
            trends.append(trend)
            if (i + 1) % 5 == 0:
                logger.info(f"  -> {i+1}/{len(tickers)} 완료")
            time.sleep(delay)
    finally:
        driver.quit()

    df = pd.DataFrame(trends)
    logger.info(f"  -> 투자자 동향 {len(df)}개 종목 수집 완료")
    return df


# ============================================================
# 4. [Selenium] 종목별 최신 뉴스 수집
# ============================================================
def scrape_news(ticker, driver=None, limit=3):
    """
    Selenium으로 종목의 최신 뉴스 제목과 날짜를 수집합니다.

    Args:
        ticker: 종목코드 (예: "005930")
        driver: 재사용할 Selenium 드라이버
        limit:  수집할 뉴스 개수
    Returns:
        list of dict - [{'제목': ..., '날짜': ..., '출처': ...}, ...]
    """
    own_driver = driver is None
    if own_driver:
        driver = create_driver()

    articles = []
    try:
        url = f"https://finance.naver.com/item/news_news.naver?code={ticker}&page=1"
        driver.get(url)

        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'table.type5'))
        )

        rows = driver.find_elements(By.CSS_SELECTOR, 'table.type5 tr')
        count = 0
        for row in rows:
            if count >= limit:
                break

            title_elem = row.find_elements(By.CSS_SELECTOR, '.title a')
            date_elem = row.find_elements(By.CSS_SELECTOR, '.date')
            source_elem = row.find_elements(By.CSS_SELECTOR, '.info')

            if title_elem:
                title = title_elem[0].text.strip().replace("'", "").replace('"', '')
                if title:
                    article = {
                        '제목': title,
                        '날짜': date_elem[0].text.strip() if date_elem else '',
                        '출처': source_elem[0].text.strip() if source_elem else '',
                    }
                    articles.append(article)
                    count += 1

    except TimeoutException:
        logger.warning(f"  [뉴스 수집 타임아웃] {ticker}")
    except WebDriverException as e:
        logger.warning(f"  [뉴스 수집 Selenium 오류] {ticker}: {e}")
    except Exception as e:
        logger.warning(f"  [뉴스 수집 실패] {ticker}: {e}")
    finally:
        if own_driver:
            driver.quit()

    return articles


def scrape_all_news(tickers, limit=3, delay=1):
    """
    여러 종목의 뉴스를 Selenium으로 일괄 수집합니다.

    Args:
        tickers: 종목코드 리스트
        limit: 종목당 수집할 뉴스 수
        delay: 요청 간 대기 시간(초)
    Returns:
        pandas DataFrame
    """
    logger.info(f"[수집] 종목 뉴스 수집 중... ({len(tickers)}개, Selenium)")
    driver = create_driver()
    news_data = []

    try:
        for i, ticker in enumerate(tickers):
            news_list = scrape_news(ticker, driver=driver, limit=limit)
            for article in news_list:
                news_data.append({
                    '종목코드': ticker,
                    '뉴스제목': article['제목'],
                    '뉴스날짜': article['날짜'],
                    '뉴스출처': article['출처'],
                    '수집시간': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                })
            if (i + 1) % 5 == 0:
                logger.info(f"  -> {i+1}/{len(tickers)} 완료")
            time.sleep(delay)
    finally:
        driver.quit()

    df = pd.DataFrame(news_data) if news_data else pd.DataFrame()
    logger.info(f"  -> 뉴스 {len(df)}건 수집 완료")
    return df


# ============================================================
# 5. 데이터 정제 & 통합
# ============================================================
def merge_and_clean(volume_df, detail_df, investor_df):
    """
    수집된 데이터를 통합하고 최종 정제합니다.

    Args:
        volume_df:   거래량 상위 종목 DataFrame
        detail_df:   종목 상세 정보 DataFrame
        investor_df: 투자자 매매 동향 DataFrame
    Returns:
        pandas DataFrame - 통합된 최종 데이터
    """
    logger.info("[정제] 데이터 통합 및 정제 중...")

    # ── STEP 1: 데이터 병합 ──
    merged = volume_df.merge(detail_df, on='종목코드', how='left')
    merged = merged.merge(investor_df, on='종목코드', how='left')

    # ── STEP 2: 데이터 타입 변환 ──
    int_cols = ['현재가', '전일비', '거래량', '거래대금', '52주최고', '52주최저']
    for col in int_cols:
        if col in merged.columns:
            merged[col] = pd.to_numeric(merged[col], errors='coerce').fillna(0).astype(int)

    float_cols = ['PER', 'PBR', '배당수익률', '외국인_보유비율']
    for col in float_cols:
        if col in merged.columns:
            merged[col] = pd.to_numeric(merged[col], errors='coerce')

    # ── STEP 3: 파생 지표 계산 ──
    if '52주최고' in merged.columns and '52주최저' in merged.columns:
        merged['52주변동폭(%)'] = merged.apply(
            lambda r: round((r['52주최고'] - r['52주최저']) / r['52주최저'] * 100, 2)
            if r['52주최저'] > 0 else None,
            axis=1
        )

    # 등락률 숫자형 변환 (시각화용)
    if '등락률' in merged.columns:
        merged['등락률(숫자)'] = merged['등락률'].apply(parse_change_pct)

    # ── STEP 4: 컬럼 정리 및 순서 정렬 ──
    col_order = [
        '종목코드', '종목명', '시장', '현재가', '전일비', '등락률', '등락률(숫자)',
        '거래량', '거래대금', '시가총액(억)', 'PER', 'PBR', '배당수익률',
        '52주최고', '52주최저', '52주변동폭(%)',
        '외국인_순매수량', '기관_순매수량', '외국인_보유비율',
        '수집시간'
    ]
    available = [c for c in col_order if c in merged.columns]
    merged = merged[available]

    # ── STEP 5: 결측치 처리 결과 출력 ──
    null_counts = merged.isnull().sum()
    if null_counts.any():
        logger.info("  [결측치 현황]")
        for col, cnt in null_counts.items():
            if cnt > 0:
                logger.info(f"    - {col}: {cnt}건 결측")

    logger.info(f"  -> 최종 데이터: {len(merged)}개 종목, {len(merged.columns)}개 컬럼")
    return merged


# ============================================================
# 6. CSV 저장
# ============================================================
def save_to_csv(df, filename, directory=None, encoding='utf-8-sig'):
    """DataFrame을 CSV 파일로 저장합니다."""
    if directory is None:
        directory = DATA_DIR
    os.makedirs(directory, exist_ok=True)
    filepath = os.path.join(directory, filename)
    df.to_csv(filepath, index=False, encoding=encoding)
    logger.info(f"  -> 저장: {filepath} ({len(df)}건)")
    return filepath


# ============================================================
# 6-1. [pykrx] 과거 시세 데이터 수집
# ============================================================
def scrape_historical_prices(tickers, days=5):
    """
    pykrx를 활용하여 과거 시세(OHLCV) 데이터를 수집합니다.

    Args:
        tickers: 종목코드 리스트
        days: 수집 일수 (기본 5일)
    Returns:
        DataFrame - 날짜, 종목코드, 시가, 고가, 저가, 종가, 거래량, 등락률
    """
    try:
        from pykrx import stock as pykrx_stock
    except ImportError:
        logger.warning("[pykrx] pykrx 라이브러리 미설치. 과거 시세 수집 건너뜀.")
        return pd.DataFrame()

    from datetime import timedelta

    end_date = datetime.now()
    start_date = end_date - timedelta(days=days + 10)  # 영업일 감안 여유
    start_str = start_date.strftime('%Y%m%d')
    end_str = end_date.strftime('%Y%m%d')

    all_data = []
    logger.info(f"[pykrx] {len(tickers)}개 종목 과거 시세 수집 ({start_str}~{end_str})...")

    for ticker in tickers:
        try:
            df = pykrx_stock.get_market_ohlcv(start_str, end_str, ticker)
            if df.empty:
                continue

            # 최근 N일만
            df = df.tail(days)
            df = df.reset_index()
            df.columns = ['날짜', '시가', '고가', '저가', '종가', '거래량', '등락률']
            df['종목코드'] = ticker
            df['날짜'] = df['날짜'].dt.strftime('%Y-%m-%d')
            all_data.append(df)
            time.sleep(0.2)  # API 부하 방지
        except Exception as e:
            logger.warning(f"  [pykrx] {ticker} 시세 수집 실패: {e}")

    if all_data:
        result = pd.concat(all_data, ignore_index=True)
        logger.info(f"  -> pykrx 시세 수집 완료: {len(result)}건 ({len(all_data)}종목)")
        return result
    return pd.DataFrame()


# ============================================================
# 7. 전체 파이프라인 실행 함수
# ============================================================
def run_full_pipeline(kospi_limit=100, kosdaq_limit=100):
    """
    전체 스크래핑 + 분석 + 저장 파이프라인을 실행합니다.

    파이프라인:
      STEP 1~4: 데이터 수집 (스크래핑)
      STEP 5:   데이터 통합 & 정제
      STEP 6:   (E) 분석 신호 생성 (BUY/HOLD/SELL)
      STEP 7:   (F) 추천 데이터 생성
      STEP 8:   (G) 뉴스레터 생성
      STEP 9:   (C~G) 전체 DB/CSV 저장

    Args:
        kospi_limit: KOSPI 거래량 상위 수집 수
        kosdaq_limit: KOSDAQ 거래량 상위 수집 수
    Returns:
        dict - { 'stock_df', 'news_df', 'signals_df', 'recs_df', 'newsletter' }
    """
    from analyzer import (
        generate_analysis_signals, score_stocks,
        build_recommendations_df, generate_newsletter,
    )
    from db_manager import save_all_to_db

    logger.info("=" * 60)
    logger.info(" 네이버 증권 데이터 수집 & 분석 시스템")
    logger.info(f" 실행 시간: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 60)

    today = datetime.now().strftime('%Y%m%d')
    session = create_session()

    # ─── STEP 1: [requests + BS4] 거래량 상위 종목 수집 ───
    kospi_df  = scrape_top_volume("KOSPI",  limit=kospi_limit, session=session)
    kosdaq_df = scrape_top_volume("KOSDAQ", limit=kosdaq_limit, session=session)
    volume_df = pd.concat([kospi_df, kosdaq_df], ignore_index=True)

    if volume_df.empty:
        logger.error("거래량 상위 종목 수집 실패. 파이프라인 중단.")
        return {'stock_df': pd.DataFrame(), 'news_df': pd.DataFrame(),
                'signals_df': pd.DataFrame(), 'recs_df': pd.DataFrame(),
                'newsletter': None}

    tickers = volume_df['종목코드'].tolist()

    # ─── STEP 2: [requests + BS4] 종목 상세 정보 수집 ───
    detail_df = scrape_all_details(tickers, session=session, delay=0.3)

    # ─── STEP 3: [Selenium] 투자자 매매 동향 수집 ───
    investor_df = scrape_all_investor_trends(tickers, delay=1)

    # ─── STEP 4: [Selenium] 뉴스 수집 (상위 5개 종목) ───
    top5 = tickers[:5]
    news_df = scrape_all_news(top5, limit=3, delay=1)

    if not news_df.empty:
        name_map = dict(zip(volume_df['종목코드'], volume_df['종목명']))
        news_df['종목명'] = news_df['종목코드'].map(name_map)

    # ─── STEP 5: 데이터 통합 & 정제 ───
    final_df = merge_and_clean(volume_df, detail_df, investor_df)

    # ─── STEP 5-1: [pykrx] 과거 시세 수집 ───
    logger.info("[pykrx] 과거 5일 시세 데이터 수집 중...")
    historical_df = scrape_historical_prices(tickers, days=5)
    if not historical_df.empty:
        name_map = dict(zip(volume_df['종목코드'], volume_df['종목명']))
        historical_df['종목명'] = historical_df['종목코드'].map(name_map)

    # ─── STEP 6: (E) 분석 신호 생성 ───
    logger.info("[분석] 분석 신호 생성 중...")
    signals_df = generate_analysis_signals(final_df, window='1D')

    # ─── STEP 7: (F) 추천 데이터 생성 (기본: 위험중립형) ───
    logger.info("[추천] 기본 추천 데이터 생성 중...")
    scored_df = score_stocks(final_df, '위험중립형')
    recs_df = build_recommendations_df(scored_df, user_id=1, top_n=10)

    # ─── STEP 8: (G) 뉴스레터 생성 ───
    logger.info("[뉴스레터] 뉴스레터 생성 중...")
    newsletter = generate_newsletter(
        stock_df=final_df,
        scored_df=scored_df,
        signals_df=signals_df,
        investor_type='위험중립형',
        user_id=1,
        news_df=news_df,
    )

    # ─── STEP 9: 전체 저장 (C~G) ───
    logger.info("[저장] 전체 데이터 저장 중...")

    # 기존 CSV도 저장 (호환성 유지)
    save_to_csv(final_df, f"stock_data_{today}.csv")
    save_to_csv(volume_df, f"top_volume_{today}.csv")
    if not news_df.empty:
        save_to_csv(news_df, f"stock_news_{today}.csv")
    if not historical_df.empty:
        save_to_csv(historical_df, f"historical_{today}.csv")

    # DB 형식 저장 (C~G 테이블)
    save_all_to_db(
        stock_df=final_df,
        signals_df=signals_df,
        recs_df=recs_df,
        newsletter_dict=newsletter,
    )

    # ─── 결과 요약 ───
    buy_cnt = (signals_df['signal'] == 'BUY').sum() if not signals_df.empty else 0
    hold_cnt = (signals_df['signal'] == 'HOLD').sum() if not signals_df.empty else 0
    sell_cnt = (signals_df['signal'] == 'SELL').sum() if not signals_df.empty else 0

    logger.info(f"{'='*60}")
    logger.info(f" [완료] 데이터 수집 & 분석 & 저장 끝!")
    logger.info(f"   (C) stocks           : {len(final_df)}개 종목")
    logger.info(f"   (D) price_snapshots   : {len(final_df)}건")
    logger.info(f"   (E) analysis_signals  : {len(signals_df)}건 "
                f"(BUY:{buy_cnt} / HOLD:{hold_cnt} / SELL:{sell_cnt})")
    logger.info(f"   (F) recommendations   : {len(recs_df)}건")
    logger.info(f"   (G) newsletters       : 1건")
    logger.info(f"   뉴스                  : {len(news_df)}건")
    logger.info(f"   과거 시세(pykrx)      : {len(historical_df)}건")
    logger.info(f"{'='*60}")

    return {
        'stock_df': final_df,
        'news_df': news_df,
        'historical_df': historical_df,
        'signals_df': signals_df,
        'recs_df': recs_df,
        'newsletter': newsletter,
    }



# ============================================================
# [Phase 2] pykrx 재무지표 수집 (PER, PBR, ROE, EPS, DIV)
# ============================================================
def scrape_fundamentals(tickers, date_str=None):
    """
    pykrx API를 통해 종목별 재무지표를 수집합니다.
    (PER, PBR, EPS, DIV, BPS)

    Args:
        tickers: 종목코드 리스트
        date_str: 기준일 (YYYYMMDD), None이면 오늘
    Returns:
        DataFrame: 재무지표
    """
    from pykrx import stock as pykrx_stock
    if date_str is None:
        from datetime import datetime
        date_str = datetime.today().strftime('%Y%m%d')

    logger.info(f"[재무지표] pykrx 재무지표 수집 시작 ({len(tickers)}종목, 기준일:{date_str})")

    all_records = []
    for ticker in tickers:
        try:
            df_fund = pykrx_stock.get_market_fundamental(date_str, date_str, ticker)
            if df_fund is None or df_fund.empty:
                continue
            row = df_fund.iloc[-1]
            all_records.append({
                '종목코드': ticker,
                'PER':  round(float(row.get('PER', 0) or 0), 2),
                'PBR':  round(float(row.get('PBR', 0) or 0), 2),
                'EPS':  int(row.get('EPS', 0) or 0),
                'BPS':  int(row.get('BPS', 0) or 0),
                'DIV':  round(float(row.get('DIV', 0) or 0), 2),   # 배당수익률
                'DPS':  int(row.get('DPS', 0) or 0),
            })
            time.sleep(0.1)   # API 부하 방지
        except Exception as e:
            logger.warning(f"  [재무지표] {ticker} 수집 오류: {e}")
            continue

    fund_df = pd.DataFrame(all_records)
    logger.info(f"[재무지표] 수집 완료: {len(fund_df)}건")
    return fund_df


# ============================================================
# [Phase 3] 증권사 목표주가 수집 (네이버 금융 리서치)
# ============================================================
def scrape_analyst_opinion(tickers_names, session=None):
    """
    네이버 금융 리서치 페이지에서 증권사 투자의견과 목표주가를 수집합니다.

    Args:
        tickers_names: [(종목코드, 종목명), ...] 리스트 또는 dict
        session: requests.Session
    Returns:
        DataFrame: 종목코드, 증권사, 투자의견, 목표주가, 현재가, 목표가_괴리율, analyst_score
    """
    if session is None:
        session = create_session()

    base_url = "https://finance.naver.com/research/company_list.naver"
    records = []

    logger.info("[리서치] 네이버 금융 증권사 리포트 수집 시작")

    try:
        resp = session.get(base_url, timeout=10)
        resp.encoding = 'euc-kr'
        soup = BeautifulSoup(resp.text, 'html.parser')

        rows = soup.select('table.type_1 tr')
        for row in rows:
            cols = row.select('td')
            if len(cols) < 6:
                continue
            try:
                name_tag = cols[0].find('a')
                if not name_tag:
                    continue
                stock_name = name_tag.text.strip()
                opinion    = cols[1].text.strip()     # 투자의견 (매수/중립 등)
                target_raw = cols[2].text.strip().replace(',', '')  # 목표주가
                target_price = int(target_raw) if target_raw.isdigit() else 0
                current_raw  = cols[3].text.strip().replace(',', '')
                current_price = int(current_raw) if current_raw.isdigit() else 0

                if target_price > 0 and current_price > 0:
                    gap_pct = round((target_price - current_price) / current_price * 100, 1)
                else:
                    gap_pct = 0.0

                # 투자의견 점수화: 매수=100, 매수(유지)=90, 중립=50, 매도=0
                OPINION_SCORE = {'매수': 100, '강력매수': 100, '적극매수': 100,
                                 '매수(유지)': 90, 'BUY': 100,
                                 '중립': 50, 'HOLD': 50, '보유': 50,
                                 '매도': 0,  'SELL': 0}
                opinion_score = OPINION_SCORE.get(opinion, 60)

                # 괴리율 점수 (괴리율 클수록 저평가 = 높은 점수)
                gap_score = min(100, max(0, gap_pct + 50))

                # 종합 analyst_score
                analyst_score = round(opinion_score * 0.6 + gap_score * 0.4, 1)

                # 종목명으로 종목코드 매핑
                code_map = {v: k for k, v in tickers_names} if isinstance(tickers_names, list) else tickers_names
                code = code_map.get(stock_name, '')

                records.append({
                    '종목코드':    code,
                    '종목명':      stock_name,
                    '투자의견':    opinion,
                    '목표주가':    target_price,
                    '현재가_리서치': current_price,
                    '목표가_괴리율': gap_pct,
                    'analyst_score': analyst_score,
                })
            except Exception as e:
                logger.debug(f"  [리서치] 행 파싱 오류: {e}")
                continue

    except Exception as e:
        logger.warning(f"[리서치] 수집 실패: {e}")

    analyst_df = pd.DataFrame(records)
    # 코드가 있는 것만 반환
    if not analyst_df.empty and '종목코드' in analyst_df.columns:
        analyst_df = analyst_df[analyst_df['종목코드'] != '']
    logger.info(f"[리서치] 수집 완료: {len(analyst_df)}건")
    return analyst_df


# ============================================================
# 8. 메인 실행
# ============================================================
if __name__ == "__main__":
    result = run_full_pipeline()

    final_df = result['stock_df']
    if not final_df.empty:
        print("\n[미리보기] 통합 데이터 상위 5개:")
        print(final_df.head(5).to_string(index=False))

    signals_df = result['signals_df']
    if not signals_df.empty:
        print("\n[미리보기] 분석 신호:")
        print(signals_df.head(10).to_string(index=False))

    newsletter = result['newsletter']
    if newsletter:
        print(f"\n[뉴스레터] {newsletter['title']}")
        print(newsletter['content'][:500] + '...')

