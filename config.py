"""
환경 설정 모듈
=============
.env 파일에서 환경 변수를 로드하고 프로젝트 전역 설정을 관리합니다.
"""
import os
from pathlib import Path

# python-dotenv가 설치되어 있으면 .env 파일 로드
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# ── 프로젝트 경로 ──
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / 'data'
DATA_DIR.mkdir(exist_ok=True)

# ── DB 설정 (코드 초안 - 실제 연동 X) ──
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = int(os.getenv('DB_PORT', '3306'))
DB_USER = os.getenv('DB_USER', 'root')
DB_PASS = os.getenv('DB_PASS', '')
DB_NAME = os.getenv('DB_NAME', 'stock_recommend')

# ── 스크래핑 설정 ──
SCRAPE_DELAY = float(os.getenv('SCRAPE_DELAY', '0.3'))
SELENIUM_DELAY = float(os.getenv('SELENIUM_DELAY', '1.0'))
KOSPI_LIMIT = int(os.getenv('KOSPI_LIMIT', '20'))
KOSDAQ_LIMIT = int(os.getenv('KOSDAQ_LIMIT', '20'))
NEWS_LIMIT = int(os.getenv('NEWS_LIMIT', '3'))

# ── 투자 성향 설정 ──
INVESTOR_TYPES = [
    '안정형',
    '안정추구형',
    '위험중립형',
    '적극투자형',
    '공격투자형',
]
