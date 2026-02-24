import time
import logging
import pandas as pd
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from scraper import create_session, scrape_top_volume
from db_manager import save_to_db

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def job_realtime_market_data():
    """매 정시 KOSPI/KOSDAQ 거래량 상위 종목을 수집하여 DB에 누적 저장합니다."""
    logger.info("=== [스케줄링] 1시간 단위 주식 시세 수집 시작 ===")
    session = create_session()
    
    try:
        # KOSPI 100 + KOSDAQ 100 수집
        kospi = scrape_top_volume('KOSPI', limit=100, session=session)
        kosdaq = scrape_top_volume('KOSDAQ', limit=100, session=session)
        
        # 병합
        df = pd.concat([kospi, kosdaq], ignore_index=True)
        
        if df.empty:
            logger.warning("[실패] 수집된 데이터가 없습니다.")
            return

        # DB 형식 매핑 (필요한 핵심 데이터만)
        now = datetime.now()
        df['수집시간'] = now.replace(minute=0, second=0, microsecond=0) # 정시 기준으로 통일
        
        # 문자열 숫자로 변환
        for col in ['현재가', '전일비', '거래량', '거래대금']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('int64')
            
        # 등락률 % 제거 후 float 변환
        df['등락률_num'] = df['등락률'].astype(str).str.replace('%', '').str.replace('+', '').str.replace(',', '')
        df['등락률_num'] = pd.to_numeric(df['등락률_num'], errors='coerce').fillna(0.0)
        
        # DB 저장용 최종 데이터셋
        db_df = df[['종목코드', '종목명', '시장', '현재가', '전일비', '등락률', '등락률_num', '거래량', '거래대금', '수집시간']]
        
        # MySQL stock_market_data 테이블에 누적(append) 저장
        success = save_to_db(db_df, 'stock_market_data', if_exists='append')
        
        if success:
            logger.info(f"=== [성공] {len(db_df)}종목 시세 DB 저장 완료 ({now.strftime('%H:%M')}) ===")
        else:
            logger.error("=== [실패] DB 저장 실패 ===")
            
    except Exception as e:
        logger.error(f"=== [오류] 스케줄링 작업 중 예외 발생: {e} ===")


if __name__ == "__main__":
    logger.info("=== 백그라운드 스케줄러를 시작합니다 ===")
    logger.info("주식 시세 수집: 평일(월-금) 09:00 ~ 15:00 매 정시 동작")
    
    scheduler = BackgroundScheduler(timezone='Asia/Seoul')
    
    # 평일 오전 9시부터 오후 3시까지 정각에 실행
    scheduler.add_job(
        job_realtime_market_data,
        'cron',
        day_of_week='mon-fri',
        hour='9-15',
        minute=0,
        id='realtime_market_job'
    )
    
    # 즉시 테스트를 위해 1회 실행 (개발용)
    logger.info("-> 스케줄러 등록 완료. 초기 테스트를 위해 즉시 1회 실행합니다.")
    job_realtime_market_data()
    
    scheduler.start()
    
    try:
        # 무한 루프 (앱 유지)
        while True:
            time.sleep(60)
            
    except (KeyboardInterrupt, SystemExit):
        logger.info("=== 스케줄러를 종료합니다 ===")
        scheduler.shutdown()
