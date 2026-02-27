import time
import logging
import pandas as pd
from datetime import datetime
from apscheduler.schedulers.background import BackgroundScheduler
from scraper import run_full_pipeline
import subprocess
import os

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# 글로벌 카운터 (1시간마다 실행되므로 24가 되면 1일)
hourly_run_count = 0

def job_realtime_market_data():
    global hourly_run_count
    hourly_run_count += 1
    
    """매 정시 전체 파이프라인(스크래핑 -> JSON 저장 -> DB C~G 갱신)을 실행합니다."""
    logger.info("=== [스케줄링] 1시간 단위 주식 시세 전체 수집 및 DB 동기화 파이프라인 시작 ===")
    try:
        # 1. 스크래퍼 전체 파이프라인 실행 (JSON 파일들 생성됨)
        logger.info("1. Scraper run_full_pipeline() 실행 중...")
        result = run_full_pipeline()
        logger.info("-> 스크래핑 및 JSON 저장 완료.")
        
        # 2. 생성된 JSON 들을 DB에 반영하는 C ~ G 스크립트 순차 실행
        scripts_to_run = [
            'C_stocks_table.py',
            'D_price_snapshots_table.py',
            'E_analysis_signals.py',
            'F_recommendations.py',
            'G_newsletters.py',
            'H_stock_fundamentals.py',
            'I_investor_trends.py'
        ]
        
        script_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'database_script')
        
        logger.info("2. Database Scripts (C~G) 순차 실행 중...")
        for script_name in scripts_to_run:
            script_path = os.path.join(script_dir, script_name)
            if os.path.exists(script_path):
                logger.info(f" -> 실행: {script_name}")
                res = subprocess.run(['python', script_path], capture_output=True, text=True)
                if res.returncode == 0:
                    logger.info(f"    [성공] {script_name}")
                else:
                    logger.error(f"    [실패] {script_name}\nError: {res.stderr}")
            else:
                logger.warning(f" -> 경고: {script_path} 파일을 찾을 수 없습니다.")
                
        # 3. 하루 간격 (24번 카운트) 달성 시 또는 오전 9시 정각일 때 이메일 뉴스레터 발송
        current_hour = datetime.now().hour
        if hourly_run_count >= 24 or current_hour == 9:
            logger.info(f"=== ⏰ 이벤트 트리거 (카운트: {hourly_run_count}, 현재시각: {current_hour}시): 일간 뉴스레터 이메일 발송 시작 ===")
            
            mailer_res = subprocess.run(
                ['python', '-m', 'mailer.send_newsletters'], 
                capture_output=True, 
                text=True, 
                cwd=os.path.dirname(os.path.abspath(__file__))
            )
            
            if mailer_res.returncode == 0:
                logger.info("    [성공] 뉴스레터 이메일 발송 완료")
            else:
                logger.error(f"    [실패] 뉴스레터 이메일 발송 오류\nError: {mailer_res.stderr}")
                logger.error(f"          Output: {mailer_res.stdout}")
            
            # 발송 후 카운트 초기화
            hourly_run_count = 0

        logger.info("=== [완료] 전체 파이프라인 스케줄링 정상 종료 ===")
        
    except Exception as e:
        logger.error(f"=== [오류] 스케줄링 작업 중 예외 발생: {e} ===")


if __name__ == "__main__":
    logger.info("=== 백그라운드 스케줄러를 시작합니다 ===")
    logger.info("주식 시세 수집: 24시간 매 정시 동작 (1시간 간격)")
    
    scheduler = BackgroundScheduler(timezone='Asia/Seoul')
    
    # 1시간 스케줄링 카운트를 위해 매 정각마다 실행
    scheduler.add_job(
        job_realtime_market_data,
        'cron',
        day_of_week='mon-sun',
        hour='*',
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
