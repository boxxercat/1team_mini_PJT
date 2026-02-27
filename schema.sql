-- ============================================
-- 주식 데이터 분석 & 종목 추천 시스템 DB 스키마
-- MySQL 8.0+
-- ============================================

CREATE DATABASE IF NOT EXISTS stock_recommend
  DEFAULT CHARACTER SET utf8mb4
  DEFAULT COLLATE utf8mb4_unicode_ci;

USE stock_recommend;

-- ──────────────────────────────────────────────
-- (A) users — 사용자
-- ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS users (
    user_id       INT AUTO_INCREMENT PRIMARY KEY,
    username      VARCHAR(50)  NOT NULL UNIQUE,
    email         VARCHAR(120) NOT NULL UNIQUE,
    created_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

-- ──────────────────────────────────────────────
-- (B) user_profile — 투자 성향 / 관심사
-- ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS user_profile (
    user_id       INT PRIMARY KEY,
    risk_level    ENUM('안정형','안정추구형','위험중립형','적극투자형','공격투자형')
                  NOT NULL DEFAULT '위험중립형'
                  COMMENT '투자 성향 (한양증권 5단계)',
    horizon       ENUM('SHORT','MID','LONG') NOT NULL DEFAULT 'MID'
                  COMMENT '투자 보유 기간',
    survey_score  INT DEFAULT NULL
                  COMMENT '설문 총점',
    updated_at    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
                  ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id)
        ON DELETE CASCADE
) ENGINE=InnoDB;

-- ──────────────────────────────────────────────
-- (C) stocks — 종목 마스터
-- ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS stocks (
    ticker        VARCHAR(20) PRIMARY KEY
                  COMMENT '종목코드 (예: 005930)',
    name          VARCHAR(100) NOT NULL
                  COMMENT '종목 이름',
    market        ENUM('KOSPI','KOSDAQ') NOT NULL
                  COMMENT '시장 구분'
) ENGINE=InnoDB;

-- ──────────────────────────────────────────────
-- (D) price_snapshots — 시세 스냅샷 (실시간 수집)
-- ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS price_snapshots (
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    ticker        VARCHAR(20) NOT NULL,
    captured_at   DATETIME    NOT NULL
                  COMMENT '수집 시간',
    price         BIGINT      NOT NULL
                  COMMENT '현재가',
    volume        BIGINT      NOT NULL
                  COMMENT '거래량',
    trade_value   BIGINT      NOT NULL DEFAULT 0
                  COMMENT '거래대금',
    change_val    BIGINT      DEFAULT 0
                  COMMENT '전일비',
    change_rate   VARCHAR(20) DEFAULT '0.00%'
                  COMMENT '등락률 문자열',
    change_rate_num FLOAT     DEFAULT 0.0
                  COMMENT '등락률 숫자형',
    UNIQUE KEY uq_snapshot (ticker, captured_at),
    FOREIGN KEY (ticker) REFERENCES stocks(ticker)
        ON DELETE CASCADE
) ENGINE=InnoDB;

-- ──────────────────────────────────────────────
-- (E) analysis_signals — 분석 결과
-- ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS analysis_signals (
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    ticker        VARCHAR(20)  NOT NULL,
    as_of         DATETIME     NOT NULL
                  COMMENT '분석 기준 시점',
    window        VARCHAR(10)  NOT NULL
                  COMMENT '분석 기간 (1D/1W/1M)',
    trend_score   FLOAT        DEFAULT NULL
                  COMMENT '추세 점수',
    signal        ENUM('BUY','HOLD','SELL') NOT NULL
                  COMMENT '매수/보유/매도 신호',
    UNIQUE KEY uq_signal (ticker, as_of, window),
    FOREIGN KEY (ticker) REFERENCES stocks(ticker)
        ON DELETE CASCADE
) ENGINE=InnoDB;

-- ──────────────────────────────────────────────
-- (F) recommendations — 사용자별 추천 결과
-- ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS recommendations (
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id       INT          NOT NULL,
    ticker        VARCHAR(20)  NOT NULL,
    as_of         DATE         NOT NULL
                  COMMENT '추천 생성 날짜',
    score         FLOAT        DEFAULT NULL
                  COMMENT '추천 점수',
    reason        TEXT         DEFAULT NULL
                  COMMENT '추천 이유 (예: 거래대금 급증 + 상승추세)',
    UNIQUE KEY uq_recommend (user_id, ticker, as_of),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
        ON DELETE CASCADE,
    FOREIGN KEY (ticker) REFERENCES stocks(ticker)
        ON DELETE CASCADE
) ENGINE=InnoDB;

-- ──────────────────────────────────────────────
-- (G) newsletters — 뉴스레터
-- ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS newsletters (
    id            BIGINT AUTO_INCREMENT PRIMARY KEY,
    user_id       INT          NOT NULL,
    created_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
    title         VARCHAR(255) NOT NULL,
    content       TEXT         NOT NULL
                  COMMENT '뉴스레터 본문',
    FOREIGN KEY (user_id) REFERENCES users(user_id)
        ON DELETE CASCADE
) ENGINE=InnoDB;
