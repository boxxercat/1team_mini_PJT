"""
투자 성향 분석 & 종목 추천 엔진
================================
한양증권 투자성향진단 기준 5단계 분류:
  안정형 → 안정추구형 → 위험중립형 → 적극투자형 → 공격투자형

설문 11문항에 대한 점수를 합산하여 투자 성향을 분류하고,
성향에 맞는 종목 추천 스코어를 계산합니다.
"""

import pandas as pd
import numpy as np
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

# ============================================================
# 1. 투자 성향 설문 정의 (한양증권 기준 11문항)
# ============================================================
SURVEY_QUESTIONS = [
    {
        'id': 'q1',
        'question': '고객님의 연령대는 어떻게 되십니까?',
        'options': [
            ('만 19세 이하', 1),
            ('만 20세~30세', 5),
            ('만 31세~54세', 4),
            ('만 55세~64세', 3),
            ('만 65세 이상', 1),
        ],
    },
    {
        'id': 'q2',
        'question': '투자하실 자금의 투자가능 기간은 어느 정도입니까?',
        'options': [
            ('6개월 미만', 1),
            ('6개월 이상~1년 미만', 2),
            ('1년 이상~2년 미만', 3),
            ('2년 이상~3년 미만', 4),
            ('3년 이상', 5),
        ],
    },
    {
        'id': 'q3',
        'question': '다음 중 투자경험과 가장 가까운 상품은 무엇입니까?',
        'options': [
            ('은행 예/적금, 국채, MMF, CMA 등', 1),
            ('금융채, 회사채, 채권형 펀드 등', 2),
            ('혼합형 펀드, 원금 일부 보장 ELS 등', 3),
            ('주식, 원금 비보장 ELS, 주식형 펀드 등', 4),
            ('ELW, 선물옵션, 파생상품 펀드, 신용거래 등', 5),
        ],
    },
    {
        'id': 'q4',
        'question': '금융투자상품 투자경험 기간은 어떻게 되십니까?',
        'options': [
            ('전혀 없음', 1),
            ('1년 미만', 2),
            ('1년 이상~3년 미만', 3),
            ('3년 이상~5년 미만', 4),
            ('5년 이상', 5),
        ],
    },
    {
        'id': 'q5',
        'question': '금융투자상품 취득 및 목적은 어떤 것입니까?',
        'options': [
            ('채무상환', 1),
            ('생활비', 2),
            ('주택마련', 3),
            ('여유자금', 4),
            ('자산증식', 5),
        ],
    },
    {
        'id': 'q6',
        'question': '금융투자상품 투자에 대한 지식수준은 어느 정도입니까?',
        'options': [
            ('금융상품에 투자해 본 경험이 없음', 1),
            ('주식, 채권, 펀드 등의 구조 및 위험을 일정 부분 이해', 3),
            ('주식, 채권, 펀드 등의 구조 및 위험을 깊이 있게 이해', 4),
            ('파생상품 포함 대부분의 금융투자상품 이해', 5),
        ],
    },
    {
        'id': 'q7',
        'question': '투자수익·투자위험에 대한 태도는 어떻습니까?',
        'options': [
            ('투자수익을 고려하나 원금보존 추구', 1),
            ('원금 보존을 고려하나 투자수익 추구 또는 손실위험 감수', 5),
        ],
    },
    {
        'id': 'q8',
        'question': '고객님의 총 자산은 어떻습니까?',
        'options': [
            ('1억 미만', 1),
            ('1억 이상~2억 미만', 2),
            ('2억 이상~5억 미만', 3),
            ('5억 이상~10억 미만', 4),
            ('10억 이상', 5),
        ],
    },
    {
        'id': 'q9',
        'question': '향후 수입원에 대한 예상은 어떻게 하고 계십니까?',
        'options': [
            ('일정 수입 + 유지 또는 증가 예상', 5),
            ('일정 수입 있으나 감소/불안정 예상', 3),
            ('일정 수입 없으며 연금이 주 수입원', 1),
        ],
    },
    {
        'id': 'q10',
        'question': '기대이익 수준은 어떻게 되십니까?',
        'options': [
            ('원금 기준 10% 범위', 1),
            ('원금 기준 20% 범위', 2),
            ('원금 기준 50% 범위', 3),
            ('원금 기준 70% 범위', 4),
            ('원금 기준 100% 범위', 5),
        ],
    },
    {
        'id': 'q11',
        'question': '감내할 수 있는 손실 수준은 어느 정도입니까?',
        'options': [
            ('원금보존추구', 1),
            ('원금 기준 -10% 범위', 2),
            ('원금 기준 -20% 범위', 3),
            ('원금 기준 -50% 범위', 4),
            ('원금 기준 -70% 범위', 5),
            ('전액손실감내가능', 6),
        ],
    },
]


# ============================================================
# 2. 투자 성향 분류
# ============================================================
def classify_investor_type(answers):
    """
    11문항 설문 점수를 합산하여 투자 성향을 5단계로 분류합니다.

    Args:
        answers: dict - {q1: 선택 인덱스(0-based), q2: ..., ...}
    Returns:
        tuple(str, int) - (투자성향, 총점)
    """
    total_score = 0
    max_possible = 0

    for q in SURVEY_QUESTIONS:
        qid = q['id']
        selected_idx = answers.get(qid, 0)
        # 선택된 옵션의 점수
        if 0 <= selected_idx < len(q['options']):
            total_score += q['options'][selected_idx][1]
        # 최대 점수
        max_possible += max(score for _, score in q['options'])

    # 점수 비율로 5단계 분류
    ratio = total_score / max_possible if max_possible > 0 else 0

    if ratio <= 0.25:
        investor_type = '안정형'
    elif ratio <= 0.40:
        investor_type = '안정추구형'
    elif ratio <= 0.60:
        investor_type = '위험중립형'
    elif ratio <= 0.80:
        investor_type = '적극투자형'
    else:
        investor_type = '공격투자형'

    logger.info(f"[성향 분류] 총점={total_score}/{max_possible}, "
                f"비율={ratio:.2%}, 성향={investor_type}")
    return investor_type, total_score


# ============================================================
# 3. 투자 성향별 종목 스코어링
# ============================================================

# 성향별 가중치 프로필
WEIGHT_PROFILES = {
    '안정형': {
        '배당수익률': 0.30,
        '시가총액_순위': 0.25,     # 대형주일수록 높은 점수
        '변동폭_역순위': 0.25,    # 변동폭이 낮을수록 높은 점수
        'PBR_역순위': 0.10,       # 낮은 PBR (저평가)
        '기관_순매수': 0.10,
    },
    '안정추구형': {
        '배당수익률': 0.20,
        '시가총액_순위': 0.20,
        '기관_순매수': 0.20,
        'PER_적정': 0.20,         # PER이 적정 범위
        '변동폭_역순위': 0.20,
    },
    '위험중립형': {
        '외국인_순매수': 0.25,
        'PER_적정': 0.20,
        '거래량_순위': 0.20,
        '시가총액_순위': 0.15,
        '등락률_절대값': 0.20,
    },
    '적극투자형': {
        '거래량_순위': 0.30,
        '등락률_절대값': 0.25,
        '외국인_순매수': 0.20,
        '변동폭_순위': 0.15,      # 변동폭이 높을수록 높은 점수
        'PER_적정': 0.10,
    },
    '공격투자형': {
        '거래량_순위': 0.35,
        '등락률_절대값': 0.30,
        '변동폭_순위': 0.20,
        '외국인_순매수': 0.15,
    },
}

# 성향별 설명
TYPE_DESCRIPTIONS = {
    '안정형': {
        'emoji': '🛡️',
        'title': '안정형 투자자',
        'desc': '예금 또는 적금 수준의 수익률을 기대하며, '
                '투자원금에 손실이 발생하는 것을 원하지 않습니다.',
        'strategy': '고배당 대형 우량주, 낮은 변동성 종목 위주 추천',
        'color': '#2E86AB',
    },
    '안정추구형': {
        'emoji': '🔒',
        'title': '안정추구형 투자자',
        'desc': '투자 원금의 손실위험은 최소화하고, '
                '이자소득이나 배당소득 수준의 안정적인 투자를 목표로 합니다. '
                '예·적금보다 높은 수익을 위해 일부 변동성을 허용합니다.',
        'strategy': '배당+가치주, 기관 순매수 양호 중대형주 추천',
        'color': '#A23B72',
    },
    '위험중립형': {
        'emoji': '⚖️',
        'title': '위험중립형 투자자',
        'desc': '투자에는 그에 상응하는 위험이 있음을 충분히 인식하고 있으며, '
                '예·적금보다 높은 수익을 기대할 수 있다면 일정 수준의 손실을 감수합니다.',
        'strategy': '성장주, 적정 PER, 외국인 매수 종목 위주 추천',
        'color': '#F18F01',
    },
    '적극투자형': {
        'emoji': '🚀',
        'title': '적극투자형 투자자',
        'desc': '투자원금의 보전보다는 위험을 감내하더라도 '
                '높은 수준의 투자수익 실현을 추구합니다.',
        'strategy': '거래량 급등, 모멘텀 종목, 고변동성 추천',
        'color': '#C73E1D',
    },
    '공격투자형': {
        'emoji': '🔥',
        'title': '공격투자형 투자자',
        'desc': '시장 평균 수익률을 훨씬 넘어서는 높은 수준의 투자수익을 추구하며, '
                '자산 가치의 변동에 따른 손실 위험을 적극 수용합니다.',
        'strategy': '테마주, 최고 거래량, 고등락률 종목 추천',
        'color': '#D00000',
    },
}


def _normalize_series(s, ascending=True):
    """시리즈를 0~100 사이로 정규화합니다."""
    s = pd.to_numeric(s, errors='coerce').fillna(0)
    min_v, max_v = s.min(), s.max()
    if max_v == min_v:
        return pd.Series(50, index=s.index)
    normalized = (s - min_v) / (max_v - min_v) * 100
    if not ascending:
        normalized = 100 - normalized
    return normalized


def score_stocks(df, investor_type):
    """
    투자 성향에 따라 종목별 추천 점수를 계산합니다.

    Args:
        df: 통합 주식 데이터 DataFrame
        investor_type: 투자 성향 (5단계 중 하나)
    Returns:
        DataFrame with '추천점수' and '추천이유' columns added
    """
    if df.empty:
        return df

    result = df.copy()
    weights = WEIGHT_PROFILES.get(investor_type, WEIGHT_PROFILES['위험중립형'])

    # ── 지표별 점수 계산 ──
    scores = pd.DataFrame(index=result.index)

    # 배당수익률 (높을수록 좋음)
    if '배당수익률' in result.columns:
        scores['배당수익률'] = _normalize_series(result['배당수익률'], ascending=True)
    else:
        scores['배당수익률'] = 0

    # 시가총액 순위 (대형주 = 높은 점수)
    if '시가총액(억)' in result.columns:
        scores['시가총액_순위'] = _normalize_series(result['시가총액(억)'], ascending=True)
    else:
        scores['시가총액_순위'] = 50

    # 변동폭 역순위 (낮을수록 안정적)
    if '52주변동폭(%)' in result.columns:
        scores['변동폭_역순위'] = _normalize_series(result['52주변동폭(%)'], ascending=False)
        scores['변동폭_순위'] = _normalize_series(result['52주변동폭(%)'], ascending=True)
    else:
        scores['변동폭_역순위'] = 50
        scores['변동폭_순위'] = 50

    # PBR 역순위 (낮을수록 저평가)
    if 'PBR' in result.columns:
        scores['PBR_역순위'] = _normalize_series(result['PBR'], ascending=False)
    else:
        scores['PBR_역순위'] = 50

    # 기관 순매수
    if '기관_순매수량' in result.columns:
        scores['기관_순매수'] = _normalize_series(result['기관_순매수량'], ascending=True)
    else:
        scores['기관_순매수'] = 50

    # 외국인 순매수
    if '외국인_순매수량' in result.columns:
        scores['외국인_순매수'] = _normalize_series(result['외국인_순매수량'], ascending=True)
    else:
        scores['외국인_순매수'] = 50

    # PER 적정 (10~20 범위가 가장 높은 점수)
    if 'PER' in result.columns:
        per = pd.to_numeric(result['PER'], errors='coerce').fillna(0)
        # PER이 10~20 사이면 100점, 멀어질수록 감소
        scores['PER_적정'] = per.apply(
            lambda x: max(0, 100 - abs(x - 15) * 5) if x > 0 else 0
        )
    else:
        scores['PER_적정'] = 50

    # 거래량 순위 (높을수록 좋음)
    if '거래량' in result.columns:
        scores['거래량_순위'] = _normalize_series(result['거래량'], ascending=True)
    else:
        scores['거래량_순위'] = 50

    # 등락률 절대값 (높을수록 변동성 큼)
    if '등락률(숫자)' in result.columns:
        scores['등락률_절대값'] = _normalize_series(
            result['등락률(숫자)'].abs(), ascending=True
        )
    elif '등락률' in result.columns:
        from scraper import parse_change_pct
        pct_values = result['등락률'].apply(parse_change_pct).abs()
        scores['등락률_절대값'] = _normalize_series(pct_values, ascending=True)
    else:
        scores['등락률_절대값'] = 50

    # ── 가중치 적용 총점 계산 ──
    total = pd.Series(0.0, index=result.index)
    reasons = []

    for metric, weight in weights.items():
        if metric in scores.columns:
            total += scores[metric] * weight

    result['추천점수'] = total.round(2)

    # ── 추천 이유 생성 ──
    def make_reason(row, idx):
        parts = []
        score_data = scores.loc[idx]
        top_metrics = sorted(weights.items(), key=lambda x: x[1], reverse=True)[:3]

        for metric, weight in top_metrics:
            if metric in score_data.index:
                val = score_data[metric]
                if val >= 65: # Changed from 70 to 65 for richer reasons
                    metric_names = {
                        '배당수익률': '안정적인 배당 매력 부각',
                        '시가총액_순위': '우수한 펀더멘털 기반 안정성',
                        '변동폭_역순위': '견조한 하방 경직성 확보',
                        '변동폭_순위': '단기 변동성 확대를 통한 모멘텀 부각',
                        'PBR_역순위': '자산가치 대비 저평가 밸류에이션',
                        '기관_순매수': '메이저 기관 수급 집중',
                        '외국인_순매수': '외국인 투심 개선 및 견조한 순매수',
                        'PER_적정': '실적 감안 시 합리적 밸류에이션',
                        '거래량_순위': '유의미한 거래대금 동반',
                        '등락률_절대값': '강력한 시세 탄력성',
                    }
                    parts.append(metric_names.get(metric, metric))

        if not parts:
            return '종합 퀀트 지표 양호 및 시장 트렌드 부합'
            
        return ', '.join(parts) + ' 측면에서 긍정적 시그널 포착'

    result['추천이유'] = [make_reason(result.iloc[i], result.index[i])
                        for i in range(len(result))]

    # 추천 점수 기준 정렬
    result = result.sort_values('추천점수', ascending=False).reset_index(drop=True)

    logger.info(f"[스코어링] {investor_type} 기준 {len(result)}개 종목 점수 계산 완료")
    return result


def get_top_recommendations(df, investor_type, top_n=10):
    """
    투자 성향에 맞는 상위 N개 종목을 추천합니다.

    Args:
        df: 통합 주식 데이터 DataFrame
        investor_type: 투자 성향
        top_n: 추천 종목 수
    Returns:
        DataFrame - 추천 종목 리스트
    """
    scored_df = score_stocks(df, investor_type)
    result = scored_df.head(top_n)
    logger.info(f"[추천] {investor_type} 성향 상위 {len(result)}개 종목 추천 생성")
    return result


# ============================================================
# 4. 시장 분석 요약
# ============================================================
def generate_analysis_summary(df):
    """
    전체 시장 데이터에 대한 요약 통계를 생성합니다.

    Args:
        df: 통합 주식 데이터 DataFrame
    Returns:
        dict - 요약 통계
    """
    summary = {
        '총 종목 수': len(df),
        '수집 시간': df['수집시간'].iloc[0] if '수집시간' in df.columns and len(df) > 0 else 'N/A',
    }

    if 'KOSPI' in df['시장'].values:
        kospi = df[df['시장'] == 'KOSPI']
        summary['KOSPI 종목 수'] = len(kospi)
    if 'KOSDAQ' in df['시장'].values:
        kosdaq = df[df['시장'] == 'KOSDAQ']
        summary['KOSDAQ 종목 수'] = len(kosdaq)

    # 등락 통계
    pct = None
    if '등락률(숫자)' in df.columns:
        pct = pd.to_numeric(df['등락률(숫자)'], errors='coerce')
    elif '등락률' in df.columns:
        from scraper import parse_change_pct
        pct = df['등락률'].apply(parse_change_pct)

    if pct is not None:
        summary['상승 종목 수'] = int((pct > 0).sum())
        summary['하락 종목 수'] = int((pct < 0).sum())
        summary['보합 종목 수'] = int((pct == 0).sum())
        summary['평균 등락률(%)'] = round(pct.mean(), 2)

    # 거래량 통계
    if '거래량' in df.columns:
        vol = pd.to_numeric(df['거래량'], errors='coerce')
        summary['평균 거래량'] = f"{int(vol.mean()):,}"
        summary['최대 거래량 종목'] = df.loc[vol.idxmax(), '종목명'] if not vol.empty else 'N/A'

    # 외국인/기관 통계
    if '외국인_순매수량' in df.columns:
        foreign = pd.to_numeric(df['외국인_순매수량'], errors='coerce')
        summary['외국인 순매수 종목'] = int((foreign > 0).sum())
        summary['외국인 순매도 종목'] = int((foreign < 0).sum())

    if '기관_순매수량' in df.columns:
        inst = pd.to_numeric(df['기관_순매수량'], errors='coerce')
        summary['기관 순매수 종목'] = int((inst > 0).sum())
        summary['기관 순매도 종목'] = int((inst < 0).sum())

    return summary


# ============================================================
# 5. (E) analysis_signals — 분석 신호 생성
#    DB 스키마: ticker, as_of, window, trend_score, signal
# ============================================================
def generate_analysis_signals(df, window='1D'):
    """
    종목별 추세 점수와 BUY/HOLD/SELL 신호를 생성합니다.

    추세 점수 산출 로직:
      ① 등락률 점수   (가중치 40%) : 상승 → +, 하락 → -
      ② 거래량 점수   (가중치 20%) : 평균 대비 비율
      ③ 외국인 순매수 (가중치 20%) : 양(+) → 매수 신호
      ④ 기관 순매수   (가중치 20%) : 양(+) → 매수 신호

    signal 판정:
      trend_score >= 60  → BUY
      trend_score >= 40  → HOLD
      trend_score <  40  → SELL

    Args:
        df: 통합 주식 데이터 DataFrame
        window: 분석 기간 라벨 ('1D', '1W', '1M')
    Returns:
        DataFrame — analysis_signals 테이블 형식
    """
    if df.empty:
        return pd.DataFrame()

    records = []
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # 정규화를 위한 전체 통계
    vol_mean = pd.to_numeric(df.get('거래량', pd.Series(dtype=float)),
                             errors='coerce').mean()
    if pd.isna(vol_mean) or vol_mean == 0:
        vol_mean = 1

    for _, row in df.iterrows():
        # ── ① 등락률 점수 (0~100) ──
        pct = 0.0
        if '등락률(숫자)' in df.columns:
            pct = pd.to_numeric(row.get('등락률(숫자)', 0), errors='coerce')
        elif '등락률' in df.columns:
            from scraper import parse_change_pct
            pct = parse_change_pct(str(row.get('등락률', '0')))
        if pd.isna(pct):
            pct = 0.0
        # 등락률을 0~100 스케일로 변환 (-30% → 0, 0% → 50, +30% → 100)
        pct_score = max(0, min(100, 50 + pct * (50 / 30)))

        # ── ② 거래량 점수 (0~100) ──
        vol = pd.to_numeric(row.get('거래량', 0), errors='coerce')
        if pd.isna(vol):
            vol = 0
        vol_ratio = vol / vol_mean
        vol_score = max(0, min(100, vol_ratio * 50))

        # ── ③ 외국인 순매수 점수 (0~100) ──
        foreign = pd.to_numeric(row.get('외국인_순매수량', 0), errors='coerce')
        if pd.isna(foreign):
            foreign = 0
        foreign_score = 50 + (50 if foreign > 0 else -30 if foreign < 0 else 0)
        foreign_score = max(0, min(100, foreign_score))

        # ── ④ 기관 순매수 점수 (0~100) ──
        inst = pd.to_numeric(row.get('기관_순매수량', 0), errors='coerce')
        if pd.isna(inst):
            inst = 0
        inst_score = 50 + (50 if inst > 0 else -30 if inst < 0 else 0)
        inst_score = max(0, min(100, inst_score))

        # ── 가중 평균 추세 점수 ──
        trend_score = round(
            pct_score * 0.40 +
            vol_score * 0.20 +
            foreign_score * 0.20 +
            inst_score * 0.20,
            2
        )

        # ── 신호 판정 ──
        if trend_score >= 60:
            signal = 'BUY'
        elif trend_score >= 40:
            signal = 'HOLD'
        else:
            signal = 'SELL'

        records.append({
            'ticker': row.get('종목코드', ''),
            'as_of': now,
            'window': window,
            'trend_score': trend_score,
            'signal': signal,
        })

    signals_df = pd.DataFrame(records)
    logger.info(f"[분석 신호] {len(signals_df)}개 종목 {window} 신호 생성 완료 "
                f"(BUY:{(signals_df['signal']=='BUY').sum()}, "
                f"HOLD:{(signals_df['signal']=='HOLD').sum()}, "
                f"SELL:{(signals_df['signal']=='SELL').sum()})")
    return signals_df


# ============================================================
# 6. (F) recommendations — DB 형식 추천 데이터 생성
#    DB 스키마: user_id, ticker, as_of, score, reason
# ============================================================
def build_recommendations_df(scored_df, user_id=1, top_n=10):
    """
    스코어링된 DataFrame을 recommendations 테이블 형식으로 변환합니다.

    Args:
        scored_df: score_stocks() 결과 DataFrame (추천점수, 추천이유 포함)
        user_id: 사용자 ID (기본값: 1)
        top_n: 추천 종목 수
    Returns:
        DataFrame — recommendations 테이블 형식
    """
    if scored_df.empty:
        return pd.DataFrame()

    top = scored_df.head(top_n)
    today = datetime.now().strftime('%Y-%m-%d')

    recs = pd.DataFrame({
        'user_id': user_id,
        'ticker': top['종목코드'].values,
        'as_of': today,
        'score': top['추천점수'].values,
        'reason': top['추천이유'].values,
    })

    logger.info(f"[추천 DB] user_id={user_id}, {len(recs)}건 추천 데이터 생성")
    return recs


# ============================================================
# 7. (G) newsletters — 뉴스레터 생성
#    DB 스키마: user_id, created_at, title, content
# ============================================================
def generate_newsletter(stock_df, scored_df, signals_df, investor_type,
                        user_id=1, news_df=None):
    """
    투자 성향별 맞춤 뉴스레터를 생성합니다.

    Args:
        stock_df: 전체 주식 데이터 DataFrame
        scored_df: 스코어링된 추천 DataFrame
        signals_df: 분석 신호 DataFrame
        investor_type: 투자 성향 (5단계)
        user_id: 사용자 ID
        news_df: 뉴스 DataFrame (선택)
    Returns:
        dict — newsletters 테이블 형식
    """
    now = datetime.now()
    type_info = TYPE_DESCRIPTIONS.get(investor_type, TYPE_DESCRIPTIONS['위험중립형'])
    summary = generate_analysis_summary(stock_df)

    # ── 제목 ──
    title = f"[{now.strftime('%Y-%m-%d')}] {type_info['emoji']} {investor_type} 맞춤 투자 뉴스레터"

    # ── 본문 구성 ──
    lines = []
    lines.append(f"{'='*50}")
    lines.append(f"📊 {investor_type} 투자자를 위한 일일 뉴스레터")
    lines.append(f"📅 {now.strftime('%Y년 %m월 %d일 %H:%M')}")
    lines.append(f"{'='*50}")
    lines.append("")

    # 시장 개요
    lines.append("■ 시장 개요")
    lines.append(f"  - 분석 종목 수: {summary.get('총 종목 수', 0)}개")
    if '상승 종목 수' in summary:
        lines.append(f"  - 상승: {summary['상승 종목 수']}개 | "
                     f"하락: {summary['하락 종목 수']}개 | "
                     f"보합: {summary.get('보합 종목 수', 0)}개")
    if '평균 등락률(%)' in summary:
        lines.append(f"  - 평균 등락률: {summary['평균 등락률(%)']}%")
    lines.append("")

    # 분석 신호 요약
    if not signals_df.empty:
        buy_cnt = (signals_df['signal'] == 'BUY').sum()
        hold_cnt = (signals_df['signal'] == 'HOLD').sum()
        sell_cnt = (signals_df['signal'] == 'SELL').sum()
        lines.append("■ 분석 신호 요약")
        lines.append(f"  - 🟢 매수(BUY): {buy_cnt}개")
        lines.append(f"  - 🟡 보유(HOLD): {hold_cnt}개")
        lines.append(f"  - 🔴 매도(SELL): {sell_cnt}개")
        lines.append("")

    # 추천 종목 TOP 5
    lines.append(f"■ {type_info['emoji']} {investor_type} 추천 종목 TOP 5")
    lines.append(f"  전략: {type_info['strategy']}")
    lines.append("")

    top5 = scored_df.head(5)
    for i, (_, row) in enumerate(top5.iterrows(), 1):
        name = row.get('종목명', row.get('종목코드', ''))
        price = row.get('현재가', 0)
        pct = row.get('등락률', 'N/A')
        score = row.get('추천점수', 0)
        reason = row.get('추천이유', '')
        lines.append(f"  {i}. {name}")
        lines.append(f"     현재가: {price:,}원 ({pct})")
        lines.append(f"     추천점수: {score:.1f}점 | 사유: {reason}")

        # 해당 종목의 분석 신호
        if not signals_df.empty and '종목코드' in row.index:
            sig_row = signals_df[signals_df['ticker'] == row['종목코드']]
            if not sig_row.empty:
                sig = sig_row.iloc[0]
                lines.append(f"     분석신호: {sig['signal']} "
                             f"(추세점수: {sig['trend_score']})")
        lines.append("")

    # 관련 뉴스
    if news_df is not None and not news_df.empty:
        lines.append("■ 관련 뉴스")
        top_tickers = top5['종목코드'].tolist() if '종목코드' in top5.columns else []
        relevant_news = news_df[news_df['종목코드'].isin(top_tickers)] if top_tickers else news_df
        for _, nrow in relevant_news.head(5).iterrows():
            stock_name = nrow.get('종목명', nrow.get('종목코드', ''))
            news_title = nrow.get('뉴스제목', '')
            lines.append(f"  📰 [{stock_name}] {news_title}")
        lines.append("")

    lines.append(f"{'='*50}")
    lines.append("※ 본 뉴스레터는 투자 참고용이며, 투자 판단의 책임은 투자자에게 있습니다.")

    content = '\n'.join(lines)

    newsletter = {
        'user_id': user_id,
        'created_at': now.strftime('%Y-%m-%d %H:%M:%S'),
        'title': title,
        'content': content,
    }

    logger.info(f"[뉴스레터] user_id={user_id}, '{title}' 생성 완료")
    return newsletter
