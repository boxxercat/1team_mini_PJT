import pandas as pd
from pykrx import stock as pykrx_stock
import time

stock_df = pd.read_csv('data/stock_data_20260224.csv')
tickers = stock_df['종목코드'].astype(str).str.zfill(6).tolist()
print('외국인/기관 순매수 수집:', len(tickers), '종목')

results = []
for i, ticker in enumerate(tickers):
    try:
        df = pykrx_stock.get_market_trading_volume_by_date('20260220', '20260224', ticker)
        if not df.empty:
            latest = df.iloc[-1]
            results.append({
                '종목코드_str': ticker,
                '외국인_순매수량_new': int(latest.iloc[3]),
                '기관_순매수량_new': int(latest.iloc[0]),
            })
        time.sleep(0.05)
    except:
        pass
    if (i+1) % 50 == 0:
        print(f'  {i+1}/{len(tickers)} 완료')

inv_df = pd.DataFrame(results)

# Type 맞추기
stock_df['종목코드_str'] = stock_df['종목코드'].astype(str).str.zfill(6)
merged = stock_df.merge(inv_df, on='종목코드_str', how='left')

# _new가 붙은 컬럼 값을 기존 컬럼에 채움 (없으면 새로 생성)
if '외국인_순매수량_new' in merged.columns:
    if '외국인_순매수량' not in merged.columns:
        merged['외국인_순매수량'] = 0
    merged['외국인_순매수량'] = merged['외국인_순매수량_new'].fillna(merged['외국인_순매수량']).fillna(0).astype(int)
    
if '기관_순매수량_new' in merged.columns:
    if '기관_순매수량' not in merged.columns:
        merged['기관_순매수량'] = 0
    merged['기관_순매수량'] = merged['기관_순매수량_new'].fillna(merged['기관_순매수량']).fillna(0).astype(int)

merged = merged.drop(columns=[c for c in merged.columns if c.endswith('_new') or c == '종목코드_str'], errors='ignore')

merged.to_csv('data/stock_data_20260224.csv', index=False, encoding='utf-8-sig')
print('완료! 외국인/기관 데이터 merge:', len(merged), '종목')
foreign_nonzero = (merged['외국인_순매수량'] != 0).sum()
inst_nonzero = (merged['기관_순매수량'] != 0).sum()
print('외국인 데이터 있는 종목:', foreign_nonzero)
print('기관 데이터 있는 종목:', inst_nonzero)

# Analysis signals 재생성
from analyzer import generate_analysis_signals
signals_df = generate_analysis_signals(merged, '1D')
signals_df.to_csv('data/analysis_signals_20260224.csv', index=False, encoding='utf-8-sig')
print(f'분석 신호 재생성 완료: {len(signals_df)}건')
