import asyncio
import os
import io
import time
import concurrent.futures
from typing import List, Optional

import pandas as pd
import yfinance as yf
import requests as std_requests
from curl_cffi import requests as curl_requests
from yahooquery import Ticker as YQTicker
from bs4 import BeautifulSoup
import re

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

app = FastAPI()

# Renderのメモリ制限(無料枠512MB)と安定性を考慮し、ワーカーを2に削減
_executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)

def get_disguised_headers():
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "ja-JP,ja;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://finance.yahoo.co.jp/",
        "Cache-Control": "max-age=0",
    }

# yfinance用の標準セッション（yfinanceは自前でCrumbを取得するため、標準的なrequestsが安定する）
_std_session = std_requests.Session()
_std_session.headers.update(get_disguised_headers())
_std_session.verify = False # Bypass SSL certificate verification for local/cloud restricted environments

# クラウド環境からのブロックを防ぐ＆フリーズ防止のためのタイムアウト設定(10秒)
_curl_session = curl_requests.Session(impersonate="chrome", verify=False, timeout=10.0)

# Frontend setup
app.mount("/static", StaticFiles(directory="static"), name="static")

class StockInfo(BaseModel):
    code: str
    name: str
    per: str
    pbr: str
    dividend_yield: str
    decision: str
    yuutai: str
    shares: Optional[str] = "-"          # 保有株数
    cost_price: Optional[str] = "-"      # 取得単価
    current_price: Optional[str] = "-"   # 現在値
    acquisition_cost: Optional[str] = "-" # 取得金額
    market_value: Optional[str] = "-"    # 評価額
    profit_loss: Optional[str] = "-"     # 評価損益
    return_rate: Optional[str] = "-"     # 騰落率

@app.get("/", response_class=HTMLResponse)
async def read_index():
    with open("static/index.html", "r", encoding="utf-8") as f:
        return f.read()

def fetch_minkabu_yuutai(code: str) -> str:
    """みんかぶ (minkabu.jp) のページから株主優待の有無を判定します。"""
    code_str = str(code).strip()
    match = re.match(r'^(\d{4})', code_str)
    if not match:
        return "対象外(ETF等)"

    num_code = match.group(1)
    url = f"https://minkabu.jp/stock/{num_code}/yutai"
    
    try:
        # curl_session (impersonate="chrome") を使用してブロックを回避
        response = _curl_session.get(url)
        
        # 判定1: URL転送チェック (優待なしの場合はトップページ等にリダイレクトされる)
        if not response.url.endswith('yutai'):
            return "無し"
            
        soup = BeautifulSoup(response.text, 'html.parser')
        clean_text = re.sub(r'\s+', '', soup.get_text())
        
        # 判定2: 特定キーワードのチェック
        no_yutai_keywords = ["株主優待はありません", "株主優待情報はありません", "実施しておりません"]
        if any(kw in clean_text for kw in no_yutai_keywords):
            return "無し"

        # 判定3: 権利確定月のチェック
        for el in soup.find_all(['th', 'dt']):
            if "権利確定月" in el.get_text():
                next_el = el.find_next_sibling(['td', 'dd'])
                if next_el:
                    val = next_el.get_text().strip()
                    if val and val not in ["-", "ー", "なし", "無し", ""]:
                        return "有り"

        # 判定4: 文言検索
        if re.search(r'権利確定月[^\d]*?\d{1,2}月', clean_text):
            return "有り"

        return "無し"
    except Exception as e:
        print(f"  [Minkabu] {code} failed: {e}")
        return "不明"

def fetch_single_stock(code: str, name: str) -> StockInfo:
    """安定性最優先のデータ取得。yfinance -> yahooquery -> finance.yahoo.co.jp の順で、高度なセッション管理を使用。"""
    ticker_str = str(code).strip()
    if not ticker_str.endswith(".T"):
        ticker_str = f"{ticker_str}.T"

    time.sleep(2.0) # レート制限対策を強化 (みんかぶ等への配慮)
    print(f"--- Fetching {ticker_str} ({name}) ---")

    # --- Step 1: yfinance (改良版セッション) ---
    for attempt in range(2):
        try:
            # Try with session first, then without if session is the problem
            try:
                ticker = yf.Ticker(ticker_str, session=_std_session if attempt == 0 else None)
                info = ticker.info
            except Exception as e:
                if "session" in str(e).lower() or "curl_cffi" in str(e).lower():
                    ticker = yf.Ticker(ticker_str)
                    info = ticker.info
                else:
                    raise e
            
            if info and len(info) > 10: # キーがある程度取れているかチェック
                per = info.get("trailingPE") or info.get("forwardPE")
                pbr = info.get("priceToBook")
                div_yield = (info.get("dividendYield") or 
                             info.get("trailingAnnualDividendYield") or 
                             info.get("dividendRate"))

                quote_type = info.get("quoteType", "").upper()
                
                if per or pbr or div_yield or quote_type == "ETF":
                    print(f"  [yfinance] {ticker_str}: Success (PER:{per}, PBR:{pbr}, Div:{div_yield})")
                    yuutai = fetch_minkabu_yuutai(code)
                    return _construct_stock_info(code, name, per, pbr, div_yield, yuutai, quote_type)
            
            print(f"  [yfinance] {ticker_str} attempt {attempt+1}: No metrics found.")
        except Exception as e:
            print(f"  [yfinance] {ticker_str} attempt {attempt+1} failed: {e}")
            time.sleep(1.0)

    # --- Step 2: yahooquery (yfinanceが失敗または空の場合) ---
    print(f"  [yahooquery] Switching to yahooquery for {ticker_str}")
    try:
        # Add verify=False to bypass SSL errors in restricted environments
        yq = YQTicker(ticker_str, verify=False)
        sd = yq.summary_detail.get(ticker_str, {})
        ks = yq.key_statistics.get(ticker_str, {})
        
        if isinstance(sd, dict) and sd:
            per = sd.get("trailingPE") or sd.get("forwardPE") or ks.get("trailingPE") or ks.get("forwardPE")
            pbr = sd.get("priceToBook") or ks.get("priceToBook")
            div_yield = (sd.get("dividendYield") or 
                         sd.get("trailingAnnualDividendYield") or 
                         sd.get("dividendRate"))

            quote_type = "EQUITY"
            if sd.get("quoteType") == "ETF": quote_type = "ETF"

            if per or pbr or div_yield or quote_type == "ETF":
                print(f"  [yahooquery] {ticker_str}: Success")
                yuutai = fetch_minkabu_yuutai(code)
                return _construct_stock_info(code, name, per, pbr, div_yield, yuutai, quote_type)
    except Exception as e:
        print(f"  [yahooquery] {ticker_str} failed: {e}")

    # --- Step 3: Minimal Scrape of Yahoo Finance Japan (最終手段/無料) ---
    # APIが遮断されていてもブラウザ向けのページなら取れる場合がある
    print(f"  [Fallback] Attempting direct scrape for {code}")
    try:
        url = f"https://finance.yahoo.co.jp/quote/{code}.T"
        resp = _curl_session.get(url)
        if resp.status_code == 200:
            html = resp.text
            # Robust regex to extract numeric values from Yahoo Finance JP desktop layout
            def extract_styled_number(label, html_str):
                # Search for the label, then look for the next StyledNumber__value span
                # This is more robust against HTML changes than fixed path regex
                pattern = rf'{label}.*?<span class="StyledNumber__value.*?">(.*?)</span>'
                matches = pd.Series(html_str).str.extract(pattern)
                if not matches.empty and not pd.isna(matches.iloc[0,0]):
                    val = matches.iloc[0,0].replace(',', '').replace('倍', '').replace('%', '').replace('---', '').strip()
                    try: return float(val)
                    except: return None
                return None

            f_per = extract_styled_number("PER", html)
            f_pbr = extract_styled_number("PBR", html)
            f_div_raw = extract_styled_number("配当利回り", html)
            f_div = f_div_raw / 100.0 if f_div_raw is not None else None

            # ETF判定 (業種欄から判定)
            import re
            industry_match = re.search(r'PriceBoard__industryName.*?">(.*?)</span>', html)
            industry_text = industry_match.group(1) if industry_match else ""
            is_etf = any(kw in industry_text for kw in ["ETF", "REIT", "上場投信"])
            q_type = "ETF" if is_etf else "EQUITY"

            # 正常に何らかの値が取れた場合、またはETFの場合は正常終了扱いとする
            if f_per or f_pbr or f_div or is_etf:
                 print(f"  [Scrape] {ticker_str}: Success from Yahoo JP Web (Type: {q_type})")
                 yuutai = fetch_minkabu_yuutai(code)
                 return _construct_stock_info(code, name, f_per, f_pbr, f_div, yuutai, q_type)
    except Exception as e:
        print(f"  [Scrape] {ticker_str} failed: {e}")

    # すべて失敗（またはETFと判定できなかった場合のみエラー）
    return StockInfo(
        code=str(code),
        name=str(name),
        per="エラー",
        pbr="-",
        dividend_yield="-",
        decision="再取得推奨",
        yuutai="エラー"
    )

def _is_valid(v):
    if v is None: return False
    try: 
        fv = float(v)
        return not (fv != fv) # nan check
    except: return False

def _construct_stock_info(code, name, per, pbr, div_yield, yuutai="不明", quote_type="EQUITY"):
    # ETFの場合は、PER/PBRがなくても正常（ハイフン表示）とする
    if quote_type == "ETF":
        per_str = f"{round(float(per), 2)}倍" if _is_valid(per) else "-"
        pbr_str = f"{round(float(pbr), 2)}倍" if _is_valid(pbr) else "-"
    else:
        per_str = f"{round(float(per), 2)}倍" if _is_valid(per) else "-"
        pbr_str = f"{round(float(pbr), 2)}倍" if _is_valid(pbr) else "-"
    
    if _is_valid(div_yield):
        dy_val = float(div_yield)
        # 0.03 -> 3.0%, 3.0 -> 3.0% の両方に対応
        if dy_val < 0.2: # 0.2(20%)未満なら係数100を検討
             div_yield_str = f"{round(dy_val * 100, 2)}%"
        else:
             div_yield_str = f"{round(dy_val, 2)}%"
    else:
        div_yield_str = "-"

    decision = "要検討"
    try:
        # ETF/REITの場合はPER/PBRによる判定をスキップ(常に合格扱い)
        decision_per = (quote_type == "ETF") or (_is_valid(per) and float(per) <= 15)
        decision_pbr = (quote_type == "ETF") or (_is_valid(pbr) and float(pbr) <= 1.0)
        
        dy_num = 0
        if _is_valid(div_yield):
            dy_num = float(div_yield) * 100 if float(div_yield) < 0.2 else float(div_yield)
        decision_div = dy_num >= 3.0 # 利回り3%以上なら合格
        
        if decision_per and decision_pbr and decision_div:
            decision = "保持"
        
        # ETFかつ利回り条件に満たない場合は、判定不能または要検討とする
        if quote_type == "ETF" and decision != "保持":
            decision = "ETF"
    except: pass

    return StockInfo(
        code=str(code),
        name=str(name),
        per=per_str,
        pbr=pbr_str,
        dividend_yield=div_yield_str,
        decision=decision,
        yuutai=yuutai
    )

def _clean_numeric(val):
    if val is None or str(val).strip() in ("", "nan", "NaN", "-"):
        return None
    try:
        s = str(val).replace(",", "").replace("円", "").replace("株", "").replace("%", "").strip()
        return float(s)
    except:
        return None

def _fmt_number(val) -> str:
    n = _clean_numeric(val)
    if n is None:
        return str(val).strip() if val else "-"
    try:
        if n == int(n):
            return f"{int(n):,}"
        return f"{n:,.2f}"
    except:
        return str(val).strip()

@app.post("/upload", response_model=List[StockInfo])
async def upload_csv(file: UploadFile = File(...)):
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are allowed.")
    
    contents = await file.read()
    
    # 修正ポイント①: エンコード判定
    # Linux環境でもSBI証券のShift-JIS(cp932)を確実に最優先でデコードさせる
    text = None
    for encoding in ['cp932', 'shift_jis', 'utf-8']:
        try:
            text = contents.decode(encoding)
            break
        except UnicodeDecodeError:
            continue
    
    if text is None:
        raise HTTPException(status_code=400, detail="Failed to decode CSV. Ensure it is Shift-JIS or UTF-8.")
        
    lines = text.splitlines()
    header_idx = -1
    for i, line in enumerate(lines):
        # ダブルクォーテーション等も除去して柔軟に判定
        normalized_line = line.replace('\ufeff', '').replace('"', '').strip()
        if normalized_line.startswith("コード") or "銘柄コード" in normalized_line or "コード" == normalized_line.split(",")[0].strip():
            header_idx = i
            break
            
    if header_idx == -1:
        raise HTTPException(status_code=400, detail="CSV内に「コード」または「銘柄コード」から始まるヘッダー行が見つかりませんでした。")
        
    # 株主優待ツールと同様のロジックで株式セクションのみを抽出
    stock_lines = [lines[header_idx]]
    for line in lines[header_idx+1:]:
        line_strip = line.strip()
        if not line_strip: continue
        # 1列目が銘柄コード（4〜5桁の英数字）かどうかをチェック
        first_col = line_strip.split(',')[0].replace('"', '').strip()
        if re.match(r'^[0-9A-Z]{4,5}$', first_col):
            stock_lines.append(line)
        else:
            # 投資信託などの株式以外のセクションに到達したら終了
            break

    df = pd.read_csv(io.StringIO('\n'.join(stock_lines)))
    
    # 修正ポイント②: カラム名のクリーニング強化
    # Linux環境で改行や見えない文字が悪さをして列のマッチングが外れるのを防ぐ
    df.columns = df.columns.astype(str).str.strip().str.replace('\ufeff', '').str.replace('"', '').str.replace(' ', '').str.replace('　', '')
    
    code_col = None
    for col in df.columns:
        if "コード" in col:
            code_col = col
            break
            
    if code_col is None:
        raise HTTPException(status_code=400, detail="CSVに「コード」を含む列が存在しません。")
        
    name_col = None
    for col in df.columns:
        if ("銘柄" in col or "名前" in col) and "コード" not in col:
            name_col = col
            break
    if name_col is None:
        name_col = code_col

    df = df.dropna(subset=[code_col])
    
    def find_col(keywords):
        for kw in keywords:
            for col in df.columns:
                if kw in col:
                    return col
        return None

    shares_col = find_col(["保有数量", "保有株数", "数量", "株数", "残高", "保有数"])
    cost_col   = find_col(["取得単価", "取得価格", "購入単価", "平均取得単価", "取得単価", "取得コスト"])
    curr_col   = find_col(["現在値", "現在価格", "株価", "時価"])
    yuutai_col = find_col(["株主優待", "優待"])

    results = []
    for index, row in df.iterrows():
        code = row[code_col]
        name = str(row[name_col]) if name_col in df.columns else str(code)
        
        if "投資信託" in name:
            continue
            
        if str(code).strip() != "" and str(code).lower() != "nan":
            shares_raw     = row[shares_col] if shares_col and shares_col in df.columns else None
            cost_raw       = row[cost_col]   if cost_col   and cost_col   in df.columns else None
            current_raw    = row[curr_col]   if curr_col   and curr_col   in df.columns else None
            yuutai_raw     = row[yuutai_col] if yuutai_col and yuutai_col in df.columns else "-"

            shares_str  = _fmt_number(shares_raw)
            cost_str    = _fmt_number(cost_raw)
            current_str = _fmt_number(current_raw)

            shares_n  = _clean_numeric(shares_raw)
            cost_n    = _clean_numeric(cost_raw)
            current_n = _clean_numeric(current_raw)

            if shares_n is not None and cost_n is not None:
                acq = shares_n * cost_n
                acq_str = f"{int(acq):,}" if acq == int(acq) else f"{acq:,.0f}"
            else:
                acq_str = "-"

            if shares_n is not None and current_n is not None:
                mval = shares_n * current_n
                mval_str = f"{int(mval):,}" if mval == int(mval) else f"{mval:,.0f}"
            else:
                mval_str = "-"

            if acq_str != "-" and mval_str != "-":
                try:
                    acq_n  = float(acq_str.replace(",",""))
                    mval_n = float(mval_str.replace(",",""))
                    pl     = mval_n - acq_n
                    pl_str = f"{int(pl):+,}" if pl == int(pl) else f"{pl:+,.0f}"
                    rr     = (mval_n / acq_n - 1) * 100 if acq_n != 0 else None
                    rr_str = f"{rr:+.2f}%" if rr is not None else "-"
                except:
                    pl_str = "-"
                    rr_str = "-"
            else:
                pl_str = "-"
                rr_str = "-"

            results.append(StockInfo(
                code=str(code),
                name=name,
                per="取得中",
                pbr="取得中",
                dividend_yield="取得中",
                decision="取得中",
                yuutai=str(yuutai_raw) if yuutai_raw else "-",
                shares=shares_str,
                cost_price=cost_str,
                current_price=current_str,
                acquisition_cost=acq_str,
                market_value=mval_str,
                profit_loss=pl_str,
                return_rate=rr_str
            ))
            
    return results

@app.get("/fetch/{code}", response_model=StockInfo)
async def fetch_stock_api(code: str, name: str = ""):
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(_executor, fetch_single_stock, code, name or str(code))
    return result

if __name__ == "__main__":
    import os
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)