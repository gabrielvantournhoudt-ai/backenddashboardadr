#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Backend Real para Calculadora ADRs v2.3
Dados reais do Yahoo Finance - SEM auto-incremento
"""

import json
import os
import time
import urllib.request
from urllib.parse import urlparse, parse_qs
import logging
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler
from threading import Thread
try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cache de dados
data_cache = {
    'vix': None,
    'gold': None,
    'iron': None,
    'winfut': None,
    'adrs': {},
    'adrs_snapshots': {},
    'macro': {},
    'timestamp': None,
    'status': 'initializing'
}

# Persistência simples em disco para snapshots de ADRs
SNAPSHOT_FILE = 'adrs_snapshots.json'

# Cache específico para /api/quote (evita excesso de chamadas externas)
QUOTE_CACHE = {}
QUOTE_TTL = 30  # segundos


def to_iso_utc(timestamp):
    """Converte timestamp unix para ISO UTC (sem warnings)."""
    if not timestamp:
        return None
    try:
        return datetime.fromtimestamp(int(timestamp), timezone.utc).isoformat()
    except Exception:
        return None


def parse_iso_datetime(value):
    """Converte string ISO em datetime (UTC) para comparações."""
    if not value:
        return None
    try:
        if isinstance(value, datetime):
            return value
        return datetime.fromisoformat(value.replace('Z', '+00:00'))
    except Exception:
        return None


def now_in_et():
    """Retorna datetime atual em America/New_York (fallback para UTC se indisponível)."""
    try:
        if ZoneInfo:
            return datetime.now(ZoneInfo('America/New_York'))
    except Exception:
        pass
    return datetime.now(timezone.utc)


def is_after_regular_close_et(dt_et=None):
    """Retorna True se horário em ET for 17:00 ou depois (fim do pregão regular)."""
    dt = dt_et or now_in_et()
    try:
        return dt.hour > 17 or (dt.hour == 17 and dt.minute >= 0)
    except Exception:
        return False


def load_snapshots_from_disk():
    try:
        with open(SNAPSHOT_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
            if isinstance(data, dict):
                data_cache['adrs_snapshots'] = data
                logger.info("🗂️ Snapshots ADRs carregados do disco")
    except FileNotFoundError:
        logger.info("ℹ️ Nenhum snapshot em disco encontrado (primeira execução)")
    except Exception as e:
        logger.warning(f"⚠️ Falha ao carregar snapshots: {e}")


def persist_snapshots_to_disk():
    try:
        with open(SNAPSHOT_FILE, 'w', encoding='utf-8') as f:
            json.dump(data_cache.get('adrs_snapshots', {}), f, ensure_ascii=False)
        logger.info("💾 Snapshots ADRs persistidos em disco")
    except Exception as e:
        logger.warning(f"⚠️ Falha ao salvar snapshots: {e}")


def trim_series_last_hours(timestamps, closes, hours=36, fallback_points=160):
    """Mantém apenas os pontos das últimas `hours` horas; se não houver dados suficientes, retorna o fallback."""
    if not timestamps or not closes:
        return [], []

    paired = [(ts, val) for ts, val in zip(timestamps, closes)]

    latest_ts = None
    for ts in reversed(timestamps):
        if ts:
            latest_ts = ts
            break

    if latest_ts:
        cutoff = latest_ts - int(hours * 3600)
        trimmed = [(ts, val) for ts, val in paired if ts and ts >= cutoff]
    else:
        trimmed = []

    if len(trimmed) < 16:  # fallback para garantir volume mínimo de pontos
        trimmed = paired[-fallback_points:]

    trimmed_ts = [ts for ts, _ in trimmed]
    trimmed_closes = [val for _, val in trimmed]
    return trimmed_ts, trimmed_closes


def get_cached_quote(ticker):
    entry = QUOTE_CACHE.get(ticker)
    if not entry:
        return None
    if time.time() - entry['timestamp'] > QUOTE_TTL:
        QUOTE_CACHE.pop(ticker, None)
        return None
    return entry['data']


def store_quote_cache(ticker, data):
    if data:
        QUOTE_CACHE[ticker] = {
            'timestamp': time.time(),
            'data': data
        }

# Tickers reais e funcionais
TICKERS = {
    'vix': '^VIX',
    'gold': 'GC=F',
    'iron': 'HG=F',  # Copper Futures (proxy para Iron Ore)
    'winfut': '^BVSP',  # Ibovespa como proxy para WINFUT
    'adrs': ['VALE', 'ITUB', 'PBR', 'PBR-A', 'BBD', 'BBDO', 'ABEV', 'ERJ', 'BSBR', 'BDORY'],
    'macro': {
        'ewz': 'EWZ',
        'sp500': '^GSPC',
        'oil': 'CL=F',
        'brent': 'BZ=F',
        'dxy': 'DX-Y.NYB'
    }
}

def fetch_yahoo_data(ticker, is_adr=False):
    """Busca dados reais do Yahoo Finance - After Market para ADRs"""
    try:
        # Usamos o endpoint de chart pois contém campos de post-market e regular
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=3d&interval=15m&includePrePost=true"
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        request = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(request, timeout=10) as response:
            data = json.loads(response.read().decode())
            
        if 'chart' in data and 'result' in data['chart'] and data['chart']['result']:
            result = data['chart']['result'][0]
            meta = result.get('meta', {})
            timestamps = result.get('timestamp', []) or []
            indicators = (result.get('indicators', {}) or {}).get('quote', [{}])
            closes = (indicators[0] if indicators else {}).get('close', []) or []
            
            # Encontrar último candle pós-fechamento para after-hours
            regular_time = meta.get('regularMarketTime')
            after_hours_price = meta.get('postMarketPrice')
            post_change_pct = meta.get('postMarketChangePercent')
            post_time = meta.get('postMarketTime')

            if regular_time and timestamps and closes:
                for ts, close in zip(reversed(timestamps), reversed(closes)):
                    if ts is None or close is None:
                        continue
                    if ts > regular_time:
                        after_hours_price = float(close)
                        post_time = ts
                        break

            if is_adr:
                regular_price = meta.get('regularMarketPrice')
                previous_close = meta.get('previousClose')

                if regular_price and previous_close:
                    regular_price = float(regular_price)
                    previous_close = float(previous_close)
                    variation = ((regular_price - previous_close) / previous_close) * 100 if previous_close else 0.0

                    if after_hours_price is not None and regular_price:
                        post_change_pct = ((after_hours_price - regular_price) / regular_price) * 100

                    trimmed_ts, trimmed_closes = trim_series_last_hours(timestamps, closes)

                    return {
                        'current': round(regular_price, 2),
                        'variation': round(variation, 2),
                        'timestamp': datetime.now().isoformat(),
                        'ticker': ticker,
                        'source': 'closing-price',
                        'has_after_market': bool(after_hours_price),
                        'data_type': 'closing',
                        'message': 'Dados de fechamento (after-market disponível para preenchimento manual)',
                        'at_close': {
                            'price': round(regular_price, 2),
                            'change_percent': round(variation, 2),
                            'time': to_iso_utc(regular_time)
                        },
                        'after_hours': {
                            'available': after_hours_price is not None,
                            'price': round(after_hours_price, 2) if after_hours_price is not None else None,
                            'change_percent': round(post_change_pct, 2) if (post_change_pct is not None) else None,
                            'time': to_iso_utc(post_time)
                        },
                        'series': {
                            'timestamps': [to_iso_utc(t) for t in trimmed_ts],
                            'closes': [round(c, 4) if c is not None else None for c in trimmed_closes]
                        },
                        'snapshots': {
                            'closing': {
                                'price': round(previous_close, 2),
                                'variation': round(variation, 2),  # Usar a variação calculada do fechamento atual
                                'time': to_iso_utc(regular_time) if regular_time else to_iso_utc(meta.get('regularMarketPreviousCloseTime'))
                            },
                            'after_hours': {
                                'price': round(after_hours_price, 2) if after_hours_price is not None else None,
                                'variation': round(post_change_pct, 2) if (post_change_pct is not None) else None,
                                'time': to_iso_utc(post_time)
                            }
                        }
                    }
                else:
                    return {
                        'current': None,
                        'variation': None,
                        'timestamp': datetime.now().isoformat(),
                        'ticker': ticker,
                        'source': 'no-data',
                        'has_after_market': False,
                        'data_type': 'closing',
                        'message': 'Sem dados de fechamento disponíveis',
                        'snapshots': {
                            'closing': {
                                'price': None,
                                'variation': None,
                                'time': None
                            },
                            'after_hours': {
                                'price': None,
                                'variation': None,
                                'time': None
                            }
                        }
                    }
            else:
                # Para outros ativos (WTI, Brent, Gold, HG=F): usar regular e robustez de previous close
                current_price = meta.get('regularMarketPrice')
                chart_prev = meta.get('chartPreviousClose')
                previous_close = meta.get('previousClose')
                regular_time = meta.get('regularMarketTime')

                # Fallback para preço atual: último close válido da série
                if current_price is None:
                    try:
                        for c in reversed(closes):
                            if c is not None:
                                current_price = float(c)
                                break
                    except Exception:
                        pass

                # Resolver previous_close com múltiplos fallbacks
                prev_candidates = []
                if previous_close is not None:
                    prev_candidates.append(previous_close)
                if chart_prev is not None:
                    prev_candidates.append(chart_prev)

                if not prev_candidates:
                    # Tentar último close do dia anterior com base na série (3d range)
                    try:
                        tz = ZoneInfo('America/New_York') if ZoneInfo else timezone.utc
                        dt_series = []
                        for ts, c in zip(timestamps, closes):
                            if ts is None or c is None:
                                continue
                            dt_series.append((datetime.fromtimestamp(int(ts), tz), float(c)))
                        if dt_series:
                            last_day = dt_series[-1][0].date()
                            prev_day_values = [val for (dtv, val) in dt_series if dtv.date() < last_day]
                            if prev_day_values:
                                prev_candidates.append(prev_day_values[-1])
                    except Exception:
                        pass

                if not prev_candidates:
                    # Fallback mínimo: primeiro close válido da série
                    try:
                        for c in closes:
                            if c is not None:
                                prev_candidates.append(float(c))
                                break
                    except Exception:
                        pass

                prev_value = None
                for cand in prev_candidates:
                    try:
                        fv = float(cand)
                        if fv > 0:
                            prev_value = fv
                            break
                    except Exception:
                        continue

                variation = None
                try:
                    if current_price is not None and prev_value is not None and prev_value != 0:
                        variation = ((float(current_price) - float(prev_value)) / float(prev_value)) * 100
                except Exception:
                    variation = None

                # Tratamento da série (36h window)
                series_ts = timestamps
                series_closes = closes
                series_ts, series_closes = trim_series_last_hours(series_ts, series_closes)

                return {
                    'current': round(float(current_price), 2) if current_price is not None else None,
                    'variation': round(float(variation), 2) if variation is not None else None,
                    'timestamp': datetime.now().isoformat(),
                    'ticker': ticker,
                    'source': 'regular-market',
                    'at_close': {
                        'price': round(float(current_price), 2) if current_price is not None else None,
                        'change_percent': round(float(variation), 2) if variation is not None else None,
                        'time': to_iso_utc(regular_time if regular_time else (series_ts[-1] if series_ts else None))
                    },
                    'series': {
                        'timestamps': [to_iso_utc(t) for t in series_ts],
                        'closes': [round(c, 4) if c is not None else None for c in series_closes]
                    }
                }
        return None
    except urllib.error.HTTPError as e:
        if e.code == 404:
            logger.warning(f"Ticker {ticker} não encontrado (404)")
        else:
            logger.error(f"Erro HTTP {e.code} ao buscar {ticker}")
        return None
    except Exception as e:
        logger.error(f"Erro ao buscar {ticker}: {str(e)}")
        return None


def fetch_yfinance_data(ticker, is_adr=False):
    """Busca dados usando yfinance e trata limites de rate limit."""
    try:
        import yfinance as yf  # importar apenas quando necessário
    except Exception:
        yf = None

    if yf is None:
        return fetch_yahoo_data(ticker, is_adr=is_adr)

    try:
        ticker_obj = yf.Ticker(ticker)

        # `history` já respeita pre/post e evita várias requisições para quoteSummary
        hist = None
        try:
            hist = ticker_obj.history(period='1d', interval='15m', prepost=True)
        except Exception as err:
            logger.warning(f"yfinance history falhou para {ticker}: {err}")

        # Fast-info tem menos probabilidade de 429; fallback mínimo
        info = {}
        try:
            if hasattr(ticker_obj, 'fast_info'):
                info = ticker_obj.fast_info or {}
        except Exception as err:
            logger.warning(f"yfinance fast_info falhou para {ticker}: {err}")

        regular_price = info.get('last_price')
        previous_close = info.get('previous_close')
        post_price = info.get('post_price')
        post_change_pct = info.get('post_market_change_percent')
        regular_time = info.get('last_update')
        post_time = info.get('post_market_time')

        # fallback leve usando quote (mais estável que quoteSummary)
        if not regular_price or not previous_close:
            try:
                quote_data = ticker_obj.history(period='1d', interval='15m', prepost=False)
                if quote_data is not None and len(quote_data) > 0:
                    regular_price = regular_price or float(quote_data['Close'].iloc[-1])
                    previous_close = previous_close or float(quote_data['Close'].iloc[0])
            except Exception:
                pass

        series_ts = []
        series_closes = []
        after_time_hist = None
        if hist is not None and len(hist) > 0:
            raw_ts = [int(dt.timestamp()) if hasattr(dt, 'timestamp') else None for dt in hist.index]
            raw_closes = [round(float(v), 4) if v == v else None for v in hist['Close'].tolist()]
            trimmed_ts, trimmed_closes = trim_series_last_hours(raw_ts, raw_closes)
            series_ts = [to_iso_utc(ts) for ts in trimmed_ts]
            series_closes = trimmed_closes
            if regular_time:
                for dt, price in zip(reversed(hist.index), reversed(hist['Close'].tolist())):
                    try:
                        ts_val = int(dt.timestamp())
                    except Exception:
                        continue
                    if price is None:
                        continue
                    if ts_val > int(regular_time):
                        post_price = float(price)
                        after_time_hist = ts_val
                        break

        if regular_price and previous_close:
            variation = ((regular_price - previous_close) / (previous_close or regular_price)) * 100
            if post_price is not None and regular_price:
                post_change_pct = ((post_price - regular_price) / regular_price) * 100
                post_time = post_time or after_time_hist
            trimmed_ts, trimmed_closes = trim_series_last_hours(
                [int(datetime.fromisoformat(ts).timestamp()) if ts else None for ts in series_ts],
                series_closes
            )
            return {
                'current': round(float(regular_price), 2),
                'variation': round(float(variation), 2),
                'timestamp': datetime.now(timezone.utc).isoformat(),
                'ticker': ticker,
                'source': 'yfinance',
                'data_type': 'closing' if is_adr else 'regular',
                'has_after_market': bool(post_price),
                'at_close': {
                    'price': round(float(regular_price), 2),
                    'change_percent': round(float(variation), 2),
                    'time': to_iso_utc(regular_time)
                },
                'after_hours': {
                    'available': post_price is not None,
                    'price': round(float(post_price), 2) if post_price is not None else None,
                    'change_percent': round(float(post_change_pct), 2) if post_change_pct is not None else None,
                    'time': to_iso_utc(post_time)
                },
                'series': {
                    'timestamps': [to_iso_utc(ts) for ts in trimmed_ts],
                    'closes': trimmed_closes
                }
            }

        data = fetch_yahoo_data(ticker, is_adr=is_adr)
        if data:
            store_quote_cache(ticker, data)
        return data
    except Exception as err:
        logger.warning(f"yfinance falhou para {ticker}: {err}")
        data = fetch_yahoo_data(ticker, is_adr=is_adr)
        if data:
            store_quote_cache(ticker, data)
        return data

def fetch_iron_ore_data():
    """Busca dados do Iron Ore usando Copper Futures (HG=F) como proxy"""
    logger.info("🔍 Buscando dados do Iron Ore...")
    
    try:
        # Usar Copper Futures como proxy para Iron Ore
        data = fetch_yahoo_data('HG=F')
        if data and data['current'] and data['current'] > 0:
            logger.info(f"✅ Iron Ore (HG=F): {data['current']} ({data['variation']:+.2f}%)")
            return data
    except Exception as e:
        logger.warning(f"⚠️ HG=F falhou: {e}")
    
    # Fallback: dados simulados
    logger.warning("⚠️ Usando dados simulados para Iron Ore")
    import random
    base_price = 4.5  # Preço base do Copper (proxy para Iron Ore)
    variation = random.uniform(-1.0, 1.0)  # Variação simulada
    
    return {
        'current': round(base_price, 2),
        'variation': round(variation, 2),
        'ticker': 'IRON-SIM',
        'source': 'simulated',
        'timestamp': datetime.now().isoformat()
    }

def fetch_market_data():
    """Busca dados reais do mercado - APENAS quando solicitado"""
    logger.info("Buscando dados reais do mercado...")
    
    try:
        # VIX
        vix_data = fetch_yfinance_data(TICKERS['vix'])
        if vix_data:
            data_cache['vix'] = vix_data
            logger.info(f"VIX: {vix_data['current']} ({vix_data['variation']:+.2f}%)")
        
        # Gold (GC=F) – usar endpoint chart direto (mais estável no Render)
        gold_data = fetch_yahoo_data(TICKERS['gold'])
        if gold_data:
            data_cache['gold'] = gold_data
            logger.info(f"Gold: {gold_data['current']} ({gold_data['variation']:+.2f}%)")
        
        # Iron Ore - Estratégia multi-fonte
        iron_data = fetch_iron_ore_data()
        if iron_data:
            data_cache['iron'] = iron_data
            logger.info(f"Iron Ore ({iron_data['ticker']}): {iron_data['current']} ({iron_data['variation']:+.2f}%)")
        
        # WINFUT - Ibovespa como proxy
        winfut_data = fetch_yfinance_data(TICKERS['winfut'])
        if winfut_data:
            data_cache['winfut'] = winfut_data
            logger.info(f"WINFUT (Ibovespa): {winfut_data['current']} ({winfut_data['variation']:+.2f}%)")
        
        # ADRs - Buscar APENAS after-market
        for ticker in TICKERS['adrs']:
            adr_data = fetch_yahoo_data(ticker, is_adr=True)
            if adr_data:
                data_cache['adrs'][ticker] = adr_data

                snapshots = data_cache.setdefault('adrs_snapshots', {}).setdefault(ticker, {
                    'closing': None,
                    'after_hours': None,
                    'closing_source_time': None,
                    'after_hours_source_time': None
                })

                # Sempre atualizar snapshot de fechamento se houver dados válidos e for após 17:00 ET
                closing_snapshot = (adr_data.get('snapshots') or {}).get('closing')
                if closing_snapshot and closing_snapshot.get('price') is not None and closing_snapshot.get('variation') is not None:
                    # Só vira fechamento quando passar o horário do fechamento regular nos EUA
                    if is_after_regular_close_et():
                        closing_time = parse_iso_datetime(closing_snapshot.get('time'))
                        existing_time = parse_iso_datetime(snapshots.get('closing_source_time'))
                        if not existing_time or (closing_time and closing_time > existing_time):
                            snapshots['closing'] = closing_snapshot
                            snapshots['closing_source_time'] = closing_snapshot.get('time')

                # Atualizar snapshot after-hours se houver dados válidos
                after_snapshot = (adr_data.get('snapshots') or {}).get('after_hours')
                if after_snapshot and after_snapshot.get('price') is not None and after_snapshot.get('variation') is not None:
                    after_time = parse_iso_datetime(after_snapshot.get('time'))
                    existing_after_time = parse_iso_datetime(snapshots.get('after_hours_source_time'))
                    
                    # Atualizar se não existe ou se é mais recente
                    if not existing_after_time or (after_time and after_time > existing_after_time):
                        snapshots['after_hours'] = after_snapshot
                        snapshots['after_hours_source_time'] = after_snapshot.get('time')

                if adr_data.get('has_after_market', False):
                    logger.info(f"✅ {ticker}: {adr_data['current']} ({adr_data['variation']:+.2f}%) 🕐 After-Market")
                else:
                    logger.info(f"❌ {ticker}: {adr_data.get('message', 'Sem after-market')} 📊 Regular Market")

        # Persistir snapshots ao final de uma atualização bem-sucedida
        persist_snapshots_to_disk()
        
        # Indicadores Macro
        for key, ticker in TICKERS['macro'].items():
            # WTI/Brent via endpoint chart (yfinance tem falhas intermitentes no Render)
            if key in ('oil', 'brent'):
                macro_data = fetch_yahoo_data(ticker)
            else:
                macro_data = fetch_yfinance_data(ticker)
            if macro_data:
                data_cache['macro'][key] = macro_data
                logger.info(f"{key.upper()}: {macro_data['current']} ({macro_data['variation']:+.2f}%)")
        
        data_cache['timestamp'] = datetime.now().isoformat()
        data_cache['status'] = 'success'
        
        logger.info("✅ Dados reais atualizados com sucesso")
        
    except Exception as e:
        logger.error(f"❌ Erro ao buscar dados reais: {str(e)}")
        data_cache['status'] = 'error'
        data_cache['error'] = str(e)

class APIHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        """Manipula requisições GET"""
        # Configurar CORS
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        
        # Roteamento
        parsed = urlparse(self.path)
        path_only = parsed.path
        query = parse_qs(parsed.query or '')

        if path_only == '/api/market-data':
            # Estrutura compatível com o frontend
            response_data = {
                "status": "success",
                "timestamp": data_cache.get('timestamp', datetime.now().isoformat()),
                "data": {
                    "vix": data_cache.get('vix', {}),
                    "gold": data_cache.get('gold', {}),
                    "iron": data_cache.get('iron', {}),
                    "winfut": data_cache.get('winfut', {}),
                    "adrs": data_cache.get('adrs', {}),
                    "adrs_snapshots": data_cache.get('adrs_snapshots', {}),
                    "macro": data_cache.get('macro', {})
                }
            }
            response = json.dumps(response_data, ensure_ascii=False)
        elif path_only == '/api/health':
            response = json.dumps({
                "status": "ok",
                "timestamp": datetime.now().isoformat(),
                "data_status": data_cache.get('status', 'unknown'),
                "last_update": data_cache.get('timestamp', 'never'),
                "mode": "real"
            }, ensure_ascii=False)
        elif path_only == '/api/update':
            # Endpoint para forçar atualização manual
            fetch_market_data()
            response = json.dumps({
                "status": "updated",
                "timestamp": datetime.now().isoformat()
            }, ensure_ascii=False)
        elif path_only == '/api/vix':
            response = json.dumps(data_cache.get('vix', {}), ensure_ascii=False)
        elif path_only == '/api/gold':
            response = json.dumps(data_cache.get('gold', {}), ensure_ascii=False)
        elif path_only == '/api/iron':
            response = json.dumps(data_cache.get('iron', {}), ensure_ascii=False)
        elif path_only == '/api/adrs':
            response = json.dumps({
                'data': data_cache.get('adrs', {}),
                'snapshots': data_cache.get('adrs_snapshots', {})
            }, ensure_ascii=False)
        elif path_only == '/api/macro':
            response = json.dumps(data_cache.get('macro', {}), ensure_ascii=False)
        elif path_only == '/api/quote':
            ticker = (query.get('ticker') or [''])[0].strip().upper()
            if not ticker:
                response = json.dumps({"error": "ticker query param required"}, ensure_ascii=False)
            else:
                cached = get_cached_quote(ticker)
                if cached:
                    response = json.dumps({
                        "status": "success",
                        "data": cached,
                        "cached": True
                    }, ensure_ascii=False)
                    self.wfile.write(response.encode('utf-8'))
                    return

                detailed = fetch_yfinance_data(ticker, is_adr=ticker.isalpha() and len(ticker) <= 5)
                if detailed:
                    store_quote_cache(ticker, detailed)
                    response = json.dumps({
                        "status": "success",
                        "data": detailed
                    }, ensure_ascii=False)
                else:
                    response = json.dumps({"status": "error", "error": "No data"}, ensure_ascii=False)
        else:
            response = json.dumps({"error": "Endpoint not found"}, ensure_ascii=False)
        
        try:
            self.wfile.write(response.encode('utf-8'))
        except ConnectionAbortedError:
            logger.warning("Cliente fechou a conexão durante a resposta")
        except Exception as exc:
            logger.error(f"Erro ao enviar resposta: {exc}")
    
    def do_OPTIONS(self):
        """Manipula requisições OPTIONS para CORS"""
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()

def run_server():
    """Executa o servidor HTTP"""
    try:
        port = int(os.environ.get('PORT', '5000'))
    except Exception:
        port = 5000
    server_address = ('', port)
    httpd = HTTPServer(server_address, APIHandler)
    
    logger.info(f"🚀 Servidor REAL iniciado na porta {port}")
    logger.info("📊 Modo: Dados reais do Yahoo Finance")
    logger.info("🔄 Atualização: MANUAL (sem auto-incremento)")
    logger.info("")
    logger.info("Endpoints disponíveis:")
    logger.info("  GET /api/market-data - Todos os dados")
    logger.info("  GET /api/update - Forçar atualização")
    logger.info("  GET /api/health - Status do servidor")
    logger.info("  GET /api/vix - Dados do VIX")
    logger.info("  GET /api/gold - Dados do Gold")
    logger.info("  GET /api/iron - Dados do Iron Ore")
    logger.info("  GET /api/adrs - Dados dos ADRs")
    logger.info("  GET /api/macro - Dados macro")
    logger.info("  GET /api/quote?ticker=XYZ - Detalhe (At Close, After Hours e série)")
    logger.info("")
    logger.info("💡 Para atualizar dados: http://localhost:5000/api/update")
    
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("Servidor parado pelo usuário")
        httpd.shutdown()

if __name__ == '__main__':
    logger.info("Iniciando Calculadora ADRs Backend REAL v2.3")
    
    # Carregar snapshots persistidos (se existirem)
    load_snapshots_from_disk()

    # Buscar dados iniciais
    fetch_market_data()
    
    # Executar servidor
    run_server()
