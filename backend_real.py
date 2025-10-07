#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Backend Real para Calculadora ADRs v2.3
Dados reais do Yahoo Finance - SEM auto-incremento
"""

import json
import time
import urllib.request
from urllib.parse import urlparse, parse_qs
import logging
from datetime import datetime, timezone, time as dt_time, timedelta
from typing import Any, Dict, List, Optional

from supabase import Client, create_client
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except Exception:  # pragma: no cover - fallback if zoneinfo not available
    ZoneInfo = None
from http.server import HTTPServer, BaseHTTPRequestHandler
try:
    # Python 3.7+
    from http.server import ThreadingHTTPServer  # type: ignore
except Exception:  # pragma: no cover
    ThreadingHTTPServer = HTTPServer  # fallback
import os
import schedule
import threading

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

# Cache específico para /api/quote (evita excesso de chamadas externas)
QUOTE_CACHE = {}
QUOTE_TTL = 30  # segundos

SUPABASE_CLIENT: Optional[Client] = None


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


def now_in_new_york():
    """Retorna datetime atual em Nova York, com fallback UTC caso zoneinfo indisponível."""
    if ZoneInfo is not None:
        try:
            return datetime.now(ZoneInfo('America/New_York'))
        except Exception:
            pass
    return datetime.utcnow()


def is_after_hours_et(now=None):
    """Retorna True se horário atual em ET estiver dentro da janela de after hours (16h-21h)."""
    current = now or now_in_new_york()
    hour = current.hour
    # Expandir janela para 16-21h para capturar dados after-hours mais cedo
    return 16 <= hour < 21


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


def get_supabase_client():
    global SUPABASE_CLIENT
    if SUPABASE_CLIENT is not None:
        return SUPABASE_CLIENT

    url = os.environ.get('SUPABASE_URL')
    key = os.environ.get('SUPABASE_SERVICE_KEY') or os.environ.get('SUPABASE_KEY')
    if not url or not key:
        logger.warning('Supabase não configurado. Variáveis SUPABASE_URL e SUPABASE_SERVICE_KEY ausentes.')
        SUPABASE_CLIENT = None
        return SUPABASE_CLIENT

    try:
        SUPABASE_CLIENT = create_client(url, key)
        logger.info('Cliente Supabase inicializado com sucesso.')
    except Exception as exc:
        logger.error(f'Falha ao inicializar Supabase: {exc}')
        SUPABASE_CLIENT = None
    return SUPABASE_CLIENT


def store_snapshot_in_supabase(ticker: str, snapshot_type: str, snapshot: Dict[str, Any]):
    client = get_supabase_client()
    if client is None:
        return

    try:
        payload = {
            'ticker': ticker,
            'snapshot_type': snapshot_type,
            'price': snapshot.get('price'),
            'variation': snapshot.get('variation'),
            'source_time': snapshot.get('time'),
            'raw_payload': snapshot,
        }
        client.table('adr_snapshots').insert(payload).execute()
    except Exception as exc:
        logger.error(f'Erro ao salvar snapshot {ticker}/{snapshot_type} no Supabase: {exc}')


def get_snapshot_history_from_supabase(ticker: str, snapshot_type: str, limit: int = 50):
    client = get_supabase_client()
    if client is None:
        return []
    try:
        resp = client.table('adr_snapshots') \
            .select('*') \
            .eq('ticker', ticker) \
            .eq('snapshot_type', snapshot_type) \
            .order('captured_at', desc=True) \
            .limit(limit) \
            .execute()
        return resp.data or []
    except Exception as exc:
        logger.error(f'Erro ao consultar histórico Supabase {ticker}/{snapshot_type}: {exc}')
        return []


def cleanup_old_snapshots():
    """Remove snapshots antigos (mais de 2 dias) do Supabase para evitar acumulo."""
    client = get_supabase_client()
    if client is None:
        return
    
    try:
        # Calcular data limite (2 dias atras)
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=2)
        cutoff_iso = cutoff_date.isoformat()
        
        # Deletar registros antigos
        result = client.table('adr_snapshots') \
            .delete() \
            .lt('captured_at', cutoff_iso) \
            .execute()
        
        if result.data:
            logger.info(f"Limpeza Supabase: {len(result.data)} registros antigos removidos (anteriores a {cutoff_iso})")
        else:
            logger.info("Limpeza Supabase: Nenhum registro antigo encontrado")
            
    except Exception as exc:
        logger.error(f'Erro ao limpar snapshots antigos do Supabase: {exc}')

# Tickers reais e funcionais
TICKERS = {
    'vix': '^VIX',
    'gold': 'GC=F',
    'iron': 'HG=F',  # Copper Futures (proxy para Iron Ore)
    'winfut': '^BVSP',  # Ibovespa como proxy para WINFUT
    'nasdaq': 'NQ=F',  # Nasdaq 100 Futures
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
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}?range=2d&interval=15m&includePrePost=true"
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
                        'timestamp': datetime.now(timezone.utc).isoformat(),
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
                        'timestamp': datetime.now(timezone.utc).isoformat(),
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
                # Para outros ativos: usar regular market price
                current_price = meta.get('regularMarketPrice')
                previous_close = meta.get('previousClose')
                if previous_close is None:
                    previous_close = meta.get('chartPreviousClose')
                regular_time = meta.get('regularMarketTime')
                prev_close_time = meta.get('regularMarketPreviousCloseTime')

                variation = None
                if current_price not in (None, 0) and previous_close not in (None, 0):
                    variation = ((current_price - previous_close) / previous_close) * 100

                series_ts = timestamps
                series_closes = closes
                series_ts, series_closes = trim_series_last_hours(series_ts, series_closes)

                open_price = None
                open_ts = None
                open_change_pct = None
                close_price = None
                close_ts = None
                close_change_pct = None
                previous_close_ts = None

                is_vix = ticker == TICKERS['vix']

                session_date = None
                exchange_tz = None
                if regular_time:
                    try:
                        rt_dt = datetime.fromtimestamp(int(regular_time), timezone.utc)
                        if ZoneInfo is not None:
                            try:
                                exchange_tz = ZoneInfo(meta.get('exchangeTimezoneName') or 'America/New_York')
                            except Exception:
                                exchange_tz = None
                        session_date = rt_dt.astimezone(exchange_tz).date() if exchange_tz else rt_dt.date()
                    except Exception:
                        session_date = None

                if timestamps and closes and session_date is not None:
                    for ts, close in zip(timestamps, closes):
                        if ts is None or close is None:
                            continue
                        try:
                            ts_int = int(ts)
                        except Exception:
                            continue
                        dt_utc = datetime.fromtimestamp(ts_int, timezone.utc)
                        dt_local = dt_utc.astimezone(exchange_tz) if exchange_tz else dt_utc
                        if dt_local.date() != session_date:
                            if dt_local.date() < session_date and previous_close_ts is None:
                                previous_close_ts = ts_int
                            continue
                        open_ts = ts_int
                        open_price = float(close)
                        break

                    if open_price is None:
                        first_valid = next(((ts, close) for ts, close in zip(timestamps, closes) if ts and close is not None), None)
                        if first_valid:
                            try:
                                open_ts = int(first_valid[0])
                                open_price = float(first_valid[1])
                            except Exception:
                                open_ts = None
                                open_price = None

                if open_price not in (None, 0) and current_price not in (None, 0):
                    try:
                        open_change_pct = ((current_price - open_price) / open_price) * 100
                    except Exception:
                        open_change_pct = None

                if timestamps and closes and session_date is not None:
                    for ts, close in zip(reversed(timestamps), reversed(closes)):
                        if ts is None or close is None:
                            continue
                        try:
                            ts_int = int(ts)
                        except Exception:
                            continue
                        dt_utc = datetime.fromtimestamp(ts_int, timezone.utc)
                        dt_local = dt_utc.astimezone(exchange_tz) if exchange_tz else dt_utc
                        if dt_local.date() != session_date:
                            continue
                        close_ts = ts_int
                        close_price = float(close)
                        break

                if close_price is None and series_closes:
                    try:
                        close_price = float(next(x for x in reversed(series_closes) if x is not None))
                        close_ts = next((int(ts) for ts in reversed(series_ts) if ts is not None), None)
                    except Exception:
                        close_price = None
                        close_ts = None

                close_reference_time = close_ts
                market_closed_today = not is_vix

                if is_vix and session_date is not None:
                    try:
                        now_br = datetime.now(ZoneInfo('America/Sao_Paulo')) if ZoneInfo else datetime.utcnow()
                    except Exception:
                        now_br = datetime.utcnow()

                    if now_br.date() > session_date:
                        market_closed_today = True
                    elif now_br.date() == session_date:
                        cutoff = dt_time(17, 15)
                        if now_br.time() >= cutoff:
                            market_closed_today = True

                    if not market_closed_today:
                        close_price = None
                        close_reference_time = None
                        close_change_pct = None

                if close_reference_time is None:
                    if prev_close_time:
                        close_reference_time = prev_close_time
                    elif previous_close_ts:
                        close_reference_time = previous_close_ts

                display_close_br_iso = None

                if is_vix:
                    try:
                        br_tz = ZoneInfo('America/Sao_Paulo') if ZoneInfo else None
                        base_tz = exchange_tz or timezone.utc
                        close_dt_local = None

                        if close_ts:
                            close_dt_local = datetime.fromtimestamp(close_ts, timezone.utc).astimezone(base_tz)
                        elif close_reference_time is not None:
                            parsed = parse_iso_datetime(close_reference_time)
                            if parsed:
                                close_dt_local = parsed.astimezone(base_tz)
                            elif isinstance(close_reference_time, (int, float)):
                                close_dt_local = datetime.fromtimestamp(int(close_reference_time), timezone.utc).astimezone(base_tz)

                        if br_tz is not None:
                            if market_closed_today and close_dt_local:
                                adjusted = close_dt_local.astimezone(br_tz).replace(hour=17, minute=15, second=0, microsecond=0)
                                display_close_br_iso = adjusted.astimezone(timezone.utc).isoformat()
                            elif not market_closed_today and prev_close_time:
                                prev_dt = parse_iso_datetime(prev_close_time)
                                if prev_dt:
                                    prev_local = prev_dt.astimezone(br_tz)
                                    adjusted_prev = prev_local.replace(hour=17, minute=15, second=0, microsecond=0)
                                    display_close_br_iso = adjusted_prev.astimezone(timezone.utc).isoformat()
                    except Exception:
                        pass

                if close_price not in (None, 0) and previous_close not in (None, 0):
                    try:
                        close_change_pct = ((close_price - previous_close) / previous_close) * 100
                    except Exception:
                        close_change_pct = None

                return {
                        'current': round(current_price, 2) if current_price is not None else None,
                    'variation': round(variation, 2) if variation is not None else None,
                        'timestamp': datetime.now(timezone.utc).isoformat(),
                    'ticker': ticker,
                    'source': 'regular-market',
                    # Campos adicionais padronizados
                    'at_close': {
                        'price': round(close_price, 2) if close_price is not None else (round(previous_close, 2) if previous_close is not None else None),
                        'change_percent': round(close_change_pct, 2) if close_change_pct is not None else (round(variation, 2) if variation is not None else None),
                        'time': (to_iso_utc(close_reference_time) if close_reference_time else (to_iso_utc(prev_close_time) if prev_close_time else to_iso_utc(regular_time)))
                    },
                    'series': {
                        'timestamps': [to_iso_utc(t) for t in series_ts],
                        'closes': [round(c, 4) if c is not None else None for c in series_closes]
                    },
                    'open_price': round(open_price, 2) if open_price is not None else None,
                    'open_time': to_iso_utc(open_ts),
                    'open_change_percent': round(open_change_pct, 2) if open_change_pct is not None else None,
                    'at_close_display_time': display_close_br_iso
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
        'timestamp': datetime.now(timezone.utc).isoformat()
    }

def load_latest_snapshots_from_supabase():
    """Carrega últimos snapshots do Supabase para preencher cache em memória ao iniciar."""
    client = get_supabase_client()
    if client is None:
        logger.warning("Supabase não disponível - snapshots não serão carregados")
        return
    
    logger.info("📥 Carregando últimos snapshots do Supabase...")
    
    for ticker in TICKERS['adrs']:
        try:
            # Buscar último snapshot de closing
            closing_history = get_snapshot_history_from_supabase(ticker, 'closing', limit=1)
            if closing_history and len(closing_history) > 0:
                last_closing = closing_history[0]
                snapshots = data_cache.setdefault('adrs_snapshots', {}).setdefault(ticker, {
                    'closing': None,
                    'after_hours': None,
                    'closing_source_time': None,
                    'after_hours_source_time': None
                })
                snapshots['closing'] = {
                    'price': last_closing.get('price'),
                    'variation': last_closing.get('variation'),
                    'time': last_closing.get('source_time')
                }
                snapshots['closing_source_time'] = last_closing.get('source_time')
                logger.info(f"✅ {ticker} closing carregado: {last_closing.get('variation')}%")
            
            # Buscar último snapshot de after_hours
            after_history = get_snapshot_history_from_supabase(ticker, 'after_hours', limit=1)
            if after_history and len(after_history) > 0:
                last_after = after_history[0]
                snapshots = data_cache.setdefault('adrs_snapshots', {}).setdefault(ticker, {
                    'closing': None,
                    'after_hours': None,
                    'closing_source_time': None,
                    'after_hours_source_time': None
                })
                snapshots['after_hours'] = {
                    'price': last_after.get('price'),
                    'variation': last_after.get('variation'),
                    'time': last_after.get('source_time')
                }
                snapshots['after_hours_source_time'] = last_after.get('source_time')
                logger.info(f"✅ {ticker} after-hours carregado: {last_after.get('variation')}%")
                
        except Exception as exc:
            logger.error(f"Erro ao carregar snapshots do {ticker}: {exc}")
    
    logger.info("✅ Snapshots do Supabase carregados no cache")

def fetch_market_data():
    """Busca dados reais do mercado - APENAS quando solicitado"""
    logger.info("🔄 fetch_market_data chamada (manual ou automática)")
    logger.info("Buscando dados reais do mercado...")
    
    try:
        current_et = now_in_new_york()
        in_after_hours = is_after_hours_et(current_et)
        logger.info(f"🕐 Current ET time: {current_et}, in_after_hours: {in_after_hours}")

        # VIX
        vix_data = fetch_yahoo_data(TICKERS['vix'])
        if vix_data:
            data_cache['vix'] = vix_data
            logger.info(f"VIX: {vix_data['current']} ({vix_data['variation']:+.2f}%)")
        
        # Gold
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
        winfut_data = fetch_yahoo_data(TICKERS['winfut'])
        if winfut_data:
            data_cache['winfut'] = winfut_data
            logger.info(f"WINFUT (Ibovespa): {winfut_data['current']} ({winfut_data['variation']:+.2f}%)")
        
        # Nasdaq 100 Futures
        nasdaq_data = fetch_yahoo_data(TICKERS['nasdaq'])
        if nasdaq_data:
            data_cache['nasdaq'] = nasdaq_data
            logger.info(f"Nasdaq 100: {nasdaq_data['current']} ({nasdaq_data['variation']:+.2f}%)")
        
        # ADRs - Buscar dados; snapshots after-hours apenas durante janela específica
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

                # Sempre atualizar snapshot de fechamento se houver dados válidos
                closing_snapshot = (adr_data.get('snapshots') or {}).get('closing')
                if closing_snapshot and closing_snapshot.get('price') is not None and closing_snapshot.get('variation') is not None:
                    closing_time = parse_iso_datetime(closing_snapshot.get('time'))
                    existing_time = parse_iso_datetime(snapshots.get('closing_source_time'))
                    
                    # Atualizar se não existe ou se é mais recente
                    if not existing_time or (closing_time and closing_time > existing_time):
                        snapshots['closing'] = closing_snapshot
                        snapshots['closing_source_time'] = closing_snapshot.get('time')
                        store_snapshot_in_supabase(ticker, 'closing', closing_snapshot)

                # Atualizar snapshot after-hours se houver dados válidos
                after_snapshot = (adr_data.get('snapshots') or {}).get('after_hours')
                logger.info(f"🔍 {ticker} after_hours debug: in_after_hours={in_after_hours}, after_snapshot={after_snapshot}")
                if in_after_hours and after_snapshot and after_snapshot.get('price') is not None and after_snapshot.get('variation') is not None:
                    after_time = parse_iso_datetime(after_snapshot.get('time'))
                    existing_after_time = parse_iso_datetime(snapshots.get('after_hours_source_time'))
                    
                    # Atualizar se não existe ou se é mais recente
                    if not existing_after_time or (after_time and after_time > existing_after_time):
                        snapshots['after_hours'] = after_snapshot
                        snapshots['after_hours_source_time'] = after_snapshot.get('time')
                        store_snapshot_in_supabase(ticker, 'after_hours', after_snapshot)
                        logger.info(f"✅ {ticker} after_hours snapshot SAVED: {after_snapshot}")
                else:
                    logger.info(f"❌ {ticker} after_hours snapshot NOT saved: in_after_hours={in_after_hours}, price={after_snapshot.get('price') if after_snapshot else None}, variation={after_snapshot.get('variation') if after_snapshot else None}")

                if adr_data.get('has_after_market', False):
                    logger.info(f"✅ {ticker}: {adr_data['current']} ({adr_data['variation']:+.2f}%) 🕐 After-Market")
                else:
                    logger.info(f"❌ {ticker}: {adr_data.get('message', 'Sem after-market')} 📊 Regular Market")
        
        # Indicadores Macro
        for key, ticker in TICKERS['macro'].items():
            macro_data = fetch_yahoo_data(ticker)
            if macro_data:
                data_cache['macro'][key] = macro_data
                logger.info(f"{key.upper()}: {macro_data['current']} ({macro_data['variation']:+.2f}%)")
        
        data_cache['timestamp'] = datetime.now(timezone.utc).isoformat()
        data_cache['status'] = 'success'
        
        logger.info("✅ Dados reais atualizados com sucesso")

        # Limpeza automatica de dados antigos (apenas uma vez por execucao)
        cleanup_old_snapshots()
        
    except Exception as e:
        logger.error(f"❌ Erro ao buscar dados reais: {str(e)}")
        data_cache['status'] = 'error'
        data_cache['error'] = str(e)

class APIHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        """Manipula requisições GET"""
        parsed = urlparse(self.path)
        path_only = parsed.path
        query = parse_qs(parsed.query or '')

        status_code = 200
        payload = None

        if path_only == '/':
            payload = {
                "service": "Calculadora ADRs Backend REAL",
                "mode": "real",
                "status": data_cache.get('status', 'unknown'),
                "last_update": data_cache.get('timestamp', 'never'),
                "timestamp": datetime.now().isoformat(),
                "endpoints": [
                    "/api/market-data",
                    "/api/update",
                    "/api/health",
                    "/api/vix",
                    "/api/gold",
                    "/api/iron",
                    "/api/adrs",
                    "/api/macro",
                    "/api/quote?ticker=XYZ"
                ]
            }
        elif path_only == '/api/market-data':
            payload = {
                "status": "success",
                "timestamp": data_cache.get('timestamp', datetime.now().isoformat()),
                "data": {
                    "vix": data_cache.get('vix', {}),
                    "gold": data_cache.get('gold', {}),
                    "iron": data_cache.get('iron', {}),
                    "winfut": data_cache.get('winfut', {}),
                    "nasdaq": data_cache.get('nasdaq', {}),
                    "adrs": data_cache.get('adrs', {}),
                    "adrs_snapshots": data_cache.get('adrs_snapshots', {}),
                    "macro": data_cache.get('macro', {})
                }
            }
        elif path_only == '/api/health':
            payload = {
                "status": "ok",
                "timestamp": datetime.now().isoformat(),
                "data_status": data_cache.get('status', 'unknown'),
                "last_update": data_cache.get('timestamp', 'never'),
                "mode": "real"
            }
        elif path_only == '/api/update':
            fetch_market_data()
            payload = {
                "status": "updated",
                "timestamp": datetime.now().isoformat()
            }
        elif path_only == '/api/vix':
            payload = data_cache.get('vix', {})
        elif path_only == '/api/gold':
            payload = data_cache.get('gold', {})
        elif path_only == '/api/iron':
            payload = data_cache.get('iron', {})
        elif path_only == '/api/adrs':
            payload = {
                'data': data_cache.get('adrs', {}),
                'snapshots': data_cache.get('adrs_snapshots', {})
            }
        elif path_only == '/api/adr-history':
            ticker = (query.get('ticker') or [''])[0].strip().upper()
            snap_type = (query.get('type') or ['closing'])[0].strip()
            limit = int((query.get('limit') or ['50'])[0])
            if not ticker:
                status_code = 400
                payload = {'status': 'error', 'error': 'ticker required'}
            else:
                data = get_snapshot_history_from_supabase(ticker, snap_type, limit)
                payload = {'status': 'success', 'data': data, 'ticker': ticker, 'type': snap_type}
        elif path_only == '/api/macro':
            payload = data_cache.get('macro', {})
        elif path_only == '/api/debug':
            payload = {
                "status": "success",
                "scheduler_jobs": len(schedule.jobs),
                "jobs": [str(job) for job in schedule.jobs],
                "data_cache_status": data_cache.get('status', 'unknown'),
                "last_update": data_cache.get('timestamp', 'never')
            }
        elif path_only == '/api/quote':
            ticker = (query.get('ticker') or [''])[0].strip().upper()
            if not ticker:
                status_code = 400
                payload = {"error": "ticker query param required"}
            else:
                cached = get_cached_quote(ticker)
                if cached:
                    payload = {"status": "success", "data": cached, "cached": True}
                else:
                    detailed = fetch_yfinance_data(ticker, is_adr=ticker.isalpha() and len(ticker) <= 5)
                    if detailed:
                        store_quote_cache(ticker, detailed)
                        payload = {"status": "success", "data": detailed}
                    else:
                        status_code = 404
                        payload = {"status": "error", "error": "No data"}
        else:
            status_code = 404
            payload = {"error": "Endpoint not found"}

        response = json.dumps(payload or {}, ensure_ascii=False)

        # Enviar cabeçalhos após definir status e payload
        self.send_response(status_code)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.end_headers()

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
    server_address = ('', int(os.environ.get('PORT', '5000')))
    httpd = ThreadingHTTPServer(server_address, APIHandler)
    
    logger.info("🚀 Servidor REAL iniciado em http://localhost:5000")
    logger.info("📊 Modo: Dados reais do Yahoo Finance")
    logger.info("🔄 Atualização: AUTOMÁTICA a cada 5 minutos")
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
    logger.info("⏰ Atualizações automáticas a cada 5 minutos")
    
    # Configurar agendamento automático
    schedule.every(5).minutes.do(fetch_market_data)
    logger.info("⏰ Agendamento automático configurado: a cada 5 minutos")
    
    # Iniciar atualização automática em thread separada
    def run_scheduler():
        logger.info("🔄 Scheduler thread iniciada")
        while True:
            schedule.run_pending()
            time.sleep(1)
    
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    logger.info("✅ Thread do scheduler iniciada com sucesso")
    
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        logger.info("Servidor parado pelo usuário")
        httpd.shutdown()

if __name__ == '__main__':
    logger.info("Iniciando Calculadora ADRs Backend REAL v2.3")
    
    # Carregar snapshots históricos do Supabase ao iniciar
    load_latest_snapshots_from_supabase()
    
    # Executar servidor
    run_server()
