import sys
import json
import re
import statistics
import urllib.parse
import urllib.request
import os
import random
from typing import Dict, List, Optional, Any
from abc import ABC, abstractmethod
from curl_cffi import requests
from bs4 import BeautifulSoup
_SCRAPLING_OK = False
_SCRAPLING_ERR = "Not attempted"

try:
    from scrapling.fetchers import StealthyFetcher as _StealthyFetcher
    _SCRAPLING_OK = True
    _SCRAPLING_ERR = None
except Exception as e:
    _SCRAPLING_OK = False
    _SCRAPLING_ERR = str(e)

# Load .env file if present (local dev)
try:
    _env_path = os.path.join(os.path.dirname(__file__), '.env')
    if os.path.exists(_env_path):
        with open(_env_path, encoding='utf-8') as _ef:
            for _line in _ef:
                _line = _line.strip()
                if _line and not _line.startswith('#') and '=' in _line:
                    _k, _v = _line.split('=', 1)
                    os.environ.setdefault(_k.strip(), _v.strip())
except Exception:
    pass

# ==========================================
# TERMINAL UI: ANSI COLOR CODES
# ==========================================
class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    CYAN = '\033[96m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    RESET = '\033[0m'
    BOLD = '\033[1m'

# Ensure stdout can handle emojis on Windows CMD
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8')

# ==========================================
# CONFIGURATION
# ==========================================
# ==========================================
# FX RATES: FETCHED LIVE ON STARTUP
# ==========================================
def fetch_fx_rates() -> dict:
    """Fetch live USD-based FX rates from fawazahmed0/exchange-api (no key, daily updates).
    Falls back to hardcoded safe defaults on any failure."""
    _DEFAULTS = {'eur': 1.08, 'gbp': 1.26, 'ron': 1 / 4.65}
    try:
        cdn = 'https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/usd.json'
        req = urllib.request.Request(cdn, headers={'User-Agent': 'Mozilla/5.0'})
        with urllib.request.urlopen(req, timeout=8) as resp:
            data = json.loads(resp.read().decode())
        rates = data.get('usd', {})
        eur = 1 / rates['eur'] if rates.get('eur') else _DEFAULTS['eur']
        gbp = 1 / rates['gbp'] if rates.get('gbp') else _DEFAULTS['gbp']
        ron = 1 / rates['ron'] if rates.get('ron') else _DEFAULTS['ron']
        print(f"{Colors.GREEN}[FX] Live rates fetched: 1 EUR = ${eur:.4f} | 1 GBP = ${gbp:.4f} | 1 RON = ${ron:.4f}{Colors.RESET}")
        return {'eur': round(eur, 6), 'gbp': round(gbp, 6), 'ron': round(ron, 6)}
    except Exception as ex:
        print(f"{Colors.YELLOW}⚠️  [FX] Live rate fetch failed ({ex}). Using hardcoded fallback rates.{Colors.RESET}")
        return _DEFAULTS

class Config:
    NGC_BASE_URL = "https://www.ngccoin.com/price-guide/world"
    MASHOPS_BASE_URL = "https://www.ma-shops.com/shops/search.php"
    # Populated dynamically at startup by fetch_fx_rates()
    EUR_TO_USD: float = 1.08
    GBP_TO_USD: float = 1.26
    RON_TO_USD: float = 1 / 4.65

# ---- NUMISTA API KEY ----
NUMISTA_API_KEY = os.environ.get("NUMISTA_API_KEY", "")

class ProxyNetwork:
    @staticmethod
    def get_proxies() -> List[str]:
        # Secure Cloud Fallback: Try Render Environment Variables First
        env_proxies = os.environ.get("PROXY_LIST")
        if env_proxies:
            return [p.strip() for p in env_proxies.split(",") if p.strip()]
            
        # Local Desktop Fallback
        proxy_file = "proxies.txt"
        if os.path.exists(proxy_file):
            with open(proxy_file, "r", encoding="utf-8") as f:
                return [line.strip() for line in f if line.strip() and not line.startswith("#")]
        return []

    @staticmethod
    def get_random_proxy() -> Optional[Dict[str, str]]:
        proxies = ProxyNetwork.get_proxies()
        if proxies:
            px = random.choice(proxies)
            return {"http": px, "https": px}
        return None

def smart_fetch(url: str, headers: dict = None, expected_texts: list = None, retry_limit: int = 10, label: str = "CORE") -> Optional[str]:
    """Retries HTTP GET requests behind rotating proxies with Cloudflare fingerprint mimicry."""
    for attempt in range(1, retry_limit + 1):
        proxy_dict = ProxyNetwork.get_random_proxy()
        proxy_ip = proxy_dict['http'].split('@')[-1] if proxy_dict else "Local IP"
        timeout_val = 15 if attempt == 1 else 25
        
        try:
            with requests.Session(impersonate="chrome110", proxies=proxy_dict, timeout=timeout_val) as session:
                res = session.get(url, headers=headers)
                
            if res.status_code in [403, 429, 502, 503]:
                print(f"{Colors.YELLOW}⚠️  [!] [{label}] Attempt {attempt} Blocked (HTTP {res.status_code}). Rotating IP...{Colors.RESET}")
                continue
            elif res.status_code != 200:
                print(f"{Colors.YELLOW}⚠️  [!] [{label}] Attempt {attempt} Failed (HTTP {res.status_code}). Rotating IP...{Colors.RESET}")
                continue
                
            text_lower = res.text.lower()
            if "captcha" in text_lower or (hasattr(res, 'url') and "splashui/challenge" in res.url.lower()):
                print(f"{Colors.YELLOW}⚠️  [!] [{label}] Attempt {attempt} hit CAPTCHA/Validation fail on proxy {proxy_ip}. Rotating IP...{Colors.RESET}")
                continue
                
            if expected_texts:
                if not any(t in text_lower for t in expected_texts):
                    print(f"{Colors.YELLOW}⚠️  [!] [{label}] Attempt {attempt} missing expected payload. Rotating IP...{Colors.RESET}")
                    continue
                
            if attempt > 1:
                print(f"{Colors.GREEN}✅ [+] [{label}] Retry successful on proxy ({proxy_ip}).{Colors.RESET}")
            else:
                 print(f"{Colors.GREEN}✅ [+] [{label}] WAF Bypassed headlessly via Proxy ({proxy_ip}).{Colors.RESET}")
                 
            return res.text
            
        except Exception as e:
            print(f"{Colors.YELLOW}⚠️  [!] [{label}] Attempt {attempt} Network Error: {str(e)}. Rotating IP...{Colors.RESET}")
            continue
            
    print(f"{Colors.RED}❌ [!] [{label}] Max retries ({retry_limit}) exhausted. Endpoint unreachable.{Colors.RESET}")
    return None

# ==========================================
# PHASE 1: NGC CATALOG BASELINE EXTRACTOR
# ==========================================
class NGCScraper:
    @classmethod
    def get_ngc_url(cls, country: str, km_num: str) -> Optional[str]:
        """Uses DuckDuckGo HTML to inherently bypass ALL GDPR cookie walls and WAF blocks."""
        # Strip any KM# / KM prefix so query reads 'KM 17.1' not 'KM KM# 17.1'
        clean_km = km_num.upper().replace('KM#', '').replace('KM', '').strip()
        proxy_query = f'site:ngccoin.com/price-guide/world "{country}" "KM {clean_km}"'
        encoded_query = urllib.parse.quote_plus(proxy_query)
        search_url = f"https://html.duckduckgo.com/html/?q={encoded_query}"
        
        print(f"{Colors.CYAN}🔍 [*] [NGC] Querying Discovery Proxy (DuckDuckGo): {proxy_query}{Colors.RESET}")
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9"
        }
        
        try:
            page_text = smart_fetch(search_url, headers=headers, retry_limit=10, label="NGC DISCOVERY")
            if not page_text:
                return None
            soup = BeautifulSoup(page_text, 'html.parser')
            
            for a_tag in soup.select('a.result__url'):
                href = a_tag.get('href', '')
                if 'uddg=' in href:
                    raw_url = href.split('uddg=')[1].split('&')[0]
                    clean_url = urllib.parse.unquote(raw_url)
                    if "ngccoin.com/price-guide/world" in clean_url and country.lower() in clean_url.lower():
                        print(f"{Colors.GREEN}🎯 [+] [NGC] Discovered Target Catalog URL: {clean_url}{Colors.RESET}")
                        return clean_url
        except Exception as e:
            print(f"{Colors.RED}❌ [-] [NGC] Discovery engine failed: {str(e)}{Colors.RESET}")
            
        print(f"{Colors.RED}❌ [-] [NGC] Failed to find a matching NGC catalog page.{Colors.RESET}")
        return None

    @classmethod
    def extract_baselines(cls, url: str, target_year: str) -> List[Dict[str, Any]]:
        """Extracts NGC price baselines using StealthyFetcher (Camoufox hardened Firefox)
        which can bypass Cloudflare Turnstile. Falls back to smart_fetch if Scrapling
        is not installed."""
        print(f"{Colors.BLUE}🌐 [~] [NGC] Network: Harvesting Official Data Matrix via StealthyFetcher...{Colors.RESET}")

        proxies = ProxyNetwork.get_proxies()
        page_text = None

        if _SCRAPLING_OK and proxies:
            # Rotate through proxies until one succeeds
            proxy_pool = proxies.copy()
            random.shuffle(proxy_pool)
            for proxy in proxy_pool:
                try:
                    print(f"{Colors.CYAN}🥷 [NGC] StealthyFetcher attempt via proxy {proxy.split('@')[-1]}...{Colors.RESET}")
                    resp = _StealthyFetcher.fetch(url, headless=True, timeout=45000, proxy=proxy)
                    if resp and resp.status == 200:
                        html = getattr(resp, 'html_content', None) or ""
                        if html and len(html) > 5000:  # real page, not an error stub
                            page_text = html
                            print(f"{Colors.GREEN}✅ [+] [NGC] StealthyFetcher bypassed Turnstile via {proxy.split('@')[-1]}{Colors.RESET}")
                            break
                        else:
                            print(f"{Colors.YELLOW}⚠️  [NGC] Got 200 but body too small ({len(html)} bytes), rotating...{Colors.RESET}")
                    else:
                        status = getattr(resp, 'status', '?') if resp else 'None'
                        print(f"{Colors.YELLOW}⚠️  [NGC] StealthyFetcher got status {status} via {proxy.split('@')[-1]}, rotating...{Colors.RESET}")
                except Exception as e:
                    print(f"{Colors.YELLOW}⚠️  [NGC] StealthyFetcher failed on {proxy.split('@')[-1]}: {e}{Colors.RESET}")
        elif not _SCRAPLING_OK:
            print(f"{Colors.YELLOW}⚠️  [NGC] Scrapling not available (Error: {_SCRAPLING_ERR}) — falling back to smart_fetch{Colors.RESET}")
            headers = {
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "en-US,en;q=0.9",
                "Referer": "https://www.google.com/",
            }
            page_text = smart_fetch(url, headers=headers, retry_limit=5, label="NGC")
        else:
            print(f"{Colors.YELLOW}⚠️  [!] [NGC] No proxies configured — bare StealthyFetcher attempt{Colors.RESET}")
            try:
                resp = _StealthyFetcher.fetch(url, headless=True, timeout=45000)
                if resp and resp.status == 200:
                    page_text = getattr(resp, 'html_content', None) or ""
            except Exception as e:
                print(f"{Colors.RED}❌ [NGC] Bare StealthyFetcher failed: {e}{Colors.RESET}")

        if not page_text:
            print(f"{Colors.RED}⛔ [!] [NGC] All fetch attempts failed — Cloudflare Turnstile or proxy exhausted.{Colors.RESET}")
            return []
             
        soup = BeautifulSoup(page_text, 'html.parser')
        
        fixed_table = soup.select('[id$="uxPriceTableFixedColumns_DXMainTable"]')
        scroll_table = soup.select('[id$="uxPriceTable_DXMainTable"]')
        
        if not fixed_table or not scroll_table:
            print(f"{Colors.RED}⚠️  [!] [NGC] Could not locate the dynamic DevExpress split-tables.{Colors.RESET}")
            return []
            
        matched_rows = []
        
        for row in fixed_table[0].select('tr[id*="DXDataRow"]'):
            cells = [td.get_text(strip=True) for td in row.select('th, td')]
            if cells and target_year in cells[0]:
                raw_cell_text = cells[0].replace('\n', ' ').strip()
                isolated_mint = raw_cell_text.replace(target_year, "").strip() or "Standard"
                row_id = row.get('id', '')
                target_row_num = row_id.split('DXDataRow')[-1]
                
                matched_rows.append((isolated_mint, target_row_num))
                print(f"{Colors.GREEN}✅ [+] [NGC] Parsed Catalog Mint Variant: {target_year} '{isolated_mint}'{Colors.RESET}")
                
        if not matched_rows:
            print(f"{Colors.YELLOW}👻 [-] [NGC] Target year '{target_year}' has no registered NGC baseline data.{Colors.RESET}")
            return []

        header_row = scroll_table[0].select('tr[id$="DXDataRow0"]')
        if not header_row:
            return []
            
        headers = [td.get_text(strip=True) for td in header_row[0].select('th, td')]
        variants_list = []
        
        for mint_mark, row_num in matched_rows:
            target_data_row = scroll_table[0].select(f'tr[id$="DXDataRow{row_num}"]')
            if not target_data_row:
                continue
                
            prices = [td.get_text(strip=True) for td in target_data_row[0].select('th, td')]
            unified = {'PrAg': None, 'G': None, 'VG': None, 'F': None, 'VF': None, 'XF': None, 'AU': None, 'UNC': None}
            
            for header, price in zip(headers, prices):
                clean_price = re.sub(r'[^\d.]', '', price)
                val = float(clean_price) if clean_price else None
                if val is None: continue
                
                target = None
                if header in unified:
                    target = header
                elif header.isdigit():
                    num = int(header)
                    if num >= 60: target = 'UNC'
                    elif num >= 50: target = 'AU'
                    elif num >= 40: target = 'XF'
                    elif num >= 20: target = 'VF'
                    elif num >= 12: target = 'F'
                    elif num >= 8: target = 'VG'
                    elif num >= 4: target = 'G'
                    else: target = 'PrAg'
                    
                if target and unified[target] is None:
                    unified[target] = val
                        
            if any(v is not None for v in unified.values()):
                variants_list.append({
                    "mint_mark": mint_mark,
                    "NGC_baseline_prices": unified
                })
                
        return variants_list

# ==========================================
# PHASE 2: NUMISTA API v3 BASELINE EXTRACTOR
# ==========================================
class NumistaAPIScraper:
    """Uses the official Numista REST API v3 for reliable, structured baseline data.
    Requires NUMISTA_API_KEY environment variable.
    Flow: search types by query -> match best type_id -> list issues for target_year
          -> fetch prices per issue -> map to unified grade schema.
    """
    API_BASE = "https://api.numista.com/api/v3"

    # Numista grade labels -> our canonical grade keys
    GRADE_MAP = {
        'poor': 'PrAg', 'ag': 'PrAg', 'prag': 'PrAg',
        'g': 'G', 'good': 'G',
        'vg': 'VG', 'very good': 'VG',
        'f': 'F', 'fine': 'F',
        'vf': 'VF', 'very fine': 'VF',
        'xf': 'XF', 'ef': 'XF', 'extremely fine': 'XF',
        'au': 'AU', 'about uncirculated': 'AU',
        'unc': 'UNC', 'ms': 'UNC', 'bu': 'UNC', 'uncirculated': 'UNC',
    }

    @classmethod
    def _api_get(cls, path: str, params: dict = None) -> Optional[dict]:
        """Makes an authenticated GET to the Numista API. Returns parsed JSON or None."""
        if not NUMISTA_API_KEY:
            print(f"{Colors.RED}❌ [NUMISTA API] NUMISTA_API_KEY is not set. Skipping.{Colors.RESET}")
            return None
        url = f"{cls.API_BASE}{path}"
        if params:
            url += '?' + urllib.parse.urlencode(params)
        req = urllib.request.Request(url, headers={'Numista-API-Key': NUMISTA_API_KEY})
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read().decode())
        except Exception as e:
            print(f"{Colors.YELLOW}⚠️  [NUMISTA API] Request failed for {path}: {e}{Colors.RESET}")
            return None

    @classmethod
    def _find_type_id(cls, query: str, nominal: str) -> Optional[int]:
        """Searches Numista for the coin type and returns the best matching type_id."""
        # Query often includes country+nominal+year. Nominal is the 'clean' version (e.g. '5 Lei').
        data = cls._api_get('/types', {'q': query, 'lang': 'en'})
        if not data:
            return None
        types = data.get('types', [])
        if not types:
            print(f"{Colors.YELLOW}👻 [NUMISTA API] No types found for query '{query}'.{Colors.RESET}")
            return None
            
        # 1. High confidence: Title contains both nominal value AND country
        # (This is the most reliable match for coins like '5 Lei')
        for t in types:
            title = t.get('title', '').lower()
            if nominal.lower() in title:
                print(f"{Colors.GREEN}✅ [NUMISTA API] Matched type: {t['title']} (ID: {t['id']}){Colors.RESET}")
                return t['id']
                
        # 2. Fallback: Title contains nominal value
        for t in types:
            if nominal.lower() in t.get('title', '').lower():
                return t['id']
                
        # 3. Last resort: trust first result if it seems related
        print(f"{Colors.YELLOW}⚠️  [NUMISTA API] No strict nominal match within results. Using first result: {types[0]['title']}{Colors.RESET}")
        return types[0]['id']

    @classmethod
    def _map_prices(cls, prices_data: dict) -> dict:
        """Maps Numista API price records to our unified grade schema.
        API response format: {"currency": "EUR", "prices": [{"grade": "vf", "price": 146.24}, ...]}
        """
        matrix = {'PrAg': None, 'G': None, 'VG': None, 'F': None,
                  'VF': None, 'XF': None, 'AU': None, 'UNC': None}
        currency = prices_data.get('currency', 'USD').upper()
        for entry in prices_data.get('prices', []):
            # Actual API field names are 'grade' (short code) and 'price' (numeric value)
            raw_grade = entry.get('grade', '').lower().strip()
            canonical = cls.GRADE_MAP.get(raw_grade)
            if not canonical:
                for k, v in cls.GRADE_MAP.items():
                    if raw_grade.startswith(k):
                        canonical = v
                        break
            if not canonical or matrix.get(canonical) is not None:
                continue
            val = entry.get('price')  # API field is 'price', not 'value'
            if val is None:
                continue
            try:
                val = float(val)
                if currency == 'EUR':
                    val = round(val * Config.EUR_TO_USD, 2)
                elif currency == 'GBP':
                    val = round(val * Config.GBP_TO_USD, 2)
                elif currency == 'RON':
                    val = round(val * Config.RON_TO_USD, 2)
                else:
                    val = round(val, 2)
                matrix[canonical] = val
            except (ValueError, TypeError):
                continue
        return matrix

    @classmethod
    def extract_baselines(cls, query: str, nominal: str, target_year: str) -> List[Dict[str, Any]]:
        """Main entry-point: returns list of variant baseline dicts for target_year."""
        print(f"{Colors.BLUE}🌐 [~] [NUMISTA API] Fetching official baseline data for '{query}'...{Colors.RESET}")

        type_id = cls._find_type_id(query, nominal)
        if not type_id:
            return []

        issues_data = cls._api_get(f'/types/{type_id}/issues', {'lang': 'en'})
        if not issues_data:
            return []

        results = []
        for issue in issues_data:
            issue_year = str(issue.get('year', ''))
            issue_title = str(issue.get('title', ''))
            if target_year not in issue_year and target_year not in issue_title:
                continue

            issue_id = issue['id']
            variant_label = issue.get('comments') or issue.get('title') or target_year
            print(f"{Colors.GREEN}  ✅ [NUMISTA API] Found issue: {variant_label} (Issue ID: {issue_id}){Colors.RESET}")

            prices_data = cls._api_get(f'/types/{type_id}/issues/{issue_id}/prices')
            if not prices_data:
                continue

            matrix = cls._map_prices(prices_data)
            if any(v is not None for v in matrix.values()):
                results.append({
                    'mint_mark': variant_label,
                    'NGC_baseline_prices': matrix  # Key kept for UI parity
                })

        if not results:
            print(f"{Colors.YELLOW}👻 [NUMISTA API] No price data for year {target_year}.{Colors.RESET}")
        return results

# ==========================================
# POLYMORPHIC SOURCE INTERFACE
# ==========================================
class AbstractMarketSource(ABC):
    @classmethod
    @abstractmethod
    def fetch_active(cls, query: str, target_year: str, country: str) -> List[Dict[str, Any]]:
        pass
        
    @classmethod
    @abstractmethod
    def fetch_sold(cls, query: str, target_year: str, country: str) -> List[Dict[str, Any]]:
        pass

    @classmethod
    @abstractmethod
    def validate_integrity(cls, title: str, target_year: str) -> bool:
        pass


# ==========================================
# PHASE 3: MA-SHOPS LIVE RETAIL EXTRACTOR
# ==========================================
class MAShopsSource(AbstractMarketSource):
    @classmethod
    def fetch_sold(cls, query: str, target_year: str, country: str) -> List[Dict[str, Any]]:
        # MA-Shops does not expose public historical liquidity/sold deals out of the box
        return []

    @classmethod
    def validate_integrity(cls, title: str, target_year: str) -> bool:
        """High Trust bounds: Exclusively verifies target_year exists without arbitrary secondary bounding."""
        t_lower = title.lower()
        if not re.search(rf'(?:^|[^a-z0-9]){target_year}(?:[^a-z0-9]|$)', t_lower):
            return False
        return True

    @staticmethod
    def get_headers():
        return {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "accept-language": "en-US,en;q=0.9",
            "sec-ch-ua": "\"Chromium\";v=\"110\", \"Google Chrome\";v=\"110\"",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "upgrade-insecure-requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
        }

    @classmethod
    def fetch_active(cls, query: str, target_year: str, country: str) -> List[Dict[str, Any]]:
        """Maps live retail table rows safely mapping Dual Layout A/B variants."""
        encoded_query = urllib.parse.quote_plus(query)
        search_url = f"{Config.MASHOPS_BASE_URL}?searchstr={encoded_query}&submitBtn=Search"
        
        print(f"{Colors.CYAN}🌐 [NETWORK] [MA-SHOPS] Querying Live Retail Ceiling: {search_url}{Colors.RESET}")
        page_text = smart_fetch(search_url, headers=cls.get_headers(), retry_limit=10, label="MA-SHOPS")
        
        if not page_text:
            return []
            
        soup = BeautifulSoup(page_text, 'html.parser')
        rows = soup.find_all('tr')
        print(f"{Colors.BLUE}🔍 [PARSER] [MA-SHOPS] Searching DOM entries...{Colors.RESET}")
        
        parsed_listings = []
        for row in rows:
            if row.get('id') == 'alternativeSearchInfo' or 'alternativeSearchInfo' in row.get('class', []):
                break

            tds = row.find_all('td')
            if len(tds) < 5:
                continue

            link = row.find('a', href=True)
            img = row.find('img', src=True)
            item_url = link['href'] if link else ""
            if item_url and not item_url.startswith("http"):
                 item_url = urllib.parse.urljoin("https://www.ma-shops.com", item_url)
                 
            image_url = img['src'] if img else ""
            if image_url and not image_url.startswith("http"):
                 image_url = urllib.parse.urljoin("https://www.ma-shops.com", image_url)
                
            if len(tds) >= 7:
                # Layout A: 7-column table. Year column (tds[3]) contains reign range e.g. '1881-1914'.
                country_val = tds[1].get_text(strip=True)
                nominal = tds[2].get_text(strip=True)
                year = tds[3].get_text(strip=True)
                if str(target_year) not in year:
                    continue

                info_td = tds[4]
                for bad_tag in info_td.find_all(['span', 'b', 'strong'], class_=re.compile(r'newgold|bold', re.I)):
                    bad_tag.decompose()
                info = info_td.get_text(separator=" ", strip=True)

                grade = tds[5].get_text(strip=True) or "UNGRADED"
                price_td = tds[6]
            else:
                # Layout B: 5-column table — info is the description column
                country_val = tds[1].get_text(strip=True)
                info_td = tds[2]
                for bad_tag in info_td.find_all(['span', 'b', 'strong'], class_=re.compile(r'newgold|bold', re.I)):
                    bad_tag.decompose()
                info = info_td.get_text(separator=" ", strip=True)

                # Layout B: verify year appears in the descriptive text
                if str(target_year) not in info:
                    continue

                nominal = query.replace(str(target_year), "").strip()
                year = str(target_year)
                
                # NUMISMATIC EXACT GRADE MATCHER
                grade_match = re.search(r'\b(VG|VF|XF|EF|AU|UNC|BU|Proof|PR|PF|MS|PrAg|F|G|ss|vz|stgl)\b.*$', info, re.IGNORECASE)
                grade = grade_match.group(0).strip() if grade_match else "UNGRADED"
                price_td = tds[3]
                
            for del_tag in price_td.find_all('del'):
                del_tag.decompose()
                
            price_text = price_td.get_text(separator=" ", strip=True)
            usd_normalized = 0.0
            match = re.search(r'([$€£]|USD|EUR|GBP)\s*([\d.,]+)|([\d.,]+)\s*([$€£]|USD|EUR|GBP)', price_text, re.IGNORECASE)
            
            if match:
                cur_str = (match.group(1) or match.group(4)).upper()
                num_str = match.group(2) or match.group(3)
                
                if num_str.count(',') == 1 and num_str.count('.') == 1:
                    clean_num = num_str.replace('.', '').replace(',', '.')
                elif ',' in num_str:
                    clean_num = num_str.replace(',', '.')
                else:
                    clean_num = num_str
                    
                try:
                    val = float(clean_num)
                    if 'EUR' in cur_str or '€' in cur_str:
                        usd_normalized = val * Config.EUR_TO_USD
                    elif 'GBP' in cur_str or '£' in cur_str:
                        usd_normalized = val * Config.GBP_TO_USD
                    else:
                        usd_normalized = val
                except ValueError:
                    continue

            if usd_normalized > 0:
                if not cls.validate_integrity(info, str(target_year)):
                    continue
                parsed_listings.append({
                    "source": "MA-Shops",
                    "country": country_val,
                    "nominal": nominal,
                    "year": year,
                    "grade": grade,
                    "info": info,
                    "price_usd": round(usd_normalized, 2),
                    "item_url": item_url,
                    "image_url": image_url
                })

        return parsed_listings

# ==========================================
# PHASE 3: EBAY SOLD LIQUIDITY EXTRACTOR
# ==========================================
class eBaySource(AbstractMarketSource):
    SLAB_TERMS = ["pcgs", "ngc", "anacs", "icg", "details", "ms6", "ms-", "pr6", "pf6", "slab"]
    FAKE_TERMS = ["copy", "replica", "novelty", "tribute", "token", "fake", "banknote", "polymer", "folder", "paper", "commemorative", "specimen"]
    DAMAGE_TERMS = ["cleaned", "holed", "scratched", "plugged", "bent", "environmental", "corroded"]

    @classmethod
    def validate_integrity(cls, title: str, target_year: str) -> bool:
        t_lower = title.lower()
        # 1. Year must appear as an isolated token
        if not re.search(rf'(?:^|[^a-z0-9]){target_year}(?:[^a-z0-9]|$)', t_lower):
            return False
        # 2. Generic fake/replica filter
        fake_keywords = ['fantasy', 'replica', 'copy', 'fake', 'novelty', 'tribute']
        if any(fw in t_lower for fw in fake_keywords):
            return False
        # 3. Reject if any OTHER year appears in the title (catches wrong-year listings
        #    e.g. "5 Lei 1880 Carol I (1866-1881)" when searching for 1881)
        years_found = re.findall(r'(?:^|[^a-z0-9])(1[789]\d\d|20\d\d)(?:[^a-z0-9]|$)', t_lower)
        for y in years_found:
            if y != str(target_year):
                return False
        return True

    @staticmethod
    def extract_grade(title: str) -> str:
        slab_match = re.search(r'\b(MS|PR|PF|AU|XF|VF|F|VG|G)[\s\-]*([1-7][0-9])\b', title, re.IGNORECASE)
        if slab_match:
            prefix = slab_match.group(1).upper()
            number = slab_match.group(2)
            if prefix in ['PR', 'PF']: prefix = 'PF'
            num = int(number)
            if num >= 60: return str(num)
            return prefix

        raw_match = re.search(r'\b(UNCIRCULATED|UNC|BU|AU|XF|EF|VF|VG|F)(?:\+|-)?\b', title, re.IGNORECASE)
        if raw_match:
            grade = raw_match.group(1).upper()
            if grade in ['UNCIRCULATED', 'BU']: return 'UNC'
            if grade == 'EF': return 'XF'
            return grade

        return "UNGRADED"

    @classmethod
    def run_ebay_search(cls, url: str, query: str, target_year: str, country: str, source_tag: str) -> List[Dict[str, Any]]:
        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "accept-language": "en-US,en;q=0.9",
            "upgrade-insecure-requests": "1"
        }
        
        page_text = smart_fetch(url, headers=headers, retry_limit=10, label=source_tag.upper())
        if not page_text:
            return []

        soup = BeautifulSoup(page_text, 'lxml')
        results_list = soup.find('ul', class_=re.compile(r'srp-results'))
        if not results_list: return []

        items = results_list.find_all('li', recursive=False)
        print(f"{Colors.BLUE}🔍 [PARSER] [{source_tag.upper()}] Searching {len(items)} DOM entries...{Colors.RESET}")
        
        parsed_listings = []
        for item in items:
            title_el = item.select_one('.s-item__title, .s-card__title')
            price_el = item.select_one('.s-item__price, .s-card__price')
            link_el = item.select_one('a.s-item__link, a.s-card__link, a')
            img_el = item.select_one('img')

            if not title_el or not price_el: continue

            title = title_el.get_text(strip=True)
            title_lower = title.lower()
            
            if title_lower in ["shop on ebay", "new listing"]: continue
            if str(target_year) not in title: continue
            
            nominal = query.replace(str(target_year), "").strip().lower()
            # Strict phrase match: "5 lei" must appear as a contiguous phrase, not scattered words
            if nominal not in title_lower:
                continue
                
            if any(term in title_lower for term in cls.SLAB_TERMS): continue
            if any(term in title_lower for term in cls.FAKE_TERMS): continue
            if any(term in title_lower for term in cls.DAMAGE_TERMS): continue
                
            title = title.replace("New Listing", "").strip()
            price_str = price_el.get_text(strip=True)
            url_href = link_el.get('href', '') if link_el else ""
            image_url = img_el.get('src', '') if img_el else ""
                
            try:
                clean_price = float(re.sub(r'[^\d.]', '', price_str.split(' to ')[0]))
            except ValueError:
                continue

            grade = cls.extract_grade(title)
            
            item_text = item.get_text(separator=" ")
            
            date_match = re.search(r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4}', item_text)
            date_str = date_match.group(0) if date_match else None
            
            if not date_str:
                # Try international formats like 05-Mar-2024
                date_match_int = re.search(r'\d{1,2}-(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)-\d{4}', item_text, re.IGNORECASE)
                date_str = date_match_int.group(0) if date_match_int else ("Active" if not 'Sold' in source_tag else "Recent")

            info_str = f"[{'SOLD' if 'Sold' in source_tag else 'RETAIL'}] " + title
            if not cls.validate_integrity(info_str, str(target_year)):
                continue

            parsed_listings.append({
                "source": source_tag,
                "country": country,
                "nominal": query.replace(str(target_year), "").strip(),
                "year": str(target_year),
                "grade": grade,
                "info": info_str,
                "price_usd": clean_price,
                "item_url": url_href,
                "image_url": image_url,
                "date": date_str
            })

        return parsed_listings

    @classmethod
    def fetch_sold(cls, query: str, target_year: str, country: str) -> List[Dict[str, Any]]:
        encoded_query = urllib.parse.quote_plus(query)
        url = f"https://www.ebay.com/sch/i.html?_nkw={encoded_query}&LH_Sold=1&LH_Complete=1"
        print(f"{Colors.CYAN}🌐 [NETWORK] [EBAY SOLD] Querying Sold Liquidity Floor: {url}{Colors.RESET}")
        return cls.run_ebay_search(url, query, target_year, country, "eBay (Sold)")

    @classmethod
    def fetch_active(cls, query: str, target_year: str, country: str) -> List[Dict[str, Any]]:
        encoded_query = urllib.parse.quote_plus(query)
        url = f"https://www.ebay.com/sch/i.html?_nkw={encoded_query}&LH_ItemCondition=4|10|3000&LH_BIN=1"
        print(f"{Colors.CYAN}🌐 [NETWORK] [EBAY ACTIVE] Querying Fixed-Price Deals: {url}{Colors.RESET}")
        return cls.run_ebay_search(url, query, target_year, country, "eBay (Active)")

# ==========================================
# PHASE 5: OKAZII ROMANIAN MARKET EXTRACTOR
# ==========================================
class OkaziiSource(AbstractMarketSource):
    @classmethod
    def validate_integrity(cls, title: str, target_year: str) -> bool:
        t_lower = title.lower()
        # 1. Year must appear as an isolated token
        if not re.search(rf'(?:^|[^a-z0-9]){target_year}(?:[^a-z0-9]|$)', t_lower):
            return False
        # 2. Romanian-language fake/replica filter
        # 2. Romanian-language fake/replica filter
        fake_keywords = ['copie', 'replica', 'fals', 'fantezie']
        if any(fw in t_lower for fw in fake_keywords):
            return False
        # 3. Reject if any OTHER year appears in the title
        years_found = re.findall(r'(?:^|[^a-z0-9])(1[789]\d\d|20\d\d)(?:[^a-z0-9]|$)', t_lower)
        for y in years_found:
            if y != str(target_year):
                return False
        return True

    @staticmethod
    def get_headers():
        return {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "accept-language": "en-US,en;q=0.9,ro;q=0.8",
            "sec-ch-ua": "\"Chromium\";v=\"110\", \"Google Chrome\";v=\"110\"",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "upgrade-insecure-requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
        }

    @classmethod
    def get_okazii_links(cls, query, is_sold):
        if not is_sold:
            encoded_query = urllib.parse.quote_plus(query)
            search_url = f"https://www.okazii.ro/cautare/{encoded_query}.html"
            page_text = smart_fetch(search_url, headers=cls.get_headers(), retry_limit=10, label="OKAZII SEARCH")
            if page_text:
                soup = BeautifulSoup(page_text, "lxml")
                links = []
                for a in soup.find_all('a', href=True):
                    href = a['href']
                    if 'okazii.ro' in href and '-a' in href and 'cautare' not in href:
                        links.append(href)
                return list(dict.fromkeys(links))[:10]

        archived_term = '"stoc epuizat"' if is_sold else '-"stoc epuizat"'
        proxy_query = f'site:okazii.ro {archived_term} "{query}"'
        encoded_query = urllib.parse.quote_plus(proxy_query)
        search_url = f"https://html.duckduckgo.com/html/?q={encoded_query}"
        
        page_text = smart_fetch(search_url, headers=cls.get_headers(), retry_limit=10, label="OKAZII DDG")
        if not page_text:
            return []
            
        decoded_html = urllib.parse.unquote(page_text)
        okazii_pattern = r'https://(?:www\.)?okazii\.ro/(?!recomandate|cautare|catalog)[^\s"\'<>]+-a\d{8,}'
        raw_matches = re.findall(okazii_pattern, decoded_html)
        return list(dict.fromkeys(raw_matches))[:10]

    @classmethod
    def fetch_active(cls, query: str, target_year: str, country: str) -> List[Dict[str, Any]]:
        return cls._fetch_listings(query, target_year, country, False)

    @classmethod
    def fetch_sold(cls, query: str, target_year: str, country: str) -> List[Dict[str, Any]]:
        return cls._fetch_listings(query, target_year, country, True)

    @classmethod
    def _fetch_listings(cls, query: str, target_year: str, country: str, is_sold: bool) -> List[Dict[str, Any]]:
        mode_str = "SOLD" if is_sold else "ACTIVE"
        print(f"{Colors.CYAN}🌐 [NETWORK] [OKAZII {mode_str}] Querying Discovery Proxy for Romanian Listings...{Colors.RESET}")
        
        links = cls.get_okazii_links(query, is_sold)
        if not links: return []
        
        print(f"{Colors.BLUE}🔍 [PARSER] [OKAZII {mode_str}] Evaluating {len(links)} DOM entry points...{Colors.RESET}")
        
        results = []
        for link in links:
            page_text = smart_fetch(link, headers=cls.get_headers(), retry_limit=10, label=f"OKAZII {mode_str}")
            if not page_text: continue
            
            soup = BeautifulSoup(page_text, 'lxml')
            page_text = soup.get_text(separator=" ", strip=True).lower()
            
            is_archived = "stoc epuizat" in page_text or "produs indisponibil" in page_text or "vandut" in page_text or "nu este pe stoc" in page_text
            
            # Since DDG is returning limited results, if we are specifically fetching active, and the DDG search was for active (no "stoc epuizat"),
            # but the page says it's out of stock, we drop it. Conversely, if we want sold, and it IS out of stock, we keep it.
            # But the user mentioned it was wrongly classed. Let's force the label based on what it actually is, then append to the respective list.
            if is_sold and not is_archived: continue
            if not is_sold and is_archived: continue
                
            title_el = soup.find('h1')
            title = title_el.get_text(strip=True) if title_el else "Unknown Title"
            
            if str(target_year) not in title: continue
            
            raw_price = 0.0
            currency = "RON"
            
            pret_match = re.search(r'pre[tț]\s*:\s*([\d.,]+)\s*(lei|ron|€|eur)', page_text, re.IGNORECASE)
            if pret_match:
                clean_str = pret_match.group(1).replace('.', '').replace(',', '.')
                try: raw_price = float(clean_str)
                except ValueError: pass
                if "€" in pret_match.group(2) or "eur" in pret_match.group(2).lower(): currency = "EUR"
                    
            if raw_price == 0.0:
                price_el = soup.select_one('.item-price')
                if price_el:
                    price_text = price_el.get_text(strip=True).replace('.', '').replace(',', '.')
                    pmatch = re.search(r'([\d.]+)', price_text)
                    if pmatch:
                        raw_price = float(pmatch.group(1))
                        if 'eur' in price_text.lower() or '€' in price_text:
                            currency = "EUR"
                            
            if raw_price == 0.0:
                price_meta = soup.find('meta', itemprop='price')
                if price_meta and price_meta.get('content'):
                    try: raw_price = float(price_meta.get('content'))
                    except ValueError: pass
                    
            if raw_price == 0.0: continue
            
            normalized_usd = raw_price / 4.85 if currency == "RON" else raw_price * Config.EUR_TO_USD
            
            # Extract Image
            img_el = soup.select_one('#main-image-placeholder img, .gallery-top-wrapper img')
            image_url = img_el.get('src', '') if img_el else ""
            if not image_url:
                img_meta = soup.find('meta', property='og:image')
                image_url = img_meta.get('content', '') if img_meta else ""

            # Attempt a grade scrape out of title
            grade = eBaySource.extract_grade(title)
            
            # Date extraction for JS sorting (YYYY-MM-DD works natively in JS timestamp conversions)
            date_str = "Active"
            if is_sold:
                d_match = re.search(r'expirat la:\s*(\d{1,2})\.(\d{1,2})\.(\d{4})', page_text, re.I)
                if d_match:
                    date_str = f"{d_match.group(3)}-{d_match.group(2)}-{d_match.group(1)}"
                else:
                    date_str = "Recent"

            info_str = f"[{'SOLD' if is_sold else 'RETAIL'}] " + title
            if not cls.validate_integrity(info_str, str(target_year)):
                continue

            results.append({
                "source": "Okazii (Archive)" if is_sold else "Okazii",
                "country": country,
                "nominal": query.replace(str(target_year), "").strip(),
                "year": str(target_year),
                "grade": grade,
                "info": info_str,
                "price_usd": round(normalized_usd, 2),
                "item_url": link,
                "image_url": image_url,
                "date": date_str
            })
            
        return results

# ==========================================
# PHASE 6: GRAND UNIFICATION ORCHESTRATOR
# ==========================================
def orchestrate_market_scan(country: str, km_num: str, target_year: str, nominal: str):
    # DEDUPLICATE: If nominal starts with country (e.g. "Romania 5 Lei"), clean it for better matching (e.g. "5 Lei")
    clean_nominal = nominal
    if country.lower() in nominal.lower():
        # Strip country name, optional "Coin", and trailing/leading separators
        clean_nominal = re.sub(rf'^{re.escape(country)}[\s\-]*', '', nominal, flags=re.IGNORECASE)
        clean_nominal = re.sub(r'^(Coin|Moneda|Monedă)[\s\-]*', '', clean_nominal, flags=re.IGNORECASE).strip()
    
    # search_query used for broad web searches (eBay, DDG, etc.)
    search_query = f"{nominal} {target_year}"
    # Use clean nominal if drastically different (shorter) for precise matching tools
    match_nominal = clean_nominal if len(clean_nominal) < len(nominal) else nominal
    
    output_payload = {
        "metadata": {
            "query": search_query,
            "target_country": country,
            "km_num": km_num,
            "target_year": target_year
        },
        "baselines": {
            "ngc": [],
            "numista": []
        },
        "metrics": {
            "active": {},
            "sold": {}
        },
        "active_listings": [],
        "sold_listings": []
    }
    
    def normalize_market_grade(raw_grade: str) -> str:
        g = raw_grade.upper().strip()
        # Edge cases & Direct hits
        if 'UNGRADED' in g: return 'UNGRADED'
        if 'PRAG' in g or 'POOR' in g: return 'PrAg'
        
        # German Normalizers
        if 'STGL' in g or 'UNZ' in g or 'STEMPEL' in g or 'SSP' in g or 'PP' in g: return 'UNC'
        if 'VZ' in g: return 'XF'
        if 'SS' in g: return 'VF'
        if ' S ' in f' {g} ': return 'F'
        if 'SGE' in g: return 'VG'
        
        # French Normalizers
        if 'FDC' in g or 'BU' in g: return 'UNC'
        if 'SUP' in g: return 'XF'
        if 'TTB' in g: return 'VF'
        if 'TB' in g: return 'F'
        
        # Spanish / Italian Normalizers
        if 'SPL' in g or 'EBC' in g or ' SC ' in f' {g} ': return 'XF'
        if 'BB' in g or 'MBC' in g: return 'VF'
        if 'MB' in g or 'BC+' in g: return 'F'
        if 'BC' in g: return 'VG'
        
        # English Standard Fallbacks
        if 'UNC' in g or 'MS' in g or 'PROOF' in g: return 'UNC'
        if 'AU' in g: return 'AU'
        if 'XF' in g or 'EF' in g: return 'XF'
        if 'VF' in g: return 'VF'
        if 'F' == g or (' F ' in f' {g} '): return 'F'
        if 'VG' in g: return 'VG'
        if 'G' == g or (' G ' in f' {g} '): return 'G'
        
        return "UNGRADED"

    # 1. FETCH LIVE FX RATES FOR THIS SCAN SESSION
    fx = fetch_fx_rates()
    Config.EUR_TO_USD = fx['eur']
    Config.GBP_TO_USD = fx['gbp']
    Config.RON_TO_USD = fx['ron']

    print(f"\n{Colors.BLUE}===================================================={Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.CYAN}  SEQUENTIAL MARKET SCAN ENGINE — 7 STEPS{Colors.RESET}")
    print(f"{Colors.BLUE}===================================================={Colors.RESET}")

    # STEP 1 — NGC CATALOG BASELINE
    print(f"\n{Colors.BOLD}[STEP 1/7] NGC Catalog Baseline{Colors.RESET}")
    try:
        url = NGCScraper.get_ngc_url(country, km_num)
        output_payload["baselines"]["ngc"] = NGCScraper.extract_baselines(url, target_year) if url else []
    except Exception as e:
        print(f"{Colors.RED}❌ [NGC] Failed: {e}{Colors.RESET}")
        output_payload["baselines"]["ngc"] = []

    # STEP 2 — NUMISTA API BASELINE
    print(f"\n{Colors.BOLD}[STEP 2/7] Numista API Baseline{Colors.RESET}")
    try:
        # Use match_nominal for precision logic
        output_payload["baselines"]["numista"] = NumistaAPIScraper.extract_baselines(
            search_query, match_nominal, target_year
        )
    except Exception as e:
        print(f"{Colors.RED}❌ [NUMISTA API] Failed: {e}{Colors.RESET}")
        output_payload["baselines"]["numista"] = []

    # STEP 3 — MA-SHOPS ACTIVE LISTINGS
    print(f"\n{Colors.BOLD}[STEP 3/7] MA-Shops Live Retail{Colors.RESET}")
    try:
        mashops_data = MAShopsSource.fetch_active(search_query, target_year, country)
    except Exception as e:
        print(f"{Colors.RED}❌ [MA-SHOPS] Failed: {e}{Colors.RESET}")
        mashops_data = []

    # STEP 4 — EBAY ACTIVE LISTINGS
    print(f"\n{Colors.BOLD}[STEP 4/7] eBay Active Listings{Colors.RESET}")
    try:
        ebay_active_data = eBaySource.fetch_active(search_query, target_year, country)
    except Exception as e:
        print(f"{Colors.RED}❌ [EBAY ACTIVE] Failed: {e}{Colors.RESET}")
        ebay_active_data = []

    # STEP 5 — EBAY SOLD LISTINGS
    print(f"\n{Colors.BOLD}[STEP 5/7] eBay Sold Listings{Colors.RESET}")
    try:
        ebay_sold_data = eBaySource.fetch_sold(search_query, target_year, country)
    except Exception as e:
        print(f"{Colors.RED}❌ [EBAY SOLD] Failed: {e}{Colors.RESET}")
        ebay_sold_data = []

    # STEP 6 — OKAZII ACTIVE LISTINGS
    print(f"\n{Colors.BOLD}[STEP 6/7] Okazii Active Listings{Colors.RESET}")
    try:
        okazii_active_data = OkaziiSource.fetch_active(search_query, target_year, country)
    except Exception as e:
        print(f"{Colors.RED}❌ [OKAZII ACTIVE] Failed: {e}{Colors.RESET}")
        okazii_active_data = []

    # STEP 7 — OKAZII SOLD LISTINGS
    print(f"\n{Colors.BOLD}[STEP 7/7] Okazii Sold (Archive){Colors.RESET}")
    try:
        okazii_sold_data = OkaziiSource.fetch_sold(search_query, target_year, country)
    except Exception as e:
        print(f"{Colors.RED}❌ [OKAZII SOLD] Failed: {e}{Colors.RESET}")
        okazii_sold_data = []

    combined_active = mashops_data + ebay_active_data + okazii_active_data
    combined_sold = ebay_sold_data + okazii_sold_data

    # 2. DEDUPLICATE ACTIVE LIQUIDITY
    seen_ids = set()
    seen_images = set()
    dedup_active = []
    
    def score_english(t):
        tl = t['info'].lower()
        return 1 if any(w in tl for w in ['coin', 'romania ', 'silver']) else 2
    
    combined_active.sort(key=score_english)
    
    for item in combined_active:
        id_str = item['item_url']
        if 'ebay.' in id_str and '/itm/' in id_str:
            match = re.search(r'/itm/(\d+)', id_str)
            if match: id_str = 'ebay_' + match.group(1)
        elif 'okazii.ro' in id_str:
            match = re.search(r'-a(\d+)', id_str)
            if match: id_str = 'okazii_' + match.group(1)
        
        img_hash = None
        if 'ebayimg.com/images/g/' in item['image_url']:
            img_match = re.search(r'/images/g/([^/]+)/', item['image_url'])
            if img_match: img_hash = img_match.group(1)
            
        is_dup = False
        if img_hash and img_hash in seen_images:
            is_dup = True
        if id_str in seen_ids:
            is_dup = True
            
        if not is_dup:
            seen_ids.add(id_str)
            if img_hash: seen_images.add(img_hash)
            dedup_active.append(item)
            
    for item in dedup_active: item['grade'] = normalize_market_grade(item['grade'])
    output_payload["active_listings"] = sorted(dedup_active, key=lambda x: x['price_usd'])
    
    # 3. DEDUPLICATE SOLD LIQUIDITY
    seen_ids_sold = set()
    seen_images_sold = set()
    dedup_sold = []
    
    combined_sold.sort(key=score_english)
    
    for item in combined_sold:
        id_str = item['item_url']
        if 'ebay.' in id_str and '/itm/' in id_str:
            match = re.search(r'/itm/(\d+)', id_str)
            if match: id_str = 'ebay_' + match.group(1)
        elif 'okazii.ro' in id_str:
            match = re.search(r'-a(\d+)', id_str)
            if match: id_str = 'okazii_' + match.group(1)
        
        img_hash = None
        if 'ebayimg.com/images/g/' in item['image_url']:
            img_match = re.search(r'/images/g/([^/]+)/', item['image_url'])
            if img_match: img_hash = img_match.group(1)
        
        is_dup = False
        if img_hash and img_hash in seen_images_sold:
            is_dup = True
        if id_str in seen_ids_sold:
            is_dup = True
            
        if not is_dup:
            seen_ids_sold.add(id_str)
            if img_hash: seen_images_sold.add(img_hash)
            dedup_sold.append(item)
            
    for item in dedup_sold: item['grade'] = normalize_market_grade(item['grade'])
    output_payload["sold_listings"] = sorted(dedup_sold, key=lambda x: x['price_usd'])
    
    # 4. METRICS CALCULATION
    if dedup_active:
        prices = [item["price_usd"] for item in dedup_active]
        output_payload["metrics"]["active"] = {
            "median": round(statistics.median(prices), 2),
            "min": round(min(prices), 2),
            "max": round(max(prices), 2),
            "supply": len(dedup_active)
        }
    if dedup_sold:
        prices = [item["price_usd"] for item in dedup_sold]
        output_payload["metrics"]["sold"] = {
            "median": round(statistics.median(prices), 2),
            "min": round(min(prices), 2),
            "max": round(max(prices), 2),
            "supply": len(dedup_sold)
        }

    # 5. COMPILE AND EXPORT
    if os.environ.get("DEBUG_LOCAL"):
        with open("market_data.json", "w", encoding="utf-8") as f:
            json.dump(output_payload, f, indent=4)
        print(f"{Colors.GREEN}[+] Debug: output flushed to market_data.json{Colors.RESET}")
        
    print(f"\n{Colors.BOLD}{Colors.HEADER}====================================================")
    print(f"             📦 MARKET MATRIX PACKAGED 📦")
    print(f"===================================================={Colors.RESET}")
    print(f"NGC Catalog Baseline   : {len(output_payload['baselines']['ngc'])}")
    print(f"Numista API Baseline   : {len(output_payload['baselines']['numista'])}")
    print(f"Active Retail Deals    : {len(output_payload['active_listings'])}")
    print(f"Historical Sold Assets : {len(output_payload['sold_listings'])}")
    print(f"{Colors.GREEN}[+] Scan complete. Returning unified payload.{Colors.RESET}\n")

    return output_payload

if __name__ == "__main__":
    print(f"\n{Colors.BOLD}{Colors.HEADER}====================================================")
    print("      🦅  DENARII DISTRICT: UNIFIED MARKET SCANNER 🦅")
    print(f"===================================================={Colors.RESET}\n")
    try:
        user_country = input(f"{Colors.BOLD}🌍 [?] NGC Country (e.g. Romania) : {Colors.RESET}").strip()
        user_km = input(f"{Colors.BOLD}🪙  [?] NGC KM# (e.g. 17.1)        : {Colors.RESET}").strip()
        user_nom = input(f"{Colors.BOLD}💰 [?] Nominal (e.g. 5 Lei)       : {Colors.RESET}").strip()
        user_year = input(f"{Colors.BOLD}📅 [?] Target Year (e.g. 1881)    : {Colors.RESET}").strip()
    except KeyboardInterrupt:
        sys.exit(0)
        
    if not all([user_country, user_km, user_nom, user_year]):
        print(f"{Colors.RED}❌ [!] All parameters required.{Colors.RESET}")
        sys.exit(1)
        
    orchestrate_market_scan(
        country=user_country,
        km_num=user_km.upper().replace("KM", "").strip(),
        target_year=user_year,
        nominal=user_nom
    )
