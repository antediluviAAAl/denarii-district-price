import sys
import json
import re
import statistics
import urllib.parse
import os
import random
from typing import Dict, List, Optional, Any
from abc import ABC, abstractmethod
import concurrent.futures
from curl_cffi import requests
from bs4 import BeautifulSoup

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
class Config:
    NGC_BASE_URL = "https://www.ngccoin.com/price-guide/world"
    MASHOPS_BASE_URL = "https://www.ma-shops.com/shops/search.php"
    EUR_TO_USD = 1.08
    GBP_TO_USD = 1.26

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

def smart_fetch(url: str, headers: dict = None, expected_texts: list = None, retry_limit: int = 3, label: str = "CORE") -> Optional[str]:
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
        proxy_query = f'site:ngccoin.com/price-guide/world "{country}" "KM {km_num}"'
        encoded_query = urllib.parse.quote_plus(proxy_query)
        search_url = f"https://html.duckduckgo.com/html/?q={encoded_query}"
        
        print(f"{Colors.CYAN}🔍 [*] [NGC] Querying Discovery Proxy (DuckDuckGo): {proxy_query}{Colors.RESET}")
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9"
        }
        
        try:
            page_text = smart_fetch(search_url, headers=headers, retry_limit=3, label="NGC DISCOVERY")
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
        """Extracts numerical numismatic valuations (baseline values) strictly from the HTML using headless datacenter proxy rotation."""
        print(f"{Colors.BLUE}🌐 [~] [NGC] Network: Harvesting Official Data Matrix...{Colors.RESET}")
        
        proxies = ProxyNetwork.get_proxies()
        if not proxies:
            print(f"{Colors.YELLOW}⚠️  [!] [NGC] NO PROXIES CONFIGURED in proxies.txt. Attempting bare connection (high risk).{Colors.RESET}")
            proxies = [None]
            
        headers = {
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.google.com/",
            "Upgrade-Insecure-Requests": "1"
        }
        
        page_text = smart_fetch(url, headers=headers, expected_texts=["value", "5 francs", "mintage", "uxpricetablefixedcolumns_dxmaintable"], retry_limit=3, label="NGC")
                
        if not page_text:
             print(f"{Colors.RED}⛔ [!] [NGC] Cloudflare Turnstile explicitly blocked requests or proxy failed.{Colors.RESET}")
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
# PHASE 2: NUMISTA CATALOG BASELINE EXTRACTOR
# ==========================================
class NumistaScraper:
    BASE_URL = "https://en.numista.com"

    @staticmethod
    def get_headers():
        return {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
            "accept-language": "en-US,en;q=0.9",
            "sec-ch-ua": "\"Chromium\";v=\"110\", \"Google Chrome\";v=\"110\"",
            "sec-ch-ua-platform": "\"Windows\"",
            "sec-fetch-dest": "document",
            "sec-fetch-mode": "navigate",
            "sec-fetch-site": "none",
            "upgrade-insecure-requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"
        }

    @classmethod
    def find_numista_url_via_proxy(cls, query):
        proxy_query = f"{query} numista"
        encoded_query = urllib.parse.quote_plus(proxy_query)
        search_url = f"https://html.duckduckgo.com/html/?q={encoded_query}"
        
        print(f"{Colors.CYAN}🔍 [*] [NUMISTA] Querying Discovery Proxy (DuckDuckGo): {proxy_query}{Colors.RESET}")
        
        page_text = smart_fetch(search_url, headers=cls.get_headers(), retry_limit=3, label="NUMISTA DISCOVERY")
        if not page_text:
            return None
            
        decoded_html = urllib.parse.unquote(page_text)
        numista_pattern = r'https://en\.numista\.com/(?:catalogue/pieces\d+\.html|\d+)'
        matches = re.findall(numista_pattern, decoded_html)
        
        if matches:
            print(f"{Colors.GREEN}🎯 [+] [NUMISTA] Discovered Target Catalog URL: {matches[0]}{Colors.RESET}")
            return matches[0]
        return None

    @classmethod
    def extract_baselines(cls, query: str, target_year: str) -> List[Dict[str, Any]]:
        print(f"{Colors.BLUE}🌐 [~] [NUMISTA] Network: Harvesting Secondary Data Matrix...{Colors.RESET}")
        
        coin_url = cls.find_numista_url_via_proxy(query)
        if not coin_url:
            print(f"{Colors.YELLOW}👻 [-] [NUMISTA] Target query '{query}' not found via proxy.{Colors.RESET}")
            return []
            
        page_text = smart_fetch(coin_url, headers=cls.get_headers(), retry_limit=3, label="NUMISTA")
        if not page_text:
            print(f"{Colors.YELLOW}⚠️  [!] [NUMISTA] Failed to access catalog page directly.{Colors.RESET}")
            return []

        soup = BeautifulSoup(page_text, 'lxml')
        
        page_text = soup.get_text(separator=" ", strip=True)
        curr_match = re.search(r'Values.{0,50}in\s*([A-Za-z]+)', page_text)
        if curr_match and 'USD' not in curr_match.group(1).upper():
            num_cur = curr_match.group(1).upper()
            print(f"{Colors.YELLOW}⚠️  [!] [NUMISTA] Values are in {num_cur}. Conversion applied.{Colors.RESET}")
            numista_currency = num_cur
        else:
            numista_currency = "USD"
            
        target_table = None
        
        for table in soup.find_all('table'):
            headers = [th.get_text(strip=True).upper() for th in table.find_all('th')]
            if 'VF' in headers and 'XF' in headers:
                target_table = table
                break
                
        if not target_table:
            return []
            
        headers = [th.get_text(strip=True).upper() for th in target_table.find_all('th')]
        grade_map = {}
        for grade in ['VG', 'F', 'VF', 'XF', 'AU', 'UNC']:
            if grade in headers:
                grade_map[grade] = headers.index(grade)
                
        extracted_rows = []
        for row in target_table.find_all('tr'):
            cells = row.find_all(['td', 'th'])
            if not cells: continue
            
            row_text = row.get_text(separator=" ", strip=True)
            if str(target_year) in row_text:
                if len(cells) < max(grade_map.values()) + 1:
                    continue
                    
                variant_name = cells[0].get_text(strip=True)
                if not variant_name:
                    variant_name = str(target_year)
                    
                matrix = {'PrAg': None, 'G': None, 'VG': None, 'F': None, 'VF': None, 'XF': None, 'AU': None, 'UNC': None}
                
                for grade, col_index in grade_map.items():
                    if len(cells) <= col_index: continue
                    cell_text = cells[col_index].get_text(strip=True)
                    try:
                        if bool(re.search(r'\d', cell_text)):
                            clean_val = float(re.sub(r'[^\d.]', '', cell_text))
                            if numista_currency == "EUR":
                                clean_val = round(clean_val * Config.EUR_TO_USD, 2)
                            elif numista_currency == "RON":
                                clean_val = round(clean_val / 4.85, 2)
                            elif numista_currency == "GBP":
                                clean_val = round(clean_val * Config.GBP_TO_USD, 2)
                        else:
                            clean_val = None
                    except ValueError:
                        clean_val = None
                        
                    matrix[grade] = clean_val
                    
                extracted_rows.append({
                    "mint_mark": variant_name,
                    "NGC_baseline_prices": matrix # Kept identical key naming for UI rendering parity
                })
        
        return extracted_rows

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
        page_text = smart_fetch(search_url, headers=cls.get_headers(), retry_limit=3, label="MA-SHOPS")
        
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
                country = tds[1].get_text(strip=True)
                nominal = tds[2].get_text(strip=True)
                year = tds[3].get_text(strip=True)
                if str(target_year) not in year: continue
                
                info_td = tds[4]
                for bad_tag in info_td.find_all(['span', 'b', 'strong'], class_=re.compile(r'newgold|bold', re.I)):
                    bad_tag.decompose()
                info = info_td.get_text(separator=" ", strip=True)

                grade = tds[5].get_text(strip=True) or "UNGRADED"
                price_td = tds[6]
            else:
                country = tds[1].get_text(strip=True)
                info_td = tds[2]
                for bad_tag in info_td.find_all(['span', 'b', 'strong'], class_=re.compile(r'newgold|bold', re.I)):
                    bad_tag.decompose()
                info = info_td.get_text(separator=" ", strip=True)
                
                pass
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
                    "country": country,
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
        if not re.search(rf'(?:^|[^a-z0-9]){target_year}(?:[^a-z0-9]|$)', t_lower):
            return False
            
        if target_year == "1881" and ("enescu" in t_lower or "1881-1955" in t_lower.replace(" ", "")):
            return False
            
        fake_keywords = ['fantasy', 'replica', 'copy', 'fake', 'novelty', 'tribute']
        if any(fw in t_lower for fw in fake_keywords):
            return False
            
        years_found = re.findall(r'(?:^|[^a-z0-9])(1[789]\d\d|20\d\d)(?:[^a-z0-9]|$)', t_lower)
        if years_found:
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
        
        page_text = smart_fetch(url, headers=headers, retry_limit=4, label=source_tag.upper())
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
            nominal_parts = nominal.split()
            if not all(part in title_lower for part in nominal_parts):
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
        url = f"https://www.ebay.com/sch/i.html?_nkw={encoded_query}&LH_ItemCondition=4|10|3000"
        print(f"{Colors.CYAN}🌐 [NETWORK] [EBAY ACTIVE] Querying Active Live Deals: {url}{Colors.RESET}")
        return cls.run_ebay_search(url, query, target_year, country, "eBay (Active)")

# ==========================================
# PHASE 5: OKAZII ROMANIAN MARKET EXTRACTOR
# ==========================================
class OkaziiSource(AbstractMarketSource):
    @classmethod
    def validate_integrity(cls, title: str, target_year: str) -> bool:
        t_lower = title.lower()
        if not re.search(rf'(?:^|[^a-z0-9]){target_year}(?:[^a-z0-9]|$)', t_lower):
            return False
            
        if target_year == "1881" and ("enescu" in t_lower or "1881-1955" in t_lower.replace(" ", "")):
            return False
            
        fake_keywords = ['copie', 'replica', 'fals', 'fantezie']
        if any(fw in t_lower for fw in fake_keywords):
            return False
            
        years_found = re.findall(r'(?:^|[^a-z0-9])(1[789]\d\d|20\d\d)(?:[^a-z0-9]|$)', t_lower)
        if years_found:
            for y in years_found:
                if y != str(target_year) and y not in ['1914', '1901', '1882', '1883', '1884', '1885', '1880']:
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
            page_text = smart_fetch(search_url, headers=cls.get_headers(), retry_limit=3, label="OKAZII SEARCH")
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
        
        page_text = smart_fetch(search_url, headers=cls.get_headers(), retry_limit=3, label="OKAZII DDG")
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
            page_text = smart_fetch(link, headers=cls.get_headers(), retry_limit=3, label=f"OKAZII {mode_str}")
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
    search_query = f"{nominal} {target_year}"
    
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

    # 1. ORCHESTRATE THREAD POOL EXECUTION
    def fetch_ngc() -> list:
        try:
            url = NGCScraper.get_ngc_url(country, km_num)
            return NGCScraper.extract_baselines(url, target_year) if url else []
        except Exception as e:
            print(f"{Colors.RED}❌ [!] [NGC] Thread Error: {str(e)}{Colors.RESET}")
            return []

    def fetch_numista() -> list:
        try:
            return NumistaScraper.extract_baselines(search_query, target_year)
        except Exception as e:
            print(f"{Colors.RED}❌ [!] [NUMISTA] Thread Error: {str(e)}{Colors.RESET}")
            return []

    print(f"\n{Colors.BLUE}===================================================={Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.CYAN}  SPINNING UP ASYNCHRONOUS THREAD POOL ENGINE{Colors.RESET}")
    print(f"{Colors.BLUE}===================================================={Colors.RESET}")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        f_ngc = executor.submit(fetch_ngc)
        f_numista = executor.submit(fetch_numista)
        f_mashops = executor.submit(MAShopsSource.fetch_active, search_query, target_year, country)
        f_ebay_active = executor.submit(eBaySource.fetch_active, search_query, target_year, country)
        f_ebay_sold = executor.submit(eBaySource.fetch_sold, search_query, target_year, country)
        f_okazii_active = executor.submit(OkaziiSource.fetch_active, search_query, target_year, country)
        f_okazii_sold = executor.submit(OkaziiSource.fetch_sold, search_query, target_year, country)
        
        output_payload["baselines"]["ngc"] = f_ngc.result()
        output_payload["baselines"]["numista"] = f_numista.result()
        mashops_data = f_mashops.result()
        ebay_active_data = f_ebay_active.result()
        ebay_sold_data = f_ebay_sold.result()
        okazii_active_data = f_okazii_active.result()
        okazii_sold_data = f_okazii_sold.result()

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
    with open("market_data.json", "w", encoding="utf-8") as f:
        json.dump(output_payload, f, indent=4)
        
    print(f"\n{Colors.BOLD}{Colors.HEADER}====================================================")
    print(f"             📦 MARKET MATRIX PACKAGED 📦")
    print(f"===================================================={Colors.RESET}")
    print(f"NGC Catalog Baseline   : {len(output_payload['baselines']['ngc'])}")
    print(f"Numista Catalog Base   : {len(output_payload['baselines']['numista'])}")
    
    print(f"Active Retail Deals    : {len(output_payload['active_listings'])}")
    print(f"Historical Sold Assets : {len(output_payload['sold_listings'])}")
    print(f"{Colors.GREEN}[+] Unified output securely flushed to market_data.json{Colors.RESET}\n")

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
