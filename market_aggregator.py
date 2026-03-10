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
        env_proxies = os.environ.get("PROXY_LIST")
        if env_proxies:
            return [p.strip() for p in env_proxies.split(",") if p.strip()]
            
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
                print(f"{Colors.YELLOW}⚠️  [!] [{label}] Attempt {attempt} hit CAPTCHA fail on proxy {proxy_ip}. Rotating IP...{Colors.RESET}")
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
            
    print(f"{Colors.RED}❌ [!] [{label}] Max retries exhausted.{Colors.RESET}")
    return None

# ==========================================
# PHASE 1 & 2: CATALOG DATA (NGC & NUMISTA)
# ==========================================
class NGCScraper:
    @classmethod
    def get_ngc_url(cls, country: str, km_num: str) -> Optional[str]:
        proxy_query = f'site:ngccoin.com/price-guide/world "{country}" "KM {km_num}"'
        encoded_query = urllib.parse.quote_plus(proxy_query)
        search_url = f"https://html.duckduckgo.com/html/?q={encoded_query}"
        
        print(f"{Colors.CYAN}🔍 [*] [NGC] Querying Discovery Proxy: {proxy_query}{Colors.RESET}")
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/110.0.0.0 Safari/537.36"}
        
        try:
            page_text = smart_fetch(search_url, headers=headers, retry_limit=10, label="NGC DISCOVERY")
            if not page_text: return None
            soup = BeautifulSoup(page_text, 'html.parser')
            
            for a_tag in soup.select('a.result__url'):
                href = a_tag.get('href', '')
                if 'uddg=' in href:
                    clean_url = urllib.parse.unquote(href.split('uddg=')[1].split('&')[0])
                    if "ngccoin.com/price-guide/world" in clean_url and country.lower() in clean_url.lower():
                        print(f"{Colors.GREEN}🎯 [+] [NGC] Discovered URL: {clean_url}{Colors.RESET}")
                        return clean_url
        except Exception as e:
            pass
        return None

    @classmethod
    def extract_baselines(cls, url: str, target_year: str) -> List[Dict[str, Any]]:
        if not url: return []
        headers = {"Upgrade-Insecure-Requests": "1"}
        page_text = smart_fetch(url, headers=headers, expected_texts=["uxpricetablefixedcolumns"], retry_limit=10, label="NGC")
        if not page_text: return []
             
        soup = BeautifulSoup(page_text, 'html.parser')
        fixed_table = soup.select('[id$="uxPriceTableFixedColumns_DXMainTable"]')
        scroll_table = soup.select('[id$="uxPriceTable_DXMainTable"]')
        
        if not fixed_table or not scroll_table: return []
            
        matched_rows = []
        for row in fixed_table[0].select('tr[id*="DXDataRow"]'):
            cells = [td.get_text(strip=True) for td in row.select('th, td')]
            if cells and target_year in cells[0]:
                isolated_mint = cells[0].replace('\n', ' ').replace(target_year, "").strip() or "Standard"
                row_num = row.get('id', '').split('DXDataRow')[-1]
                matched_rows.append((isolated_mint, row_num))
                
        if not matched_rows: return []

        header_row = scroll_table[0].select('tr[id$="DXDataRow0"]')
        if not header_row: return []
        headers = [td.get_text(strip=True) for td in header_row[0].select('th, td')]
        
        variants_list = []
        for mint_mark, row_num in matched_rows:
            target_data_row = scroll_table[0].select(f'tr[id$="DXDataRow{row_num}"]')
            if not target_data_row: continue
                
            prices = [td.get_text(strip=True) for td in target_data_row[0].select('th, td')]
            unified = {'PrAg': None, 'G': None, 'VG': None, 'F': None, 'VF': None, 'XF': None, 'AU': None, 'UNC': None}
            
            for header, price in zip(headers, prices):
                clean_price = re.sub(r'[^\d.]', '', price)
                val = float(clean_price) if clean_price else None
                if val is None: continue
                
                target = header if header in unified else None
                if not target and header.isdigit():
                    num = int(header)
                    if num >= 60: target = 'UNC'
                    elif num >= 50: target = 'AU'
                    elif num >= 40: target = 'XF'
                    elif num >= 20: target = 'VF'
                    elif num >= 12: target = 'F'
                    elif num >= 8: target = 'VG'
                    elif num >= 4: target = 'G'
                    else: target = 'PrAg'
                    
                if target and unified[target] is None: unified[target] = val
                        
            if any(v is not None for v in unified.values()):
                variants_list.append({"mint_mark": mint_mark, "NGC_baseline_prices": unified})
        return variants_list

class NumistaScraper:
    @staticmethod
    def get_headers():
        return {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/110.0.0.0 Safari/537.36"}

    @classmethod
    def find_numista_url_via_proxy(cls, query):
        encoded_query = urllib.parse.quote_plus(f"{query} numista")
        search_url = f"https://html.duckduckgo.com/html/?q={encoded_query}"
        
        page_text = smart_fetch(search_url, headers=cls.get_headers(), retry_limit=10, label="NUMISTA DISCOVERY")
        if not page_text: return None
            
        decoded_html = urllib.parse.unquote(page_text)
        matches = re.findall(r'https://en\.numista\.com/(?:catalogue/pieces\d+\.html|\d+)', decoded_html)
        if matches: return matches[0]
        return None

    @classmethod
    def extract_baselines(cls, query: str, target_year: str) -> List[Dict[str, Any]]:
        coin_url = cls.find_numista_url_via_proxy(query)
        if not coin_url: return []
            
        page_text = smart_fetch(coin_url, headers=cls.get_headers(), retry_limit=10, label="NUMISTA")
        if not page_text: return []

        soup = BeautifulSoup(page_text, 'lxml')
        page_text_clean = soup.get_text(separator=" ", strip=True)
        
        num_cur = "USD"
        curr_match = re.search(r'Values.{0,50}in\s*([A-Za-z]+)', page_text_clean)
        if curr_match and 'USD' not in curr_match.group(1).upper():
            num_cur = curr_match.group(1).upper()
            
        target_table = None
        for table in soup.find_all('table'):
            headers = [th.get_text(strip=True).upper() for th in table.find_all('th')]
            if 'VF' in headers and 'XF' in headers:
                target_table = table
                break
                
        if not target_table: return []
            
        headers = [th.get_text(strip=True).upper() for th in target_table.find_all('th')]
        grade_map = {g: headers.index(g) for g in ['VG', 'F', 'VF', 'XF', 'AU', 'UNC'] if g in headers}
                
        extracted_rows = []
        for row in target_table.find_all('tr'):
            cells = row.find_all(['td', 'th'])
            if not cells: continue
            
            row_text = row.get_text(separator=" ", strip=True)
            if str(target_year) in row_text:
                if len(cells) < max(grade_map.values(), default=0) + 1: continue
                    
                variant_name = cells[0].get_text(strip=True) or str(target_year)
                matrix = {'PrAg': None, 'G': None, 'VG': None, 'F': None, 'VF': None, 'XF': None, 'AU': None, 'UNC': None}
                
                for grade, col_index in grade_map.items():
                    if len(cells) <= col_index: continue
                    cell_text = cells[col_index].get_text(strip=True)
                    try:
                        if bool(re.search(r'\d', cell_text)):
                            clean_val = float(re.sub(r'[^\d.]', '', cell_text))
                            if num_cur == "EUR": clean_val = round(clean_val * Config.EUR_TO_USD, 2)
                            elif num_cur == "RON": clean_val = round(clean_val / 4.85, 2)
                            elif num_cur == "GBP": clean_val = round(clean_val * Config.GBP_TO_USD, 2)
                        else: clean_val = None
                    except ValueError: clean_val = None
                    matrix[grade] = clean_val
                    
                extracted_rows.append({"mint_mark": variant_name, "NGC_baseline_prices": matrix})
        return extracted_rows

# ==========================================
# POLYMORPHIC SOURCE INTERFACE
# ==========================================
class AbstractMarketSource(ABC):
    @classmethod
    @abstractmethod
    def fetch_active(cls, nominal: str, target_year: str, country: str) -> List[Dict[str, Any]]: pass
    @classmethod
    @abstractmethod
    def fetch_sold(cls, nominal: str, target_year: str, country: str) -> List[Dict[str, Any]]: pass
    @classmethod
    @abstractmethod
    def validate_integrity(cls, title: str, target_year: str) -> bool: pass

# ==========================================
# PHASE 3: MA-SHOPS
# ==========================================
class MAShopsSource(AbstractMarketSource):
    @classmethod
    def fetch_sold(cls, nominal: str, target_year: str, country: str) -> List[Dict[str, Any]]: return []

    @classmethod
    def validate_integrity(cls, title: str, target_year: str) -> bool:
        t_lower = title.lower()
        # SCRUBBER: Removes reign ranges like '1881-1914' before checking for target_year
        clean_title = re.sub(r'\b1[789]\d{2}\s*-\s*(?:1[789]\d{2}|20\d{2}|\d{2})\b', '', t_lower)
        if not re.search(rf'\b{target_year}\b', clean_title):
            return False
        return True

    @classmethod
    def fetch_active(cls, nominal: str, target_year: str, country: str) -> List[Dict[str, Any]]:
        # LOW DENSITY QUERY: Excludes country to bypass German translation barrier (Rumänien)
        query = f"{nominal} {target_year}" 
        encoded_query = urllib.parse.quote_plus(query)
        search_url = f"{Config.MASHOPS_BASE_URL}?searchstr={encoded_query}&submitBtn=Search"
        
        page_text = smart_fetch(search_url, headers=NumistaScraper.get_headers(), retry_limit=10, label="MA-SHOPS")
        if not page_text: return []
            
        soup = BeautifulSoup(page_text, 'html.parser')
        parsed_listings = []
        
        for row in soup.find_all('tr'):
            if row.get('id') == 'alternativeSearchInfo' or 'alternativeSearchInfo' in row.get('class', []): break
            tds = row.find_all('td')
            if len(tds) < 5: continue

            link = row.find('a', href=True)
            img = row.find('img', src=True)
            item_url = urllib.parse.urljoin("https://www.ma-shops.com", link['href']) if link else ""
            image_url = urllib.parse.urljoin("https://www.ma-shops.com", img['src']) if img else ""
                
            if len(tds) >= 7:
                item_country = tds[1].get_text(strip=True)
                year_cell = tds[3].get_text(strip=True)
                if str(target_year) not in year_cell: continue
                
                info_td = tds[4]
                for bad_tag in info_td.find_all(['span', 'b', 'strong'], class_=re.compile(r'newgold|bold', re.I)):
                    bad_tag.decompose()
                info = info_td.get_text(separator=" ", strip=True)
                grade = tds[5].get_text(strip=True) or "UNGRADED"
                price_td = tds[6]
            else:
                item_country = tds[1].get_text(strip=True)
                info_td = tds[2]
                for bad_tag in info_td.find_all(['span', 'b', 'strong'], class_=re.compile(r'newgold|bold', re.I)):
                    bad_tag.decompose()
                info = info_td.get_text(separator=" ", strip=True)
                
                grade_match = re.search(r'\b(VG|VF|XF|EF|AU|UNC|BU|Proof|PR|PF|MS|PrAg|F|G|ss|vz|stgl)\b.*$', info, re.IGNORECASE)
                grade = grade_match.group(0).strip() if grade_match else "UNGRADED"
                price_td = tds[3]
                
            for del_tag in price_td.find_all('del'): del_tag.decompose()
                
            price_text = price_td.get_text(separator=" ", strip=True)
            usd_normalized = 0.0
            match = re.search(r'([$€£]|USD|EUR|GBP)\s*([\d.,]+)|([\d.,]+)\s*([$€£]|USD|EUR|GBP)', price_text, re.IGNORECASE)
            
            if match:
                cur_str = (match.group(1) or match.group(4)).upper()
                num_str = match.group(2) or match.group(3)
                clean_num = num_str.replace('.', '').replace(',', '.') if num_str.count(',') == 1 and num_str.count('.') == 1 else num_str.replace(',', '.')
                try:
                    val = float(clean_num)
                    usd_normalized = val * Config.EUR_TO_USD if 'EUR' in cur_str or '€' in cur_str else val * Config.GBP_TO_USD if 'GBP' in cur_str or '£' in cur_str else val
                except ValueError: continue

            if usd_normalized > 0 and cls.validate_integrity(info, str(target_year)):
                parsed_listings.append({
                    "source": "MA-Shops", "country": country, "nominal": nominal,
                    "year": str(target_year), "grade": grade, "info": info,
                    "price_usd": round(usd_normalized, 2), "item_url": item_url, "image_url": image_url, "date": "Active"
                })
        return parsed_listings

# ==========================================
# PHASE 4: EBAY
# ==========================================
class eBaySource(AbstractMarketSource):
    @classmethod
    def validate_integrity(cls, title: str, target_year: str) -> bool:
        t_lower = title.lower()
        # SCRUBBER: Removes reign ranges
        clean_title = re.sub(r'\b1[789]\d{2}\s*-\s*(?:1[789]\d{2}|20\d{2}|\d{2})\b', '', t_lower)
        if not re.search(rf'\b{target_year}\b', clean_title):
            return False
            
        fake_keywords = ['fantasy', 'replica', 'copy', 'fake', 'novelty', 'tribute']
        if any(fw in t_lower for fw in fake_keywords):
            return False
            
        # Ensures no secondary standalone years exist to prevent false matches
        years_found = re.findall(r'\b(1[789]\d\d|20\d\d)\b', clean_title)
        if years_found:
            for y in years_found:
                if y != str(target_year): return False
        return True

    @staticmethod
    def extract_grade(title: str) -> str:
        slab_match = re.search(r'\b(MS|PR|PF|AU|XF|VF|F|VG|G)[\s\-]*([1-7][0-9])\b', title, re.IGNORECASE)
        if slab_match:
            prefix = 'PF' if slab_match.group(1).upper() in ['PR', 'PF'] else slab_match.group(1).upper()
            num = int(slab_match.group(2))
            return str(num) if num >= 60 else prefix
        raw_match = re.search(r'\b(UNCIRCULATED|UNC|BU|AU|XF|EF|VF|VG|F)(?:\+|-)?\b', title, re.IGNORECASE)
        if raw_match:
            grade = raw_match.group(1).upper()
            return 'UNC' if grade in ['UNCIRCULATED', 'BU'] else 'XF' if grade == 'EF' else grade
        return "UNGRADED"

    @classmethod
    def run_ebay_search(cls, url: str, nominal: str, target_year: str, country: str, source_tag: str) -> List[Dict[str, Any]]:
        page_text = smart_fetch(url, headers={"Upgrade-Insecure-Requests": "1"}, retry_limit=10, label=source_tag.upper())
        if not page_text: return []

        soup = BeautifulSoup(page_text, 'lxml')
        results_list = soup.find('ul', class_=re.compile(r'srp-results'))
        if not results_list: return []

        parsed_listings = []
        nominal_parts = nominal.lower().split() # e.g. ["5", "lei"]
        
        for item in results_list.find_all('li', recursive=False):
            title_el = item.select_one('.s-item__title, .s-card__title')
            price_el = item.select_one('.s-item__price, .s-card__price')
            if not title_el or not price_el: continue

            title = title_el.get_text(strip=True)
            title_lower = title.lower()
            
            if title_lower in ["shop on ebay", "new listing"]: continue
            
            # LOOSE MATCHING: All parts of the nominal must exist, but order doesn't matter
            if not all(part in title_lower for part in nominal_parts): continue
                
            bad_terms = ["pcgs", "ngc", "anacs", "details", "ms6", "fake", "replica", "copy", "cleaned", "holed", "scratched"]
            if any(term in title_lower for term in bad_terms): continue
                
            title = title.replace("New Listing", "").strip()
            price_str = price_el.get_text(strip=True)
                
            try: clean_price = float(re.sub(r'[^\d.]', '', price_str.split(' to ')[0]))
            except ValueError: continue

            item_text = item.get_text(separator=" ")
            date_match = re.search(r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},\s+\d{4}', item_text)
            date_str = date_match.group(0) if date_match else "Active" if not 'Sold' in source_tag else "Recent"

            info_str = f"[{'SOLD' if 'Sold' in source_tag else 'RETAIL'}] " + title
            if not cls.validate_integrity(info_str, str(target_year)): continue

            link_el = item.select_one('a.s-item__link, a.s-card__link, a')
            img_el = item.select_one('img')

            parsed_listings.append({
                "source": source_tag, "country": country, "nominal": nominal,
                "year": str(target_year), "grade": cls.extract_grade(title),
                "info": info_str, "price_usd": clean_price,
                "item_url": link_el.get('href', '') if link_el else "",
                "image_url": img_el.get('src', '') if img_el else "", "date": date_str
            })
        return parsed_listings

    @classmethod
    def fetch_sold(cls, nominal: str, target_year: str, country: str) -> List[Dict[str, Any]]:
        # HIGH DENSITY QUERY
        encoded_query = urllib.parse.quote_plus(f"{country} {nominal} {target_year}")
        url = f"https://www.ebay.com/sch/i.html?_nkw={encoded_query}&LH_Sold=1&LH_Complete=1"
        return cls.run_ebay_search(url, nominal, target_year, country, "eBay (Sold)")

    @classmethod
    def fetch_active(cls, nominal: str, target_year: str, country: str) -> List[Dict[str, Any]]:
        # HIGH DENSITY QUERY
        encoded_query = urllib.parse.quote_plus(f"{country} {nominal} {target_year}")
        url = f"https://www.ebay.com/sch/i.html?_nkw={encoded_query}&LH_ItemCondition=4|10|3000&LH_BIN=1"
        return cls.run_ebay_search(url, nominal, target_year, country, "eBay (Active)")

# ==========================================
# PHASE 5: OKAZII
# ==========================================
class OkaziiSource(AbstractMarketSource):
    @classmethod
    def validate_integrity(cls, title: str, target_year: str) -> bool:
        t_lower = title.lower()
        clean_title = re.sub(r'\b1[789]\d{2}\s*-\s*(?:1[789]\d{2}|20\d{2}|\d{2})\b', '', t_lower)
        if not re.search(rf'\b{target_year}\b', clean_title): return False
            
        if any(fw in t_lower for fw in ['copie', 'replica', 'fals', 'fantezie']): return False
            
        years_found = re.findall(r'\b(1[789]\d\d|20\d\d)\b', clean_title)
        if years_found:
            for y in years_found:
                if y != str(target_year) and y not in ['1914', '1901', '1882', '1883', '1884', '1885', '1880']:
                    return False
        return True

    @classmethod
    def get_okazii_links(cls, query, is_sold):
        headers = NumistaScraper.get_headers()
        if not is_sold:
            search_url = f"https://www.okazii.ro/cautare/{urllib.parse.quote_plus(query)}.html"
            page_text = smart_fetch(search_url, headers=headers, retry_limit=10, label="OKAZII SEARCH")
            if page_text:
                soup = BeautifulSoup(page_text, "lxml")
                links = [a['href'] for a in soup.find_all('a', href=True) if 'okazii.ro' in a['href'] and '-a' in a['href'] and 'cautare' not in a['href']]
                return list(dict.fromkeys(links))[:10]

        archived_term = '"stoc epuizat"' if is_sold else '-"stoc epuizat"'
        search_url = f"https://html.duckduckgo.com/html/?q={urllib.parse.quote_plus(f'site:okazii.ro {archived_term} {query}')}"
        page_text = smart_fetch(search_url, headers=headers, retry_limit=10, label="OKAZII DDG")
        if not page_text: return []
            
        raw_matches = re.findall(r'https://(?:www\.)?okazii\.ro/(?!recomandate|cautare|catalog)[^\s"\'<>]+-a\d{8,}', urllib.parse.unquote(page_text))
        return list(dict.fromkeys(raw_matches))[:10]

    @classmethod
    def fetch_active(cls, nominal: str, target_year: str, country: str) -> List[Dict[str, Any]]:
        return cls._fetch_listings(f"{nominal} {target_year}", nominal, target_year, country, False)

    @classmethod
    def fetch_sold(cls, nominal: str, target_year: str, country: str) -> List[Dict[str, Any]]:
        return cls._fetch_listings(f"{nominal} {target_year}", nominal, target_year, country, True)

    @classmethod
    def _fetch_listings(cls, query: str, nominal: str, target_year: str, country: str, is_sold: bool) -> List[Dict[str, Any]]:
        links = cls.get_okazii_links(query, is_sold)
        if not links: return []
        
        results = []
        for link in links:
            page_text = smart_fetch(link, headers=NumistaScraper.get_headers(), retry_limit=10, label=f"OKAZII")
            if not page_text: continue
            
            soup = BeautifulSoup(page_text, 'lxml')
            page_text_lower = soup.get_text(separator=" ", strip=True).lower()
            
            is_archived = any(term in page_text_lower for term in ["stoc epuizat", "produs indisponibil", "vandut", "nu este pe stoc"])
            if is_sold and not is_archived: continue
            if not is_sold and is_archived: continue
                
            title_el = soup.find('h1')
            title = title_el.get_text(strip=True) if title_el else "Unknown Title"
            
            raw_price, currency = 0.0, "RON"
            pret_match = re.search(r'pre[tț]\s*:\s*([\d.,]+)\s*(lei|ron|€|eur)', page_text_lower)
            if pret_match:
                try: raw_price = float(pret_match.group(1).replace('.', '').replace(',', '.'))
                except ValueError: pass
                if "€" in pret_match.group(2) or "eur" in pret_match.group(2): currency = "EUR"
                    
            if raw_price == 0.0:
                price_el = soup.select_one('.item-price')
                if price_el:
                    pmatch = re.search(r'([\d.]+)', price_el.get_text(strip=True).replace('.', '').replace(',', '.'))
                    if pmatch:
                        raw_price = float(pmatch.group(1))
                        if 'eur' in price_el.get_text(strip=True).lower(): currency = "EUR"
                            
            if raw_price == 0.0: continue
            normalized_usd = raw_price / 4.85 if currency == "RON" else raw_price * Config.EUR_TO_USD
            
            img_el = soup.select_one('#main-image-placeholder img, .gallery-top-wrapper img')
            image_url = img_el.get('src', '') if img_el else ""

            date_str = "Active"
            if is_sold:
                d_match = re.search(r'expirat la:\s*(\d{1,2})\.(\d{1,2})\.(\d{4})', page_text_lower, re.I)
                date_str = f"{d_match.group(3)}-{d_match.group(2)}-{d_match.group(1)}" if d_match else "Recent"

            info_str = f"[{'SOLD' if is_sold else 'RETAIL'}] " + title
            if not cls.validate_integrity(info_str, str(target_year)): continue

            results.append({
                "source": "Okazii (Archive)" if is_sold else "Okazii", "country": country,
                "nominal": nominal, "year": str(target_year), "grade": eBaySource.extract_grade(title),
                "info": info_str, "price_usd": round(normalized_usd, 2),
                "item_url": link, "image_url": image_url, "date": date_str
            })
        return results

# ==========================================
# PHASE 6: GRAND UNIFICATION ORCHESTRATOR
# ==========================================
def orchestrate_market_scan(country: str, km_num: str, target_year: str, nominal: str):
    output_payload = {
        "metadata": {
            "query": f"{nominal} {target_year}",
            "target_country": country,
            "km_num": km_num,
            "target_year": target_year
        },
        "baselines": {"ngc": [], "numista": []},
        "metrics": {"active": {}, "sold": {}},
        "active_listings": [], "sold_listings": []
    }
    
    def normalize_market_grade(raw_grade: str) -> str:
        g = raw_grade.upper().strip()
        if 'UNGRADED' in g: return 'UNGRADED'
        if 'PRAG' in g or 'POOR' in g: return 'PrAg'
        if any(x in g for x in ['STGL', 'UNZ', 'STEMPEL', 'SSP', 'PP', 'FDC', 'BU', 'UNC', 'MS', 'PROOF']): return 'UNC'
        if any(x in g for x in ['VZ', 'SUP', 'SPL', 'EBC', ' SC ', 'AU']): return 'AU' if 'AU' in g else 'XF'
        if any(x in g for x in ['SS', 'TTB', 'BB', 'MBC', 'VF']): return 'VF'
        if any(x in g for x in [' S ', 'TB', 'MB', 'BC+', 'F', ' F ']): return 'F'
        if any(x in g for x in ['SGE', 'BC', 'VG']): return 'VG'
        if 'G' == g or ' G ' in f' {g} ': return 'G'
        return "UNGRADED"

    def fetch_ngc() -> list:
        try:
            url = NGCScraper.get_ngc_url(country, km_num)
            return NGCScraper.extract_baselines(url, target_year) if url else []
        except: return []

    def fetch_numista() -> list:
        try: return NumistaScraper.extract_baselines(f"{country} {nominal} {target_year}", target_year)
        except: return []

    print(f"\n{Colors.BOLD}{Colors.CYAN}  SPINNING UP ASYNCHRONOUS THREAD POOL ENGINE{Colors.RESET}")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        f_ngc = executor.submit(fetch_ngc)
        f_numista = executor.submit(fetch_numista)
        f_mashops = executor.submit(MAShopsSource.fetch_active, nominal, target_year, country)
        f_ebay_active = executor.submit(eBaySource.fetch_active, nominal, target_year, country)
        f_ebay_sold = executor.submit(eBaySource.fetch_sold, nominal, target_year, country)
        f_okazii_active = executor.submit(OkaziiSource.fetch_active, nominal, target_year, country)
        f_okazii_sold = executor.submit(OkaziiSource.fetch_sold, nominal, target_year, country)
        
        output_payload["baselines"]["ngc"] = f_ngc.result()
        output_payload["baselines"]["numista"] = f_numista.result()
        combined_active = f_mashops.result() + f_ebay_active.result() + f_okazii_active.result()
        combined_sold = f_ebay_sold.result() + f_okazii_sold.result()

    def process_and_dedupe(listings_list):
        seen_ids, seen_images, deduped = set(), set(), []
        listings_list.sort(key=lambda t: 1 if any(w in t['info'].lower() for w in ['coin', 'romania', 'silver']) else 2)
        
        for item in listings_list:
            id_str = item['item_url']
            if 'ebay.' in id_str and '/itm/' in id_str:
                m = re.search(r'/itm/(\d+)', id_str)
                if m: id_str = 'ebay_' + m.group(1)
            elif 'okazii.ro' in id_str:
                m = re.search(r'-a(\d+)', id_str)
                if m: id_str = 'okazii_' + m.group(1)
            
            img_hash = None
            if 'ebayimg.com/images/g/' in item['image_url']:
                m = re.search(r'/images/g/([^/]+)/', item['image_url'])
                if m: img_hash = m.group(1)
                
            if id_str not in seen_ids and (not img_hash or img_hash not in seen_images):
                seen_ids.add(id_str)
                if img_hash: seen_images.add(img_hash)
                item['grade'] = normalize_market_grade(item['grade'])
                deduped.append(item)
        return sorted(deduped, key=lambda x: x['price_usd'])

    output_payload["active_listings"] = process_and_dedupe(combined_active)
    output_payload["sold_listings"] = process_and_dedupe(combined_sold)
    
    for key, lst in [("active", output_payload["active_listings"]), ("sold", output_payload["sold_listings"])]:
        if lst:
            prices = [item["price_usd"] for item in lst]
            output_payload["metrics"][key] = {
                "median": round(statistics.median(prices), 2),
                "min": round(min(prices), 2),
                "max": round(max(prices), 2),
                "supply": len(lst)
            }

    with open("market_data.json", "w", encoding="utf-8") as f:
        json.dump(output_payload, f, indent=4)
        
    print(f"{Colors.GREEN}[+] Unified output securely flushed to market_data.json{Colors.RESET}\n")
    return output_payload

if __name__ == "__main__":
    try:
        user_country = input(f"{Colors.BOLD}🌍 [?] NGC Country (e.g. Romania) : {Colors.RESET}").strip()
        user_km = input(f"{Colors.BOLD}🪙  [?] NGC KM# (e.g. 17.1)        : {Colors.RESET}").strip()
        user_nom = input(f"{Colors.BOLD}💰 [?] Nominal (e.g. 5 Lei)       : {Colors.RESET}").strip()
        user_year = input(f"{Colors.BOLD}📅 [?] Target Year (e.g. 1881)    : {Colors.RESET}").strip()
    except KeyboardInterrupt: sys.exit(0)
        
    orchestrate_market_scan(user_country, user_km.upper().replace("KM", "").strip(), user_year, user_nom)