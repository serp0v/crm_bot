import requests
import logging
from typing import List, Dict, Optional
from bs4 import BeautifulSoup
from config import Config

logger = logging.getLogger(__name__)

class CRMParser:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(Config.HEADERS)
        self.is_logged_in = False
    
    def login(self) -> bool:
        """Авторизация в Yii2 CRM"""
        try:
            logger.info("Попытка авторизации в CRM...")
            
            # 1. Получаем страницу логина для CSRF-токена
            login_url = f"{Config.CRM_BASE_URL}/admin/login"
            response = self.session.get(login_url, timeout=30)
            
            if response.status_code != 200:
                logger.error(f"Ошибка загрузки страницы логина: {response.status_code}")
                return False
            
            # 2. Ищем CSRF-токен (Yii2 обычно в meta-теге или input)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Вариант 1: Ищем в meta-теге
            csrf_meta = soup.find('meta', {'name': 'csrf-token'})
            # Вариант 2: Ищем в input
            csrf_input = soup.find('input', {'name': '_csrf-frontend'})
            
            csrf_token = ""
            if csrf_meta:
                csrf_token = csrf_meta.get('content', '')
            elif csrf_input:
                csrf_token = csrf_input.get('value', '')
            
            if not csrf_token:
                logger.warning("CSRF-токен не найден, пробуем без него")
            
            # 3. Формируем данные для входа
            login_data = {
                'LoginForm[username]': Config.CRM_LOGIN,
                'LoginForm[password]': Config.CRM_PASSWORD,
                '_csrf-frontend': csrf_token,
                'LoginForm[rememberMe]': '0',  # Обычно в Yii2 есть этот параметр
            }
            
            # 4. Отправляем запрос на авторизацию
            response = self.session.post(
                login_url,
                data=login_data,
                allow_redirects=True,
                timeout=30
            )
            
            # 5. Проверяем успешность по редиректу или содержимому
            if response.status_code == 200:
                # Проверяем, что мы не на странице логина
                if "login" not in response.url.lower():
                    self.is_logged_in = True
                    logger.info("Успешная авторизация в CRM")
                    return True
                else:
                    # Пробуем найти ошибку на странице
                    soup = BeautifulSoup(response.text, 'html.parser')
                    error_div = soup.find('div', class_='help-block-error')
                    if error_div:
                        logger.error(f"Ошибка авторизации: {error_div.get_text(strip=True)}")
                    else:
                        logger.error("Не удалось авторизоваться - остались на странице логина")
                    return False
            else:
                logger.error(f"Ошибка HTTP при авторизации: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Неожиданная ошибка при авторизации: {e}")
            return False
    
    def get_requests_page(self, page: int = 1) -> Optional[str]:
        """Получаем HTML страницу с заявками"""
        try:
            params = {
                'page': page,
                # Добавьте другие параметры, если нужны
            }
            
            response = self.session.get(
                Config.CRM_REQUESTS_URL,
                params=params,
                timeout=30
            )
            
            if response.status_code == 200:
                return response.text
            else:
                logger.error(f"Ошибка при загрузке страницы {page}: {response.status_code}")
                return None
                
        except requests.RequestException as e:
            logger.error(f"Сетевая ошибка при загрузке страницы {page}: {e}")
            return None
    
    def parse_requests_from_html(self, html: str) -> List[Dict]:
        """Парсим заявки из HTML"""
        requests_found = []
        
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Ищем все строки таблицы с классом bg-status-awaitOnly
            rows = soup.find_all('tr', class_=lambda x: x and 'bg-status-awaitOnly' in x)
            
            logger.info(f"Найдено строк с заявками: {len(rows)}")
            
            for row in rows:
                request_data = self._parse_request_row(row)
                if request_data:
                    requests_found.append(request_data)
            
            return requests_found
            
        except Exception as e:
            logger.error(f"Ошибка при парсинке HTML: {e}")
            return []
    
    def _parse_request_row(self, row) -> Optional[Dict]:
        """Парсим одну строку с заявкой"""
        try:
            # Находим ссылку с ID заявки
            link = row.find('a', href=lambda x: x and 'customer-request/update' in x)
            if not link:
                return None
            
            # Извлекаем ID из текста ссылки
            link_text = link.get_text(strip=True)
            request_id = int(link_text.split()[0])
            
            # Извлекаем все ячейки
            cells = row.find_all('td')
            
            # Формируем данные заявки
            request_data = {
                'id': request_id,
                'date': cells[1].get_text(strip=True) if len(cells) > 1 else "",
                'type': cells[2].get_text(strip=True) if len(cells) > 2 else "",
                'status': cells[3].get_text(strip=True) if len(cells) > 3 else "",
                'city': cells[4].get_text(strip=True) if len(cells) > 4 else "",
                'phone': cells[5].get_text(strip=True) if len(cells) > 5 else "",
                'address': cells[6].get_text(strip=True) if len(cells) > 6 else "",
                'created_at': cells[7].get_text(strip=True) if len(cells) > 7 else "",
                'client_name': cells[9].get_text(strip=True) if len(cells) > 9 else "",
                'url': f"{Config.CRM_BASE_URL}{link['href']}"
            }
            
            return request_data
            
        except Exception as e:
            logger.error(f"Ошибка при парсинке строки заявки: {e}")
            return None
    
    def find_all_awaiting_calls(self) -> List[Dict]:
        """Находим все заявки на прозвоне на всех страницах"""
        all_requests = []
        
        if not self.is_logged_in and not self.login():
            logger.error("Не удалось авторизоваться в CRM")
            return all_requests
        
        for page in range(1, Config.MAX_PAGES + 1):
            logger.info(f"Проверяем страницу {page}")
            
            html = self.get_requests_page(page)
            if not html:
                break
            
            page_requests = self.parse_requests_from_html(html)
            all_requests.extend(page_requests)
            
            # Если на странице меньше 30 заявок, значит это последняя страница
            if len(page_requests) < 30:
                break
        
        logger.info(f"Всего найдено заявок на прозвоне: {len(all_requests)}")
        return all_requests
