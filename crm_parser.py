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
        """Авторизация в CRM"""
        try:
            logger.info("Попытка авторизации в CRM...")
            
            # 1. Получаем страницу логина (если нужен CSRF токен)
            response = self.session.get(Config.CRM_LOGIN_URL, timeout=30)
            
            # 2. Парсим форму логина (адаптируйте под вашу CRM)
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Ищем CSRF токен если он есть
            csrf_token = ""
            csrf_input = soup.find('input', {'name': '_csrf'}) or \
                        soup.find('input', {'name': 'csrf_token'}) or \
                        soup.find('input', {'name': 'YII_CSRF_TOKEN'})
            
            if csrf_input:
                csrf_token = csrf_input.get('value', '')
            
            # 3. Формируем данные для входа
            login_data = {
                'login': Config.CRM_LOGIN,
                'password': Config.CRM_PASSWORD,
            }
            
            if csrf_token:
                login_data['_csrf'] = csrf_token
                login_data['csrf_token'] = csrf_token
                login_data['YII_CSRF_TOKEN'] = csrf_token
            
            # 4. Отправляем запрос на авторизацию
            response = self.session.post(
                Config.CRM_LOGIN_URL,
                data=login_data,
                allow_redirects=True,
                timeout=30
            )
            
            # 5. Проверяем успешность авторизации
            if response.status_code == 200:
                # Проверяем, что мы на нужной странице (не на странице логина)
                if "login" not in response.url.lower():
                    self.is_logged_in = True
                    logger.info("Успешная авторизация в CRM")
                    return True
                else:
                    logger.error("Не удалось авторизоваться - остались на странице логина")
                    return False
            else:
                logger.error(f"Ошибка HTTP при авторизации: {response.status_code}")
                return False
                
        except requests.RequestException as e:
            logger.error(f"Сетевая ошибка при авторизации: {e}")
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