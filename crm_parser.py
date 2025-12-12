import requests
import logging
import re
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
            
            login_url = f"{Config.CRM_BASE_URL}/admin/login"
            response = self.session.get(login_url, timeout=30)
            
            if response.status_code != 200:
                logger.error(f"Ошибка загрузки страницы логина: {response.status_code}")
                return False
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # CSRF токен
            csrf_input = soup.find('input', {'name': '_csrf-frontend'})
            csrf_token = csrf_input.get('value', '') if csrf_input else ""
            
            # Данные для входа
            login_data = {
                'LoginForm[email]': Config.CRM_LOGIN,
                'LoginForm[password]': Config.CRM_PASSWORD,
                '_csrf-frontend': csrf_token,
                'LoginForm[rememberMe]': '1',
            }
            
            # Авторизация
            response = self.session.post(
                login_url,
                data=login_data,
                allow_redirects=True,
                timeout=30
            )
            
            if response.status_code == 200 and "login" not in response.url.lower():
                self.is_logged_in = True
                logger.info("Успешная авторизация в CRM")
                return True
            else:
                logger.error("Ошибка авторизации")
                return False
                
        except Exception as e:
            logger.error(f"Ошибка при авторизации: {e}")
            return False
    
    def extract_scheduled_time(self, cell) -> str:
        """Извлекаем запланированное время из ячейки"""
        if not cell:
            return ""
        
        # Пытаемся извлечь из title атрибута
        time_span = cell.find('span')
        if time_span and time_span.get('title'):
            title = time_span['title']
            # Ищем время в формате "Назначено в: 08:00"
            match = re.search(r'Назначено в:\s*(\d{1,2}:\d{2})', title)
            if match:
                return match.group(1)
        
        # Если в title нет, берем текст span
        if time_span:
            text = time_span.get_text(strip=True)
            # Ищем дату и время в тексте
            match = re.search(r'\d{2}\.\d{2}\.\d{2}\s+\d{1,2}:\d{2}', text)
            if match:
                return match.group(0)
        
        return cell.get_text(strip=True)
    
    def parse_requests_from_html(self, html: str) -> List[Dict]:
        """Парсим заявки из HTML"""
        requests_found = []
        
        try:
            soup = BeautifulSoup(html, 'html.parser')
            
            # Ищем все строки с классом bg-status-awaitOnly
            rows = soup.find_all('tr', class_=lambda x: x and 'bg-status-awaitOnly' in x)
            
            logger.info(f"Найдено строк с заявками: {len(rows)}")
            
            for row in rows:
                request_data = self._parse_request_row(row)
                if request_data:
                    requests_found.append(request_data)
            
            return requests_found
            
        except Exception as e:
            logger.error(f"Ошибка при парсинге HTML: {e}")
            return []
    
    def _parse_request_row(self, row) -> Optional[Dict]:
        """Парсим одну строку с заявкой"""
        try:
            # ID заявки
            link = row.find('a', href=lambda x: x and 'customer-request/update' in x)
            if not link:
                return None
            
            link_text = link.get_text(strip=True)
            request_id = int(link_text.split()[0])
            
            # Все ячейки
            cells = row.find_all('td')
            
            # Запланированное время (вторая ячейка)
            scheduled_time = ""
            if len(cells) > 1:
                scheduled_time = self.extract_scheduled_time(cells[1])
            
            # Проверка срочности (time-warning)
            is_urgent = False
            for td in cells:
                time_warning = td.find('div', class_='time-warning')
                if time_warning:
                    is_urgent = True
                    break
            
            # Проверка, в работе ли заявка
            row_classes = row.get('class', [])
            is_processing = any(cls in row_classes for cls in ['bg-is_processing_by', 'bg-is_processing_by_me'])
            
            # Формируем данные
            request_data = {
                'id': request_id,
                'scheduled_time': scheduled_time,
                'is_urgent': is_urgent,
                'is_processing': is_processing,
                'date': cells[1].get_text(strip=True) if len(cells) > 1 else "",
                'type': cells[2].get_text(strip=True) if len(cells) > 2 else "",
                'city': cells[4].get_text(strip=True) if len(cells) > 4 else "",
                'url': f"{Config.CRM_BASE_URL}{link['href']}"
            }
            
            return request_data
            
        except Exception as e:
            logger.error(f"Ошибка при парсинге строки: {e}")
            return None
    
    def get_requests_page(self, page: int = 1) -> Optional[str]:
        """Получаем HTML страницу с заявками"""
        try:
            params = {'page': page} if page > 1 else {}
            
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
                
        except Exception as e:
            logger.error(f"Ошибка при загрузке страницы {page}: {e}")
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
            
            if len(page_requests) < 30:
                break
        
        logger.info(f"Всего найдено заявок на прозвоне: {len(all_requests)}")
        urgent_count = sum(1 for r in all_requests if r.get('is_urgent', False))
        logger.info(f"Из них срочных: {urgent_count}")
        
        return all_requests
