import requests
import logging
import re
from typing import List, Dict, Optional
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
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
    
    def _get_utc_offset_for_city(self, city: str) -> Optional[int]:
        """Возвращает смещение в часах от UTC для города/региона внутри России.

        Если город не опознан — возвращает None.
        """
        if not city:
            return None

        name = city.strip().lower()

        # Простейшая таблица соответствий (можно расширять по необходимости)
        mapping = {
            # Калининград
            'калининград': 2,
            # Москва и запад/центр
            'москва': 3,
            'московская область': 3,
            # Самара
            'самара': 4,
            # Екатеринбург
            'екатеринбург': 5,
            'свердловская область': 5,
            # Омск
            'омск': 6,
            # Красноярск
            'красноярск': 7,
            # Иркутск
            'иркутск': 8,
            # Якутск
            'якутск': 9,
            # Владивосток
            'владивосток': 10,
            # Магадан
            'магадан': 11,
            # Петропавловск-Камчатский
            'петропавловск-камчатский': 12,
        }

        # Попытка точного совпадения
        for k, v in mapping.items():
            if k in name:
                return v

        return None

    def _convert_utc_to_local(self, time_str: str, offset_hours: Optional[int]) -> str:
        """Конвертирует время-строку `HH:MM` из UTC в локальное время, добавляя `offset_hours`.

        Если offset_hours is None — возвращает исходную строк.
        """
        if offset_hours is None:
            return time_str

        try:
            match = re.search(r'(\d{1,2}):(\d{2})', time_str)
            if not match:
                return time_str

            hour = int(match.group(1))
            minute = int(match.group(2))

            time_obj = datetime.strptime(f"{hour:02d}:{minute:02d}", "%H:%M")
            time_obj += timedelta(hours=offset_hours)

            return time_obj.strftime("%H:%M")
        except Exception as e:
            logger.error(f"Ошибка конверсии времени '{time_str}': {e}")
            return time_str
    
    def extract_scheduled_time(self, cell, city: Optional[str] = None) -> str:
        """Извлекаем запланированное время из ячейки и возвращаем в формате 'YYYY-MM-DD HH:MM'.

        Логика:
        - Пытаемся найти видимую дату+время, например: "12.12.2025 19:00" или "12.12.25 19:00".
        - Если видна только метка с временем в title (в UTC), пытаемся взять дату из общего текста ячейки.
        - При наличии смещения для города конвертируем время из UTC в локальное.
        - Если ничего не найдено — возвращаем пустую строку.
        """
        if not cell:
            return ""

        full_text = cell.get_text(" ", strip=True)

        # Попытка найти дату и время в тексте: dd.mm.yyyy HH:MM или dd.mm.yy HH:MM
        m = re.search(r'(\d{1,2}\.\d{1,2}\.\d{2,4})\s+(\d{1,2}:\d{2})', full_text)
        if m:
            date_str = m.group(1)
            time_str = m.group(2)
            # Парсим дату с учётом двух- или четырёхзначного года
            for fmt in ('%d.%m.%Y %H:%M', '%d.%m.%y %H:%M'):
                try:
                    dt = datetime.strptime(f"{date_str} {time_str}", fmt)
                    return dt.strftime('%Y-%m-%d %H:%M')
                except Exception:
                    continue

        # Если в видимом тексте нет даты+времени, посмотрим в span title (например: "Назначено в: 08:00")
        time_span = cell.find('span')
        if time_span and time_span.get('title'):
            title = time_span['title']
            tm = re.search(r'Назначено в:\s*(\d{1,2}:\d{2})', title)
            if tm:
                time_only = tm.group(1)
                # Попытка достать дату из полного текста ячейки
                mdate = re.search(r'(\d{1,2}\.\d{1,2}\.\d{2,4})', full_text)
                if mdate:
                    date_str = mdate.group(1)
                    for fmt in ('%d.%m.%Y %H:%M', '%d.%m.%y %H:%M'):
                        try:
                            dt = datetime.strptime(f"{date_str} {time_only}", fmt)
                            offset = self._get_utc_offset_for_city(city or "")
                            if offset is not None:
                                dt = dt + timedelta(hours=offset)
                            return dt.strftime('%Y-%m-%d %H:%M')
                        except Exception:
                            continue
                else:
                    # Нет даты в тексте — возьмём сегодняшнюю дату как последнюю попытку
                    try:
                        today = datetime.now()
                        dt = datetime.strptime(f"{today.strftime('%d.%m.%Y')} {time_only}", '%d.%m.%Y %H:%M')
                        offset = self._get_utc_offset_for_city(city or "")
                        if offset is not None:
                            dt = dt + timedelta(hours=offset)
                        return dt.strftime('%Y-%m-%d %H:%M')
                    except Exception:
                        return ""

        # Если нашли только время в span или тексте — пытаемся извлечь время и объединить с датой из текста
        tm2 = re.search(r'(\d{1,2}:\d{2})', full_text)
        if tm2:
            time_only = tm2.group(1)
            mdate = re.search(r'(\d{1,2}\.\d{1,2}\.\d{2,4})', full_text)
            if mdate:
                date_str = mdate.group(1)
                for fmt in ('%d.%m.%Y %H:%M', '%d.%m.%y %H:%M'):
                    try:
                        dt = datetime.strptime(f"{date_str} {time_only}", fmt)
                        return dt.strftime('%Y-%m-%d %H:%M')
                    except Exception:
                        continue

        return ""
    
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
            
            # Город (пятая ячейка) — используем его для определения часового пояса
            city_text = cells[4].get_text(strip=True) if len(cells) > 4 else ""

            # Запланированное время (вторая ячейка) — передаём город для корректной конверсии
            scheduled_time = ""
            if len(cells) > 1:
                scheduled_time = self.extract_scheduled_time(cells[1], city_text)
            
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
                'city': city_text,
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

    def parse_partner_alerts_from_html(self, html: str) -> List[Dict]:
        """Парсим оповещения партнёров (строки с классом 'bg-partner-alert')."""
        alerts = []
        try:
            soup = BeautifulSoup(html, 'html.parser')
            rows = soup.find_all('tr', class_=lambda x: x and 'bg-partner-alert' in x)
            logger.info(f"Найдено partner-alert строк: {len(rows)}")
            for row in rows:
                data = self._parse_request_row(row)
                if data:
                    # Пометим явно тип
                    data['partner_alert'] = True
                    alerts.append(data)
        except Exception as e:
            logger.error(f"Ошибка при парсинге partner alerts: {e}")
        return alerts

    def find_partner_alerts(self) -> List[Dict]:
        """Находим оповещения партнёров на всех страницах. Требует авторизации."""
        all_alerts = []
        if not self.is_logged_in and not self.login():
            logger.error("Не удалось авторизоваться в CRM для partner alerts")
            return all_alerts

        for page in range(1, Config.MAX_PAGES + 1):
            html = self.get_requests_page(page)
            if not html:
                break
            alerts = self.parse_partner_alerts_from_html(html)
            all_alerts.extend(alerts)
            # Если на странице мало строк — вероятно это последняя
            if len(alerts) < 30:
                break

        logger.info(f"Всего найдено partner alerts: {len(all_alerts)}")
        return all_alerts
    
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
