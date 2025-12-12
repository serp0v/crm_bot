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
        """Авторизация в Yii2 CRM с двухэтапным входом"""
        try:
            logger.info("=== НАЧАЛО ПРОЦЕССА АВТОРИЗАЦИИ ===")
            
            # ШАГ 1: Получаем главную страницу для нахождения кнопки входа
            logger.info(f"1. Загружаем главную страницу: {Config.CRM_BASE_URL}/")
            main_response = self.session.get(Config.CRM_BASE_URL + "/", timeout=30)
            logger.debug(f"   Статус: {main_response.status_code}, Размер: {len(main_response.text)} символов")
            
            if main_response.status_code != 200:
                logger.error(f"   Ошибка загрузки главной страницы: {main_response.status_code}")
                return False
            
            # Ищем ссылку на административную панель
            soup = BeautifulSoup(main_response.text, 'html.parser')
            admin_link = soup.find('a', {'href': '/admin'})
            
            if not admin_link:
                logger.warning("   Ссылка '/admin' не найдена на главной странице")
                logger.debug(f"   Содержимое страницы (первые 500 символов): {main_response.text[:500]}")
                # Пробуем прямой переход
                login_url = f"{Config.CRM_BASE_URL}/admin/login"
            else:
                # ШАГ 2: Переходим по ссылке входа
                admin_url = f"{Config.CRM_BASE_URL}/admin"
                logger.info(f"2. Переходим по ссылке входа: {admin_url}")
                admin_response = self.session.get(admin_url, timeout=30)
                logger.debug(f"   Статус: {admin_response.status_code}, Конечный URL: {admin_response.url}")
                
                # Проверяем, не перенаправило ли сразу на логин
                if "login" in admin_response.url.lower():
                    login_url = admin_response.url
                else:
                    login_url = f"{Config.CRM_BASE_URL}/admin/login"
            
            # ШАГ 3: Загружаем страницу логина для CSRF-токена
            logger.info(f"3. Загружаем страницу логина: {login_url}")
            login_response = self.session.get(login_url, timeout=30)
            logger.debug(f"   Статус: {login_response.status_code}, Конечный URL: {login_response.url}")
            
            if login_response.status_code != 200:
                logger.error(f"   Ошибка загрузки страницы логина: {login_response.status_code}")
                logger.debug(f"   Заголовки ответа: {dict(login_response.headers)}")
                return False
            
            # Анализируем страницу логина
            soup = BeautifulSoup(login_response.text, 'html.parser')
            
            # Ищем CSRF-токен разными способами
            csrf_token = ""
            csrf_meta = soup.find('meta', {'name': 'csrf-token'})
            csrf_input = soup.find('input', {'name': '_csrf-frontend'})
            csrf_param = soup.find('input', {'name': '_csrf'})
            
            if csrf_meta:
                csrf_token = csrf_meta.get('content', '')
                logger.debug(f"   Найден CSRF в meta-теге: {csrf_token[:20]}...")
            elif csrf_input:
                csrf_token = csrf_input.get('value', '')
                logger.debug(f"   Найден CSRF в input: {csrf_token[:20]}...")
            elif csrf_param:
                csrf_token = csrf_param.get('value', '')
                logger.debug(f"   Найден CSRF как _csrf: {csrf_token[:20]}...")
            else:
                logger.warning("   CSRF-токен не найден стандартными методами")
                # Пробуем найти через JavaScript или другие атрибуты
                all_metas = soup.find_all('meta')
                for meta in all_metas:
                    if 'csrf' in str(meta).lower():
                        csrf_token = meta.get('content', '')
                        logger.debug(f"   Найден CSRF в другом meta: {csrf_token[:20]}...")
                        break
            
            # Ищем форму логина и все её поля
            forms = soup.find_all('form')
            logger.info(f"   Найдено форм на странице: {len(forms)}")
            
            for i, form in enumerate(forms):
                logger.debug(f"   Форма #{i+1}: action='{form.get('action', '')}', method='{form.get('method', 'GET')}'")
            
            # Определяем, какая форма для логина (обычно содержит поля username/password)
            login_form = None
            for form in forms:
                inputs = form.find_all('input')
                input_names = [inp.get('name', '').lower() for inp in inputs]
                if any('username' in name or 'login' in name for name in input_names):
                    login_form = form
                    break
            
            if not login_form:
                logger.warning("   Форма логина не найдена по стандартным признакам")
                # Берём первую форму
                login_form = forms[0] if forms else None
            
            # Собираем все поля формы
            form_data = {}
            if login_form:
                for inp in login_form.find_all('input'):
                    name = inp.get('name', '')
                    value = inp.get('value', '')
                    if name:  # Пропускаем кнопки без name
                        form_data[name] = value
                        logger.debug(f"   Поле формы: name='{name}', value='{value[:30]}...'")
            
            # ШАГ 4: Формируем данные для входа
            # Основные поля для Yii2
            login_data = {
                'LoginForm[email]': Config.CRM_LOGIN,
                'LoginForm[password]': Config.CRM_PASSWORD,
            }
            
            # Добавляем CSRF-токен, если нашли
            if csrf_token:
                login_data['_csrf-frontend'] = csrf_token
            
            # Добавляем остальные поля из формы (rememberMe и другие)
            for key, value in form_data.items():
                if key not in login_data and not key.startswith('LoginForm['):
                    login_data[key] = value
            
            # Убедимся, что rememberMe установлен (обычно 0 или 1)
            if 'LoginForm[rememberMe]' not in login_data:
                login_data['LoginForm[rememberMe]'] = '0'
            
            logger.info(f"4. Отправляем данные авторизации на {login_url}")
            logger.debug(f"   Данные для отправки: { {k: '***' if 'password' in k else v for k, v in login_data.items()} }")
            
            # ШАГ 5: Отправляем запрос на авторизацию
            auth_response = self.session.post(
                login_url,
                data=login_data,
                allow_redirects=True,
                timeout=30
            )
            
            logger.debug(f"   Статус после авторизации: {auth_response.status_code}")
            logger.debug(f"   Конечный URL: {auth_response.url}")
            logger.debug(f"   История редиректов: {[r.url for r in auth_response.history]}")
            logger.debug(f"   Количество кук в сессии: {len(self.session.cookies)}")
            
            # ШАГ 6: Проверяем успешность авторизации
            if auth_response.status_code == 200:
                # Проверяем различные признаки успешного входа
                
                # 1. По URL - если не на странице логина
                if "login" not in auth_response.url.lower():
                    self.is_logged_in = True
                    logger.info("✅ УСПЕШНАЯ АВТОРИЗАЦИЯ В CRM")
                    logger.info(f"   Перенаправлены на: {auth_response.url}")
                    
                    # Быстрая проверка доступности админки
                    test_admin = self.session.get(f"{Config.CRM_BASE_URL}/admin", timeout=10)
                    logger.debug(f"   Проверка доступа к /admin: {test_admin.status_code}")
                    
                    return True
                else:
                    # Мы остались на странице логина - ищем ошибки
                    logger.error("❌ НЕ УДАЛОСЬ АВТОРИЗОВАТЬСЯ")
                    logger.info("   Анализируем страницу на наличие ошибок...")
                    
                    soup = BeautifulSoup(auth_response.text, 'html.parser')
                    
                    # Ищем сообщения об ошибках (разные варианты)
                    error_selectors = [
                        {'class': 'help-block-error'},
                        {'class': 'error'},
                        {'class': 'alert-danger'},
                        {'class': 'has-error'},
                        {'class': 'invalid-feedback'},
                    ]
                    
                    errors_found = []
                    for selector in error_selectors:
                        error_elements = soup.find_all(attrs=selector)
                        for elem in error_elements:
                            error_text = elem.get_text(strip=True)
                            if error_text and error_text not in errors_found:
                                errors_found.append(error_text)
                    
                    if errors_found:
                        for error in errors_found:
                            logger.error(f"   Ошибка на странице: {error}")
                    else:
                        # Проверяем наличие полей формы (значит, форма снова показана)
                        form_count = len(soup.find_all('form'))
                        logger.info(f"   Форм на странице после отправки: {form_count}")
                        logger.debug(f"   Первые 1000 символов ответа: {auth_response.text[:1000]}")
                    
                    return False
            else:
                logger.error(f"   Ошибка HTTP при авторизации: {auth_response.status_code}")
                logger.debug(f"   Тело ответа (первые 500 символов): {auth_response.text[:500]}")
                return False
                
        except requests.RequestException as e:
            logger.error(f"Сетевая ошибка при авторизации: {e}")
            return False
        except Exception as e:
            logger.error(f"Неожиданная ошибка при авторизации: {e}", exc_info=True)
            return False
    
    def get_requests_page(self, page: int = 1) -> Optional[str]:
        """Получаем HTML страницу с заявками"""
        try:
            params = {
                'page': page,
                # Добавьте другие параметры, если нужны
            }
            
            logger.debug(f"Загружаем страницу {page} заявок с URL: {Config.CRM_REQUESTS_URL}")
            response = self.session.get(
                Config.CRM_REQUESTS_URL,
                params=params,
                timeout=30
            )
            
            if response.status_code == 200:
                logger.debug(f"Страница {page} успешно загружена, размер: {len(response.text)} символов")
                return response.text
            else:
                logger.error(f"Ошибка при загрузке страницы {page}: {response.status_code}")
                logger.debug(f"Заголовки ответа: {dict(response.headers)}")
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
            
            if rows:
                # Показать структуру первой строки для отладки
                first_row = rows[0]
                logger.debug(f"Структура первой строки: {first_row.prettify()[:500]}...")
            
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
                logger.debug("Строка не содержит ссылку на заявку")
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
            
            logger.debug(f"Распарсена заявка ID: {request_id}")
            return request_data
            
        except Exception as e:
            logger.error(f"Ошибка при парсинке строки заявки: {e}")
            logger.debug(f"Содержимое строки: {row.prettify()[:300]}")
            return None
    
    def find_all_awaiting_calls(self) -> List[Dict]:
        """Находим все заявки на прозвоне на всех страницах"""
        all_requests = []
        
        if not self.is_logged_in and not self.login():
            logger.error("Не удалось авторизоваться в CRM")
            return all_requests
        
        logger.info(f"Начинаем поиск заявок на прозвоне (макс. {Config.MAX_PAGES} страниц)")
        
        for page in range(1, Config.MAX_PAGES + 1):
            logger.info(f"Проверяем страницу {page}")
            
            html = self.get_requests_page(page)
            if not html:
                logger.warning(f"Не удалось загрузить страницу {page}, прекращаем поиск")
                break
            
            page_requests = self.parse_requests_from_html(html)
            all_requests.extend(page_requests)
            
            logger.info(f"На странице {page} найдено: {len(page_requests)} заявок")
            
            # Если на странице меньше 30 заявок, значит это последняя страница
            if len(page_requests) < 30:
                logger.info(f"На странице меньше 30 заявок ({len(page_requests)}), прекращаем поиск")
                break
        
        logger.info(f"Всего найдено заявок на прозвоне: {len(all_requests)}")
        
        if all_requests:
            logger.debug(f"ID найденных заявок: {[r['id'] for r in all_requests]}")
        
        return all_requests

