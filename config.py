import os
from dotenv import load_dotenv

load_dotenv()

class Config:
    # CRM
    CRM_BASE_URL = os.getenv("CRM_BASE_URL", "https://kp-lead-centre.ru")
    CRM_REQUESTS_URL = f"{CRM_BASE_URL}/admin/domain/customer-request/index?__view-mode=chats"
    
    # Авторизация
    CRM_LOGIN = os.getenv("CRM_LOGIN")
    CRM_PASSWORD = os.getenv("CRM_PASSWORD")
    
    # Telegram
    TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
    TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
    
    # Настройки
    MAX_PAGES = int(os.getenv("MAX_PAGES", 5))
    DB_PATH = os.getenv("DB_PATH", "crm_requests.db")
    
    # Заголовки
    HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    }
