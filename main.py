import asyncio
import logging
from datetime import datetime
from typing import List, Dict

from config import Config
from database import Database
from crm_parser import CRMParser
from telegram_notifier import TelegramNotifier

# Логирование
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/crm_bot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CRMTelegramBot:
    def __init__(self):
        self.db = Database()
        self.crm_parser = CRMParser()
        self.telegram_notifier = TelegramNotifier()
        self.is_running = True
        
        logger.info("CRM Telegram Bot инициализирован")
    
    async def process_requests_batch(self) -> Dict[str, List[Dict]]:
        """Обрабатываем все найденные заявки"""
        logger.info("Запуск проверки заявок...")
        
        # Получаем заявки из CRM
        all_requests = self.crm_parser.find_all_awaiting_calls()
        
        regular_to_send = []
        urgent_to_send = []
        
        # Обрабатываем каждую заявку
        for request_data in all_requests:
            # Пропускаем заявки в работе
            if request_data.get('is_processing', False):
                continue
            
            # Добавляем/обновляем в базе
            is_new_task, should_send = self.db.add_or_update_task(request_data)
            
            if should_send:
                if request_data.get('is_urgent', False):
                    urgent_to_send.append(request_data)
                else:
                    regular_to_send.append(request_data)
        
        # Очищаем старые задачи (раз в сутки)
        if datetime.now().hour == 0 and datetime.now().minute < 5:
            self.db.cleanup_old_tasks(hours_old=24)
        
        return {
            'regular': regular_to_send,
            'urgent': urgent_to_send,
            'all': all_requests
        }
    
    async def send_batch_if_needed(self, requests_data: Dict[str, List[Dict]]):
        """Отправляем пачки если нужно"""
        
        # Всегда отправляем срочные заявки
        if requests_data['urgent']:
            logger.info(f"Найдено {len(requests_data['urgent'])} срочных заявок")
            batch_number = self.db.get_next_batch_number()
            
            if await self.telegram_notifier.send_batch(requests_data['urgent'], batch_number, is_urgent_only=True):
                # Отмечаем как отправленные
                for req in requests_data['urgent']:
                    self.db.mark_as_sent(req['id'], req.get('scheduled_time', ''), batch_number)
        
        # Проверяем время для обычных пачек
        if self.telegram_notifier.should_send_now() and requests_data['regular']:
            logger.info(f"Время отправки! Найдено {len(requests_data['regular'])} обычных заявок")
            batch_number = self.db.get_next_batch_number()
            
            if await self.telegram_notifier.send_batch(requests_data['regular'], batch_number):
                # Отмечаем как отправленные
                for req in requests_data['regular']:
                    self.db.mark_as_sent(req['id'], req.get('scheduled_time', ''), batch_number)
    
    async def run(self):
        """Основной цикл работы"""
        logger.info("Запуск основного цикла бота...")
        
        try:
            while self.is_running:
                # Обрабатываем заявки
                requests_data = await self.process_requests_batch()
                
                # Отправляем если нужно
                await self.send_batch_if_needed(requests_data)
                
                # Ждем до следующей проверки
                wait_minutes = self.telegram_notifier.get_minutes_to_next_send()
                wait_minutes = min(wait_minutes, 1)  # Не больше 5 минут
                
                logger.info(f"Следующая проверка через {wait_minutes} минут")
                await asyncio.sleep(wait_minutes * 60)
                
        except KeyboardInterrupt:
            logger.info("Остановка по запросу пользователя")
            self.is_running = False
        except Exception as e:
            logger.error(f"Критическая ошибка: {e}")
            self.is_running = False
    
    def stop(self):
        """Остановка бота"""
        self.is_running = False
        logger.info("Бот остановлен")

async def main():
    bot = CRMTelegramBot()
    try:
        await bot.run()
    except Exception as e:
        logger.error(f"Фатальная ошибка: {e}")
    finally:
        bot.stop()

if __name__ == "__main__":
    import os
    os.makedirs("logs", exist_ok=True)
    asyncio.run(main())

