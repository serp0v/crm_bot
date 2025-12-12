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
    
    async def process_requests(self) -> Dict[str, List[Dict]]:
        """Обрабатываем найденные заявки и возвращаем списки для отправки"""
        logger.info("Запуск обработки заявок...")
        
        # Получаем заявки из CRM
        all_requests = self.crm_parser.find_all_awaiting_calls()
        
        regular_to_send = []  # Обычные заявки для отправки по расписанию
        urgent_to_send = []   # Срочные заявки для немедленной отправки
        
        for request_data in all_requests:
            # Определяем, нужно ли отправлять эту заявку
            should_send = self.db.add_or_update_task(request_data)
            
            if should_send:
                if request_data.get('is_urgent', False):
                    urgent_to_send.append(request_data)
                else:
                    regular_to_send.append(request_data)
        
        logger.info(f"Найдено заявок: {len(all_requests)}")
        logger.info(f"Для отправки: {len(regular_to_send)} обычных, {len(urgent_to_send)} срочных")
        
        return {
            'regular': regular_to_send,
            'urgent': urgent_to_send,
            'all': all_requests
        }
    
    async def send_requests(self, requests_data: Dict[str, List[Dict]]):
        """Отправляем заявки согласно логике"""
        current_time = datetime.now()
        current_minute = current_time.minute
        
        # Определяем время отправки
        send_minutes = [1, 5, 11, 16, 21, 26, 31]
        should_send_scheduled = current_minute in send_minutes
        
        logger.info(f"Текущее время: {current_time.strftime('%H:%M')} (минута: {current_minute})")
        logger.info(f"Время отправки по расписанию: {should_send_scheduled}")
        
        # ВСЕГДА отправляем срочные заявки (но только если они есть)
        if requests_data['urgent']:
            logger.info(f"Отправка {len(requests_data['urgent'])} срочных заявок")
            batch_number = self.db.get_next_batch_number()
            
            success = await self.telegram_notifier.send_batch(
                requests_data['urgent'], 
                batch_number, 
                is_urgent=True
            )
            
            if success:
                for req in requests_data['urgent']:
                    self.db.mark_as_sent(req['id'], req.get('scheduled_time', ''), batch_number)
        
        # Обычные заявки отправляем только по расписанию
        if should_send_scheduled and requests_data['regular']:
            logger.info(f"Отправка {len(requests_data['regular'])} обычных заявок по расписанию")
            batch_number = self.db.get_next_batch_number()
            
            success = await self.telegram_notifier.send_batch(
                requests_data['regular'], 
                batch_number, 
                is_urgent=False
            )
            
            if success:
                for req in requests_data['regular']:
                    self.db.mark_as_sent(req['id'], req.get('scheduled_time', ''), batch_number)
    
    async def run(self):
        """Основной цикл работы"""
        logger.info("Запуск основного цикла бота...")
        
        try:
            while self.is_running:
                # Обрабатываем заявки
                requests_data = await self.process_requests()
                
                # Отправляем если нужно
                await self.send_requests(requests_data)
                
                # Ждём 1 минуту до следующей проверки
                logger.info("Ожидание 1 минуту до следующей проверки...")
                await asyncio.sleep(60)
                
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
