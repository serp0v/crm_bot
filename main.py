import asyncio
import logging
import json
from datetime import datetime
from typing import List, Dict

from config import Config
from database import Database
from crm_parser import CRMParser
from telegram_notifier import TelegramNotifier

# Настройка логирования
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
        self.last_send_time = None
        self.is_running = True
        
        logger.info("CRM Telegram Bot инициализирован")
    
    async def check_for_new_requests(self) -> List[Dict]:
        """Проверяем новые заявки в CRM и возвращаем все заявки на прозвоне"""
        logger.info("Запуск проверки новых заявок...")
        
        try:
            # Получаем все заявки на прозвоне
            all_requests = self.crm_parser.find_all_awaiting_calls()
            
            new_requests_count = 0
            for request_data in all_requests:
                request_id = request_data['id']
                
                # Проверяем, есть ли уже такая заявка в БД
                if not self.db.request_exists(request_id):
                    # Сохраняем в БД
                    data_json = json.dumps(request_data, ensure_ascii=False)
                    is_urgent = request_data.get('is_urgent', False)
                    
                    if self.db.add_request(request_id, data_json, is_urgent):
                        new_requests_count += 1
                        urgency = " (СРОЧНАЯ)" if is_urgent else ""
                        logger.info(f"Обнаружена новая заявка: {request_id}{urgency}")
            
            if new_requests_count > 0:
                logger.info(f"Найдено новых заявок: {new_requests_count}")
            else:
                logger.info("Новых заявок не найдено")
            
            return all_requests
                
        except Exception as e:
            logger.error(f"Ошибка при проверке заявок: {e}")
            return []
    
    async def send_batch_if_needed(self, all_requests: List[Dict]):
        """Отправляем пачку если пришло время"""
        if not all_requests:
            logger.debug("Нет заявок для отправки")
            return
        
        # Проверяем, не пора ли отправить срочные заявки отдельно
        urgent_requests = [r for r in all_requests if r.get('is_urgent', False)]
        if urgent_requests:
            logger.info(f"Найдено {len(urgent_requests)} срочных заявок для повторной отправки")
            batch_number = self.db.get_next_batch_number()
            successful_ids = await self.telegram_notifier.send_batch(
                urgent_requests, 
                batch_number, 
                is_urgent_only=True
            )
            if successful_ids:
                self.db.mark_as_sent(successful_ids, batch_number)
        
        # Проверяем, наступило ли время отправки основной пачки
        if not self.telegram_notifier.should_send_now():
            return
        
        # Если время отправки наступило
        logger.info("Время отправки основной пачки!")
        
        # Получаем следующий номер пачки
        batch_number = self.db.get_next_batch_number()
        
        # Отправляем все заявки
        successful_ids = await self.telegram_notifier.send_batch(all_requests, batch_number)
        
        if successful_ids:
            # Отмечаем как отправленные
            self.db.mark_as_sent(successful_ids, batch_number)
            
            # Сохраняем время последней отправки
            self.last_send_time = datetime.now()
            
            logger.info(f"Основная пачка #{batch_number} успешно отправлена")
        else:
            logger.error("Не удалось отправить основную пачку")
    
    async def run(self):
        """Основной цикл работы бота"""
        logger.info("Запуск основного цикла бота...")
        
        try:
            while self.is_running:
                # Проверяем новые заявки
                all_requests = await self.check_for_new_requests()
                
                # Проверяем, не пора ли отправить пачку
                await self.send_batch_if_needed(all_requests)
                
                # Рассчитываем время до следующей проверки
                wait_minutes = self.telegram_notifier.get_minutes_to_next_send()
                # Не ждем больше 5 минут между проверками
                wait_minutes = min(wait_minutes, 5)
                
                logger.info(f"Следующая проверка через {wait_minutes} минут")
                
                # Ждем до следующей проверки (в секундах)
                await asyncio.sleep(wait_minutes * 60)
                
        except KeyboardInterrupt:
            logger.info("Остановка по запросу пользователя")
            self.is_running = False
        except Exception as e:
            logger.error(f"Критическая ошибка в основном цикле: {e}")
            self.is_running = False
    
    def stop(self):
        """Остановка бота"""
        self.is_running = False
        logger.info("Бот остановлен")

async def main():
    """Точка входа в приложение"""
    bot = CRMTelegramBot()
    
    try:
        await bot.run()
    except Exception as e:
        logger.error(f"Фатальная ошибка: {e}")
    finally:
        bot.stop()

if __name__ == "__main__":
    # Создаем папку для логов если её нет
    import os
    os.makedirs("logs", exist_ok=True)
    
    # Запускаем бота
    asyncio.run(main())
