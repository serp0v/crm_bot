import asyncio
import logging
import time
import json
from datetime import datetime, timedelta
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
        self.last_send_time = datetime.now()
        self.is_running = True
        
        logger.info("CRM Telegram Bot инициализирован")
    
    async def check_for_new_requests(self):
        """Проверяем новые заявки в CRM"""
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
                    if self.db.add_request(request_id, data_json):
                        new_requests_count += 1
                        logger.info(f"Обнаружена новая заявка: {request_id}")
            
            if new_requests_count > 0:
                logger.info(f"Найдено новых заявок: {new_requests_count}")
            else:
                logger.info("Новых заявок не найдено")
                
        except Exception as e:
            logger.error(f"Ошибка при проверке заявок: {e}")
    
    async def send_hourly_report(self):
        """Отправляем накопленные заявки раз в час"""
        current_time = datetime.now()
        time_since_last_send = current_time - self.last_send_time
        
        # Проверяем, прошёл ли час
        if time_since_last_send.total_seconds() >= Config.SEND_INTERVAL_HOURS * 3600:
            logger.info("Время отправки отчёта в Telegram")
            
            try:
                # Получаем все неотправленные заявки
                unsent_requests = self.db.get_unsent_requests()
                
                if unsent_requests:
                    logger.info(f"Найдено {len(unsent_requests)} неотправленных заявок")
                    
                    # Парсим данные из JSON
                    requests_data = []
                    for request_id, data_json in unsent_requests:
                        try:
                            request_data = json.loads(data_json)
                            requests_data.append(request_data)
                        except json.JSONDecodeError as e:
                            logger.error(f"Ошибка парсинга JSON для заявки {request_id}: {e}")
                    
                    # Отправляем заявки в Telegram
                    if requests_data:
                        successful_ids = await self.telegram_notifier.send_requests_batch(requests_data)
                        
                        # Отмечаем отправленные заявки
                        if successful_ids:
                            self.db.mark_as_sent(successful_ids)
                        
                        logger.info(f"Успешно отправлено {len(successful_ids)} заявок")
                    
                    # Обновляем время последней отправки
                    self.last_send_time = current_time
                    
                else:
                    logger.info("Нет неотправленных заявок")
                    
            except Exception as e:
                logger.error(f"Ошибка при отправке отчёта: {e}")
    
    async def run(self):
        """Основной цикл работы бота"""
        logger.info("Запуск основного цикла бота...")
        
        try:
            while self.is_running:
                # Проверяем новые заявки
                await self.check_for_new_requests()
                
                # Проверяем, не пора ли отправить отчёт
                await self.send_hourly_report()
                
                # Ждем перед следующей проверкой
                await asyncio.sleep(Config.CHECK_INTERVAL_MINUTES * 60)
                
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