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
        
        logger.info("CRM Telegram Bot инициализирован (базовая версия)")
    
    async def process_requests(self) -> List[Dict]:
        """Обрабатываем заявки и возвращаем список для отправки"""
        logger.info("Поиск заявок на прозвоне...")
        
        # Получаем все заявки
        all_requests = self.crm_parser.find_all_awaiting_calls()
        
        # Отфильтровываем заявки в работе
        active_requests = []
        for req in all_requests:
            if not req.get('is_processing', False):
                active_requests.append(req)
        
        logger.info(f"Найдено заявок: {len(all_requests)} → активных: {len(active_requests)}")
        
        # Регистрируем в базе и собираем новые
        new_requests = []
        for req in active_requests:
            request_id = req['id']
            scheduled_time = req.get('scheduled_time', '')
            
            # Добавляем в базу
            is_new = self.db.add_or_update_request(request_id, scheduled_time)
            
            # Собираем только новые заявки для отправки
            if is_new:
                new_requests.append(req)
        
        logger.info(f"Новых заявок для отправки: {len(new_requests)}")
        return new_requests
    
    async def check_and_send(self):
        """Проверяем время и отправляем если нужно"""
        current_time = datetime.now()
        current_minute = current_time.minute
        
        # Только 01 и 31 минута
        should_send = current_minute in [1, 31]
        
        logger.info(f"Время: {current_time.strftime('%H:%M')} (минута: {current_minute}) → отправка: {should_send}")
        
        if should_send:
            # Получаем заявки
            requests_to_send = await self.process_requests()
            
            if requests_to_send:
                # Получаем номер пачки
                batch_number = self.db.get_next_batch_number()
                
                # Отправляем
                success = await self.telegram_notifier.send_batch(requests_to_send, batch_number)
                
                if success:
                    # Отмечаем как отправленные
                    for req in requests_to_send:
                        self.db.mark_as_sent(req['id'], req.get('scheduled_time', ''), batch_number)
                    
                    logger.info(f"Пачка #{batch_number} успешно отправлена ({len(requests_to_send)} заявок)")
                else:
                    logger.error("Не удалось отправить пачку")
            else:
                logger.info("Нет новых заявок для отправки")
    
    async def run(self):
        """Основной цикл работы"""
        logger.info("Запуск основного цикла бота...")
        
        try:
            while self.is_running:
                # Проверяем и отправляем если нужно
                await self.check_and_send()
                
                # Ждём 5 минут до следующей проверки
                logger.info("Ожидание 5 минут до следующей проверки...")
                await asyncio.sleep(300)  # 5 минут = 300 секунд
                
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
