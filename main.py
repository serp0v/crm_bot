import asyncio
import logging
import signal
import sys
from datetime import datetime, timedelta
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
        
        # Обработка сигналов
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        logger.info("CRM Telegram Bot инициализирован")
    
    def signal_handler(self, signum, frame):
        """Обработка сигналов завершения"""
        logger.info(f"Получен сигнал {signum}, останавливаю бота...")
        self.is_running = False
    
    async def startup(self):
        """Инициализация при запуске"""
        try:
            # Отправляем уведомление о запуске
            await self.telegram_notifier.send_startup_notification()
            
            # Очистка старых записей (раз в день в 00:05)
            now = datetime.now()
            if now.hour == 0 and now.minute < 10:
                self.db.cleanup_old_requests(days=1)
                
            return True
        except Exception as e:
            logger.error(f"Ошибка при старте: {e}")
            return False
    
    def calculate_sleep_seconds(self) -> float:
        """
        Вычисляет сколько секунд спать до следующего времени отправки
        или до следующей проверки (макс 5 минут)
        """
        now = datetime.now()
        current_minute = now.minute
        current_second = now.second
        
        # Время отправки: 30 и 0 (00) минута каждого часа
        send_minutes = [0, 30]
        
        # Ищем ближайшую минуту отправки
        next_send_minute = None
        for minute in sorted(send_minutes):
            if current_minute < minute or (current_minute == minute and current_second < 30):
                next_send_minute = minute
                break
        
        # Если не нашли в этом часу, берём первую минуту следующего часа
        if next_send_minute is None:
            next_send_minute = send_minutes[0]
        
        # Вычисляем разницу во времени
        if next_send_minute >= current_minute:
            # В этом же часу
            minutes_to_wait = next_send_minute - current_minute
            target_time = now.replace(minute=next_send_minute, second=30, microsecond=0)
        else:
            # В следующем часу
            minutes_to_wait = (60 - current_minute) + next_send_minute
            target_time = (now + timedelta(hours=1)).replace(
                minute=next_send_minute, second=30, microsecond=0
            )
        
        # Учитываем секунды
        seconds_to_wait = (target_time - now).total_seconds()
        
        # Если до отправки меньше 30 секунд, можно уже начинать подготовку
        if seconds_to_wait < 30:
            logger.info(f"До отправки осталось {seconds_to_wait:.0f} секунд - начинаем подготовку")
            return 0
        
        # Если до отправки больше 5 минут, ограничиваем 5 минутами
        if seconds_to_wait > 300:
            return 300  # 5 минут
        
        return seconds_to_wait
    
    async def process_requests(self) -> List[Dict]:
        """Обрабатываем заявки и возвращаем список для отправки"""
        logger.info("Поиск заявок на прозвоне...")
        
        try:
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
            
        except Exception as e:
            logger.error(f"Ошибка при обработке заявок: {e}")
            return []
    
    def should_send_now(self) -> bool:
        """Проверяем, нужно ли отправлять сейчас (30-я секунда 0 или 30 минуты)"""
        now = datetime.now()
        current_minute = now.minute
        current_second = now.second
        
        # Проверяем, что это 0 или 30 минута И 30-я секунда (±10 секунд)
        if current_minute in [0, 30] and 20 <= current_second <= 40:
            return True
        
        return False
    
    async def send_if_needed(self):
        """Проверяем и отправляем если наступило время"""
        if not self.should_send_now():
            return
        
        current_time = datetime.now()
        logger.info(f"Время отправки! {current_time.strftime('%H:%M:%S')}")
        
        # Получаем актуальные заявки
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
        
        # Запускаем инициализацию
        if not await self.startup():
            logger.error("Не удалось инициализировать бота")
            return
        
        try:
            while self.is_running:
                # Отправляем если наступило время
                await self.send_if_needed()
                
                # Вычисляем сколько ждать до следующей проверки
                sleep_seconds = self.calculate_sleep_seconds()
                
                if sleep_seconds > 0:
                    logger.info(f"Ожидание {sleep_seconds:.0f} секунд до следующей проверки...")
                    
                    # Ждём с проверкой флага каждую секунду
                    for _ in range(int(sleep_seconds)):
                        if not self.is_running:
                            break
                        await asyncio.sleep(1)
                
        except Exception as e:
            logger.error(f"Критическая ошибка: {e}", exc_info=True)
        finally:
            await self.shutdown()
    
    async def shutdown(self):
        """Корректное завершение работы"""
        logger.info("Завершение работы бота...")
        self.is_running = False

async def main():
    bot = CRMTelegramBot()
    await bot.run()

if __name__ == "__main__":
    import os
    os.makedirs("logs", exist_ok=True)
    asyncio.run(main())
