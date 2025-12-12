import asyncio
import logging
from datetime import datetime
from typing import List, Dict
from telegram import Bot
from telegram.error import TelegramError
from config import Config

logger = logging.getLogger(__name__)

class TelegramNotifier:
    def __init__(self):
        self.bot = Bot(token=Config.TELEGRAM_BOT_TOKEN)
        self.chat_id = Config.TELEGRAM_CHAT_ID
    
    async def send_batch(self, requests_data: List[Dict], batch_number: int, is_urgent_only: bool = False) -> bool:
        """Отправляем пачку заявок"""
        if not requests_data:
            logger.info("Нет заявок для отправки")
            return False
        
        try:
            # Фильтруем заявки если нужно только срочные
            if is_urgent_only:
                requests_to_send = [r for r in requests_data if r.get('is_urgent', False)]
                if not requests_to_send:
                    logger.info("Нет срочных заявок для отправки")
                    return False
                batch_title = f"🔄 ПОВТОРНАЯ ОТПРАВКА #{batch_number}"
            else:
                requests_to_send = requests_data
                batch_title = f"#{batch_number}"
            
            # Формируем сообщение
            message_lines = [batch_title, ""]
            
            for request_data in requests_to_send:
                request_id = request_data['id']
                scheduled_time = request_data.get('scheduled_time', '')
                prefix = "🟡" if request_data.get('is_urgent', False) else ""
                
                if scheduled_time:
                    message_lines.append(f"{prefix}`{request_id}` ({scheduled_time})")
                else:
                    message_lines.append(f"{prefix}`{request_id}`")
            
            message = "\n".join(message_lines)
            
            # Отправляем
            await self.bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode='Markdown',
                disable_notification=False
            )
            
            logger.info(f"Пачка #{batch_number} отправлена: {len(requests_to_send)} заявок")
            await asyncio.sleep(1)
            return True
            
        except TelegramError as e:
            logger.error(f"Ошибка Telegram: {e}")
            return False
        except Exception as e:
            logger.error(f"Неожиданная ошибка: {e}")
            return False
    
    def should_send_now(self) -> bool:
        """Проверяем, нужно ли отправлять сейчас (31 или 01 минута часа)"""
        now = datetime.now()
        return now.minute in [1, 31]
    
    def get_minutes_to_next_send(self) -> int:
        """Минуты до следующей отправки"""
        now = datetime.now()
        current_minute = now.minute
        
        if current_minute < 1:
            return 1 - current_minute
        elif current_minute < 31:
            return 31 - current_minute
        else:
            return 61 - current_minute
