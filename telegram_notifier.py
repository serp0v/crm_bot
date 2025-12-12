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
    
    async def send_batch(self, requests_data: List[Dict], batch_number: int) -> bool:
        """Отправляем пачку заявок"""
        if not requests_data:
            logger.info("Нет заявок для отправки")
            return False
        
        try:
            # Формируем сообщение
            message_lines = [f"#{batch_number}", ""]
            
            for request_data in requests_data:
                request_id = request_data['id']
                scheduled_time = request_data.get('scheduled_time', '')
                
                if scheduled_time:
                    message_lines.append(f"`{request_id}` ({scheduled_time})")
                else:
                    message_lines.append(f"`{request_id}`")
            
            message = "\n".join(message_lines)
            
            # Отправляем
            await self.bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode='Markdown',
                disable_notification=True  # Без уведомлений (спокойные отправки)
            )
            
            logger.info(f"Пачка #{batch_number} отправлена: {len(requests_data)} заявок")
            return True
            
        except TelegramError as e:
            logger.error(f"Ошибка Telegram: {e}")
            return False
        except Exception as e:
            logger.error(f"Неожиданная ошибка: {e}")
            return False
