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
    
    async def send_batch(self, requests_data: List[Dict], batch_number: int, is_urgent: bool = False) -> bool:
        """Отправляем пачку заявок"""
        if not requests_data:
            return False
        
        try:
            # Формируем заголовок
            if is_urgent:
                batch_title = f"🚨 СРОЧНЫЕ #{batch_number}"
            else:
                batch_title = f"#{batch_number}"
            
            # Формируем сообщение
            message_lines = [batch_title, ""]
            
            for request_data in requests_data:
                request_id = request_data['id']
                scheduled_time = request_data.get('scheduled_time', '')
                
                # Временное отключение жёлтых пометок (по просьбе пользователя)
                # prefix = "🟡" if is_urgent else ""
                prefix = ""
                
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
                disable_notification=not is_urgent  # Уведомления только для срочных
            )
            
            logger.info(f"Пачка #{batch_number} отправлена: {len(requests_data)} заявок")
            await asyncio.sleep(0.5)  # Небольшая пауза
            return True
            
        except TelegramError as e:
            logger.error(f"Ошибка Telegram: {e}")
            return False
        except Exception as e:
            logger.error(f"Неожиданная ошибка: {e}")
            return False
