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
    
    async def send_batch(self, requests_data: List[Dict], batch_number: int, is_urgent_only: bool = False) -> List[int]:
        """Отправляем пачку заявок в новом формате"""
        successful_ids = []
        
        if not requests_data:
            logger.info("Нет заявок для отправки")
            return successful_ids
        
        try:
            # Фильтруем заявки если нужно только срочные
            if is_urgent_only:
                requests_to_send = [r for r in requests_data if r.get('is_urgent', False)]
                if not requests_to_send:
                    logger.info("Нет срочных заявок для повторной отправки")
                    return []
                batch_title = f"🔄 ПОВТОРНАЯ ОТПРАВКА #{batch_number}"
            else:
                requests_to_send = requests_data
                batch_title = f"#{batch_number}"
            
            # Формируем сообщение
            message_lines = [batch_title, ""]
            
            for request_data in requests_to_send:
                request_id = request_data['id']
                prefix = "🟡" if request_data.get('is_urgent', False) else ""
                message_lines.append(f"{prefix}`{request_id}`")
            
            message = "\n".join(message_lines)
            
            # Отправляем сообщение
            await self.bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode='Markdown',
                disable_notification=False
            )
            
            successful_ids = [r['id'] for r in requests_to_send]
            batch_type = "срочная" if is_urgent_only else "обычная"
            logger.info(f"Пачка #{batch_number} ({batch_type}) отправлена: {len(successful_ids)} заявок")
            
            # Небольшая пауза между сообщениями
            await asyncio.sleep(1)
            
            return successful_ids
            
        except TelegramError as e:
            logger.error(f"Ошибка Telegram при отправке пачки #{batch_number}: {e}")
            return []
        except Exception as e:
            logger.error(f"Неожиданная ошибка при отправке пачки #{batch_number}: {e}")
            return []
    
    def should_send_now(self) -> bool:
        """Проверяем, нужно ли отправлять сейчас (31 или 01 минута часа)"""
        now = datetime.now()
        current_minute = now.minute
        
        # Отправляем в 31 и 01 минуту каждого часа
        return current_minute in [1, 31]
    
    def get_minutes_to_next_send(self) -> int:
        """Получаем количество минут до следующей отправки"""
        now = datetime.now()
        current_minute = now.minute
        
        if current_minute < 1:
            return 1 - current_minute
        elif current_minute < 31:
            return 31 - current_minute
        else:  # После 31 минуты, ждем до 01 минуты следующего часа
            return 61 - current_minute
