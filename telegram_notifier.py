import asyncio
import logging
import json
from typing import List, Dict
from telegram import Bot
from telegram.error import TelegramError
from config import Config

logger = logging.getLogger(__name__)

class TelegramNotifier:
    def __init__(self):
        self.bot = Bot(token=Config.TELEGRAM_BOT_TOKEN)
        self.chat_id = Config.TELEGRAM_CHAT_ID
    
    def format_request_message(self, request_data: Dict) -> str:
        """Форматируем сообщение для одной заявки"""
        message = (
            "🚨 *Заявка на прозвоне*\n\n"
            f"*ID:* `{request_data['id']}`\n"
            f"*Дата:* {request_data['date']}\n"
            f"*Тип:* {request_data['type']}\n"
            f"*Статус:* {request_data['status']}\n"
            f"*Город:* {request_data['city']}\n"
            f"*Телефон:* {request_data['phone']}\n"
            f"*Адрес:* {request_data['address']}\n"
            f"*Создана:* {request_data['created_at']}\n"
            f"*Клиент:* {request_data['client_name']}\n"
            f"*Ссылка:* {request_data['url']}"
        )
        return message
    
    def format_summary_message(self, requests_count: int) -> str:
        """Форматируем сводное сообщение"""
        message = (
            f"📊 *Сводка за час*\n\n"
            f"*Найдено новых заявок на прозвоне:* {requests_count}\n"
            f"*Время отправки:* {asyncio.get_event_loop().time() if asyncio.get_event_loop().is_running() else 'now'}"
        )
        return message
    
    async def send_single_request(self, request_data: Dict) -> bool:
        """Отправляем одну заявку в Telegram"""
        try:
            message = self.format_request_message(request_data)
            
            await self.bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode='Markdown',
                disable_web_page_preview=True,
                disable_notification=False
            )
            
            logger.info(f"Заявка {request_data['id']} отправлена в Telegram")
            return True
            
        except TelegramError as e:
            logger.error(f"Ошибка Telegram при отправке заявки {request_data['id']}: {e}")
            return False
        except Exception as e:
            logger.error(f"Неожиданная ошибка при отправке заявки {request_data['id']}: {e}")
            return False
    
    async def send_requests_batch(self, requests_data: List[Dict]) -> List[int]:
        """Отправляем пачку заявок"""
        successful_ids = []
        
        if not requests_data:
            return successful_ids
        
        # Сначала отправляем сводку
        try:
            summary_message = self.format_summary_message(len(requests_data))
            await self.bot.send_message(
                chat_id=self.chat_id,
                text=summary_message,
                parse_mode='Markdown'
            )
            await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"Ошибка при отправке сводки: {e}")
        
        # Затем отправляем каждую заявку
        for request_data in requests_data:
            success = await self.send_single_request(request_data)
            if success:
                successful_ids.append(request_data['id'])
            
            # Пауза между сообщениями, чтобы не спамить
            await asyncio.sleep(2)
        
        return successful_ids