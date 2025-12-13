import asyncio
import logging
import socket
from datetime import datetime
from typing import List, Dict
from telegram import Bot
from telegram.error import TelegramError
from config import Config
import io

logger = logging.getLogger(__name__)

class TelegramNotifier:
    def __init__(self):
        self.bot = Bot(token=Config.TELEGRAM_BOT_TOKEN)
        self.chat_id = Config.TELEGRAM_CHAT_ID
    
    async def send_startup_notification(self):
        """Отправляем уведомление о запуске бота"""
        try:
            # Получаем информацию о сервере
            hostname = socket.gethostname()
            ip_address = socket.gethostbyname(hostname)
            
            message = (
                f"🤖 *CRM Бот запущен!*\n\n"
                f"*Время:* {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}\n"
                f"*Сервер:* `{hostname}`\n"
                f"*IP:* `{ip_address}`\n"
                f"*Статус:* ✅ Работает в фоновом режиме\n"
                f"*Режим отправки:* 00:30 и 30:30 каждого часа\n"
                f"*Проверка:* Перед каждой отправкой\n"
                f"*Управление:* PM2 (автозапуск)"
            )
            
            await self.bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode='Markdown',
                disable_notification=False
            )
            logger.info("Уведомление о запуске отправлено в Telegram")
            return True
            
        except Exception as e:
            logger.error(f"Ошибка отправки уведомления о запуске: {e}")
            return False
    
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
                disable_notification=True  # Без уведомлений
            )
            
            logger.info(f"Пачка #{batch_number} отправлена: {len(requests_data)} заявок")
            return True
            
        except TelegramError as e:
            logger.error(f"Ошибка Telegram: {e}")
            return False
        except Exception as e:
            logger.error(f"Неожиданная ошибка: {e}")
            return False

    async def send_daily_stats(self, counts: Dict[int, int], tz_name: str = 'Владивосток') -> bool:
        """Отправляет почасовой график и суммарную статистику за последние 24 часа.

        `counts` — словарь {hour_local: count} по локальному часу (0..23).
        """
        try:
            # Подготовка данных
            hours = list(range(24))
            values = [counts.get(h, 0) for h in hours]
            total = sum(values)

            # Создаём график: столбцы + линия
            try:
                import matplotlib.pyplot as plt
            except ImportError:
                # Если matplotlib не установлен — отправим текстовую сводку
                lines = [f"Статистика отправок (по {tz_name})"]
                for h in hours:
                    lines.append(f"{h:02d}: {values[h]}")
                lines.append(f"\nОтправлено за последние 24 часа: {total}")
                await self.bot.send_message(chat_id=self.chat_id, text="\n".join(lines))
                logger.warning("matplotlib not installed — отправлена текстовая статистика")
                return True

            fig, ax = plt.subplots(figsize=(12, 5))
            ax.bar(hours, values, color='orange', alpha=0.9)
            ax.set_xlabel('Час (локальное)')
            ax.set_ylabel('Количество отправленных заявок')
            ax.set_xticks(hours)
            ax.set_xticklabels([f"{h}ч" for h in hours])

            ax2 = ax.twinx()
            ax2.plot(hours, values, color='green', marker='o')
            ax2.set_ylabel('Линия (для наглядности)')

            plt.title(f'Статистика отправок по часам — {tz_name} (последние 24 часа)')
            plt.tight_layout()

            buf = io.BytesIO()
            plt.savefig(buf, format='png')
            plt.close(fig)
            buf.seek(0)

            caption = f"📊 Статистика отправок (по {tz_name})\nОтправлено за последние 24 часа: {total}"

            await self.bot.send_photo(
                chat_id=self.chat_id,
                photo=buf,
                caption=caption,
                parse_mode='Markdown'
            )

            logger.info("Ежедневная статистика отправлена")
            return True
        except TelegramError as e:
            logger.error(f"Ошибка Telegram при отправке статистики: {e}")
            return False
        except Exception as e:
            logger.error(f"Ошибка при формировании/отправке статистики: {e}")
            return False
