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

    async def send_daily_stats(self, counts: Dict[int, int], tz_name: str = 'Владивосток', start_hour: int = 8) -> bool:
        """Отправляет почасовой график и суммарную статистику за последние 24 часа.

        `counts` — словарь {hour_local: count} по локальному часу (0..23).
        Параметр `start_hour` задаёт, с какого локального часа должна начинаться шкала (по умолчанию 8).
        """
        try:
            # Подготовка данных — переставляем шкалу, чтобы она начиналась с `start_hour`
            hours = [(start_hour + i) % 24 for i in range(24)]
            values = [counts.get(h, 0) for h in hours]
            total = sum(values)

            # Попробуем отрисовать график в отдельной функции — если это не удастся, отправим текст
            try:
                buf = self._render_stats_image(values, hours, tz_name)
            except Exception as e:
                logger.warning(f"Не удалось сгенерировать изображение статистики: {e}")
                # Отправим текстовую сводку
                lines = [f"Статистика отправок (по {tz_name}) — шкала с {start_hour}:00"]
                for h, v in zip(hours, values):
                    lines.append(f"{h:02d}: {v}")
                lines.append(f"\nОтправлено за последние 24 часа: {total}")
                await self.bot.send_message(chat_id=self.chat_id, text="\n".join(lines))
                logger.info("Ежедневная статистика отправлена в текстовом виде (фолбэк)")
                return True

            caption = f"📊 Статистика отправок (по {tz_name})\nОтправлено за последние 24 часа: {total}"

            await self.bot.send_photo(
                chat_id=self.chat_id,
                photo=buf,
                caption=caption,
                parse_mode='Markdown'
            )
            logger.info("Ежедневная статистика отправлена как изображение")
            return True
        except TelegramError as e:
            logger.error(f"Ошибка Telegram при отправке статистики: {e}")
            return False
        except Exception as e:
            logger.error(f"Ошибка при формировании/отправке статистики: {e}")
            return False

    def _render_stats_image(self, values: List[int], hours: List[int], tz_name: str) -> io.BytesIO:
        """Рендерит график в памяти и возвращает BytesIO с PNG.

        Внутри — делаем максимально устойчивую к headless окружению конфигурацию matplotlib.
        """
        try:
            import matplotlib
            matplotlib.use('Agg')
            import matplotlib.pyplot as plt
            # Ограничим использование тяжёлых шрифтов
            matplotlib.rcParams['font.family'] = 'DejaVu Sans'
            matplotlib.rcParams['font.size'] = 10
        except Exception as e:
            raise RuntimeError(f"matplotlib import failed: {e}")

        fig, ax = plt.subplots(figsize=(12, 5))
        ax.bar(range(len(values)), values, color='orange', alpha=0.9)
        ax.set_xlabel('Час (локальный)')
        ax.set_ylabel('Количество отправленных заявок')
        ax.set_xticks(range(len(hours)))
        ax.set_xticklabels([f"{h}ч" for h in hours], rotation=0)

        ax2 = ax.twinx()
        ax2.plot(range(len(values)), values, color='green', marker='o')
        ax2.set_ylabel('Линия (для наглядности)')

        plt.title(f'Статистика отправок по часам — {tz_name} (последние 24 часа)')
        plt.tight_layout()

        buf = io.BytesIO()
        try:
            plt.savefig(buf, format='png')
        finally:
            plt.close(fig)

        buf.seek(0)
        return buf
