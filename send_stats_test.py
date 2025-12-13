import argparse
import os
import sqlite3
from datetime import datetime, timedelta
import json

from database import Database
from telegram_notifier import TelegramNotifier


SAMPLE_COUNTS = [120, 100, 80, 50, 30, 20, 10, 5, 3, 7, 15, 25, 40, 60, 110, 140, 170, 180, 190, 200, 185, 160, 140, 100]


def populate_sample_db(db_path: str, tz_offset_hours: int = 10):
    """Создаёт тестовую БД и заполняет таблицу `requests` так, чтобы
    локальные часы (сдвигом tz_offset_hours) имели количество событий из SAMPLE_COUNTS.
    """
    # Инициализация DB через наш класс (он удалит старые таблицы)
    db = Database(db_path)

    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    now_utc = datetime.utcnow().replace(minute=0, second=0, microsecond=0)

    req_id = 1000000
    for local_hour, count in enumerate(SAMPLE_COUNTS):
        # Для локального часа local_hour соответствующий UTC час:
        utc_hour = (local_hour - tz_offset_hours) % 24
        # Возьмём дату/время примерно в пределах последних 24 часов; используем сегодня(UTC) + utc_hour
        # Если utc_hour > now_utc.hour, значит это вчера
        candidate_date = now_utc
        candidate_date = candidate_date.replace(hour=utc_hour)
        # Смещаем на -1 день, если получилось в будущем
        if candidate_date > now_utc:
            candidate_date = candidate_date - timedelta(days=1)

        for i in range(count):
            req_id += 1
            scheduled_time = f"{(local_hour):02d}:00"
            last_sent_at = (candidate_date + timedelta(minutes=i % 60)).strftime('%Y-%m-%d %H:%M:%S')
            cur.execute(
                "INSERT OR IGNORE INTO requests (request_id, scheduled_time, first_seen_at, last_sent_at, batch_number) VALUES (?, ?, ?, ?, ?)",
                (req_id, scheduled_time, datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'), last_sent_at, 1)
            )

    conn.commit()
    conn.close()
    print(f"Populated sample DB at {db_path}")


async def run_test(args):
    db_path = args.db_path

    if args.use_sample_db:
        if os.path.exists(db_path):
            os.remove(db_path)
        populate_sample_db(db_path, tz_offset_hours=args.tz_offset)

    # Если мы только что создали sample DB — не инициируем Database(),
    # т.к. конструктор вызывает init_db() и удалит данные.
    if args.use_sample_db:
        db = Database.__new__(Database)
        db.db_path = db_path
    else:
        db = Database(db_path)

    counts = db.get_hourly_sent_counts_last_24h(tz_offset_hours=args.tz_offset)
    total = sum(counts.values())

    print(f"Hourly counts (local tz offset {args.tz_offset}):")
    for h in range(24):
        print(f"{h:02d}: {counts.get(h,0)}")
    print(f"Total (last 24h): {total}")

    # Если указано --send, попытаемся отправить (нужны TELEGRAM env vars)
    if args.send:
        if not os.getenv('TELEGRAM_BOT_TOKEN') or not os.getenv('TELEGRAM_CHAT_ID'):
            print("TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID not set in environment. Aborting send.")
            return
        notifier = TelegramNotifier()
        success = await notifier.send_daily_stats(counts, tz_name=args.tz_name)
        if success:
            print("Stats sent to Telegram")
        else:
            print("Failed to send stats to Telegram")
    else:
        # Сохраняем картинку локально через matplotlib, если установлен
        try:
            import matplotlib.pyplot as plt
            hours = list(range(24))
            values = [counts.get(h, 0) for h in hours]

            fig, ax = plt.subplots(figsize=(12, 5))
            ax.bar(hours, values, color='orange', alpha=0.9)
            ax.set_xlabel('Час (локальное)')
            ax.set_ylabel('Количество отправленных заявок')
            ax.set_xticks(hours)
            ax.set_xticklabels([f"{h}ч" for h in hours])

            ax2 = ax.twinx()
            ax2.plot(hours, values, color='green', marker='o')
            ax2.set_ylabel('Линия (для наглядности)')

            plt.title(f'Тестовая статистика (по {args.tz_name})')
            plt.tight_layout()

            out = args.output or 'daily_stats_test.png'
            plt.savefig(out)
            plt.close(fig)
            print(f"Saved test chart to {out}")
        except ImportError:
            out = args.output or 'daily_stats_test.json'
            with open(out, 'w', encoding='utf-8') as f:
                json.dump({'counts': counts, 'total': total}, f, ensure_ascii=False, indent=2)
            print(f"matplotlib not installed; saved counts JSON to {out}. Install matplotlib to generate PNG.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Test daily stats sending')
    parser.add_argument('--use-sample-db', action='store_true', help='Create and use a sample test DB (default path test_stats.db)')
    parser.add_argument('--db-path', default='test_stats.db', help='Path to sqlite DB')
    parser.add_argument('--tz-offset', type=int, default=10, help='Timezone offset hours (e.g., 10 for VL)')
    parser.add_argument('--send', action='store_true', help='Actually send to Telegram (requires env vars)')
    parser.add_argument('--tz-name', default='Владивосток', help='Timezone name for captions')
    parser.add_argument('--output', help='Output PNG filename when not sending')

    args = parser.parse_args()

    import asyncio
    asyncio.run(run_test(args))
