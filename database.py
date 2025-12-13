import sqlite3
import logging
import json
from datetime import datetime
from typing import List, Dict, Optional
from config import Config

logger = logging.getLogger(__name__)

class Database:
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or Config.DB_PATH
        self.init_db()
    
    def init_db(self):
        """Инициализация базы данных"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Удаляем старые таблицы
        cursor.execute('DROP TABLE IF EXISTS requests')
        cursor.execute('DROP TABLE IF EXISTS batch_counter')
        
        # Основная таблица (только ID + время + статус отправки)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id INTEGER NOT NULL,
                scheduled_time TEXT NOT NULL,
                first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_sent_at TIMESTAMP NULL,
                batch_number INTEGER NULL,
                UNIQUE(request_id, scheduled_time)
            )
        ''')
        
        # Счётчик пачек
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS batch_counter (
                id INTEGER PRIMARY KEY DEFAULT 1,
                last_batch_number INTEGER DEFAULT 0
            )
        ''')
        cursor.execute('INSERT OR IGNORE INTO batch_counter (id) VALUES (1)')
        
        conn.commit()
        conn.close()
        logger.info("База данных инициализирована (упрощённая версия)")
    
    def get_next_batch_number(self) -> int:
        """Получаем следующий номер пачки"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE batch_counter 
            SET last_batch_number = last_batch_number + 1
            WHERE id = 1
        ''')
        cursor.execute('SELECT last_batch_number FROM batch_counter WHERE id = 1')
        batch_number = cursor.fetchone()[0]
        
        conn.commit()
        conn.close()
        return batch_number
    
    def add_or_update_request(self, request_id: int, scheduled_time: str) -> bool:
        """
        Добавляем или обновляем заявку
        Возвращает True, если заявка новая (никогда не была в базе)
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Проверяем существование
        cursor.execute('''
            SELECT 1 FROM requests 
            WHERE request_id = ? AND scheduled_time = ?
        ''', (request_id, scheduled_time))
        
        exists = cursor.fetchone() is not None
        
        if not exists:
            # Новая заявка
            cursor.execute('''
                INSERT INTO requests (request_id, scheduled_time)
                VALUES (?, ?)
            ''', (request_id, scheduled_time))
            logger.debug(f"Добавлена новая заявка: {request_id} ({scheduled_time})")
        
        # Всегда обновляем время последнего просмотра
        cursor.execute('''
            UPDATE requests 
            SET first_seen_at = CURRENT_TIMESTAMP
            WHERE request_id = ? AND scheduled_time = ?
        ''', (request_id, scheduled_time))
        
        conn.commit()
        conn.close()
        return not exists  # True = новая, False = уже была
    
    def mark_as_sent(self, request_id: int, scheduled_time: str, batch_number: int):
        """Отмечаем заявку как отправленную"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE requests 
            SET last_sent_at = CURRENT_TIMESTAMP,
                batch_number = ?
            WHERE request_id = ? AND scheduled_time = ?
        ''', (batch_number, request_id, scheduled_time))
        
        conn.commit()
        conn.close()
        logger.debug(f"Заявка {request_id} отмечена как отправленная в пачке #{batch_number}")

    # Compatibility helpers for tests
    def add_request(self, request_id: int, payload: str) -> bool:
        """Compatibility wrapper used by `test.py`.

        Inserts a request row if it does not exist. `payload` is stored in `scheduled_time` column
        for compatibility with the simplified schema used in tests.
        Returns True if inserted, False if already existed.
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        cursor.execute('SELECT 1 FROM requests WHERE request_id = ?', (request_id,))
        exists = cursor.fetchone() is not None

        if not exists:
            cursor.execute(
                'INSERT INTO requests (request_id, scheduled_time) VALUES (?, ?)',
                (request_id, payload or '')
            )
            conn.commit()

        conn.close()
        return not exists

    def request_exists(self, request_id: int) -> bool:
        """Возвращает True если заявка с таким request_id есть в базе."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute('SELECT 1 FROM requests WHERE request_id = ?', (request_id,))
        exists = cursor.fetchone() is not None
        conn.close()
        return exists
    
    def cleanup_old_requests(self, days: int = 1):
        """Очищаем старые записи"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            DELETE FROM requests 
            WHERE date(first_seen_at) < date('now', ?)
        ''', (f'-{days} days',))
        
        deleted = cursor.rowcount
        conn.commit()
        conn.close()
        
        if deleted:
            logger.info(f"Очищено {deleted} старых записей (> {days} дней)")

    def get_hourly_sent_counts_last_24h(self, tz_offset_hours: int = 10) -> Dict[int, int]:
        """Возвращает словарь {hour: count} для последних 24 часов в часовом поясе с указанным смещением.

        Час возвращается в диапазоне 0-23 локального времени (tz_offset_hours).
        Время в БД хранится в формате UTC (SQLite CURRENT_TIMESTAMP -> UTC), поэтому
        мы выбираем записи за последние 24 часа по UTC и затем смещаем их на tz_offset.
        """
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()

        # Определяем порог UTC (24 часа назад)
        now_utc = datetime.utcnow()
        start_utc = now_utc - datetime.timedelta(hours=24) if False else None
        # We will compute start_utc properly without relying on datetime.timedelta import
        from datetime import timedelta
        start_utc = now_utc - timedelta(hours=24)

        cursor.execute('''
            SELECT last_sent_at FROM requests
            WHERE last_sent_at IS NOT NULL
            AND last_sent_at >= ?
        ''', (start_utc.strftime('%Y-%m-%d %H:%M:%S'),))

        rows = cursor.fetchall()
        conn.close()

        # Initialize counts for 0..23
        counts = {h: 0 for h in range(24)}

        for (last_sent_at_str,) in rows:
            try:
                ts = datetime.strptime(last_sent_at_str, '%Y-%m-%d %H:%M:%S')
                # ts is in UTC; convert to local by adding offset
                local_ts = ts + timedelta(hours=tz_offset_hours)
                hour = local_ts.hour
                counts[hour] = counts.get(hour, 0) + 1
            except Exception:
                continue

        return counts
