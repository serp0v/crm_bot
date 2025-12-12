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
