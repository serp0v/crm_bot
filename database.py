import sqlite3
import logging
import json
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from config import Config

logger = logging.getLogger(__name__)

class Database:
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or Config.DB_PATH
        self.init_db()
    
    def init_db(self):
        """Инициализация базы данных с новой структурой"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Удаляем старые таблицы, чтобы избежать конфликтов
        cursor.execute('DROP TABLE IF EXISTS requests')
        cursor.execute('DROP TABLE IF EXISTS batch_counter')
        
        # Основная таблица для задач прозвона (ID + scheduled_time)
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS call_tasks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id INTEGER NOT NULL,
                scheduled_time TEXT NOT NULL,
                data TEXT,
                first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_sent_at TIMESTAMP NULL,
                is_urgent BOOLEAN DEFAULT 0,
                is_processing BOOLEAN DEFAULT 0,
                batch_numbers TEXT,
                UNIQUE(request_id, scheduled_time)
            )
        ''')
        
        # Таблица для счетчика пачек
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS batch_counter (
                id INTEGER PRIMARY KEY DEFAULT 1,
                last_batch_number INTEGER DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('INSERT OR IGNORE INTO batch_counter (id) VALUES (1)')
        
        # Индексы
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_tasks_composite 
            ON call_tasks(request_id, scheduled_time)
        ''')
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_tasks_urgent 
            ON call_tasks(is_urgent, last_sent_at)
        ''')
        
        conn.commit()
        conn.close()
        logger.info("Новая структура базы данных инициализирована")
    
    def get_next_batch_number(self) -> int:
        """Получаем следующий номер пачки"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            UPDATE batch_counter 
            SET last_batch_number = last_batch_number + 1,
                updated_at = CURRENT_TIMESTAMP 
            WHERE id = 1
        ''')
        cursor.execute('SELECT last_batch_number FROM batch_counter WHERE id = 1')
        batch_number = cursor.fetchone()[0]
        
        conn.commit()
        conn.close()
        return batch_number
    
    def get_task(self, request_id: int, scheduled_time: str) -> Optional[Tuple]:
        """Получаем задачу по ID и времени"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT id, last_sent_at, is_urgent, is_processing
            FROM call_tasks 
            WHERE request_id = ? AND scheduled_time = ?
        ''', (request_id, scheduled_time))
        
        result = cursor.fetchone()
        conn.close()
        return result
    
    def add_or_update_task(self, request_data: Dict) -> Tuple[bool, bool]:
        """
        Добавляем или обновляем задачу
        Возвращает (is_new_task, should_send)
        """
        request_id = request_data['id']
        scheduled_time = request_data.get('scheduled_time', '')
        is_urgent = request_data.get('is_urgent', False)
        is_processing = request_data.get('is_processing', False)
        data_json = json.dumps(request_data, ensure_ascii=False, default=str)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Проверяем существование задачи
        existing = self.get_task(request_id, scheduled_time)
        
        if not existing:
            # Новая задача
            cursor.execute('''
                INSERT INTO call_tasks 
                (request_id, scheduled_time, data, is_urgent, is_processing, last_seen_at)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (request_id, scheduled_time, data_json, is_urgent, is_processing, datetime.now()))
            
            conn.commit()
            conn.close()
            return True, True  # Новая задача, отправляем
        
        # Задача существует, обновляем
        task_id, last_sent_at, was_urgent, was_processing = existing
        
        # Обновляем время последнего просмотра
        cursor.execute('''
            UPDATE call_tasks 
            SET last_seen_at = ?,
                is_urgent = ?,
                is_processing = ?,
                data = ?
            WHERE id = ?
        ''', (datetime.now(), is_urgent, is_processing, data_json, task_id))
        
        conn.commit()
        conn.close()
        
        # Определяем, нужно ли отправлять
        should_send = False
        
        # 1. Если заявка стала срочной (а не была)
        if is_urgent and not was_urgent:
            should_send = True
        
        # 2. Если заявка не в работе и не отправлялась >30 минут
        elif not is_processing and last_sent_at:
            last_sent = datetime.fromisoformat(last_sent_at.replace('Z', '+00:00')) if isinstance(last_sent_at, str) else last_sent_at
            minutes_passed = (datetime.now() - last_sent).total_seconds() / 60
            
            if minutes_passed > 30:
                should_send = True
        
        return False, should_send  # Не новая задача, отправляем по условиям
    
    def mark_as_sent(self, request_id: int, scheduled_time: str, batch_number: int):
        """Отмечаем задачу как отправленную"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Получаем текущие номера пачек
        cursor.execute('''
            SELECT batch_numbers FROM call_tasks 
            WHERE request_id = ? AND scheduled_time = ?
        ''', (request_id, scheduled_time))
        
        result = cursor.fetchone()
        current_batches = result[0] if result and result[0] else ""
        
        # Добавляем новый номер пачки
        if current_batches:
            new_batches = f"{current_batches},{batch_number}"
        else:
            new_batches = str(batch_number)
        
        # Обновляем запись
        cursor.execute('''
            UPDATE call_tasks 
            SET last_sent_at = ?,
                batch_numbers = ?
            WHERE request_id = ? AND scheduled_time = ?
        ''', (datetime.now(), new_batches, request_id, scheduled_time))
        
        conn.commit()
        conn.close()
    
    def cleanup_old_tasks(self, hours_old: int = 24):
        """Очищаем старые задачи (> hours_old часов)"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute('''
            DELETE FROM call_tasks 
            WHERE last_seen_at < datetime('now', ?)
        ''', (f'-{hours_old} hours',))
        
        deleted = cursor.rowcount
        conn.commit()
        conn.close()
        
        if deleted:
            logger.info(f"Очищено {deleted} старых задач (> {hours_old} часов)")
