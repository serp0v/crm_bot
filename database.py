import sqlite3
import logging
import json
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple
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
        cursor.execute('DROP TABLE IF EXISTS call_tasks')
        cursor.execute('DROP TABLE IF EXISTS batch_counter')
        
        # Основная таблица
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
                was_sent_as_urgent BOOLEAN DEFAULT 0,  # Отправлялась ли уже как срочная
                batch_numbers TEXT,
                UNIQUE(request_id, scheduled_time)
            )
        ''')
        
        # Счётчик пачек
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS batch_counter (
                id INTEGER PRIMARY KEY DEFAULT 1,
                last_batch_number INTEGER DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        cursor.execute('INSERT OR IGNORE INTO batch_counter (id) VALUES (1)')
        
        conn.commit()
        conn.close()
        logger.info("База данных инициализирована")
    
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
    
    def add_or_update_task(self, request_data: Dict) -> bool:
        """
        Добавляем или обновляем задачу
        Возвращает True, если нужно отправить
        """
        request_id = request_data['id']
        scheduled_time = request_data.get('scheduled_time', '')
        is_urgent = request_data.get('is_urgent', False)
        data_json = json.dumps(request_data, ensure_ascii=False, default=str)
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        # Проверяем существование задачи
        cursor.execute('''
            SELECT id, last_sent_at, is_urgent, was_sent_as_urgent 
            FROM call_tasks 
            WHERE request_id = ? AND scheduled_time = ?
        ''', (request_id, scheduled_time))
        
        result = cursor.fetchone()
        
        if not result:
            # Новая задача - всегда отправляем
            cursor.execute('''
                INSERT INTO call_tasks 
                (request_id, scheduled_time, data, is_urgent, last_seen_at)
                VALUES (?, ?, ?, ?, ?)
            ''', (request_id, scheduled_time, data_json, is_urgent, datetime.now()))
            
            conn.commit()
            conn.close()
            return True
        
        # Задача существует
        task_id, last_sent_at, was_urgent, was_sent_as_urgent = result
        
        # Определяем, нужно ли отправлять
        should_send = False
        
        # 1. Если заявка стала срочной (а не была) - отправляем
        if is_urgent and not was_urgent:
            should_send = True
            logger.debug(f"Заявка {request_id} стала срочной, нужно отправить")
        
        # 2. Если заявка не отправлялась >30 минут - отправляем
        # (но это только для обычных заявок, срочные обрабатываются выше)
        elif last_sent_at:
            last_sent = datetime.fromisoformat(last_sent_at.replace('Z', '+00:00')) if isinstance(last_sent_at, str) else last_sent_at
            minutes_passed = (datetime.now() - last_sent).total_seconds() / 60
            
            if minutes_passed > 30:
                should_send = True
                logger.debug(f"Заявка {request_id} не отправлялась {minutes_passed:.0f} мин, нужно отправить")
        
        # Обновляем запись
        cursor.execute('''
            UPDATE call_tasks 
            SET last_seen_at = ?,
                is_urgent = ?,
                data = ?,
                was_sent_as_urgent = ?
            WHERE id = ?
        ''', (
            datetime.now(), 
            is_urgent, 
            data_json,
            was_sent_as_urgent or is_urgent,  # Отмечаем если отправлялась как срочная
            task_id
        ))
        
        conn.commit()
        conn.close()
        return should_send
    
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
        new_batches = f"{current_batches},{batch_number}" if current_batches else str(batch_number)
        
        # Обновляем запись
        cursor.execute('''
            UPDATE call_tasks 
            SET last_sent_at = ?,
                batch_numbers = ?
            WHERE request_id = ? AND scheduled_time = ?
        ''', (datetime.now(), new_batches, request_id, scheduled_time))
        
        conn.commit()
        conn.close()
