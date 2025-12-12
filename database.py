import sqlite3
import logging
from datetime import datetime
from typing import List, Optional
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
        
        # Таблица для отслеживания заявок
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id INTEGER UNIQUE,
                data TEXT,
                found_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                sent_at TIMESTAMP NULL,
                batch_number INTEGER NULL,
                is_urgent BOOLEAN DEFAULT 0
            )
        ''')
        
        # Таблица для счетчика пачек
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS batch_counter (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                last_batch_number INTEGER DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        ''')
        
        # Инициализируем счетчик если он пустой
        cursor.execute('INSERT OR IGNORE INTO batch_counter (id, last_batch_number) VALUES (1, 0)')
        
        # Индексы для быстрого поиска
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_request_id 
            ON requests(request_id)
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_sent_at 
            ON requests(sent_at)
        ''')
        
        conn.commit()
        conn.close()
        logger.info("База данных инициализирована")
    
    def get_next_batch_number(self) -> int:
        """Получаем следующий номер пачки"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('UPDATE batch_counter SET last_batch_number = last_batch_number + 1 WHERE id = 1')
            cursor.execute('SELECT last_batch_number FROM batch_counter WHERE id = 1')
            
            batch_number = cursor.fetchone()[0]
            conn.commit()
            logger.debug(f"Получен номер пачки: {batch_number}")
            return batch_number
        except Exception as e:
            logger.error(f"Ошибка при получении номера пачки: {e}")
            return 1
        finally:
            conn.close()
    
    def add_request(self, request_id: int, data: str, is_urgent: bool = False) -> bool:
        """Добавляем новую заявку в БД"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute(
                """INSERT OR IGNORE INTO requests 
                   (request_id, data, found_at, is_urgent) 
                   VALUES (?, ?, ?, ?)""",
                (request_id, data, datetime.now(), is_urgent)
            )
            conn.commit()
            added = cursor.rowcount > 0
            if added:
                logger.info(f"Заявка {request_id} добавлена в БД (срочная: {is_urgent})")
            return added
        except Exception as e:
            logger.error(f"Ошибка при добавлении заявки {request_id}: {e}")
            return False
        finally:
            conn.close()
    
    def get_unsent_requests(self) -> List[tuple]:
        """Получаем все неотправленные заявки"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute(
                """SELECT request_id, data, is_urgent FROM requests 
                   WHERE sent_at IS NULL 
                   ORDER BY found_at"""
            )
            return cursor.fetchall()
        except Exception as e:
            logger.error(f"Ошибка при получении неотправленных заявок: {e}")
            return []
        finally:
            conn.close()
    
    def mark_as_sent(self, request_ids: List[int], batch_number: int):
        """Отмечаем заявки как отправленные с номером пачки"""
        if not request_ids:
            return
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            placeholders = ','.join('?' for _ in request_ids)
            query = f"""
                UPDATE requests 
                SET sent_at = ?, batch_number = ?
                WHERE request_id IN ({placeholders})
            """
            cursor.execute(query, [datetime.now(), batch_number] + request_ids)
            conn.commit()
            logger.info(f"Отмечено как отправлено в пачке #{batch_number}: {len(request_ids)} заявок")
        except Exception as e:
            logger.error(f"Ошибка при обновлении статуса: {e}")
        finally:
            conn.close()
    
    def request_exists(self, request_id: int) -> bool:
        """Проверяем, существует ли заявка в БД"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute(
                "SELECT 1 FROM requests WHERE request_id = ?",
                (request_id,)
            )
            return cursor.fetchone() is not None
        finally:
            conn.close()
    
    def get_last_batch_number(self) -> int:
        """Получаем последний номер пачки"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute('SELECT last_batch_number FROM batch_counter WHERE id = 1')
            result = cursor.fetchone()
            return result[0] if result else 0
        finally:
            conn.close()
