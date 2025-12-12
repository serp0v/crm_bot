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
                sent_at TIMESTAMP NULL
            )
        ''')
        
        # Индекс для быстрого поиска
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_request_id 
            ON requests(request_id)
        ''')
        
        conn.commit()
        conn.close()
        logger.info("База данных инициализирована")
    
    def add_request(self, request_id: int, data: str) -> bool:
        """Добавляем новую заявку в БД"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            cursor.execute(
                """INSERT OR IGNORE INTO requests 
                   (request_id, data, found_at) 
                   VALUES (?, ?, ?)""",
                (request_id, data, datetime.now())
            )
            conn.commit()
            added = cursor.rowcount > 0
            if added:
                logger.info(f"Заявка {request_id} добавлена в БД")
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
                """SELECT request_id, data FROM requests 
                   WHERE sent_at IS NULL 
                   ORDER BY found_at"""
            )
            return cursor.fetchall()
        except Exception as e:
            logger.error(f"Ошибка при получении неотправленных заявок: {e}")
            return []
        finally:
            conn.close()
    
    def mark_as_sent(self, request_ids: List[int]):
        """Отмечаем заявки как отправленные"""
        if not request_ids:
            return
        
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        try:
            placeholders = ','.join('?' for _ in request_ids)
            query = f"""
                UPDATE requests 
                SET sent_at = ? 
                WHERE request_id IN ({placeholders})
            """
            cursor.execute(query, [datetime.now()] + request_ids)
            conn.commit()
            logger.info(f"Отмечено как отправлено: {len(request_ids)} заявок")
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