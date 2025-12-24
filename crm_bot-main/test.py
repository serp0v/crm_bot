import asyncio
from crm_parser import CRMParser
from database import Database
import json

async def test_crm_parser():
    """Тестируем парсер CRM"""
    print("Тестирование парсера CRM...")
    
    parser = CRMParser()
    
    # Тестируем авторизацию
    if parser.login():
        print("✓ Авторизация успешна")
        
        # Тестируем получение заявок
        requests = parser.find_all_awaiting_calls()
        print(f"✓ Найдено заявок: {len(requests)}")
        
        if requests:
            print(f"Первая заявка: {requests[0]}")
    else:
        print("✗ Ошибка авторизации")

async def test_database():
    """Тестируем базу данных"""
    print("\nТестирование базы данных...")
    
    db = Database()
    
    # Добавляем тестовую заявку
    test_data = {
        'id': 999999,
        'date': 'Тестовая дата',
        'type': 'Тест'
    }
    
    if db.add_request(999999, json.dumps(test_data)):
        print("✓ Заявка добавлена в БД")
    else:
        print("✗ Ошибка добавления заявки")
    
    # Проверяем существование
    if db.request_exists(999999):
        print("✓ Заявка найдена в БД")
    else:
        print("✗ Заявка не найдена в БД")

if __name__ == "__main__":
    asyncio.run(test_crm_parser())
    asyncio.run(test_database())
