#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Миграция для расширения модели Debt
Добавляет поля для кредитов и кредитных карт
"""

import sys
import codecs
import sqlite3
from pathlib import Path

# Исправление кодировки для Windows
if sys.platform == 'win32':
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

def migrate():
    db_path = Path("personal_budget.db")
    
    if not db_path.exists():
        print("База данных не найдена. Создайте её через приложение.")
        return
    
    conn = sqlite3.connect(str(db_path))
    cursor = conn.cursor()
    
    try:
        # Проверяем, какие колонки уже есть
        cursor.execute("PRAGMA table_info(debts)")
        columns = [row[1] for row in cursor.fetchall()]
        
        print("Текущие колонки в debts:", columns)
        
        # Добавляем новые колонки, если их нет
        new_columns = {
            'debt_type': 'TEXT DEFAULT "debt"',
            'current_balance': 'REAL',
            'credit_limit': 'REAL',
            'interest_rate': 'REAL',
            'payment_date': 'DATETIME',
            'payment_amount': 'REAL',
            'min_payment': 'REAL',
            'account_id': 'INTEGER',
            'is_active': 'BOOLEAN DEFAULT 1',
        }
        
        for col_name, col_type in new_columns.items():
            if col_name not in columns:
                try:
                    cursor.execute(f"ALTER TABLE debts ADD COLUMN {col_name} {col_type}")
                    print(f"✓ Добавлена колонка: {col_name}")
                except sqlite3.OperationalError as e:
                    print(f"✗ Ошибка при добавлении {col_name}: {e}")
            else:
                print(f"- Колонка {col_name} уже существует")
        
        # Обновляем notes для увеличения размера
        if 'notes' in columns:
            try:
                # SQLite не поддерживает изменение типа колонки напрямую
                # Но можно пересоздать таблицу, если нужно
                print("Примечание: размер поля notes ограничен. Для увеличения потребуется пересоздание таблицы.")
            except Exception as e:
                print(f"Примечание: {e}")
        
        conn.commit()
        print("\n✓ Миграция завершена успешно!")
        
    except Exception as e:
        conn.rollback()
        print(f"\n✗ Ошибка при миграции: {e}")
        raise
    finally:
        conn.close()

if __name__ == "__main__":
    print("Запуск миграции для расширения модели Debt...")
    migrate()

