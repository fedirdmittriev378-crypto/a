"""
Скрипт для миграции базы данных v2
Добавляет новые таблицы для шаблонов, планирования, достижений, уведомлений
"""
# -*- coding: utf-8 -*-
import sqlite3
import os
import sys
from app import create_app, db
from app.models import *

# Fix encoding for Windows console
if sys.platform == 'win32':
    import codecs
    sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')
    sys.stderr = codecs.getwriter('utf-8')(sys.stderr.buffer, 'strict')

def migrate_database():
    app = create_app()
    with app.app_context():
        db_path = app.config['SQLALCHEMY_DATABASE_URI'].replace('sqlite:///', '')
        
        if not os.path.exists(db_path):
            print("Database not found. Creating new one...")
            db.create_all()
            print("Database created!")
            return
        
        print("Updating database v2...")
        
        # Создаём новые таблицы через SQLAlchemy
        db.create_all()
        print("+ New tables created (if needed)")
        
        # Создаём начальные достижения если их нет
        if Achievement.query.count() == 0:
            achievements = [
                Achievement(name="Первая операция", description="Добавьте первую транзакцию", 
                          condition_type="transactions_count", condition_value=1, icon="bi-star"),
                Achievement(name="10 операций", description="Добавьте 10 транзакций", 
                          condition_type="transactions_count", condition_value=10, icon="bi-star-fill"),
                Achievement(name="50 операций", description="Добавьте 50 транзакций", 
                          condition_type="transactions_count", condition_value=50, icon="bi-trophy"),
                Achievement(name="100 операций", description="Добавьте 100 транзакций", 
                          condition_type="transactions_count", condition_value=100, icon="bi-trophy-fill"),
                Achievement(name="7 дней подряд", description="Ведите учёт 7 дней подряд", 
                          condition_type="days_streak", condition_value=7, icon="bi-calendar-check"),
                Achievement(name="30 дней подряд", description="Ведите учёт 30 дней подряд", 
                          condition_type="days_streak", condition_value=30, icon="bi-calendar-heart"),
            ]
            for ach in achievements:
                db.session.add(ach)
            db.session.commit()
            print("+ Initial achievements created")
        
        print("\nMigration v2 completed successfully!")

if __name__ == "__main__":
    migrate_database()

