"""
Database migration script
Adds new columns and tables to existing database
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
        
        print("Updating database...")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        try:
            # Проверяем и добавляем колонки в таблицу categories
            try:
                cursor.execute("ALTER TABLE categories ADD COLUMN color VARCHAR(7) DEFAULT '#6366f1'")
                print("+ Added column: categories.color")
            except sqlite3.OperationalError as e:
                if "duplicate column name" not in str(e).lower():
                    print(f"  Column categories.color already exists or error: {e}")
            
            try:
                cursor.execute("ALTER TABLE categories ADD COLUMN icon VARCHAR(32)")
                print("+ Added column: categories.icon")
            except sqlite3.OperationalError as e:
                if "duplicate column name" not in str(e).lower():
                    print(f"  Column categories.icon already exists or error: {e}")
            
            # Add column to transactions table
            try:
                cursor.execute("ALTER TABLE transactions ADD COLUMN account_id INTEGER")
                print("+ Added column: transactions.account_id")
            except sqlite3.OperationalError as e:
                if "duplicate column name" not in str(e).lower():
                    print(f"  Column transactions.account_id already exists or error: {e}")
            
            # Add columns to goals table
            try:
                cursor.execute("ALTER TABLE goals ADD COLUMN current_amount FLOAT DEFAULT 0.0")
                print("+ Added column: goals.current_amount")
            except sqlite3.OperationalError as e:
                if "duplicate column name" not in str(e).lower():
                    print(f"  Column goals.current_amount already exists or error: {e}")
            
            try:
                cursor.execute("ALTER TABLE goals ADD COLUMN target_date DATETIME")
                print("+ Added column: goals.target_date")
            except sqlite3.OperationalError as e:
                if "duplicate column name" not in str(e).lower():
                    print(f"  Column goals.target_date already exists or error: {e}")
            
            # Add column to recurrings table
            try:
                cursor.execute("ALTER TABLE recurrings ADD COLUMN account_id INTEGER")
                print("+ Added column: recurrings.account_id")
            except sqlite3.OperationalError as e:
                if "duplicate column name" not in str(e).lower():
                    print(f"  Column recurrings.account_id already exists or error: {e}")
            
            conn.commit()
            
            # Create new tables via SQLAlchemy
            db.create_all()
            print("+ New tables created (if needed)")
            
            print("\nMigration completed successfully!")
            
        except Exception as e:
            print(f"\nError during migration: {e}")
            conn.rollback()
            raise
        finally:
            conn.close()

if __name__ == "__main__":
    migrate_database()

