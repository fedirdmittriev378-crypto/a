import os
from flask import current_app
import pandas as pd
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from .models import Recurring, Transaction, TransactionType
from . import db

def save_report_pie(df, filename="income_vs_expense.png"):
    # Проверяем, что данные не пустые
    if df.empty or df['amount'].isna().all() or df['amount'].sum() == 0:
        # Создаём пустой график с сообщением
        fig, ax = plt.subplots()
        ax.text(0.5, 0.5, 'Нет данных для отображения', 
                horizontalalignment='center', verticalalignment='center',
                transform=ax.transAxes, fontsize=14)
        ax.axis('off')
        path = os.path.join(current_app.config['REPORTS_FOLDER'], filename)
        fig.savefig(path, bbox_inches='tight')
        plt.close(fig)
        return path
    
    # Удаляем NaN значения
    df = df.dropna(subset=['amount'])
    df = df[df['amount'] > 0]  # Убираем нулевые значения
    
    if df.empty:
        # Создаём пустой график с сообщением
        fig, ax = plt.subplots()
        ax.text(0.5, 0.5, 'Нет данных для отображения', 
                horizontalalignment='center', verticalalignment='center',
                transform=ax.transAxes, fontsize=14)
        ax.axis('off')
        path = os.path.join(current_app.config['REPORTS_FOLDER'], filename)
        fig.savefig(path, bbox_inches='tight')
        plt.close(fig)
        return path
    
    labels = df['type']
    sizes = df['amount']
    fig, ax = plt.subplots()
    ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90)
    ax.axis('equal')
    path = os.path.join(current_app.config['REPORTS_FOLDER'], filename)
    fig.savefig(path, bbox_inches='tight')
    plt.close(fig)
    return path

def save_category_bar(df, filename="by_category.png"):
    fig, ax = plt.subplots(figsize=(8,4))
    if not df.empty:
        df.plot(kind='bar', legend=False, ax=ax)
    ax.set_ylabel('Amount')
    ax.set_xlabel('Category')
    fig.tight_layout()
    path = os.path.join(current_app.config['REPORTS_FOLDER'], filename)
    fig.savefig(path, bbox_inches='tight')
    plt.close(fig)
    return path

def parse_csv_to_transactions(file_stream):
    df = pd.read_csv(file_stream)
    df.columns = [c.strip().lower() for c in df.columns]
    expected = {'date','amount','type'}
    if not expected.issubset(set(df.columns)):
        raise ValueError(f"CSV должен содержать столбцы: {expected}")
    df['date'] = pd.to_datetime(df['date'])
    df['amount'] = df['amount'].astype(float)
    df['type'] = df['type'].str.lower()
    return df

def _advance_date(d: datetime, frequency: str):
    if frequency == "daily":
        return d + timedelta(days=1)
    if frequency == "weekly":
        return d + timedelta(weeks=1)
    if frequency == "monthly":
        return d + relativedelta(months=1)
    return d + relativedelta(months=1)

def generate_recurring_occurrences(up_to: datetime = None):
    if up_to is None:
        up_to = datetime.combine(date.today(), datetime.min.time())

    recurrings = Recurring.query.filter_by(active=True).all()
    created = 0
    for r in recurrings:
        current = r.next_date or r.start_date
        while current is not None and current <= up_to:
            if r.end_date and current.date() > r.end_date.date():
                break
            t = Transaction(
                date = current,
                amount = r.amount,
                type = r.type,
                category = r.category,
                account = r.account,
                note = (r.note or "") + " (recurring)"
            )
            # preserve owner from recurring rule if present
            if getattr(r, 'user_id', None):
                t.user_id = r.user_id
            # Обновляем баланс счёта если есть
            if r.account:
                if r.type == TransactionType.income:
                    r.account.balance += r.amount
                else:
                    r.account.balance -= r.amount
            db.session.add(t)
            created += 1
            current = _advance_date(current, r.frequency.value if hasattr(r.frequency, 'value') else r.frequency)
        # set next_date
        next_d = current
        if r.end_date and next_d and next_d.date() > r.end_date.date():
            r.active = False
            r.next_date = None
        else:
            r.next_date = next_d
    if created:
        db.session.commit()
    return created
