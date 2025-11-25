from flask import current_app as app, render_template, redirect, url_for, request, flash, send_from_directory, jsonify, session, g as flask_g
from .models import (db, User, Category, Transaction, TransactionType, Recurring, Frequency, Goal, Account, Budget, Tag, Debt, DebtType,
                    TransactionTemplate, PlannedExpense, Achievement, Notification)
from .forms import (CategoryForm, TransactionForm, ImportForm, RecurringForm, GoalForm, 
                   AccountForm, BudgetForm, TagForm, DebtForm, SearchForm,
                   TransactionTemplateForm, PlannedExpenseForm, TransferForm)
from datetime import datetime, date, timedelta
from .utils import save_report_pie, save_category_bar, parse_csv_to_transactions, generate_recurring_occurrences
from .notifications import generate_all_notifications
from dateutil.relativedelta import relativedelta
import pandas as pd
import os
import json
from sqlalchemy import func
from sqlalchemy.exc import IntegrityError
from urllib.parse import urlparse, urljoin

@app.before_request
def ensure_recurring_generated():
    # load current user (if any) and генерируем доA текущего дня
    flask_g.user = None
    try:
        if 'user_id' in session:
            flask_g.user = User.query.get(session.get('user_id'))
    except Exception:
        flask_g.user = None

    try:
        generate_recurring_occurrences()
        generate_all_notifications()  # Генерируем уведомления
    except Exception:
        pass


def login_required(fn):
    from functools import wraps
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not getattr(flask_g, 'user', None):
            flash('Требуется авторизация', 'warning')
            return redirect(url_for('login', next=request.path))
        return fn(*args, **kwargs)
    return wrapper


def _is_safe_redirect(target: str | None) -> bool:
    """Allow redirects only inside current host."""
    if not target:
        return False
    ref_url = urlparse(request.host_url)
    test_url = urlparse(urljoin(request.host_url, target))
    return (
        test_url.scheme in ('http', 'https')
        and ref_url.netloc == test_url.netloc
    )

@app.route("/")
def index():
    # Общая статистика — фильтруем по пользователю если авторизован
    if getattr(flask_g, 'user', None):
        total_income = db.session.query(Transaction).filter(Transaction.type == TransactionType.income, Transaction.user_id == flask_g.user.id).with_entities(db.func.sum(Transaction.amount)).scalar() or 0.0
        total_expense = db.session.query(Transaction).filter(Transaction.type == TransactionType.expense, Transaction.user_id == flask_g.user.id).with_entities(db.func.sum(Transaction.amount)).scalar() or 0.0
    else:
        total_income = db.session.query(Transaction).filter(Transaction.type == TransactionType.income).with_entities(db.func.sum(Transaction.amount)).scalar() or 0.0
        total_expense = db.session.query(Transaction).filter(Transaction.type == TransactionType.expense).with_entities(db.func.sum(Transaction.amount)).scalar() or 0.0
    
    # Счета
    accounts = Account.query.filter_by(is_active=True, user_id=(flask_g.user.id if getattr(flask_g, 'user', None) else None)).all() if getattr(flask_g, 'user', None) else Account.query.filter_by(is_active=True).all()
    total_accounts_balance = sum(acc.balance for acc in accounts)
    
    # Общий баланс = сумма всех счетов
    balance = total_accounts_balance
    
    # Статистика за текущий месяц
    today = date.today()
    month_start = datetime(today.year, today.month, 1)
    common_month_filters = [Transaction.date >= month_start]
    if getattr(flask_g, 'user', None):
        common_month_filters.append(Transaction.user_id == flask_g.user.id)

    month_income = db.session.query(Transaction).filter(
        Transaction.type == TransactionType.income,
        *common_month_filters
    ).with_entities(db.func.sum(Transaction.amount)).scalar() or 0.0
    month_expense = db.session.query(Transaction).filter(
        Transaction.type == TransactionType.expense,
        *common_month_filters
    ).with_entities(db.func.sum(Transaction.amount)).scalar() or 0.0
    
    # Последние операции
    recent_transactions = Transaction.query.filter_by(user_id=flask_g.user.id).order_by(Transaction.date.desc()).limit(5).all() if getattr(flask_g, 'user', None) else Transaction.query.order_by(Transaction.date.desc()).limit(5).all()
    
    # Расширенная статистика
    # Сравнение с прошлым месяцом
    if today.month == 1:
        last_month_start = datetime(today.year - 1, 12, 1)
        last_month_end = datetime(today.year, 1, 1)
    else:
        last_month_start = datetime(today.year, today.month - 1, 1)
        last_month_end = datetime(today.year, today.month, 1)
    
    last_month_income = db.session.query(Transaction).filter(
        Transaction.type == TransactionType.income,
        Transaction.date >= last_month_start,
        Transaction.date < last_month_end
    ).with_entities(db.func.sum(Transaction.amount)).scalar() or 0.0
    
    last_month_expense = db.session.query(Transaction).filter(
        Transaction.type == TransactionType.expense,
        Transaction.date >= last_month_start,
        Transaction.date < last_month_end
    ).with_entities(db.func.sum(Transaction.amount)).scalar() or 0.0
    
    # Средние расходы по категориям
    avg_expenses_by_category = {}
    categories_with_expenses = db.session.query(
        Category.id, Category.name,
        db.func.avg(Transaction.amount).label('avg_amount'),
        db.func.count(Transaction.id).label('count')
    ).join(Transaction).filter(
        Transaction.type == TransactionType.expense
    ).group_by(Category.id, Category.name).all()
    
    for cat_id, cat_name, avg_amount, count in categories_with_expenses:
        avg_expenses_by_category[cat_name] = {
            'avg': float(avg_amount) if avg_amount else 0.0,
            'count': count
        }
    
    # Топ-5 самых дорогих операций
    top_expenses = Transaction.query.filter(
        Transaction.type == TransactionType.expense
    ).order_by(Transaction.amount.desc()).limit(5).all()
    
    # Уведомления
    unread_notifications = Notification.query.filter_by(is_read=False, user_id=(flask_g.user.id if getattr(flask_g, 'user', None) else None)).order_by(Notification.created_at.desc()).limit(5).all() if getattr(flask_g, 'user', None) else Notification.query.filter_by(is_read=False).order_by(Notification.created_at.desc()).limit(5).all()
    
    # Активные цели
    active_goals = Goal.query.filter_by(active=True, user_id=(flask_g.user.id if getattr(flask_g, 'user', None) else None)).all() if getattr(flask_g, 'user', None) else Goal.query.filter_by(active=True).all()
    goals_data = []
    for g in active_goals:
        progress = g.current_amount
        percent = min(100.0, (progress / g.target_amount * 100.0) if g.target_amount > 0 else 0.0)
        goals_data.append({
            "goal": g,
            "progress": progress,
            "percent": percent,
            "remaining": max(0, g.target_amount - progress)
        })
    
    # Бюджеты
    if getattr(flask_g, 'user', None):
        current_budgets = Budget.query.filter(
            Budget.user_id == flask_g.user.id,
            Budget.is_active == True,
            Budget.period_start <= datetime.now(),
            Budget.period_end >= datetime.now()
        ).all()
    else:
        current_budgets = Budget.query.filter(
            Budget.is_active == True,
            Budget.period_start <= datetime.now(),
            Budget.period_end >= datetime.now()
        ).all()
    budgets_data = []
    for b in current_budgets:
        spent = db.session.query(Transaction).filter(
            Transaction.category_id == b.category_id,
            Transaction.type == TransactionType.expense,
            Transaction.date >= b.period_start,
            Transaction.date <= b.period_end
        ).with_entities(db.func.sum(Transaction.amount)).scalar() or 0.0
        percent = min(100.0, (spent / b.amount * 100.0) if b.amount > 0 else 0.0)
        budgets_data.append({
            "budget": b,
            "spent": spent,
            "remaining": max(0, b.amount - spent),
            "percent": percent
        })
    
    currency = app.config.get("DEFAULT_CURRENCY", "RUB")
    return render_template("index.html", 
                         income=total_income, expense=total_expense, balance=balance,
                         month_income=month_income, month_expense=month_expense,
                         last_month_income=last_month_income, last_month_expense=last_month_expense,
                         accounts=accounts, total_accounts_balance=total_accounts_balance,
                         recent_transactions=recent_transactions,
                         goals_data=goals_data, budgets_data=budgets_data,
                         avg_expenses_by_category=avg_expenses_by_category,
                         top_expenses=top_expenses,
                         unread_notifications=unread_notifications,
                         unread_count=len(unread_notifications),
                         currency=currency)

@app.route("/transactions")
def transactions():
    form = SearchForm()
    if getattr(flask_g, 'user', None):
        categories = Category.query.filter(db.or_(Category.user_id == None, Category.user_id == flask_g.user.id)).order_by(Category.name).all()
        accounts = Account.query.filter_by(is_active=True, user_id=flask_g.user.id).order_by(Account.name).all()
    else:
        categories = Category.query.order_by(Category.name).all()
        accounts = Account.query.filter_by(is_active=True).order_by(Account.name).all()
    form.category.choices = [(0, "Все категории")] + [(c.id, c.name) for c in categories]
    form.account.choices = [(0, "Все счета")] + [(a.id, a.name) for a in accounts]
    
    # Быстрые фильтры
    quick_filter = request.args.get('quick_filter', '')
    today = date.today()
    
    # Поиск и фильтрация
    qs = Transaction.query
    if getattr(flask_g, 'user', None):
        qs = qs.filter(Transaction.user_id == flask_g.user.id)
    
    # Применяем быстрый фильтр
    if quick_filter == 'today':
        qs = qs.filter(db.func.date(Transaction.date) == today)
    elif quick_filter == 'week':
        week_start = today - timedelta(days=today.weekday())
        qs = qs.filter(Transaction.date >= datetime.combine(week_start, datetime.min.time()))
    elif quick_filter == 'month':
        month_start = datetime(today.year, today.month, 1)
        qs = qs.filter(Transaction.date >= month_start)
    elif quick_filter == 'year':
        year_start = datetime(today.year, 1, 1)
        qs = qs.filter(Transaction.date >= year_start)
    elif quick_filter == 'income_only':
        qs = qs.filter(Transaction.type == TransactionType.income)
    elif quick_filter == 'expense_only':
        qs = qs.filter(Transaction.type == TransactionType.expense)
    
    if request.args.get('query'):
        search = f"%{request.args.get('query')}%"
        qs = qs.join(Category, Transaction.category_id == Category.id, isouter=True).filter(db.or_(
            Transaction.note.like(search),
            Category.name.like(search)
        ))
    
    if request.args.get('category') and request.args.get('category') != '0':
        qs = qs.filter(Transaction.category_id == request.args.get('category'))
    
    if request.args.get('type') and not quick_filter:
        qs = qs.filter(Transaction.type == TransactionType[request.args.get('type')])
    
    if request.args.get('account') and request.args.get('account') != '0':
        qs = qs.filter(Transaction.account_id == request.args.get('account'))
    
    if request.args.get('date_from'):
        try:
            date_from = datetime.strptime(request.args.get('date_from'), '%Y-%m-%d')
            qs = qs.filter(Transaction.date >= date_from)
        except:
            pass
    
    if request.args.get('date_to'):
        try:
            date_to = datetime.strptime(request.args.get('date_to'), '%Y-%m-%d')
            qs = qs.filter(Transaction.date <= date_to)
        except:
            pass
    
    # Фильтр по диапазону сумм
    if request.args.get('amount_from'):
        try:
            amount_from = float(request.args.get('amount_from'))
            qs = qs.filter(Transaction.amount >= amount_from)
        except:
            pass
    
    if request.args.get('amount_to'):
        try:
            amount_to = float(request.args.get('amount_to'))
            qs = qs.filter(Transaction.amount <= amount_to)
        except:
            pass
    
    transactions_list = qs.order_by(Transaction.date.desc()).all()
    
    # Заполняем форму значениями из запроса
    if request.args:
        form.query.data = request.args.get('query', '')
        form.type.data = request.args.get('type', '')
        if request.args.get('category'):
            form.category.data = int(request.args.get('category'))
        if request.args.get('account'):
            form.account.data = int(request.args.get('account'))
        if request.args.get('date_from'):
            try:
                form.date_from.data = datetime.strptime(request.args.get('date_from'), '%Y-%m-%d').date()
            except:
                pass
        if request.args.get('date_to'):
            try:
                form.date_to.data = datetime.strptime(request.args.get('date_to'), '%Y-%m-%d').date()
            except:
                pass
        if request.args.get('amount_from'):
            try:
                form.amount_from.data = float(request.args.get('amount_from'))
            except:
                pass
        if request.args.get('amount_to'):
            try:
                form.amount_to.data = float(request.args.get('amount_to'))
            except:
                pass
    
    currency = app.config.get("DEFAULT_CURRENCY", "RUB")
    return render_template("transactions.html", transactions=transactions_list, form=form, categories=categories, accounts=accounts, currency=currency, quick_filter=quick_filter)

@app.route("/calendar")
def calendar():
    year = int(request.args.get('year', date.today().year))
    month = int(request.args.get('month', date.today().month))
    
    # Первый день месяца
    month_start = date(year, month, 1)
    # Последний день месяца
    if month == 12:
        month_end = date(year + 1, 1, 1) - timedelta(days=1)
    else:
        month_end = date(year, month + 1, 1) - timedelta(days=1)
    
    # Получаем все операции за месяц
    transactions = Transaction.query.filter(
        Transaction.date >= datetime.combine(month_start, datetime.min.time()),
        Transaction.date <= datetime.combine(month_end, datetime.max.time())
    ).all()
    
    # Группируем по дням
    transactions_by_date = {}
    for t in transactions:
        day = t.date.date()
        if day not in transactions_by_date:
            transactions_by_date[day] = {'income': 0, 'expense': 0, 'count': 0}
        if t.type == TransactionType.income:
            transactions_by_date[day]['income'] += t.amount
        else:
            transactions_by_date[day]['expense'] += t.amount
        transactions_by_date[day]['count'] += 1
    
    # Вычисляем первый день недели месяца
    first_weekday = month_start.weekday()  # 0 = понедельник, 6 = воскресенье
    
    # Создаём календарь
    calendar_days = []
    # Пустые дни в начале месяца
    for _ in range(first_weekday):
        calendar_days.append(None)
    
    # Дни месяца
    current_date = month_start
    while current_date <= month_end:
        day_data = transactions_by_date.get(current_date, {'income': 0, 'expense': 0, 'count': 0})
        calendar_days.append({
            'date': current_date,
            'income': day_data['income'],
            'expense': day_data['expense'],
            'count': day_data['count'],
            'net': day_data['income'] - day_data['expense']
        })
        current_date += timedelta(days=1)
    
    # Навигация по месяцам
    if month == 1:
        prev_month = 12
        prev_year = year - 1
    else:
        prev_month = month - 1
        prev_year = year
    
    if month == 12:
        next_month = 1
        next_year = year + 1
    else:
        next_month = month + 1
        next_year = year
    
    currency = app.config.get("DEFAULT_CURRENCY", "RUB")
    return render_template("calendar.html", 
                         calendar_days=calendar_days, 
                         year=year, 
                         month=month,
                         month_start=month_start,
                         month_end=month_end,
                         prev_month=prev_month,
                         prev_year=prev_year,
                         next_month=next_month,
                         next_year=next_year,
                         transactions_by_date=transactions_by_date,
                         currency=currency)

@app.route("/transaction/add", methods=["GET","POST"])
@login_required
def add_transaction():
    form = TransactionForm()
    categories = Category.query.filter(db.or_(Category.user_id == None, Category.user_id == flask_g.user.id)).order_by(Category.name).all() if getattr(flask_g, 'user', None) else Category.query.order_by(Category.name).all()
    accounts = Account.query.filter_by(is_active=True, user_id=flask_g.user.id).order_by(Account.name).all() if getattr(flask_g, 'user', None) else Account.query.filter_by(is_active=True).order_by(Account.name).all()
    
    form.category.choices = [(0, "— без категории —")] + [(c.id, c.name) for c in categories]
    form.account.choices = [(0, "— без счёта —")] + [(a.id, a.name) for a in accounts]
    
    if form.validate_on_submit():
        cat = None
        if form.category.data and form.category.data != 0:
            cat = Category.query.get(form.category.data)
        
        acc = None
        if form.account.data and form.account.data != 0:
            acc = Account.query.get(form.account.data)
            # Обновляем баланс счёта
            if acc:
                if form.type.data == "income":
                    acc.balance += form.amount.data
                else:
                    acc.balance -= form.amount.data
        
        t = Transaction(
            date = datetime.combine(form.date.data, datetime.min.time()),
            amount = form.amount.data,
            type = TransactionType.income if form.type.data == "income" else TransactionType.expense,
            category = cat,
            account = acc,
            note = form.note.data
        )
        if getattr(flask_g, 'user', None):
            t.user_id = flask_g.user.id
        db.session.add(t)
        db.session.commit()
        flash("Операция сохранена", "success")
        
        # Если нажата кнопка "Сохранить и добавить ещё", возвращаемся на форму с сохранёнными данными
        if request.form.get('submit_and_add'):
            # Сохраняем данные в сессию для предзаполнения формы
            session['last_transaction'] = {
                'date': form.date.data.isoformat() if form.date.data else None,
                'type': form.type.data,
                'category': form.category.data if form.category.data and form.category.data != 0 else None,
                'account': form.account.data if form.account.data and form.account.data != 0 else None,
            }
            return redirect(url_for("add_transaction"))
        
        return redirect(url_for("transactions"))
    if request.method == "GET":
        # Проверяем, есть ли сохранённые данные из предыдущей транзакции
        if 'last_transaction' in session:
            last_data = session.pop('last_transaction')
            if last_data.get('date'):
                try:
                    form.date.data = datetime.strptime(last_data['date'], '%Y-%m-%d').date()
                except:
                    form.date.data = datetime.today().date()
            else:
                form.date.data = datetime.today().date()
            
            if last_data.get('type'):
                form.type.data = last_data['type']
            
            if last_data.get('category'):
                form.category.data = last_data['category']
            
            if last_data.get('account'):
                form.account.data = last_data['account']
        else:
            form.date.data = datetime.today().date()
            # Автоматически выбираем последний использованный счёт для расходов
            if form.type.data == "expense" or not form.type.data:
                if getattr(flask_g, 'user', None):
                    last_expense = Transaction.query.filter(
                        Transaction.user_id == flask_g.user.id,
                        Transaction.type == TransactionType.expense,
                        Transaction.account_id.isnot(None)
                    ).order_by(Transaction.date.desc(), Transaction.id.desc()).first()
                else:
                    last_expense = Transaction.query.filter(
                    Transaction.type == TransactionType.expense,
                    Transaction.account_id.isnot(None)
                    ).order_by(Transaction.date.desc(), Transaction.id.desc()).first()
                if last_expense and last_expense.account:
                    form.account.data = last_expense.account.id
    currency = app.config.get("DEFAULT_CURRENCY", "RUB")
    return render_template("add_transaction.html", form=form, currency=currency)

@app.route("/transaction/edit/<int:trans_id>", methods=["GET","POST"])
@login_required
def edit_transaction(trans_id):
    t = Transaction.query.get_or_404(trans_id)
    form = TransactionForm()
    
    categories = Category.query.filter(db.or_(Category.user_id == None, Category.user_id == flask_g.user.id)).order_by(Category.name).all() if getattr(flask_g, 'user', None) else Category.query.order_by(Category.name).all()
    accounts = Account.query.filter_by(is_active=True, user_id=flask_g.user.id).order_by(Account.name).all() if getattr(flask_g, 'user', None) else Account.query.filter_by(is_active=True).order_by(Account.name).all()
    
    form.category.choices = [(0, "— без категории —")] + [(c.id, c.name) for c in categories]
    form.account.choices = [(0, "— без счёта —")] + [(a.id, a.name) for a in accounts]
    
    if request.method == "GET":
        # Заполняем форму текущими значениями транзакции
        form.date.data = t.date.date() if t.date else datetime.today().date()
        form.amount.data = t.amount
        form.type.data = t.type.value
        form.note.data = t.note
        
        # Устанавливаем категорию и счёт
        if t.category:
            form.category.data = t.category.id
        else:
            form.category.data = 0
        
        if t.account:
            form.account.data = t.account.id
        else:
            form.account.data = 0
    
    if form.validate_on_submit():
        # Сохраняем старые значения для отката баланса
        old_account = t.account
        old_amount = t.amount
        old_type = t.type
        
        # Откатываем изменения баланса старого счёта
        if old_account:
            if old_type == TransactionType.income:
                old_account.balance -= old_amount
            else:
                old_account.balance += old_amount
        
        # Обновляем транзакцию
        cat = None
        if form.category.data and form.category.data != 0:
            cat = Category.query.get(form.category.data)
        
        acc = None
        if form.account.data and form.account.data != 0:
            acc = Account.query.get(form.account.data)
        
        # Обновляем поля транзакции (дата обновляется из формы)
        t.date = datetime.combine(form.date.data, datetime.min.time())
        t.amount = form.amount.data
        t.type = TransactionType.income if form.type.data == "income" else TransactionType.expense
        t.category = cat
        t.account = acc
        t.note = form.note.data
        if getattr(flask_g, 'user', None):
            t.user_id = flask_g.user.id
        
        # Обновляем баланс нового счёта (или того же, если не изменился)
        if acc:
            if t.type == TransactionType.income:
                acc.balance += t.amount
            else:
                acc.balance -= t.amount
        
        db.session.commit()
        flash("Операция обновлена", "success")
        return redirect(url_for("transactions"))
    
    currency = app.config.get("DEFAULT_CURRENCY", "RUB")
    return render_template("add_transaction.html", form=form, transaction=t, edit=True, currency=currency)

@app.route("/transaction/delete/<int:trans_id>", methods=["POST"])
@login_required
def delete_transaction(trans_id):
    t = Transaction.query.get_or_404(trans_id)
    
    # Откатываем изменения баланса счёта
    if t.account:
        if t.type == TransactionType.income:
            t.account.balance -= t.amount
        else:
            t.account.balance += t.amount
    
    db.session.delete(t)
    db.session.commit()
    flash("Операция удалена", "success")
    return redirect(url_for("transactions"))

# Быстрое добавление категории через AJAX
@app.route("/api/category/quick-add", methods=["POST"])
def add_quick_category():
    data = request.get_json()
    name = data.get('name', '').strip()
    color = data.get('color', '#6366f1')
    
    if not name:
        return jsonify({'success': False, 'error': 'Название категории обязательно'})
    
    # Проверяем, не существует ли уже такая категория у текущего пользователя
    if getattr(flask_g, 'user', None):
        existing = Category.query.filter_by(name=name, user_id=flask_g.user.id).first()
    else:
        existing = Category.query.filter_by(name=name).first()
    if existing:
        return jsonify({'success': False, 'error': 'Категория с таким именем уже существует'})
    
    try:
        cat = Category(name=name, color=color)
        if getattr(flask_g, 'user', None):
            cat.user_id = flask_g.user.id
        db.session.add(cat)
        db.session.commit()
        return jsonify({'success': True, 'category_id': cat.id, 'name': cat.name})
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'error': str(e)})

# Быстрое добавление счёта через AJAX
@app.route("/api/account/quick-add", methods=["POST"])
def add_quick_account():
    data = request.get_json()
    name = data.get('name', '').strip()
    balance = float(data.get('balance', 0))
    currency = data.get('currency', 'RUB')
    
    if not name:
        return jsonify({'success': False, 'error': 'Название счёта обязательно'})
    
    try:
        acc = Account(name=name, balance=balance, currency=currency)
        if getattr(flask_g, 'user', None):
            acc.user_id = flask_g.user.id
        db.session.add(acc)
        db.session.commit()
        return jsonify({'success': True, 'account_id': acc.id, 'name': acc.name})
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'error': str(e)})

# Массовое редактирование операций
@app.route("/api/transactions/bulk-edit", methods=["POST"])
@login_required
def bulk_edit_transactions():
    data = request.get_json()
    transaction_ids = data.get('transaction_ids', [])
    category_id = data.get('category_id')
    account_id = data.get('account_id')
    
    if not transaction_ids:
        return jsonify({'success': False, 'error': 'Не выбраны операции'})
    
    try:
        transactions = Transaction.query.filter(Transaction.id.in_(transaction_ids)).all()
        updated = 0
        
        for t in transactions:
            if category_id:
                cat = Category.query.get(category_id)
                if cat:
                    t.category = cat
                    updated += 1
            
            if account_id:
                # Откатываем изменения баланса старого счёта
                if t.account:
                    if t.type == TransactionType.income:
                        t.account.balance -= t.amount
                    else:
                        t.account.balance += t.amount
                
                # Обновляем баланс нового счёта
                new_acc = Account.query.get(account_id)
                if new_acc:
                    t.account = new_acc
                    if t.type == TransactionType.income:
                        new_acc.balance += t.amount
                    else:
                        new_acc.balance -= t.amount
                    updated += 1
        
        db.session.commit()
        return jsonify({'success': True, 'updated': updated})
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'error': str(e)})

# Массовое удаление операций
@app.route("/api/transactions/bulk-delete", methods=["POST"])
@login_required
def bulk_delete_transactions():
    data = request.get_json()
    transaction_ids = data.get('transaction_ids', [])
    
    if not transaction_ids:
        return jsonify({'success': False, 'error': 'Не выбраны операции'})
    
    try:
        transactions = Transaction.query.filter(Transaction.id.in_(transaction_ids)).all()
        deleted = 0
        
        for t in transactions:
            # Откатываем изменения баланса счёта
            if t.account:
                if t.type == TransactionType.income:
                    t.account.balance -= t.amount
                else:
                    t.account.balance += t.amount
            db.session.delete(t)
            deleted += 1
        
        db.session.commit()
        return jsonify({'success': True, 'deleted': deleted})
    except Exception as e:
        db.session.rollback()
        return jsonify({'success': False, 'error': str(e)})

# API для получения операций за день (для календаря)
@app.route("/api/transactions/by-date")
def transactions_by_date():
    date_str = request.args.get('date')
    if not date_str:
        return jsonify({'error': 'Не указана дата'}), 400
    
    try:
        target_date = datetime.strptime(date_str, '%Y-%m-%d').date()
        transactions = Transaction.query.filter(
            db.func.date(Transaction.date) == target_date
        ).order_by(Transaction.date).all()
        
        result = []
        for t in transactions:
            result.append({
                'id': t.id,
                'time': t.date.strftime('%H:%M') if t.date else None,
                'type': t.type.value,
                'amount': float(t.amount),
                'currency': t.account.currency if t.account else app.config.get("DEFAULT_CURRENCY", "RUB"),
                'category': t.category.name if t.category else None,
                'note': t.note or None
            })
        
        return jsonify({'transactions': result})
    except Exception as e:
        return jsonify({'error': str(e)}), 400

@app.route("/categories", methods=["GET","POST"])
def categories():
    form = CategoryForm()
    if form.validate_on_submit():
        name = form.name.data.strip()
        # проверяем с учетом пользователя (если авторизован) — пользователь может иметь свои категории
        if getattr(flask_g, 'user', None):
            exists = Category.query.filter_by(name=name, user_id=flask_g.user.id).first()
        else:
            exists = Category.query.filter_by(name=name).first()
        if exists:
            flash("Категория с таким именем уже есть", "warning")
        else:
            c = Category(
                name=name,
                color=form.color.data or "#6366f1",
                icon=form.icon.data or None
            )
            if getattr(flask_g, 'user', None):
                c.user_id = flask_g.user.id
            db.session.add(c)
            db.session.commit()
            flash("Категория добавлена", "success")
            return redirect(url_for("categories"))
    cats = Category.query.filter(db.or_(Category.user_id == None, Category.user_id == flask_g.user.id)).order_by(Category.name).all() if getattr(flask_g, 'user', None) else Category.query.order_by(Category.name).all()
    return render_template("categories.html", form=form, categories=cats)

@app.route("/categories/delete/<int:cat_id>", methods=["POST"])
def delete_category(cat_id):
    cat = Category.query.get_or_404(cat_id)
    db.session.delete(cat)
    db.session.commit()
    flash("Категория удалена", "success")
    return redirect(url_for("categories"))

@app.route("/import", methods=["GET","POST"])
@login_required
def import_csv():
    form = ImportForm()
    if form.validate_on_submit():
        f = request.files.get(form.csv_file.name)
        if not f:
            flash("Файл не получен", "danger")
            return redirect(url_for("import_csv"))
        try:
            df = parse_csv_to_transactions(f)
        except Exception as e:
            flash(f"Ошибка при чтении CSV: {e}", "danger")
            return redirect(url_for("import_csv"))
        for _, row in df.iterrows():
            cat = None
            if 'category' in df.columns and pd.notna(row.get('category')):
                cat_name = str(row.get('category')).strip()
                if cat_name:
                    if getattr(flask_g, 'user', None):
                        cat = Category.query.filter_by(name=cat_name, user_id=flask_g.user.id).first()
                    else:
                        cat = Category.query.filter_by(name=cat_name).first()
                    if not cat:
                        cat = Category(name=cat_name)
                        if getattr(flask_g, 'user', None):
                            cat.user_id = flask_g.user.id
                        db.session.add(cat)
                        db.session.flush()
            t = Transaction(
                date = row['date'].to_pydatetime(),
                amount = float(row['amount']),
                type = TransactionType.income if str(row['type']).lower().startswith('i') else TransactionType.expense,
                category = cat,
                note = str(row.get('note')) if 'note' in df.columns else None
            )
            if getattr(flask_g, 'user', None):
                t.user_id = flask_g.user.id
            db.session.add(t)
        db.session.commit()
        flash(f"Импортировано {len(df)} записей", "success")
        return redirect(url_for("transactions"))
    return render_template("import.html", form=form)

@app.route("/export")
def export_csv():
    qs = Transaction.query.order_by(Transaction.date.desc())
    if getattr(flask_g, 'user', None):
        qs = qs.filter(Transaction.user_id == flask_g.user.id)
    qs = qs.all()
    rows = []
    for t in qs:
        rows.append({
            "date": t.date.strftime("%Y-%m-%d"),
            "amount": t.amount,
            "type": t.type.value,
            "category": t.category.name if t.category else "",
            "account": t.account.name if t.account else "",
            "note": t.note or ""
        })
    df = pd.DataFrame(rows)
    csv_path = os.path.join(app.config['REPORTS_FOLDER'], "export.csv")
    df.to_csv(csv_path, index=False)
    return send_from_directory(directory=app.config['REPORTS_FOLDER'], path="export.csv", as_attachment=True)

# API для графиков
@app.route("/api/chart/income-expense")
def api_chart_income_expense():
    try:
        month = int(request.args.get("month", 0))
        year = int(request.args.get("year", 0))
    except:
        month = 0; year = 0
    today = date.today()
    if not month or not year:
        month = today.month
        year = today.year
    
    start = datetime(year, month, 1)
    if month == 12:
        end = datetime(year+1, 1, 1)
    else:
        end = datetime(year, month+1, 1)
    
    common_filters = [Transaction.date >= start, Transaction.date < end]
    if getattr(flask_g, 'user', None):
        common_filters.append(Transaction.user_id == flask_g.user.id)

    income = db.session.query(Transaction).filter(
        Transaction.type == TransactionType.income,
        *common_filters
    ).with_entities(db.func.sum(Transaction.amount)).scalar() or 0.0
    
    expense = db.session.query(Transaction).filter(
        Transaction.type == TransactionType.expense,
        *common_filters
    ).with_entities(db.func.sum(Transaction.amount)).scalar() or 0.0
    
    return jsonify({
        "income": float(income),
        "expense": float(expense)
    })

@app.route("/api/chart/categories")
def api_chart_categories():
    try:
        month = int(request.args.get("month", 0))
        year = int(request.args.get("year", 0))
    except:
        month = 0; year = 0
    today = date.today()
    if not month or not year:
        month = today.month
        year = today.year
    
    start = datetime(year, month, 1)
    if month == 12:
        end = datetime(year+1, 1, 1)
    else:
        end = datetime(year, month+1, 1)
    
    filters = [
        Transaction.type == TransactionType.expense,
        Transaction.date >= start,
        Transaction.date < end
    ]
    if getattr(flask_g, 'user', None):
        filters.append(Transaction.user_id == flask_g.user.id)
    qs = Transaction.query.filter(*filters).all()
    
    categories_data = {}
    categories_info = {}  # Для хранения цвета и иконки категории
    for t in qs:
        if t.category:
            cat_name = t.category.name
            if cat_name not in categories_data:
                categories_data[cat_name] = 0
                categories_info[cat_name] = {
                    'color': t.category.color or '#6366f1',
                    'icon': t.category.icon or 'bi-circle'
                }
            categories_data[cat_name] += t.amount
        else:
            cat_name = "Без категории"
            if cat_name not in categories_data:
                categories_data[cat_name] = 0
                categories_info[cat_name] = {
                    'color': '#8b5cf6',
                    'icon': 'bi-question-circle'
                }
            categories_data[cat_name] += t.amount
    
    # Возвращаем данные с информацией о категориях
    result = {
        'data': categories_data,
        'info': categories_info
    }
    return jsonify(result)

@app.route("/api/chart/trends")
def api_chart_trends():
    # Данные за последние 6 месяцев
    today = date.today()
    months_data = []
    for i in range(5, -1, -1):
        month_date = today - relativedelta(months=i)
        month_start = datetime(month_date.year, month_date.month, 1)
        if month_date.month == 12:
            month_end = datetime(month_date.year+1, 1, 1)
        else:
            month_end = datetime(month_date.year, month_date.month+1, 1)
        
        common_filters = [Transaction.date >= month_start, Transaction.date < month_end]
        if getattr(flask_g, 'user', None):
            common_filters.append(Transaction.user_id == flask_g.user.id)

        income = db.session.query(Transaction).filter(
            Transaction.type == TransactionType.income,
            *common_filters
        ).with_entities(db.func.sum(Transaction.amount)).scalar() or 0.0
        
        expense = db.session.query(Transaction).filter(
            Transaction.type == TransactionType.expense,
            *common_filters
        ).with_entities(db.func.sum(Transaction.amount)).scalar() or 0.0
        
        months_data.append({
            "month": month_date.strftime("%Y-%m"),
            "label": month_date.strftime("%b %Y"),
            "income": float(income),
            "expense": float(expense)
        })
    
    return jsonify(months_data)

@app.route("/report", methods=["GET","POST"])
def report():
    # месяц/год из query params или форма
    try:
        month = int(request.args.get("month", 0))
        year = int(request.args.get("year", 0))
    except:
        month = 0; year = 0
    today = date.today()
    if not month or not year:
        month = today.month
        year = today.year

    # date range for chosen month
    start = datetime(year, month, 1)
    if month == 12:
        end = datetime(year+1, 1, 1)
    else:
        end = datetime(year, month+1, 1)

    # Прошлый месяц для сравнения
    if month == 1:
        prev_month_start = datetime(year - 1, 12, 1)
        prev_month_end = datetime(year, 1, 1)
        prev_month = 12
        prev_year = year - 1
    else:
        prev_month_start = datetime(year, month - 1, 1)
        prev_month_end = datetime(year, month, 1)
        prev_month = month - 1
        prev_year = year

    # Следующий месяц
    if month == 12:
        next_month = 1
        next_year = year + 1
    else:
        next_month = month + 1
        next_year = year

    qs = Transaction.query.filter(Transaction.date >= start, Transaction.date < end)
    if getattr(flask_g, 'user', None):
        qs = qs.filter(Transaction.user_id == flask_g.user.id)
    qs = qs.all()
    if not qs:
        flash("Нет операций за выбранный месяц", "warning")
    data = []
    for t in qs:
        data.append({
            "date": t.date,
            "amount": t.amount,
            "type": t.type.value,
            "category": t.category.name if t.category else "Без категории"
        })
    df = pd.DataFrame(data) if data else pd.DataFrame(columns=['date','amount','type','category'])

    # Данные за прошлый месяц
    prev_qs = Transaction.query.filter(Transaction.date >= prev_month_start, Transaction.date < prev_month_end)
    if getattr(flask_g, 'user', None):
        prev_qs = prev_qs.filter(Transaction.user_id == flask_g.user.id)
    prev_qs = prev_qs.all()
    prev_data = []
    for t in prev_qs:
        prev_data.append({
            "date": t.date,
            "amount": t.amount,
            "type": t.type.value,
            "category": t.category.name if t.category else "Без категории"
        })
    prev_df = pd.DataFrame(prev_data) if prev_data else pd.DataFrame(columns=['date','amount','type','category'])

    # aggregate by type
    agg_type = df.groupby('type')['amount'].sum().reset_index() if not df.empty else pd.DataFrame({'type':['income','expense'],'amount':[0,0]})
    prev_agg_type = prev_df.groupby('type')['amount'].sum().reset_index() if not prev_df.empty else pd.DataFrame({'type':['income','expense'],'amount':[0,0]})
    pie_path = save_report_pie(agg_type, filename=f"pie_{year}_{month}.png")

    # aggregate by category
    if not df.empty:
        by_cat = df.groupby('category')['amount'].sum().sort_values(ascending=False).to_frame()
    else:
        by_cat = pd.DataFrame(columns=['amount'])
    bar_path = save_category_bar(by_cat, filename=f"bar_{year}_{month}.png")

    # Расчёт доходов и расходов для текущего и прошлого месяца
    current_income = float(agg_type[agg_type['type'] == 'income']['amount'].sum()) if not agg_type.empty and 'income' in agg_type['type'].values else 0.0
    current_expense = float(agg_type[agg_type['type'] == 'expense']['amount'].sum()) if not agg_type.empty and 'expense' in agg_type['type'].values else 0.0
    prev_income = float(prev_agg_type[prev_agg_type['type'] == 'income']['amount'].sum()) if not prev_agg_type.empty and 'income' in prev_agg_type['type'].values else 0.0
    prev_expense = float(prev_agg_type[prev_agg_type['type'] == 'expense']['amount'].sum()) if not prev_agg_type.empty and 'expense' in prev_agg_type['type'].values else 0.0

    # goals and progress (total contributions up to end of chosen month)
    goals = Goal.query.filter_by(user_id=(flask_g.user.id if getattr(flask_g, 'user', None) else None)).order_by(Goal.id.desc()).all() if getattr(flask_g, 'user', None) else Goal.query.order_by(Goal.id.desc()).all()
    goals_progress = []
    for g in goals:
        if g.category:
            s = db.session.query(Transaction).filter(
                Transaction.category_id == g.category.id,
                Transaction.type == TransactionType.income,
                Transaction.date <= end - pd.Timedelta(days=1)
            ).with_entities(db.func.sum(Transaction.amount)).scalar() or 0.0
            progress = float(s)
        else:
            progress = 0.0
        goals_progress.append({
            "goal": g,
            "progress": progress,
            "percent": min(100.0, (progress / g.target_amount * 100.0) if g.target_amount > 0 else 0.0)
        })

    # provide month/year selectors
    years = list(range(today.year-5, today.year+2))
    months = list(range(1,13))
    currency = app.config.get("DEFAULT_CURRENCY", "RUB")
    month_names = {
        1: "Январь", 2: "Февраль", 3: "Март", 4: "Апрель",
        5: "Май", 6: "Июнь", 7: "Июль", 8: "Август",
        9: "Сентябрь", 10: "Октябрь", 11: "Ноябрь", 12: "Декабрь"
    }
    return render_template("report.html", pie_url=os.path.basename(pie_path), bar_url=os.path.basename(bar_path),
                           by_cat=by_cat, month=month, year=year, months=months, years=years,
                           goals_progress=goals_progress, currency=currency,
                           month_names=month_names, prev_month=prev_month, prev_year=prev_year,
                           next_month=next_month, next_year=next_year,
                           current_income=current_income, current_expense=current_expense,
                           prev_income=prev_income, prev_expense=prev_expense)

# Recurring
@app.route("/recurring")
def recurring_list():
    recs = Recurring.query.filter_by(user_id=flask_g.user.id).order_by(Recurring.id.desc()).all() if getattr(flask_g, 'user', None) else Recurring.query.order_by(Recurring.id.desc()).all()
    return render_template("recurring_list.html", recurrings=recs)

@app.route("/recurring/add", methods=["GET","POST"])
@login_required
def add_recurring():
    form = RecurringForm()
    categories = Category.query.order_by(Category.name).all()
    accounts = Account.query.filter_by(is_active=True, user_id=flask_g.user.id).order_by(Account.name).all() if getattr(flask_g, 'user', None) else Account.query.filter_by(is_active=True).order_by(Account.name).all()
    
    form.category.choices = [(0, "— без категории —")] + [(c.id, c.name) for c in categories]
    form.account.choices = [(0, "— без счёта —")] + [(a.id, a.name) for a in accounts]
    
    if form.validate_on_submit():
        cat = None
        if form.category.data and form.category.data != 0:
            cat = Category.query.get(form.category.data)
        
        acc = None
        if form.account.data and form.account.data != 0:
            acc = Account.query.get(form.account.data)
        
        r = Recurring(
            start_date = datetime.combine(form.start_date.data, datetime.min.time()),
            amount = form.amount.data,
            type = TransactionType.income if form.type.data == "income" else TransactionType.expense,
            frequency = Frequency[form.frequency.data],
            category = cat,
            account = acc,
            note = form.note.data,
            end_date = datetime.combine(form.end_date.data, datetime.min.time()) if form.end_date.data else None,
            next_date = datetime.combine(form.start_date.data, datetime.min.time()),
            active = form.active.data
        )
        if getattr(flask_g, 'user', None):
            r.user_id = flask_g.user.id
        db.session.add(r)
        db.session.commit()
        try:
            generate_recurring_occurrences()
        except Exception:
            pass
        flash("Повторяющаяся операция сохранена", "success")
        return redirect(url_for("recurring_list"))
    if request.method == "GET":
        form.start_date.data = datetime.today().date()
    return render_template("add_recurring.html", form=form)

@app.route("/recurring/delete/<int:rec_id>", methods=["POST"])
@login_required
def recurring_delete(rec_id):
    r = Recurring.query.get_or_404(rec_id)
    db.session.delete(r)
    db.session.commit()
    flash("Удалено", "success")
    return redirect(url_for("recurring_list"))

# Goals
@app.route("/goals")
def goals():
    gs = Goal.query.order_by(Goal.id.desc()).all()
    goals_progress = []
    for g in gs:
        # Используем current_amount из модели, но также учитываем категорию если есть
        progress = g.current_amount
        if g.category:
            s = db.session.query(Transaction).filter(
                Transaction.category_id == g.category.id,
                Transaction.type == TransactionType.income
            ).with_entities(db.func.sum(Transaction.amount)).scalar() or 0.0
            progress = max(progress, float(s))
        
        percent = min(100.0, (progress / g.target_amount * 100.0) if g.target_amount > 0 else 0.0)
        remaining = max(0, g.target_amount - progress)
        
        # Рассчитываем дни до цели если есть target_date
        days_remaining = None
        if g.target_date:
            days_remaining = (g.target_date.date() - date.today()).days
        
        goals_progress.append({
            "goal": g,
            "progress": progress,
            "percent": percent,
            "remaining": remaining,
            "days_remaining": days_remaining
        })
    currency = app.config.get("DEFAULT_CURRENCY", "RUB")
    return render_template("goals.html", goals_progress=goals_progress, currency=currency)

@app.route("/goals/add", methods=["GET","POST"])
@login_required
def add_goal():
    form = GoalForm()
    categories = Category.query.order_by(Category.name).all()
    choices = [(0, "— не привязывать —")] + [(c.id, c.name) for c in categories]
    form.category.choices = choices
    if form.validate_on_submit():
        cat = None
        if form.category.data and form.category.data != 0:
            cat = Category.query.get(form.category.data)
        g = Goal(
            name = form.name.data.strip(),
            target_amount = form.target_amount.data,
            current_amount = form.current_amount.data or 0.0,
            category = cat,
            target_date = datetime.combine(form.target_date.data, datetime.min.time()) if form.target_date.data else None,
            notes = form.notes.data,
            active = form.active.data
        )
        if getattr(flask_g, 'user', None):
            g.user_id = flask_g.user.id
        db.session.add(g)
        db.session.commit()
        flash("Цель создана", "success")
        return redirect(url_for("goals"))
    return render_template("add_goal.html", form=form)

@app.route("/goals/edit/<int:goal_id>", methods=["GET","POST"])
@login_required
def edit_goal(goal_id):
    g = Goal.query.get_or_404(goal_id)
    form = GoalForm(obj=g)
    categories = Category.query.order_by(Category.name).all()
    choices = [(0, "— не привязывать —")] + [(c.id, c.name) for c in categories]
    form.category.choices = choices
    if g.target_date:
        form.target_date.data = g.target_date.date()
    
    if form.validate_on_submit():
        cat = None
        if form.category.data and form.category.data != 0:
            cat = Category.query.get(form.category.data)
        
        g.name = form.name.data.strip()
        g.target_amount = form.target_amount.data
        g.current_amount = form.current_amount.data or 0.0
        g.category = cat
        g.target_date = datetime.combine(form.target_date.data, datetime.min.time()) if form.target_date.data else None
        g.notes = form.notes.data
        g.active = form.active.data
        
        db.session.commit()
        flash("Цель обновлена", "success")
        return redirect(url_for("goals"))
    
    return render_template("add_goal.html", form=form, goal=g, edit=True)

@app.route("/goals/delete/<int:goal_id>", methods=["POST"])
@login_required
def delete_goal(goal_id):
    g = Goal.query.get_or_404(goal_id)
    db.session.delete(g)
    db.session.commit()
    flash("Цель удалена", "success")
    return redirect(url_for("goals"))

# Accounts
@app.route("/accounts")
def accounts():
    accs = Account.query.filter_by(user_id=flask_g.user.id).order_by(Account.id.desc()).all() if getattr(flask_g, 'user', None) else Account.query.order_by(Account.id.desc()).all()
    return render_template("accounts.html", accounts=accs)

@app.route("/accounts/add", methods=["GET","POST"])
@login_required
def add_account():
    form = AccountForm()
    if form.validate_on_submit():
        acc = Account(
            name=form.name.data.strip(),
            balance=form.balance.data or 0.0,
            currency=form.currency.data,
            notes=form.notes.data
        )
        if getattr(flask_g, 'user', None):
            acc.user_id = flask_g.user.id
        db.session.add(acc)
        db.session.commit()
        flash("Счёт создан", "success")
        return redirect(url_for("accounts"))
    return render_template("add_account.html", form=form)

@app.route("/accounts/delete/<int:acc_id>", methods=["POST"])
@login_required
def delete_account(acc_id):
    acc = Account.query.get_or_404(acc_id)
    acc.is_active = False
    db.session.commit()
    flash("Счёт деактивирован", "success")
    return redirect(url_for("accounts"))

# Budgets
@app.route("/budgets")
def budgets():
    today = date.today()
    budgets_list = Budget.query.filter_by(is_active=True).order_by(Budget.id.desc()).all() if not getattr(flask_g, 'user', None) else Budget.query.filter_by(is_active=True, user_id=flask_g.user.id).order_by(Budget.id.desc()).all()
    budgets_data = []
    for b in budgets_list:
        spent = db.session.query(Transaction).filter(
            Transaction.category_id == b.category_id,
            Transaction.type == TransactionType.expense,
            Transaction.date >= b.period_start,
            Transaction.date <= b.period_end
        ).with_entities(db.func.sum(Transaction.amount)).scalar() or 0.0
        percent = min(100.0, (spent / b.amount * 100.0) if b.amount > 0 else 0.0)
        budgets_data.append({
            "budget": b,
            "spent": spent,
            "remaining": max(0, b.amount - spent),
            "percent": percent
        })
    currency = app.config.get("DEFAULT_CURRENCY", "RUB")
    return render_template("budgets.html", budgets_data=budgets_data, currency=currency)

@app.route("/budgets/add", methods=["GET","POST"])
@login_required
def add_budget():
    form = BudgetForm()
    categories = Category.query.order_by(Category.name).all()
    form.category.choices = [(c.id, c.name) for c in categories]
    
    if form.validate_on_submit():
        b = Budget(
            category_id=form.category.data,
            amount=form.amount.data,
            period_start=datetime.combine(form.period_start.data, datetime.min.time()),
            period_end=datetime.combine(form.period_end.data, datetime.min.time())
        )
        if getattr(flask_g, 'user', None):
            b.user_id = flask_g.user.id
        db.session.add(b)
        db.session.commit()
        flash("Бюджет создан", "success")
        return redirect(url_for("budgets"))
    
    # Устанавливаем значения по умолчанию
    if request.method == "GET":
        form.period_start.data = date.today().replace(day=1)
        next_month = (date.today().replace(day=1) + relativedelta(months=1)) - timedelta(days=1)
        form.period_end.data = next_month
    
    return render_template("add_budget.html", form=form)

@app.route("/budgets/delete/<int:budget_id>", methods=["POST"])
@login_required
def delete_budget(budget_id):
    b = Budget.query.get_or_404(budget_id)
    b.is_active = False
    db.session.commit()
    flash("Бюджет удалён", "success")
    return redirect(url_for("budgets"))

# Debts
@app.route("/debts")
def debts():
    debts_list = Debt.query.filter_by(is_active=True).order_by(Debt.id.desc()).all()
    today = date.today()
    
    # Подсчитываем статистику
    total_debt = sum(d.remaining_amount for d in debts_list if not d.is_owed_to_me)
    total_owed = sum(d.remaining_amount for d in debts_list if d.is_owed_to_me)
    upcoming_payments = [d for d in debts_list if d.payment_date and d.payment_date.date() >= today]
    upcoming_payments.sort(key=lambda x: x.payment_date.date() if x.payment_date else date.max)
    
    currency = app.config.get("DEFAULT_CURRENCY", "RUB")
    return render_template("debts.html", debts=debts_list, total_debt=total_debt, total_owed=total_owed, 
                         upcoming_payments=upcoming_payments[:5], currency=currency, today=today)

@app.route("/debts/add", methods=["GET","POST"])
@login_required
def add_debt():
    form = DebtForm()
    accounts = Account.query.filter_by(is_active=True).order_by(Account.name).all()
    form.account.choices = [(0, "— не привязывать —")] + [(a.id, a.name) for a in accounts]
    
    if form.validate_on_submit():
        acc = None
        if form.account.data and form.account.data != 0:
            acc = Account.query.get(form.account.data)
        
        # Обработка в зависимости от типа долга
        if form.debt_type.data == "credit_card":
            # Для кредитной карты: amount = credit_limit, paid_amount = 0
            credit_limit = form.credit_limit.data or form.amount.data
            if not credit_limit:
                flash("Укажите лимит кредитной карты", "danger")
                currency = app.config.get("DEFAULT_CURRENCY", "RUB")
                return render_template("add_debt.html", form=form, accounts=accounts, currency=currency)
            
            current_balance = form.current_balance.data if form.current_balance.data is not None else 0.0
            
            d = Debt(
                name=form.name.data.strip(),
                debt_type=DebtType.credit_card,
                amount=credit_limit,  # amount = credit_limit для кредитной карты
                paid_amount=0.0,  # paid_amount не используется для кредитной карты
                current_balance=current_balance,
                credit_limit=credit_limit,
                is_owed_to_me=form.is_owed_to_me.data,
                interest_rate=form.interest_rate.data,
                payment_date=datetime.combine(form.payment_date.data, datetime.min.time()) if form.payment_date.data else None,
                payment_amount=form.payment_amount.data,
                min_payment=form.min_payment.data,
                due_date=None,  # Для кредитной карты нет срока окончания
                account=acc,
                notes=form.notes.data,
                is_active=form.is_active.data
            )
        else:
            # Для обычного долга или кредита
            d = Debt(
                name=form.name.data.strip(),
                debt_type=DebtType[form.debt_type.data],
                amount=form.amount.data,
                paid_amount=form.paid_amount.data or 0.0,
                current_balance=None,  # Не используется для долга/кредита
                credit_limit=form.credit_limit.data if form.debt_type.data == "credit" else None,
                is_owed_to_me=form.is_owed_to_me.data,
                interest_rate=form.interest_rate.data,
                payment_date=datetime.combine(form.payment_date.data, datetime.min.time()) if form.payment_date.data else None,
                payment_amount=form.payment_amount.data,
                min_payment=form.min_payment.data,
                due_date=datetime.combine(form.due_date.data, datetime.min.time()) if form.due_date.data else None,
                account=acc,
                notes=form.notes.data,
                is_active=form.is_active.data
            )
        
        if getattr(flask_g, 'user', None):
            d.user_id = flask_g.user.id
        db.session.add(d)
        db.session.commit()
        flash("Долг/кредит добавлен", "success")
        return redirect(url_for("debts"))
    
    currency = app.config.get("DEFAULT_CURRENCY", "RUB")
    return render_template("add_debt.html", form=form, accounts=accounts, currency=currency)

@app.route("/debts/edit/<int:debt_id>", methods=["GET","POST"])
@login_required
def edit_debt(debt_id):
    d = Debt.query.get_or_404(debt_id)
    form = DebtForm(obj=d)
    accounts = Account.query.filter_by(is_active=True).order_by(Account.name).all()
    form.account.choices = [(0, "— не привязывать —")] + [(a.id, a.name) for a in accounts]
    
    if request.method == "GET":
        form.debt_type.data = d.debt_type.value if d.debt_type else "debt"
        if d.payment_date:
            form.payment_date.data = d.payment_date.date()
        if d.due_date:
            form.due_date.data = d.due_date.date()
        if d.account:
            form.account.data = d.account.id
    
    if form.validate_on_submit():
        acc = None
        if form.account.data and form.account.data != 0:
            acc = Account.query.get(form.account.data)
        
        d.name = form.name.data.strip()
        d.debt_type = DebtType[form.debt_type.data]
        d.is_owed_to_me = form.is_owed_to_me.data
        d.interest_rate = form.interest_rate.data
        d.payment_date = datetime.combine(form.payment_date.data, datetime.min.time()) if form.payment_date.data else None
        d.payment_amount = form.payment_amount.data
        d.min_payment = form.min_payment.data
        d.account = acc
        d.notes = form.notes.data
        d.is_active = form.is_active.data
        
        # Обработка в зависимости от типа долга
        if form.debt_type.data == "credit_card":
            # Для кредитной карты: amount = credit_limit, paid_amount = 0
            credit_limit = form.credit_limit.data or form.amount.data
            if not credit_limit:
                flash("Укажите лимит кредитной карты", "danger")
                currency = app.config.get("DEFAULT_CURRENCY", "RUB")
                return render_template("add_debt.html", form=form, debt=d, edit=True, accounts=accounts, currency=currency)
            
            d.amount = credit_limit
            d.paid_amount = 0.0
            d.current_balance = form.current_balance.data if form.current_balance.data is not None else 0.0
            d.credit_limit = credit_limit
            d.due_date = None  # Для кредитной карты нет срока окончания
        else:
            # Для обычного долга или кредита
            d.amount = form.amount.data
            d.paid_amount = form.paid_amount.data or 0.0
            d.current_balance = None
            d.credit_limit = form.credit_limit.data if form.debt_type.data == "credit" else None
            d.due_date = datetime.combine(form.due_date.data, datetime.min.time()) if form.due_date.data else None
        
        db.session.commit()
        flash("Долг/кредит обновлён", "success")
        return redirect(url_for("debts"))
    
    currency = app.config.get("DEFAULT_CURRENCY", "RUB")
    return render_template("add_debt.html", form=form, debt=d, edit=True, accounts=accounts, currency=currency)

@app.route("/debts/<int:debt_id>")
def debt_detail(debt_id):
    d = Debt.query.get_or_404(debt_id)
    currency = app.config.get("DEFAULT_CURRENCY", "RUB")
    today = date.today()
    
    # Рассчитываем дни до платежа
    days_until_payment = None
    if d.payment_date:
        days_until_payment = (d.payment_date.date() - today).days
    
    return render_template("debt_detail.html", debt=d, currency=currency, today=today, days_until_payment=days_until_payment)

@app.route("/debts/<int:debt_id>/make-payment", methods=["POST"])
@login_required
def make_payment(debt_id):
    d = Debt.query.get_or_404(debt_id)
    payment_amount = float(request.form.get('amount', 0))
    payment_date = request.form.get('date')
    create_transaction = request.form.get('create_transaction') == 'on'
    
    if payment_amount <= 0:
        flash("Сумма платежа должна быть больше 0", "danger")
        return redirect(url_for("debt_detail", debt_id=debt_id))
    
    try:
        payment_date_obj = datetime.strptime(payment_date, '%Y-%m-%d') if payment_date else datetime.now()
    except:
        payment_date_obj = datetime.now()
    
    # Обновляем баланс долга
    if d.debt_type == DebtType.credit_card:
        # Для кредитки уменьшаем текущий баланс
        d.current_balance = (d.current_balance or 0) - payment_amount
        if d.current_balance < 0:
            d.current_balance = 0
    else:
        # Для кредита/долга увеличиваем выплаченную сумму
        d.paid_amount = (d.paid_amount or 0) + payment_amount
        if d.paid_amount > d.amount:
            d.paid_amount = d.amount
    
    # Создаём транзакцию, если нужно
    if create_transaction:
        # Находим категорию "Погашение долга" или создаём
        # Prefer a user-specific category if user is logged in
        if getattr(flask_g, 'user', None):
            category = Category.query.filter_by(name="Погашение долга", user_id=flask_g.user.id).first()
        else:
            category = Category.query.filter_by(name="Погашение долга").first()
        if not category:
            category = Category(name="Погашение долга", color="#f85149")
            if getattr(flask_g, 'user', None):
                category.user_id = flask_g.user.id
            db.session.add(category)
        
        t = Transaction(
            date=payment_date_obj,
            amount=payment_amount,
            type=TransactionType.expense,
            category=category,
            account=d.account,
            note=f"Платеж по {d.name}"
        )
        if getattr(flask_g, 'user', None):
            t.user_id = flask_g.user.id
        db.session.add(t)
        
        # Обновляем баланс счёта, если есть
        if d.account:
            d.account.balance -= payment_amount
    
    # Обновляем дату следующего платежа (если это был запланированный платёж)
    if d.payment_date and d.payment_date.date() == payment_date_obj.date():
        # Вычисляем следующую дату платежа (обычно через месяц)
        if d.payment_date:
            next_payment = d.payment_date + relativedelta(months=1)
            d.payment_date = next_payment
    
    db.session.commit()
    flash(f"Платёж {payment_amount:.2f} {app.config.get('DEFAULT_CURRENCY', 'RUB')} зарегистрирован", "success")
    return redirect(url_for("debt_detail", debt_id=debt_id))

@app.route("/debts/delete/<int:debt_id>", methods=["POST"])
@login_required
def delete_debt(debt_id):
    d = Debt.query.get_or_404(debt_id)
    db.session.delete(d)
    db.session.commit()
    flash("Долг удалён", "success")
    return redirect(url_for("debts"))

# Transaction Templates
@app.route("/templates")
def templates():
    templates_list = (TransactionTemplate.query.filter(db.or_(TransactionTemplate.user_id == None, TransactionTemplate.user_id == flask_g.user.id)).order_by(TransactionTemplate.use_count.desc(), TransactionTemplate.name).all() if getattr(flask_g, 'user', None) else TransactionTemplate.query.order_by(TransactionTemplate.use_count.desc(), TransactionTemplate.name).all())
    return render_template("templates.html", templates=templates_list)

@app.route("/templates/add", methods=["GET","POST"])
@login_required
def add_template():
    form = TransactionTemplateForm()
    categories = Category.query.order_by(Category.name).all()
    accounts = Account.query.filter_by(is_active=True).order_by(Account.name).all()
    
    form.category.choices = [(0, "— без категории —")] + [(c.id, c.name) for c in categories]
    form.account.choices = [(0, "— без счёта —")] + [(a.id, a.name) for a in accounts]
    
    if form.validate_on_submit():
        cat = None
        if form.category.data and form.category.data != 0:
            cat = Category.query.get(form.category.data)
        
        acc = None
        if form.account.data and form.account.data != 0:
            acc = Account.query.get(form.account.data)
        
        t = TransactionTemplate(
            name=form.name.data.strip(),
            amount=form.amount.data,
            type=TransactionType.income if form.type.data == "income" else TransactionType.expense,
            category=cat,
            account=acc,
            note=form.note.data
        )
        if getattr(flask_g, 'user', None):
            t.user_id = flask_g.user.id
        db.session.add(t)
        db.session.commit()
        flash("Шаблон создан", "success")
        return redirect(url_for("templates"))
    
    return render_template("add_template.html", form=form)

@app.route("/templates/use/<int:template_id>", methods=["GET","POST"])
@login_required
def use_template(template_id):
    template = TransactionTemplate.query.get_or_404(template_id)
    template.use_count += 1
    
    # Создаём транзакцию из шаблона
    t = Transaction(
        date=datetime.now(),
        amount=template.amount,
        type=template.type,
        category=template.category,
        account=template.account,
        note=template.note
    )
    
    # Обновляем баланс счёта если есть
    if template.account:
        if template.type == TransactionType.income:
            template.account.balance += template.amount
        else:
            template.account.balance -= template.amount
    
    # set ownership: prefer template owner or current user
    if template.user_id:
        t.user_id = template.user_id
    elif getattr(flask_g, 'user', None):
        t.user_id = flask_g.user.id
    db.session.add(t)
    db.session.commit()
    flash("Операция создана из шаблона", "success")
    return redirect(url_for("transactions"))

@app.route("/templates/delete/<int:template_id>", methods=["POST"])
@login_required
def delete_template(template_id):
    t = TransactionTemplate.query.get_or_404(template_id)
    db.session.delete(t)
    db.session.commit()
    flash("Шаблон удалён", "success")
    return redirect(url_for("templates"))

# Planned Expenses
@app.route("/planned")
def planned_expenses():
    today = date.today()
    if getattr(flask_g, 'user', None):
        planned = PlannedExpense.query.filter_by(is_completed=False, user_id=flask_g.user.id).order_by(PlannedExpense.planned_date).all()
        completed = PlannedExpense.query.filter_by(is_completed=True, user_id=flask_g.user.id).order_by(PlannedExpense.planned_date.desc()).limit(10).all()
    else:
        planned = PlannedExpense.query.filter_by(is_completed=False).order_by(PlannedExpense.planned_date).all()
        completed = PlannedExpense.query.filter_by(is_completed=True).order_by(PlannedExpense.planned_date.desc()).limit(10).all()
    return render_template("planned_expenses.html", planned=planned, completed=completed, today=today)

@app.route("/planned/add", methods=["GET","POST"])
@login_required
def add_planned_expense():
    form = PlannedExpenseForm()
    categories = Category.query.order_by(Category.name).all()
    accounts = Account.query.filter_by(is_active=True).order_by(Account.name).all()
    
    form.category.choices = [(0, "— без категории —")] + [(c.id, c.name) for c in categories]
    form.account.choices = [(0, "— без счёта —")] + [(a.id, a.name) for a in accounts]
    
    if form.validate_on_submit():
        cat = None
        if form.category.data and form.category.data != 0:
            cat = Category.query.get(form.category.data)
        
        acc = None
        if form.account.data and form.account.data != 0:
            acc = Account.query.get(form.account.data)
        
        p = PlannedExpense(
            name=form.name.data.strip(),
            amount=form.amount.data,
            planned_date=datetime.combine(form.planned_date.data, datetime.min.time()),
            category=cat,
            account=acc,
            note=form.note.data
        )
        if getattr(flask_g, 'user', None):
            p.user_id = flask_g.user.id
        db.session.add(p)
        db.session.commit()
        flash("Планируемый расход добавлен", "success")
        return redirect(url_for("planned_expenses"))
    
    if request.method == "GET":
        form.planned_date.data = date.today()
    
    return render_template("add_planned_expense.html", form=form)

@app.route("/planned/complete/<int:planned_id>", methods=["POST"])
@login_required
def complete_planned_expense(planned_id):
    p = PlannedExpense.query.get_or_404(planned_id)
    p.is_completed = True
    db.session.commit()
    flash("Планируемый расход отмечен как выполненный", "success")
    return redirect(url_for("planned_expenses"))

@app.route("/planned/delete/<int:planned_id>", methods=["POST"])
@login_required
def delete_planned_expense(planned_id):
    p = PlannedExpense.query.get_or_404(planned_id)
    db.session.delete(p)
    db.session.commit()
    flash("Планируемый расход удалён", "success")
    return redirect(url_for("planned_expenses"))

# Transfers
@app.route("/transfer", methods=["GET","POST"])
@login_required
def transfer():
    form = TransferForm()
    accounts = Account.query.filter_by(is_active=True).order_by(Account.name).all()
    
    form.from_account.choices = [(a.id, a.name) for a in accounts]
    form.to_account.choices = [(a.id, a.name) for a in accounts]
    
    if form.validate_on_submit():
        from_acc = Account.query.get(form.from_account.data)
        to_acc = Account.query.get(form.to_account.data)
        
        if from_acc.id == to_acc.id:
            flash("Нельзя переводить на тот же счёт", "danger")
            return render_template("transfer.html", form=form)
        
        if from_acc.balance < form.amount.data:
            flash("Недостаточно средств на счёте", "danger")
            return render_template("transfer.html", form=form)
        
        # Создаём две транзакции: расход со счёта и доход на счёт
        expense = Transaction(
            date=datetime.combine(form.date.data, datetime.min.time()),
            amount=form.amount.data,
            type=TransactionType.expense,
            account=from_acc,
            note=f"Перевод на {to_acc.name}" + (f": {form.note.data}" if form.note.data else "")
        )
        
        income = Transaction(
            date=datetime.combine(form.date.data, datetime.min.time()),
            amount=form.amount.data,
            type=TransactionType.income,
            account=to_acc,
            note=f"Перевод со счёта {from_acc.name}" + (f": {form.note.data}" if form.note.data else "")
        )
        
        # Обновляем балансы
        from_acc.balance -= form.amount.data
        to_acc.balance += form.amount.data
        
        if getattr(flask_g, 'user', None):
            expense.user_id = flask_g.user.id
            income.user_id = flask_g.user.id
        db.session.add(expense)
        db.session.add(income)
        db.session.commit()
        flash("Перевод выполнен", "success")
        return redirect(url_for("transactions"))
    
    if request.method == "GET":
        form.date.data = date.today()
    
    return render_template("transfer.html", form=form)

# Tags
@app.route("/tags")
def tags():
    tags_list = Tag.query.filter(db.or_(Tag.user_id == None, Tag.user_id == flask_g.user.id)).order_by(Tag.name).all() if getattr(flask_g, 'user', None) else Tag.query.order_by(Tag.name).all()
    return render_template("tags.html", tags=tags_list)

@app.route("/tags/add", methods=["GET","POST"])
@login_required
def add_tag():
    form = TagForm()
    if form.validate_on_submit():
        t = Tag(
            name=form.name.data.strip(),
            color=form.color.data or "#8b5cf6"
        )
        if getattr(flask_g, 'user', None):
            t.user_id = flask_g.user.id
        db.session.add(t)
        db.session.commit()
        flash("Тег создан", "success")
        return redirect(url_for("tags"))
    return render_template("add_tag.html", form=form)

@app.route("/tags/delete/<int:tag_id>", methods=["POST"])
@login_required
def delete_tag(tag_id):
    t = Tag.query.get_or_404(tag_id)
    db.session.delete(t)
    db.session.commit()
    flash("Тег удалён", "success")
    return redirect(url_for("tags"))

# Notifications
@app.route("/notifications")
def notifications():
    notifs = Notification.query.order_by(Notification.created_at.desc()).all()
    return render_template("notifications.html", notifications=notifs)

@app.route("/notifications/read/<int:notif_id>", methods=["POST"])
@login_required
def mark_notification_read(notif_id):
    notif = Notification.query.get_or_404(notif_id)
    notif.is_read = True
    db.session.commit()
    return redirect(url_for("notifications"))

@app.route("/notifications/read-all", methods=["POST"])
@login_required
def mark_all_notifications_read():
    Notification.query.update({Notification.is_read: True})
    db.session.commit()
    flash("Все уведомления отмечены как прочитанные", "success")
    return redirect(url_for("notifications"))

# Achievements
@app.route("/achievements")
def achievements():
    achievements_list = Achievement.query.order_by(Achievement.is_unlocked.desc(), Achievement.created_at).all()
    
    # Подсчитываем статистику для достижений
    total_transactions = Transaction.query.count()
    days_with_transactions = db.session.query(db.func.date(Transaction.date)).distinct().count()
    
    # Проверяем достижения
    for ach in achievements_list:
        if not ach.is_unlocked:
            if ach.condition_type == 'transactions_count' and total_transactions >= ach.condition_value:
                ach.is_unlocked = True
                ach.unlocked_at = datetime.now()
            elif ach.condition_type == 'days_streak' and days_with_transactions >= ach.condition_value:
                ach.is_unlocked = True
                ach.unlocked_at = datetime.now()
    
    db.session.commit()
    
    return render_template("achievements.html", achievements=achievements_list, 
                         total_transactions=total_transactions, days_with_transactions=days_with_transactions)





@app.route('/register', methods=['GET', 'POST'])
def register():
    from .forms import RegisterForm
    form = RegisterForm()
    if form.validate_on_submit():
        username = form.username.data.strip()
        username_lc = username.lower()
        if User.query.filter(func.lower(User.username) == username_lc).first():
            flash('Пользователь с таким именем уже существует', 'danger')
            return render_template('register.html', form=form)
        email = form.email.data.strip().lower() if form.email.data else None
        if email and User.query.filter(func.lower(User.email) == email).first():
            flash('Email уже используется', 'danger')
            return render_template('register.html', form=form)
        u = User(username=username, email=email)
        u.set_password(form.password.data)
        try:
            db.session.add(u)
            db.session.commit()
        except IntegrityError:
            db.session.rollback()
            flash('Не удалось создать пользователя, попробуйте позже', 'danger')
            return render_template('register.html', form=form)
        session.clear()
        session['user_id'] = u.id
        flash('Регистрация прошла успешно', 'success')
        return redirect(url_for('index'))
    return render_template('register.html', form=form)


@app.route('/login', methods=['GET', 'POST'])
def login():
    from .forms import LoginForm
    form = LoginForm()
    next_param = request.args.get('next') or request.form.get('next')
    next_url = next_param if _is_safe_redirect(next_param) else url_for('index')
    if form.validate_on_submit():
        username = form.username.data.strip()
        u = User.query.filter(func.lower(User.username) == username.lower()).first()
        if not u or not u.check_password(form.password.data):
            flash('Неверные учетные данные', 'danger')
            return render_template('login.html', form=form, next=next_param)
        session.clear()
        session['user_id'] = u.id
        flash('Вы вошли в систему', 'success')
        return redirect(next_url)
    return render_template('login.html', form=form, next=next_param)


@app.route('/logout')
def logout():
    session.pop('user_id', None)
    flash('Вы вышли из аккаунта', 'success')
    return redirect(url_for('index'))


@app.route('/account')
@login_required
def account():
    user = flask_g.user
    tx_count = Transaction.query.filter_by(user_id=user.id).count()
    acc_count = Account.query.filter_by(user_id=user.id).count()
    goals_count = Goal.query.filter_by(user_id=user.id).count()
    return render_template('account.html', user=user, tx_count=tx_count, acc_count=acc_count, goals_count=goals_count)
