"""
Утилиты для создания уведомлений
"""
from .models import db, Notification, Budget, Debt, Goal, Transaction, TransactionType
from datetime import datetime, date, timedelta

def check_budget_warnings():
    """Проверяет бюджеты и создаёт уведомления при приближении к лимиту"""
    today = datetime.now()
    budgets = Budget.query.filter(
        Budget.is_active == True,
        Budget.period_start <= today,
        Budget.period_end >= today
    ).all()
    
    for budget in budgets:
        spent = db.session.query(Transaction).filter(
            Transaction.category_id == budget.category_id,
            Transaction.type == TransactionType.expense,
            Transaction.date >= budget.period_start,
            Transaction.date <= budget.period_end
        ).with_entities(db.func.sum(Transaction.amount)).scalar() or 0.0
        
        percent = (spent / budget.amount * 100.0) if budget.amount > 0 else 0.0
        
        # Проверяем, есть ли уже уведомление для этого бюджета
        existing = Notification.query.filter_by(
            type='budget_warning',
            related_id=budget.id,
            is_read=False
        ).first()
        
        if percent >= 100 and not existing:
            notif = Notification(
                type='budget_warning',
                title=f'Бюджет превышен: {budget.category.name}',
                message=f'Вы потратили {spent:.2f} из {budget.amount:.2f} ({percent:.0f}%)',
                related_id=budget.id
            )
            # attach to budget owner if set
            notif.user_id = budget.user_id
            db.session.add(notif)
        elif percent >= 80 and percent < 100 and not existing:
            notif = Notification(
                type='budget_warning',
                title=f'Приближение к лимиту: {budget.category.name}',
                message=f'Вы потратили {spent:.2f} из {budget.amount:.2f} ({percent:.0f}%)',
                related_id=budget.id
            )
            notif.user_id = budget.user_id
            db.session.add(notif)

def check_debt_due():
    """Проверяет долги и создаёт уведомления о просроченных"""
    today = date.today()
    debts = Debt.query.filter_by(is_owed_to_me=False).all()
    
    for debt in debts:
        if debt.due_date and debt.due_date.date() < today and debt.paid_amount < debt.amount:
            # Проверяем, есть ли уже уведомление
            existing = Notification.query.filter_by(
                type='debt_due',
                related_id=debt.id,
                is_read=False
            ).first()
            
            if not existing:
                days_overdue = (today - debt.due_date.date()).days
                notif = Notification(
                    type='debt_due',
                    title=f'Просроченный долг: {debt.name}',
                    message=f'Долг просрочен на {days_overdue} дней. Осталось выплатить: {debt.amount - debt.paid_amount:.2f}',
                    related_id=debt.id
                )
                notif.user_id = debt.user_id
                db.session.add(notif)

def check_goal_reminders():
    """Проверяет цели и создаёт напоминания"""
    today = date.today()
    goals = Goal.query.filter_by(active=True).all()
    
    for goal in goals:
        if goal.target_date:
            days_remaining = (goal.target_date.date() - today).days
            
            # Напоминание за 7 дней до цели
            if 0 < days_remaining <= 7:
                existing = Notification.query.filter_by(
                    type='goal_reminder',
                    related_id=goal.id,
                    is_read=False
                ).first()
                
                if not existing:
                    remaining = goal.target_amount - goal.current_amount
                    notif = Notification(
                        type='goal_reminder',
                        title=f'Напоминание о цели: {goal.name}',
                        message=f'До цели осталось {days_remaining} дней. Нужно накопить ещё {remaining:.2f}',
                        related_id=goal.id
                    )
                    notif.user_id = goal.user_id
                    db.session.add(notif)

def generate_all_notifications():
    """Генерирует все уведомления"""
    try:
        check_budget_warnings()
        check_debt_due()
        check_goal_reminders()
        db.session.commit()
    except Exception as e:
        db.session.rollback()
        print(f"Error generating notifications: {e}")

