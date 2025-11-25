from . import db
from datetime import datetime, date
from werkzeug.security import generate_password_hash, check_password_hash
import enum

class TransactionType(enum.Enum):
    expense = "expense"
    income = "income"

class Frequency(enum.Enum):
    daily = "daily"
    weekly = "weekly"
    monthly = "monthly"

# Association table for many-to-many relationship between transactions and tags
transaction_tags = db.Table('transaction_tags',
    db.Column('transaction_id', db.Integer, db.ForeignKey('transactions.id'), primary_key=True),
    db.Column('tag_id', db.Integer, db.ForeignKey('tags.id'), primary_key=True)
)

class Category(db.Model):
    __tablename__ = "categories"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(64), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=True)
    color = db.Column(db.String(7), default="#6366f1")  # Hex color for category
    icon = db.Column(db.String(32), nullable=True)  # Icon name

    def __repr__(self):
        return f"<Category {self.name}>"

class Account(db.Model):
    __tablename__ = "accounts"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(64), nullable=False)
    balance = db.Column(db.Float, default=0.0, nullable=False)
    currency = db.Column(db.String(3), default="RUB", nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    is_active = db.Column(db.Boolean, default=True, nullable=False)
    notes = db.Column(db.String(256))
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=True)

    def __repr__(self):
        return f"<Account {self.name} {self.balance}>"

class Transaction(db.Model):
    __tablename__ = "transactions"
    id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)
    amount = db.Column(db.Float, nullable=False)
    type = db.Column(db.Enum(TransactionType), nullable=False)
    category_id = db.Column(db.Integer, db.ForeignKey("categories.id"), nullable=True)
    category = db.relationship("Category", backref=db.backref("transactions", lazy=True))
    account_id = db.Column(db.Integer, db.ForeignKey("accounts.id"), nullable=True)
    account = db.relationship("Account", backref=db.backref("transactions", lazy=True))
    note = db.Column(db.String(256))
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=True)
    tags = db.relationship('Tag', secondary=transaction_tags, lazy='subquery', backref=db.backref('transactions', lazy=True))

    def __repr__(self):
        return f"<Transaction {self.amount} {self.type} {self.date}>"

class Tag(db.Model):
    __tablename__ = "tags"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(32), nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=True)
    color = db.Column(db.String(7), default="#8b5cf6")

    def __repr__(self):
        return f"<Tag {self.name}>"

class Budget(db.Model):
    __tablename__ = "budgets"
    id = db.Column(db.Integer, primary_key=True)
    category_id = db.Column(db.Integer, db.ForeignKey("categories.id"), nullable=False)
    category = db.relationship("Category", backref=db.backref("budgets", lazy=True))
    amount = db.Column(db.Float, nullable=False)
    period_start = db.Column(db.DateTime, nullable=False)
    period_end = db.Column(db.DateTime, nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    is_active = db.Column(db.Boolean, default=True, nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=True)

    def __repr__(self):
        return f"<Budget {self.category.name} {self.amount}>"

class Recurring(db.Model):
    __tablename__ = "recurrings"
    id = db.Column(db.Integer, primary_key=True)
    start_date = db.Column(db.DateTime, nullable=False)
    amount = db.Column(db.Float, nullable=False)
    type = db.Column(db.Enum(TransactionType), nullable=False)
    frequency = db.Column(db.Enum(Frequency), nullable=False)
    category_id = db.Column(db.Integer, db.ForeignKey("categories.id"), nullable=True)
    category = db.relationship("Category", backref=db.backref("recurrings", lazy=True))
    account_id = db.Column(db.Integer, db.ForeignKey("accounts.id"), nullable=True)
    account = db.relationship("Account", backref=db.backref("recurrings", lazy=True))
    note = db.Column(db.String(256))
    end_date = db.Column(db.DateTime, nullable=True)
    next_date = db.Column(db.DateTime, nullable=False)
    active = db.Column(db.Boolean, default=True, nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=True)

    def __repr__(self):
        return f"<Recurring {self.amount} {self.type} every {self.frequency}>"

class Goal(db.Model):
    __tablename__ = "goals"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(128), nullable=False)
    target_amount = db.Column(db.Float, nullable=False)
    current_amount = db.Column(db.Float, default=0.0, nullable=False)  # Имеющиеся средства
    category_id = db.Column(db.Integer, db.ForeignKey("categories.id"), nullable=True)
    category = db.relationship("Category", backref=db.backref("goals", lazy=True))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    target_date = db.Column(db.DateTime, nullable=True)  # Целевая дата достижения
    active = db.Column(db.Boolean, default=True, nullable=False)
    notes = db.Column(db.String(256))
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=True)

    def __repr__(self):
        return f"<Goal {self.name} {self.target_amount}>"

class DebtType(enum.Enum):
    debt = "debt"  # Обычный долг
    credit = "credit"  # Кредит
    credit_card = "credit_card"  # Кредитная карта

class Debt(db.Model):
    __tablename__ = "debts"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(128), nullable=False)  # Название кредита/кредитки/долга
    debt_type = db.Column(db.Enum(DebtType), default=DebtType.debt, nullable=False)  # Тип долга
    amount = db.Column(db.Float, nullable=False)  # Общая сумма долга / лимит кредита
    paid_amount = db.Column(db.Float, default=0.0, nullable=False)  # Выплаченная сумма
    current_balance = db.Column(db.Float, nullable=True)  # Текущий баланс (для кредитки)
    credit_limit = db.Column(db.Float, nullable=True)  # Лимит кредита/кредитки
    is_owed_to_me = db.Column(db.Boolean, default=False, nullable=False)  # True = мне должны, False = я должен
    interest_rate = db.Column(db.Float, nullable=True)  # Процентная ставка (% годовых)
    payment_date = db.Column(db.DateTime, nullable=True)  # Дата следующего платежа
    payment_amount = db.Column(db.Float, nullable=True)  # Сумма следующего платежа
    min_payment = db.Column(db.Float, nullable=True)  # Минимальный платеж
    due_date = db.Column(db.DateTime, nullable=True)  # Срок возврата / окончания кредита
    account_id = db.Column(db.Integer, db.ForeignKey("accounts.id"), nullable=True)  # Привязка к счёту (для кредитки)
    account = db.relationship("Account", backref=db.backref("debts", lazy=True))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    notes = db.Column(db.String(512))
    is_active = db.Column(db.Boolean, default=True, nullable=False)  # Активен ли кредит
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=True)

    def __repr__(self):
        return f"<Debt {self.name} {self.amount}>"
    
    @property
    def remaining_amount(self):
        """Остаток долга"""
        if self.debt_type == DebtType.credit_card:
            return self.current_balance or 0.0
        return self.amount - self.paid_amount
    
    @property
    def available_credit(self):
        """Доступный кредит (для кредитки)"""
        if self.debt_type == DebtType.credit_card and self.credit_limit:
            return max(0, self.credit_limit - (self.current_balance or 0))
        return 0
    
    @property
    def utilization_rate(self):
        """Процент использования кредита (для кредитки)"""
        if self.debt_type == DebtType.credit_card and self.credit_limit and self.credit_limit > 0:
            return ((self.current_balance or 0) / self.credit_limit) * 100
        return 0

class TransactionTemplate(db.Model):
    __tablename__ = "transaction_templates"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(128), nullable=False)
    amount = db.Column(db.Float, nullable=False)
    type = db.Column(db.Enum(TransactionType), nullable=False)
    category_id = db.Column(db.Integer, db.ForeignKey("categories.id"), nullable=True)
    category = db.relationship("Category", backref=db.backref("templates", lazy=True))
    account_id = db.Column(db.Integer, db.ForeignKey("accounts.id"), nullable=True)
    account = db.relationship("Account", backref=db.backref("templates", lazy=True))
    note = db.Column(db.String(256))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    use_count = db.Column(db.Integer, default=0)  # Счётчик использования
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=True)

    def __repr__(self):
        return f"<TransactionTemplate {self.name}>"

class PlannedExpense(db.Model):
    __tablename__ = "planned_expenses"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(128), nullable=False)
    amount = db.Column(db.Float, nullable=False)
    planned_date = db.Column(db.DateTime, nullable=False)
    category_id = db.Column(db.Integer, db.ForeignKey("categories.id"), nullable=True)
    category = db.relationship("Category", backref=db.backref("planned_expenses", lazy=True))
    account_id = db.Column(db.Integer, db.ForeignKey("accounts.id"), nullable=True)
    account = db.relationship("Account", backref=db.backref("planned_expenses", lazy=True))
    note = db.Column(db.String(256))
    is_completed = db.Column(db.Boolean, default=False, nullable=False)
    transaction_id = db.Column(db.Integer, db.ForeignKey("transactions.id"), nullable=True)  # Связь с фактической транзакцией
    transaction = db.relationship("Transaction", foreign_keys=[transaction_id])
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=True)

    def __repr__(self):
        return f"<PlannedExpense {self.name} {self.planned_date}>"

class Achievement(db.Model):
    __tablename__ = "achievements"
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(128), nullable=False)
    description = db.Column(db.String(256))
    icon = db.Column(db.String(32), default="bi-trophy")
    condition_type = db.Column(db.String(32), nullable=False)  # 'days_streak', 'transactions_count', etc.
    condition_value = db.Column(db.Integer, nullable=False)
    is_unlocked = db.Column(db.Boolean, default=False, nullable=False)
    unlocked_at = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=True)

    def __repr__(self):
        return f"<Achievement {self.name}>"

class Notification(db.Model):
    __tablename__ = "notifications"
    id = db.Column(db.Integer, primary_key=True)
    type = db.Column(db.String(32), nullable=False)  # 'budget_warning', 'debt_due', 'goal_reminder', etc.
    title = db.Column(db.String(128), nullable=False)
    message = db.Column(db.String(512), nullable=False)
    is_read = db.Column(db.Boolean, default=False, nullable=False)
    related_id = db.Column(db.Integer, nullable=True)  # ID связанного объекта (бюджет, долг, цель)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=True)

    def __repr__(self):
        return f"<Notification {self.title}>"


class User(db.Model):
    __tablename__ = 'users'
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(64), unique=True, nullable=False)
    email = db.Column(db.String(128), unique=True, nullable=True)
    password_hash = db.Column(db.String(256), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def set_password(self, password: str):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password: str) -> bool:
        return check_password_hash(self.password_hash, password)

    def __repr__(self):
        return f"<User {self.username}>"
