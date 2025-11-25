from flask_wtf import FlaskForm
from wtforms import (
    StringField,
    SubmitField,
    FloatField,
    SelectField,
    TextAreaField,
    DateField,
    RadioField,
    FileField,
    BooleanField,
    IntegerField,
    PasswordField,
)
from wtforms.validators import DataRequired, InputRequired, Optional, Email, EqualTo, Length
from wtforms.widgets import ColorInput

class CategoryForm(FlaskForm):
    name = StringField("Название категории", validators=[DataRequired()])
    color = StringField("Цвет", default="#6366f1", widget=ColorInput())
    icon = StringField("Иконка (опционально)", validators=[Optional()])
    submit = SubmitField("Сохранить")

class AccountForm(FlaskForm):
    name = StringField("Название счёта", validators=[DataRequired()])
    balance = FloatField("Начальный баланс", default=0.0, validators=[InputRequired()])
    currency = SelectField("Валюта", choices=[("RUB","₽ RUB"),("USD","$ USD"),("EUR","€ EUR")], default="RUB")
    notes = TextAreaField("Примечание", validators=[Optional()])
    submit = SubmitField("Сохранить")

class TransactionForm(FlaskForm):
    date = DateField("Дата", validators=[InputRequired()], format="%Y-%m-%d")
    amount = FloatField("Сумма", validators=[InputRequired()])
    type = RadioField("Тип", choices=[("expense","Расход"),("income","Доход")], default="expense")
    category = SelectField("Категория", coerce=int, validators=[Optional()])
    account = SelectField("Счёт", coerce=int, validators=[Optional()])
    note = TextAreaField("Примечание", validators=[Optional()])
    submit = SubmitField("Сохранить")
    submit_and_add = SubmitField("Сохранить и добавить ещё")

class ImportForm(FlaskForm):
    csv_file = FileField("CSV файл", validators=[DataRequired()])
    submit = SubmitField("Импортировать")

class RecurringForm(FlaskForm):
    start_date = DateField("Дата начала", validators=[InputRequired()], format="%Y-%m-%d")
    amount = FloatField("Сумма", validators=[InputRequired()])
    type = RadioField("Тип", choices=[("expense","Расход"),("income","Доход")], default="expense")
    frequency = SelectField("Повторять", choices=[("daily","Каждый день"),("weekly","Каждую неделю"),("monthly","Каждый месяц")], validators=[InputRequired()])
    category = SelectField("Категория", coerce=int, validators=[Optional()])
    account = SelectField("Счёт", coerce=int, validators=[Optional()])
    note = TextAreaField("Примечание", validators=[Optional()])
    end_date = DateField("Дата окончания (опционально)", validators=[Optional()], format="%Y-%m-%d")
    active = BooleanField("Активно", default=True)
    submit = SubmitField("Сохранить")

class GoalForm(FlaskForm):
    name = StringField("Название цели", validators=[DataRequired()])
    target_amount = FloatField("Целевая сумма", validators=[InputRequired()])
    current_amount = FloatField("Имеющиеся средства", default=0.0, validators=[InputRequired()])
    target_date = DateField("Целевая дата (опционально)", validators=[Optional()], format="%Y-%m-%d")
    category = SelectField("Категория (опционально, для автоматического учёта вкладов)", coerce=int, validators=[Optional()])
    notes = TextAreaField("Примечание", validators=[Optional()])
    active = BooleanField("Активна", default=True)
    submit = SubmitField("Создать цель")

class BudgetForm(FlaskForm):
    category = SelectField("Категория", coerce=int, validators=[InputRequired()])
    amount = FloatField("Лимит", validators=[InputRequired()])
    period_start = DateField("Начало периода", validators=[InputRequired()], format="%Y-%m-%d")
    period_end = DateField("Конец периода", validators=[InputRequired()], format="%Y-%m-%d")
    submit = SubmitField("Сохранить")

class TagForm(FlaskForm):
    name = StringField("Название тега", validators=[DataRequired()])
    color = StringField("Цвет", default="#8b5cf6", widget=ColorInput())
    submit = SubmitField("Сохранить")

class DebtForm(FlaskForm):
    name = StringField("Название", validators=[DataRequired()])
    debt_type = RadioField("Тип", choices=[
        ("debt", "Долг"),
        ("credit", "Кредит"),
        ("credit_card", "Кредитная карта")
    ], default="debt")
    amount = FloatField("Сумма долга / Лимит кредита", validators=[Optional()])  # Сделано опциональным, проверка в views
    paid_amount = FloatField("Выплачено", default=0.0, validators=[Optional()])  # Сделано опциональным, проверка в views
    current_balance = FloatField("Текущий баланс (для кредитки)", validators=[Optional()])
    credit_limit = FloatField("Лимит кредита/кредитки", validators=[Optional()])
    is_owed_to_me = BooleanField("Мне должны", default=False)
    interest_rate = FloatField("Процентная ставка (% годовых)", validators=[Optional()])
    payment_date = DateField("Дата следующего платежа", validators=[Optional()], format="%Y-%m-%d")
    payment_amount = FloatField("Сумма следующего платежа", validators=[Optional()])
    min_payment = FloatField("Минимальный платеж", validators=[Optional()])
    due_date = DateField("Срок возврата / Окончание кредита", validators=[Optional()], format="%Y-%m-%d")
    account = SelectField("Счёт (для кредитки)", coerce=int, validators=[Optional()])
    notes = TextAreaField("Примечание", validators=[Optional()])
    is_active = BooleanField("Активен", default=True)
    submit = SubmitField("Сохранить")

class SearchForm(FlaskForm):
    query = StringField("Поиск", validators=[Optional()])
    category = SelectField("Категория", coerce=int, validators=[Optional()])
    type = SelectField("Тип", choices=[("","Все"),("income","Доход"),("expense","Расход")], validators=[Optional()])
    date_from = DateField("С", validators=[Optional()], format="%Y-%m-%d")
    date_to = DateField("По", validators=[Optional()], format="%Y-%m-%d")
    amount_from = FloatField("Сумма от", validators=[Optional()])
    amount_to = FloatField("Сумма до", validators=[Optional()])
    account = SelectField("Счёт", coerce=int, validators=[Optional()])
    submit = SubmitField("Поиск")

class TransactionTemplateForm(FlaskForm):
    name = StringField("Название шаблона", validators=[DataRequired()])
    amount = FloatField("Сумма", validators=[InputRequired()])
    type = RadioField("Тип", choices=[("expense","Расход"),("income","Доход")], default="expense")
    category = SelectField("Категория", coerce=int, validators=[Optional()])
    account = SelectField("Счёт", coerce=int, validators=[Optional()])
    note = TextAreaField("Примечание", validators=[Optional()])
    submit = SubmitField("Сохранить")

class PlannedExpenseForm(FlaskForm):
    name = StringField("Название", validators=[DataRequired()])
    amount = FloatField("Сумма", validators=[InputRequired()])
    planned_date = DateField("Планируемая дата", validators=[InputRequired()], format="%Y-%m-%d")
    category = SelectField("Категория", coerce=int, validators=[Optional()])
    account = SelectField("Счёт", coerce=int, validators=[Optional()])
    note = TextAreaField("Примечание", validators=[Optional()])
    submit = SubmitField("Сохранить")

class TransferForm(FlaskForm):
    from_account = SelectField("Со счёта", coerce=int, validators=[InputRequired()])
    to_account = SelectField("На счёт", coerce=int, validators=[InputRequired()])
    amount = FloatField("Сумма", validators=[InputRequired()])
    date = DateField("Дата", validators=[InputRequired()], format="%Y-%m-%d")
    note = TextAreaField("Примечание", validators=[Optional()])
    submit = SubmitField("Выполнить перевод")


class RegisterForm(FlaskForm):
    username = StringField("Имя пользователя", validators=[DataRequired(), Length(min=3, max=64)])
    email = StringField("Email (опционально)", validators=[Optional(), Email(), Length(max=128)])
    password = PasswordField("Пароль", validators=[DataRequired(), Length(min=6)])
    password_confirm = PasswordField("Повторите пароль", validators=[DataRequired(), EqualTo('password', message='Пароли должны совпадать')])
    submit = SubmitField("Зарегистрироваться")


class LoginForm(FlaskForm):
    username = StringField("Имя пользователя", validators=[DataRequired()])
    password = PasswordField("Пароль", validators=[DataRequired()])
    submit = SubmitField("Войти")
