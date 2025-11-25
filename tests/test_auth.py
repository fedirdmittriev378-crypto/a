import pytest
from app import create_app, db
from app.models import User, Account, Transaction

class TestConfig:
    TESTING = True
    SECRET_KEY = 'test'
    SQLALCHEMY_DATABASE_URI = 'sqlite:///:memory:'
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    WTF_CSRF_ENABLED = False

@pytest.fixture
def app():
    app = create_app(TestConfig)
    with app.app_context():
        db.create_all()
        yield app
        db.drop_all()

@pytest.fixture
def client(app):
    return app.test_client()

def test_register_login_and_create_transaction(client, app):
    # Register
    rv = client.post('/register', data={
        'username': 'alice',
        'email': 'alice@example.com',
        'password': 'password123',
        'password_confirm': 'password123'
    }, follow_redirects=True)
    assert b'Регистрация прошла успешно' in rv.data

    with app.app_context():
        u = User.query.filter_by(username='alice').first()
        assert u is not None

    # Login
    rv = client.post('/login', data={
        'username': 'alice',
        'password': 'password123'
    }, follow_redirects=True)
    assert b'Вы вошли в систему' in rv.data

    # Create account
    rv = client.post('/accounts/add', data={
        'name': 'Main',
        'balance': '1000',
        'currency': 'RUB',
        'notes': ''
    }, follow_redirects=True)
    assert b'Счёт создан' in rv.data

    # Create transaction
    # find account id
    with app.app_context():
        acc = Account.query.filter_by(name='Main').first()
        assert acc is not None
        acc_id = acc.id

    rv = client.post('/transaction/add', data={
        'date': '2025-11-25',
        'amount': '50',
        'type': 'expense',
        'category': '0',
        'account': str(acc_id),
        'note': 'groceries'
    }, follow_redirects=True)
    assert b'Операция сохранена' in rv.data

    with app.app_context():
        u = User.query.filter_by(username='alice').first()
        txs = Transaction.query.filter_by(user_id=u.id).all()
        assert len(txs) == 1


def test_protected_routes_require_login(client):
    rv = client.get('/transactions')
    assert rv.status_code == 302
    assert '/login' in rv.headers['Location']
