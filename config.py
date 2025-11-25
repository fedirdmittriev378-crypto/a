import os
from datetime import timedelta

basedir = os.path.abspath(os.path.dirname(__file__))


class Config:
    SECRET_KEY = os.environ.get("SECRET_KEY") or "change_this_secret_in_prod"
    SQLALCHEMY_DATABASE_URI = os.environ.get("DATABASE_URL") or \
        "sqlite:///" + os.path.join(basedir, "personal_budget.db")
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    REPORTS_FOLDER = os.path.join(basedir, "app", "static", "reports")
    DEFAULT_CURRENCY = "RUB"
    PERMANENT_SESSION_LIFETIME = timedelta(days=30)
