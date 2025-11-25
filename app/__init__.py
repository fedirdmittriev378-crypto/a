import os
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from config import Config

db = SQLAlchemy()
migrate = Migrate()

def create_app(config_class=Config):
    app = Flask(__name__, static_folder="static", template_folder="templates")
    app.config.from_object(config_class)

    db.init_app(app)
    migrate.init_app(app, db)

    # Context processor для уведомлений
    from .context_processors import inject_unread_notifications
    app.context_processor(inject_unread_notifications)

    os.makedirs(app.config["REPORTS_FOLDER"], exist_ok=True)

    with app.app_context():
        from . import models, views, utils, notifications
        db.create_all()
        # generate recurring occurrences up to today
        try:
            utils.generate_recurring_occurrences()
        except Exception:
            pass

    return app


app = create_app()
