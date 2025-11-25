"""
Context processors для глобальных переменных в шаблонах
"""
from .models import Notification
from flask import g as flask_g

def inject_unread_notifications():
    """Добавляет количество непрочитанных уведомлений во все шаблоны"""
    try:
        if getattr(flask_g, 'user', None):
            unread_count = Notification.query.filter_by(is_read=False, user_id=flask_g.user.id).count()
        else:
            unread_count = Notification.query.filter_by(is_read=False).count()
        return {'unread_count': unread_count, 'current_user': getattr(flask_g, 'user', None)}
    except:
        return {'unread_count': 0, 'current_user': None}

