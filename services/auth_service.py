"""Multi-user login and ticket audit logging against existing app_users / ticket_audit_log tables.

Used only by app.py. Not used by mcp_server.py, chatbot.py, or MCP tools.
"""
import json
from werkzeug.security import check_password_hash


def get_active_user_by_username(db, username):
    username = str(username or "").strip()
    if not username:
        return None
    with db.cursor() as cursor:
        cursor.execute(
            """
            SELECT id, username, password_hash, role, active
            FROM app_users
            WHERE LOWER(TRIM(username)) = LOWER(TRIM(%s))
            LIMIT 1
            """,
            (username,),
        )
        return cursor.fetchone()

def verify_login(db, username, password):
    """Return the user row on success, or None on invalid/inactive credentials."""
    user = get_active_user_by_username(db, username)
    if not user or not user.get("active"):
        return None
    if not check_password_hash(str(user.get("password_hash") or ""), str(password or "")):
        return None
    return user

def _json_safe(value):
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_safe(v) for v in value]
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value

def record_ticket_audit(
    db,
    ticket_id,
    ticket_number,
    action,
    actor_user_id=None,
    actor_username="",
    before_data=None,
    after_data=None,
    note="",
):
    before_data = _json_safe(dict(before_data or {}))
    after_data = _json_safe(dict(after_data or {}))
    changed_fields = sorted(
        {
            key
            for key in set(before_data) | set(after_data)
            if before_data.get(key) != after_data.get(key)
        }
    )
    with db.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO ticket_audit_log (
                ticket_id, ticket_number, action, actor_user_id,
                actor_username_snapshot, before_data, after_data, changed_fields, note
            )
            VALUES (%s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s)
            """,
            (
                ticket_id,
                ticket_number,
                action,
                actor_user_id,
                str(actor_username or ""),
                json.dumps(before_data),
                json.dumps(after_data),
                json.dumps(changed_fields),
                note,
            ),
        )
