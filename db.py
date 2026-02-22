import os
import psycopg2
from datetime import datetime, timedelta, timezone

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL не найден. В Railway открой worker → Variables и добавь DATABASE_URL "
        "(или подключи Postgres к worker через Shared Variables)."
    )

# Railway Postgres даёт DATABASE_URL автоматически (после подключения БД к сервису worker)
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError(
        "DATABASE_URL не найден. В Railway открой worker → Variables и добавь DATABASE_URL "
        "(или подключи Postgres к worker через Shared Variables)."
    )


def _get_conn():
    return psycopg2.connect(DATABASE_URL)


def _ensure_column(cur, table: str, column_def: str):
    """Postgres: безопасно добавляет колонку, если её нет."""
    cur.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {column_def}")


def init_db():
    """Создание таблиц + добавление недостающих колонок (без потери данных)."""
    conn = _get_conn()
    cur = conn.cursor()

    # Таблица пользователей
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            tg_id BIGINT UNIQUE,
            balance DOUBLE PRECISION DEFAULT 0,
            referrer_id BIGINT,
            activated INTEGER DEFAULT 0,
            phone TEXT,
            created_at TEXT,
            last_bonus_at TEXT,
            banned INTEGER DEFAULT 0
        )
        """
    )

    # Миграции колонок (если база старая)
    _ensure_column(cur, "users", "referrer_id BIGINT")
    _ensure_column(cur, "users", "activated INTEGER DEFAULT 0")
    _ensure_column(cur, "users", "phone TEXT")
    _ensure_column(cur, "users", "created_at TEXT")
    _ensure_column(cur, "users", "last_bonus_at TEXT")
    _ensure_column(cur, "users", "banned INTEGER DEFAULT 0")
    _ensure_column(cur, "users", "balance DOUBLE PRECISION DEFAULT 0")
    _ensure_column(cur, "users", "language TEXT DEFAULT 'unset'")

    # Таблица выводов
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS withdrawals (
            id SERIAL PRIMARY KEY,
            tg_id BIGINT,
            method TEXT,
            details TEXT,
            amount DOUBLE PRECISION,
            status TEXT,
            created_at TEXT
        )
        """
    )

    # Таблица заявок по заданиям
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS task_submissions (
            id SERIAL PRIMARY KEY,
            tg_id BIGINT,
            task_id TEXT,
            status TEXT,
            proof_file_id TEXT,
            proof_caption TEXT,
            created_at TEXT
        )
        """
    )

    conn.commit()
    conn.close()


# ---------- USERS ----------

def create_user(tg_id, referrer_id=None):
    """Создаёт пользователя, если его ещё нет."""
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT id FROM users WHERE tg_id=%s", (tg_id,))
    row = cur.fetchone()
    if row:
        conn.close()
        return None

    created_at = datetime.now(timezone.utc).isoformat()
    cur.execute(
        """
        INSERT INTO users (tg_id, balance, referrer_id, activated, phone, created_at, last_bonus_at, banned)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (tg_id, 0.0, referrer_id, 0, None, created_at, None, 0),
    )
    conn.commit()
    conn.close()
    return created_at


def get_user(tg_id):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT tg_id, balance, referrer_id, activated, phone, created_at, last_bonus_at, banned
        FROM users WHERE tg_id=%s
        """,
        (tg_id,),
    )
    row = cur.fetchone()
    conn.close()
    return row


def activate_user(tg_id):
    """Активирует пользователя. Возвращает referrer_id или None."""
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT activated, referrer_id FROM users WHERE tg_id=%s", (tg_id,))
    row = cur.fetchone()
    if not row:
        conn.close()
        return None

    activated, referrer_id = row
    if activated:
        conn.close()
        return None

    cur.execute("UPDATE users SET activated=1 WHERE tg_id=%s", (tg_id,))
    conn.commit()
    conn.close()
    return referrer_id


def add_balance(tg_id, amount):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE users SET balance = balance + %s WHERE tg_id=%s", (amount, tg_id))
    conn.commit()
    conn.close()


def get_balance(tg_id):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT balance FROM users WHERE tg_id=%s", (tg_id,))
    row = cur.fetchone()
    conn.close()
    return float(row[0]) if row else 0.0


# ---------- PHONE ----------

def set_phone(tg_id, phone):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE users SET phone=%s WHERE tg_id=%s", (phone, tg_id))
    conn.commit()
    conn.close()


def get_phone(tg_id):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT phone FROM users WHERE tg_id=%s", (tg_id,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def is_phone_used(phone: str, except_id: int | None = None) -> bool:
    conn = _get_conn()
    cur = conn.cursor()
    if except_id is None:
        cur.execute("SELECT id FROM users WHERE phone=%s", (phone,))
    else:
        cur.execute("SELECT id FROM users WHERE phone=%s AND tg_id!=%s", (phone, except_id))
    row = cur.fetchone()
    conn.close()
    return row is not None


# ---------- BONUS ----------

def get_last_bonus_at(tg_id):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT last_bonus_at FROM users WHERE tg_id=%s", (tg_id,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def set_last_bonus_at(tg_id, value: str):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE users SET last_bonus_at=%s WHERE tg_id=%s", (value, tg_id))
    conn.commit()
    conn.close()


# ---------- LANGUAGE ----------

def get_language(tg_id):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT language FROM users WHERE tg_id=%s", (tg_id,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row and row[0] else "unset"


def set_language(tg_id, lang: str):
    if lang not in ("ru", "ua", "unset"):
        lang = "ru"
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE users SET language=%s WHERE tg_id=%s", (lang, tg_id))
    conn.commit()
    conn.close()


# ---------- BAN ----------

def is_banned(tg_id):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT banned FROM users WHERE tg_id=%s", (tg_id,))
    row = cur.fetchone()
    conn.close()
    return bool(row[0]) if row else False


def ban_user(tg_id):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE users SET banned=1 WHERE tg_id=%s", (tg_id,))
    conn.commit()
    conn.close()


def unban_user(tg_id):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE users SET banned=0 WHERE tg_id=%s", (tg_id,))
    conn.commit()
    conn.close()


# ---------- WITHDRAWALS ----------

def create_withdrawal(tg_id, method, details, amount):
    conn = _get_conn()
    cur = conn.cursor()
    created_at = datetime.now(timezone.utc).isoformat()
    cur.execute(
        """
        INSERT INTO withdrawals (tg_id, method, details, amount, status, created_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (tg_id, method, details, amount, "new", created_at),
    )
    wid = cur.fetchone()[0]
    conn.commit()
    conn.close()
    return wid


def get_withdraw(wd_id):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, tg_id, method, details, amount, status, created_at
        FROM withdrawals
        WHERE id=%s
        """,
        (wd_id,),
    )
    row = cur.fetchone()
    conn.close()
    return row


def set_withdraw_status(wd_id, status):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE withdrawals SET status=%s WHERE id=%s", (status, wd_id))
    conn.commit()
    conn.close()


def list_new_withdrawals(limit: int = 30):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, tg_id, method, details, amount, status, created_at
        FROM withdrawals
        WHERE status='new'
        ORDER BY id ASC
        LIMIT %s
        """,
        (limit,),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


# ---------- TASK SUBMISSIONS ----------

def create_task_submission(tg_id, task_id, proof_file_id, proof_caption):
    conn = _get_conn()
    cur = conn.cursor()
    created_at = datetime.now(timezone.utc).isoformat()
    cur.execute(
        """
        INSERT INTO task_submissions (tg_id, task_id, status, proof_file_id, proof_caption, created_at)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id
        """,
        (tg_id, task_id, "pending", proof_file_id, proof_caption, created_at),
    )
    sid = cur.fetchone()[0]
    conn.commit()
    conn.close()
    return sid


def get_task_submission(sub_id):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, tg_id, task_id, status, proof_file_id, proof_caption, created_at
        FROM task_submissions
        WHERE id=%s
        """,
        (sub_id,),
    )
    row = cur.fetchone()
    conn.close()
    return row


def set_task_status(sub_id, status):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("UPDATE task_submissions SET status=%s WHERE id=%s", (status, sub_id))
    conn.commit()
    conn.close()


def get_last_task_submission(tg_id, task_id):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id, status
        FROM task_submissions
        WHERE tg_id=%s AND task_id=%s
        ORDER BY id DESC
        LIMIT 1
        """,
        (tg_id, task_id),
    )
    row = cur.fetchone()
    conn.close()
    return row


def has_any_approved_task(tg_id) -> bool:
    """True, если у пользователя есть хотя бы 1 одобренная заявка по заданиям."""
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT 1
        FROM task_submissions
        WHERE tg_id=%s AND status='approved'
        LIMIT 1
        """,
        (tg_id,),
    )
    row = cur.fetchone()
    conn.close()
    return row is not None



# ---------- STATS / TOP / USERS ----------

def get_stats():
    conn = _get_conn()
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM users")
    total_users = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM users WHERE activated=1")
    activated_users = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM users WHERE phone IS NOT NULL AND phone != ''")
    with_phone = cur.fetchone()[0]

    cur.execute("SELECT COUNT(*) FROM users WHERE banned=1")
    banned_users = cur.fetchone()[0]

    point = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
    cur.execute("SELECT COUNT(*) FROM users WHERE created_at > %s", (point,))
    new_24h = cur.fetchone()[0]

    conn.close()
    return {
        "total_users": total_users,
        "activated_users": activated_users,
        "with_phone": with_phone,
        "banned_users": banned_users,
        "new_24h": new_24h,
    }


def get_top_referrers(limit: int = 10):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT referrer_id, COUNT(*) as cnt
        FROM users
        WHERE activated=1 AND referrer_id IS NOT NULL
        GROUP BY referrer_id
        ORDER BY cnt DESC
        LIMIT %s
        """,
        (limit,),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def list_users(limit: int = 200):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT tg_id, balance, referrer_id, activated, phone, created_at, last_bonus_at, banned
        FROM users
        ORDER BY created_at ASC
        LIMIT %s
        """,
        (limit,),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def list_all_users(limit: int = 200):
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT tg_id, balance, phone, activated, created_at, banned
        FROM users
        ORDER BY id ASC
        LIMIT %s
        """,
        (limit,),
    )
    rows = cur.fetchall()
    conn.close()
    return rows

# ===== USERS PAGINATION (для /users) =====

def count_users():
    """Количество всех пользователей."""
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM users")
    total = cur.fetchone()[0]
    conn.close()
    return int(total)


def list_users_page(offset: int = 0, limit: int = 50):
    """Страница пользователей (для админ-команды /users)."""
    conn = _get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT tg_id, balance, activated, banned, created_at
        FROM users
        ORDER BY id ASC
        OFFSET %s LIMIT %s
        """,
        (offset, limit),
    )
    rows = cur.fetchall()
    conn.close()
    return rows
