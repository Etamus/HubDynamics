"""database.py — Módulo SQLite para Hub Dynamics.

Substitui os arquivos JSON (users.json, requests_db.json, scheduler_db.json,
automations_db.json, dashboards_db.json) por um banco de dados SQLite único:
hub_dynamics.db

Na primeira execução os dados são migrados automaticamente dos JSONs legados.
As senhas são hasheadas com bcrypt durante a migração.
"""

import sqlite3
import json
import os
import datetime
from datetime import timezone, timedelta

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH  = os.path.join(BASE_DIR, 'hub_dynamics.db')

BRASILIA_TZ = timezone(timedelta(hours=-3))


def _get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')
    conn.execute('PRAGMA foreign_keys=ON')
    return conn


def init_db():
    """Cria as tabelas se não existirem e migra JSONs legados."""
    conn = _get_conn()
    with conn:
        conn.executescript('''
            CREATE TABLE IF NOT EXISTS users (
                username       TEXT PRIMARY KEY,
                password       TEXT NOT NULL,
                role           TEXT DEFAULT 'Analista',
                area           TEXT DEFAULT 'N/A',
                display_name   TEXT,
                profile_image  TEXT,
                login_attempts INTEGER DEFAULT 0,
                lockout_until  TEXT,
                allowed_areas  TEXT DEFAULT '[]',
                connections    TEXT DEFAULT '{}'
            );

            CREATE TABLE IF NOT EXISTS requests (
                token              TEXT PRIMARY KEY,
                username           TEXT,
                area               TEXT,
                role               TEXT,
                status             TEXT DEFAULT 'Aguardando Aprovação',
                request_date       TEXT,
                justification      TEXT,
                expiration_date    TEXT,
                generated_password TEXT,
                approved_at        TEXT
            );

            CREATE TABLE IF NOT EXISTS schedules (
                schedule_key TEXT PRIMARY KEY,
                queue        TEXT DEFAULT '[]',
                history      TEXT DEFAULT '[]'
            );

            CREATE TABLE IF NOT EXISTS config (
                key   TEXT PRIMARY KEY,
                value TEXT NOT NULL DEFAULT '{}'
            );

            CREATE TABLE IF NOT EXISTS audit_log (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                username  TEXT,
                action    TEXT NOT NULL,
                details   TEXT
            );
        ''')
    conn.close()
    _migrate_legacy_json()


# ---------------------------------------------------------------------------
# Migração automática de JSONs legados
# ---------------------------------------------------------------------------

def _migrate_legacy_json():
    """Importa dados dos arquivos JSON legados se as tabelas estiverem vazias."""
    import bcrypt as _bcrypt

    conn = _get_conn()

    # ── Users ────────────────────────────────────────────────────────────────
    if conn.execute('SELECT COUNT(*) FROM users').fetchone()[0] == 0:
        legacy_path = os.path.join(BASE_DIR, 'users.json')
        if os.path.exists(legacy_path):
            try:
                with open(legacy_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                with conn:
                    for username, d in data.items():
                        raw_pw = d.get('password', '')
                        if raw_pw and not raw_pw.startswith('$2b$'):
                            hashed = _bcrypt.hashpw(raw_pw.encode(), _bcrypt.gensalt()).decode()
                        else:
                            hashed = raw_pw
                        conn.execute(
                            'INSERT OR IGNORE INTO users VALUES (?,?,?,?,?,?,?,?,?,?)',
                            (
                                username,
                                hashed,
                                d.get('role', 'Analista'),
                                d.get('area', 'N/A'),
                                d.get('display_name'),
                                d.get('profile_image'),
                                d.get('login_attempts', 0),
                                d.get('lockout_until'),
                                json.dumps(d.get('allowed_areas', [])),
                                json.dumps(d.get('connections', {})),
                            )
                        )
                print('[DB] Usuários migrados de users.json')
                os.remove(legacy_path)
            except Exception as e:
                print(f'[DB] Falha ao migrar users.json: {e}')

    # ── Requests ─────────────────────────────────────────────────────────────
    if conn.execute('SELECT COUNT(*) FROM requests').fetchone()[0] == 0:
        legacy_path = os.path.join(BASE_DIR, 'requests_db.json')
        if os.path.exists(legacy_path):
            try:
                with open(legacy_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                with conn:
                    for token, d in data.items():
                        conn.execute(
                            'INSERT OR IGNORE INTO requests VALUES (?,?,?,?,?,?,?,?,?,?)',
                            (
                                token,
                                d.get('username'),
                                d.get('area'),
                                d.get('role'),
                                d.get('status', 'Aguardando Aprovação'),
                                d.get('request_date'),
                                d.get('justification'),
                                d.get('expiration_date'),
                                d.get('generated_password'),
                                d.get('approved_at'),
                            )
                        )
                print('[DB] Requests migradas de requests_db.json')
                os.remove(legacy_path)
            except Exception as e:
                print(f'[DB] Falha ao migrar requests_db.json: {e}')

    # ── Schedules ────────────────────────────────────────────────────────────
    if conn.execute('SELECT COUNT(*) FROM schedules').fetchone()[0] == 0:
        legacy_path = os.path.join(BASE_DIR, 'scheduler_db.json')
        if os.path.exists(legacy_path):
            try:
                with open(legacy_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                with conn:
                    for key, d in data.items():
                        conn.execute(
                            'INSERT OR IGNORE INTO schedules VALUES (?,?,?)',
                            (
                                key,
                                json.dumps(d.get('queue', [])),
                                json.dumps(d.get('history', [])),
                            )
                        )
                print('[DB] Agendamentos migrados de scheduler_db.json')
                os.remove(legacy_path)
            except Exception as e:
                print(f'[DB] Falha ao migrar scheduler_db.json: {e}')

    # ── Dashboards ───────────────────────────────────────────────────────────
    row = conn.execute("SELECT value FROM config WHERE key='dashboards'").fetchone()
    if row is None or json.loads(row['value']) == {}:
        legacy_path = os.path.join(BASE_DIR, 'dashboards_db.json')
        if os.path.exists(legacy_path):
            try:
                with open(legacy_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                with conn:
                    conn.execute(
                        'INSERT OR REPLACE INTO config VALUES (?,?)',
                        ('dashboards', json.dumps(data, ensure_ascii=False))
                    )
                print('[DB] Dashboards migrados de dashboards_db.json')
                os.remove(legacy_path)
            except Exception as e:
                print(f'[DB] Falha ao migrar dashboards_db.json: {e}')

    # ── Automations ──────────────────────────────────────────────────────────
    row = conn.execute("SELECT value FROM config WHERE key='automations'").fetchone()
    if row is None or json.loads(row['value']) == {}:
        legacy_path = os.path.join(BASE_DIR, 'automations_db.json')
        if os.path.exists(legacy_path):
            try:
                with open(legacy_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                with conn:
                    conn.execute(
                        'INSERT OR REPLACE INTO config VALUES (?,?)',
                        ('automations', json.dumps(data, ensure_ascii=False))
                    )
                print('[DB] Automações migradas de automations_db.json')
                os.remove(legacy_path)
            except Exception as e:
                print(f'[DB] Falha ao migrar automations_db.json: {e}')

    conn.close()


# ---------------------------------------------------------------------------
# Users
# ---------------------------------------------------------------------------

def _row_to_user(row) -> dict:
    return {
        'password':       row['password'],
        'role':           row['role'],
        'area':           row['area'],
        'display_name':   row['display_name'],
        'profile_image':  row['profile_image'],
        'login_attempts': row['login_attempts'],
        'lockout_until':  row['lockout_until'],
        'allowed_areas':  json.loads(row['allowed_areas'] or '[]'),
        'connections':    json.loads(row['connections']   or '{}'),
    }


def load_users() -> dict:
    conn = _get_conn()
    rows = conn.execute('SELECT * FROM users').fetchall()
    conn.close()
    return {row['username']: _row_to_user(row) for row in rows}


def save_users(users_data: dict):
    conn = _get_conn()
    with conn:
        existing = {r[0] for r in conn.execute('SELECT username FROM users')}
        for u in existing - set(users_data.keys()):
            conn.execute('DELETE FROM users WHERE username = ?', (u,))
        for username, d in users_data.items():
            conn.execute('''
                INSERT INTO users VALUES (?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(username) DO UPDATE SET
                    password=excluded.password,
                    role=excluded.role,
                    area=excluded.area,
                    display_name=excluded.display_name,
                    profile_image=excluded.profile_image,
                    login_attempts=excluded.login_attempts,
                    lockout_until=excluded.lockout_until,
                    allowed_areas=excluded.allowed_areas,
                    connections=excluded.connections
            ''', (
                username,
                d.get('password', ''),
                d.get('role', 'Analista'),
                d.get('area', 'N/A'),
                d.get('display_name'),
                d.get('profile_image'),
                d.get('login_attempts', 0),
                d.get('lockout_until'),
                json.dumps(d.get('allowed_areas', [])),
                json.dumps(d.get('connections', {})),
            ))
    conn.close()


# ---------------------------------------------------------------------------
# Requests
# ---------------------------------------------------------------------------

def _row_to_request(row) -> dict:
    return {
        'username':          row['username'],
        'area':              row['area'],
        'role':              row['role'],
        'status':            row['status'],
        'request_date':      row['request_date'],
        'justification':     row['justification'],
        'expiration_date':   row['expiration_date'],
        'generated_password': row['generated_password'],
        'approved_at':       row['approved_at'],
    }


def load_requests() -> dict:
    conn = _get_conn()
    rows = conn.execute('SELECT * FROM requests').fetchall()
    conn.close()
    return {row['token']: _row_to_request(row) for row in rows}


def save_requests(requests_data: dict):
    conn = _get_conn()
    with conn:
        existing = {r[0] for r in conn.execute('SELECT token FROM requests')}
        for t in existing - set(requests_data.keys()):
            conn.execute('DELETE FROM requests WHERE token = ?', (t,))
        for token, d in requests_data.items():
            conn.execute('''
                INSERT INTO requests VALUES (?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(token) DO UPDATE SET
                    username=excluded.username,
                    area=excluded.area,
                    role=excluded.role,
                    status=excluded.status,
                    request_date=excluded.request_date,
                    justification=excluded.justification,
                    expiration_date=excluded.expiration_date,
                    generated_password=excluded.generated_password,
                    approved_at=excluded.approved_at
            ''', (
                token,
                d.get('username'),
                d.get('area'),
                d.get('role'),
                d.get('status', 'Aguardando Aprovação'),
                d.get('request_date'),
                d.get('justification'),
                d.get('expiration_date'),
                d.get('generated_password'),
                d.get('approved_at'),
            ))
    conn.close()


# ---------------------------------------------------------------------------
# Schedules
# ---------------------------------------------------------------------------

def load_schedules() -> dict:
    conn = _get_conn()
    rows = conn.execute('SELECT * FROM schedules').fetchall()
    conn.close()
    return {
        row['schedule_key']: {
            'queue':   json.loads(row['queue']   or '[]'),
            'history': json.loads(row['history'] or '[]'),
        }
        for row in rows
    }


def save_schedules(schedules_data: dict):
    conn = _get_conn()
    with conn:
        existing = {r[0] for r in conn.execute('SELECT schedule_key FROM schedules')}
        for k in existing - set(schedules_data.keys()):
            conn.execute('DELETE FROM schedules WHERE schedule_key = ?', (k,))
        for key, d in schedules_data.items():
            conn.execute('''
                INSERT INTO schedules VALUES (?,?,?)
                ON CONFLICT(schedule_key) DO UPDATE SET
                    queue=excluded.queue,
                    history=excluded.history
            ''', (key, json.dumps(d.get('queue', [])), json.dumps(d.get('history', []))))
    conn.close()


# ---------------------------------------------------------------------------
# Config — Automations & Dashboards (JSON blobs)
# ---------------------------------------------------------------------------

def _load_config(key: str) -> dict:
    conn = _get_conn()
    row = conn.execute('SELECT value FROM config WHERE key=?', (key,)).fetchone()
    conn.close()
    return json.loads(row['value']) if row else {}


def _save_config(key: str, value: dict):
    conn = _get_conn()
    with conn:
        conn.execute(
            'INSERT INTO config(key,value) VALUES(?,?) '
            'ON CONFLICT(key) DO UPDATE SET value=excluded.value',
            (key, json.dumps(value, ensure_ascii=False))
        )
    conn.close()


def load_dashboards() -> dict:
    return _load_config('dashboards')


def load_automations() -> dict:
    return _load_config('automations')


def save_dashboards(data: dict):
    _save_config('dashboards', data)


def save_automations(data: dict):
    _save_config('automations', data)


# ---------------------------------------------------------------------------
# Audit Log
# ---------------------------------------------------------------------------

def audit(username: str, action: str, details: str = None):
    """Registra uma ação no log de auditoria."""
    ts = datetime.datetime.now(BRASILIA_TZ).isoformat()
    conn = _get_conn()
    with conn:
        conn.execute(
            'INSERT INTO audit_log (timestamp, username, action, details) VALUES (?,?,?,?)',
            (ts, username or 'anonymous', action, details)
        )
    conn.close()
