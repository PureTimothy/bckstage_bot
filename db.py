import json
import sqlite3
from datetime import date
from pathlib import Path
from typing import List, Optional, Tuple

import config
from translations import LANGUAGE_OPTIONS


CandidateRow = Tuple[int, str, int, str, Optional[str], str, int]


def init_db() -> None:
    db_path = Path(config.DB_PATH)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=5.0, isolation_level=None)
    cur = conn.cursor()
    # Faster/more concurrent SQLite defaults
    try:
        cur.execute("PRAGMA journal_mode=WAL;")
        cur.execute("PRAGMA synchronous=NORMAL;")
        cur.execute("PRAGMA busy_timeout=3000;")
    except Exception:
        pass

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS user_profiles (
            user_id INTEGER PRIMARY KEY,
            gender TEXT,
            language TEXT
        )
        """
    )
    try:
        cur.execute("ALTER TABLE user_stats ADD COLUMN boosts_credited INTEGER DEFAULT 0")
    except sqlite3.OperationalError:
        pass
    try:
        cur.execute("ALTER TABLE user_profiles ADD COLUMN language TEXT")
    except sqlite3.OperationalError:
        pass

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            local_id INTEGER UNIQUE,
            username TEXT,
            first_name TEXT,
            last_name TEXT,
            language_code TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            last_seen DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS candidates (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            name TEXT NOT NULL,
            age INTEGER,
            gender TEXT,
            instagram TEXT,
            photo_file_id TEXT NOT NULL,
            approved INTEGER DEFAULT 0
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS votes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            candidate_id INTEGER,
            rating INTEGER NOT NULL,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """
    )
    cur.execute(
        "INSERT OR IGNORE INTO settings (key, value) VALUES ('mode', 'collect')"
    )
    # Admin users
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS admin_users (
            user_id INTEGER PRIMARY KEY
        )
        """
    )
    cur.execute("INSERT OR IGNORE INTO admin_users (user_id) VALUES (?)", (config.ADMIN_ID,))

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS user_wallets (
            user_id INTEGER PRIMARY KEY,
            balance INTEGER DEFAULT 20,
            last_checkin DATE
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS user_stats (
            user_id INTEGER PRIMARY KEY,
            referrals INTEGER DEFAULT 0,
            referral_rewards INTEGER DEFAULT 0,
            boosts_credited INTEGER DEFAULT 0,
            profiles_created INTEGER DEFAULT 0,
            profiles_updated INTEGER DEFAULT 0,
            votes_cast INTEGER DEFAULT 0,
            likes_given INTEGER DEFAULT 0,
            swipes INTEGER DEFAULT 0,
            matches INTEGER DEFAULT 0,
            purchases INTEGER DEFAULT 0,
            gifts_sent INTEGER DEFAULT 0,
            gifts_received INTEGER DEFAULT 0,
            games_played INTEGER DEFAULT 0,
            candidates_submitted INTEGER DEFAULT 0
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS tinder_profiles (
            user_id INTEGER PRIMARY KEY,
            age INTEGER,
            gender TEXT,
            interest TEXT,
            city TEXT,
            normalized_city TEXT,
            lat REAL,
            lon REAL,
            name TEXT,
            bio TEXT,
            active INTEGER DEFAULT 1,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    try:
        cur.execute("ALTER TABLE tinder_profiles ADD COLUMN normalized_city TEXT")
    except sqlite3.OperationalError:
        pass

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS tinder_media (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            file_id TEXT,
            kind TEXT,
            position INTEGER
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS secret_santa (
            gift_number INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER UNIQUE,
            name TEXT,
            instagram TEXT,
            gift_photo_id TEXT,
            assigned_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    try:
        cur.execute("ALTER TABLE secret_santa ADD COLUMN assigned_at DATETIME DEFAULT CURRENT_TIMESTAMP")
    except sqlite3.OperationalError:
        pass
    try:
        cur.execute("ALTER TABLE secret_santa ADD COLUMN name TEXT")
    except sqlite3.OperationalError:
        pass
    try:
        cur.execute("ALTER TABLE secret_santa ADD COLUMN instagram TEXT")
    except sqlite3.OperationalError:
        pass
    try:
        cur.execute("ALTER TABLE secret_santa ADD COLUMN gift_photo_id TEXT")
    except sqlite3.OperationalError:
        pass

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS tinder_swipes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            target_id INTEGER,
            status TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS tinder_matches (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_a INTEGER,
            user_b INTEGER,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS phantom_users (
            user_id INTEGER PRIMARY KEY
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS referrals (
            user_id INTEGER PRIMARY KEY,
            referrer_id INTEGER,
            credited INTEGER DEFAULT 0
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS chat_boosts (
            boost_id TEXT PRIMARY KEY,
            user_id INTEGER,
            credited INTEGER DEFAULT 0
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS blackjack_boosts (
            user_id INTEGER PRIMARY KEY,
            boost REAL DEFAULT 0
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS shop_items (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            code TEXT,
            name TEXT,
            kind TEXT,
            price INTEGER
        )
        """
    )
    # Ensure code is unique for upsert logic
    cur.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_shop_items_code ON shop_items(code)
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS purchases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            item_id INTEGER,
            recipient_id INTEGER,
            recipient_username TEXT,
            data TEXT,
            status TEXT DEFAULT 'pending',
            notified INTEGER DEFAULT 0,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS user_contacts (
            user_id INTEGER PRIMARY KEY,
            full_name TEXT,
            email TEXT,
            address TEXT,
            size TEXT,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
        """
    )

    # Migrations for shop_items
    cur.execute("PRAGMA table_info(shop_items)")
    shop_cols = [row[1] for row in cur.fetchall()]
    if "code" not in shop_cols:
        try:
            cur.execute("ALTER TABLE shop_items ADD COLUMN code TEXT")
        except sqlite3.OperationalError:
            pass
    if "kind" not in shop_cols:
        try:
            cur.execute("ALTER TABLE shop_items ADD COLUMN kind TEXT")
        except sqlite3.OperationalError:
            pass
    # Backfill missing codes/kinds
    cur.execute(
        "UPDATE shop_items SET code = COALESCE(code, 'legacy_'||id), kind = COALESCE(kind, 'legacy') WHERE code IS NULL OR kind IS NULL"
    )

    # Migrations for purchases
    cur.execute("PRAGMA table_info(purchases)")
    purchase_cols = [row[1] for row in cur.fetchall()]
    if "recipient_id" not in purchase_cols:
        try:
            cur.execute("ALTER TABLE purchases ADD COLUMN recipient_id INTEGER")
        except sqlite3.OperationalError:
            pass
    if "recipient_username" not in purchase_cols:
        try:
            cur.execute("ALTER TABLE purchases ADD COLUMN recipient_username TEXT")
        except sqlite3.OperationalError:
            pass
    if "data" not in purchase_cols:
        try:
            cur.execute("ALTER TABLE purchases ADD COLUMN data TEXT")
        except sqlite3.OperationalError:
            pass
    if "status" not in purchase_cols:
        try:
            cur.execute("ALTER TABLE purchases ADD COLUMN status TEXT DEFAULT 'pending'")
        except sqlite3.OperationalError:
            pass
    if "notified" not in purchase_cols:
        try:
            cur.execute("ALTER TABLE purchases ADD COLUMN notified INTEGER DEFAULT 0")
        except sqlite3.OperationalError:
            pass

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS blackjack_sessions (
            user_id INTEGER PRIMARY KEY,
            deck TEXT,
            player_hand TEXT,
            dealer_hand TEXT,
            bet INTEGER,
            status TEXT
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS referrals (
            user_id INTEGER PRIMARY KEY,
            referrer_id INTEGER,
            credited INTEGER DEFAULT 0
        )
        """
    )

    # Indexes to speed up hotspots
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_votes_user_candidate ON votes(user_id, candidate_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_candidates_approved_gender ON candidates(approved, gender)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_tinder_swipes_user ON tinder_swipes(user_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_tinder_matches_usera ON tinder_matches(user_a)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_tinder_matches_userb ON tinder_matches(user_b)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_tinder_profiles_city ON tinder_profiles(normalized_city)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_purchases_notified ON purchases(notified)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_purchases_recipient ON purchases(recipient_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_purchases_recipient_username ON purchases(recipient_username)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_user_profiles_user ON user_profiles(user_id)")
        cur.execute("CREATE INDEX IF NOT EXISTS idx_referrals_referrer ON referrals(referrer_id)")
    except Exception:
        pass

    # default shop items
    try:
        cur.execute("INSERT OR IGNORE INTO shop_items (id, code, name, kind, price) VALUES (1, 'ticket', 'Ticket', 'ticket', 100)")
        cur.execute("INSERT OR IGNORE INTO shop_items (id, code, name, kind, price) VALUES (2, 'hoodie', 'Hoodie', 'hoodie', 450)")
        cur.execute("INSERT OR IGNORE INTO shop_items (id, code, name, kind, price) VALUES (3, 'bottle', 'Bottle Service', 'bottle', 1000)")
    except sqlite3.OperationalError as e:
        # Recover if legacy table missed new columns
        if "code" in str(e):
            try:
                cur.execute("ALTER TABLE shop_items ADD COLUMN code TEXT")
            except sqlite3.OperationalError:
                pass
            try:
                cur.execute("ALTER TABLE shop_items ADD COLUMN kind TEXT")
            except sqlite3.OperationalError:
                pass
            cur.execute(
                "UPDATE shop_items SET code = COALESCE(code, 'legacy_'||id), kind = COALESCE(kind, 'legacy') WHERE code IS NULL OR kind IS NULL"
            )
            cur.execute("INSERT OR IGNORE INTO shop_items (id, code, name, kind, price) VALUES (1, 'ticket', 'Ticket', 'ticket', 100)")
            cur.execute("INSERT OR IGNORE INTO shop_items (id, code, name, kind, price) VALUES (2, 'hoodie', 'Hoodie', 'hoodie', 450)")
            cur.execute("INSERT OR IGNORE INTO shop_items (id, code, name, kind, price) VALUES (3, 'bottle', 'Bottle Service', 'bottle', 1000)")
        else:
            raise

    conn.commit()
    conn.close()
    ensure_user_local_ids()
    # Ensure canonical shop items exist/updated even if legacy rows remained (run after closing initial conn to avoid locks)
    ensure_shop_item("ticket", "Ticket", "ticket", 100)
    ensure_shop_item("hoodie", "Hoodie", "hoodie", 450)
    ensure_shop_item("bottle", "Bottle Service", "bottle", 1000)


def ensure_user_local_ids() -> None:
    """Ensure users table has local_id column populated and indexed."""
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("PRAGMA table_info(users)")
    columns = [row[1] for row in cur.fetchall()]
    if "local_id" not in columns:
        try:
            cur.execute("ALTER TABLE users ADD COLUMN local_id INTEGER")
        except sqlite3.OperationalError:
            pass
    # Backfill missing local_id values
    cur.execute("SELECT user_id FROM users WHERE local_id IS NULL ORDER BY user_id")
    rows = cur.fetchall()
    if rows:
        cur.execute("SELECT COALESCE(MAX(local_id), 0) FROM users")
        start = cur.fetchone()[0] or 0
        next_id = start + 1
        for (uid,) in rows:
            cur.execute("UPDATE users SET local_id = ? WHERE user_id = ?", (next_id, uid))
            next_id += 1
    try:
        cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_local_id ON users(local_id)")
    except sqlite3.OperationalError:
        pass
    conn.commit()
    conn.close()


def get_mode() -> str:
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT value FROM settings WHERE key = 'mode'")
    row = cur.fetchone()
    conn.close()
    return row[0] if row else "collect"


def set_mode(mode: str) -> None:
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO settings (key, value) VALUES ('mode', ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (mode,),
    )
    conn.commit()
    conn.close()


def set_feature_enabled(name: str, enabled: bool) -> None:
    """Store a feature flag in settings as feature:<name> = on/off."""
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO settings (key, value) VALUES (?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (f"feature:{name}", "on" if enabled else "off"),
    )
    conn.commit()
    conn.close()


def is_feature_enabled(name: str, default: bool = True) -> bool:
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT value FROM settings WHERE key = ?", (f"feature:{name}",))
    row = cur.fetchone()
    conn.close()
    if row:
        return row[0] == "on"
    return default


# ---------- Admin users ----------


def is_admin_user(user_id: int) -> bool:
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM admin_users WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return bool(row)


def add_admin_user(user_id: int) -> None:
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO admin_users (user_id) VALUES (?)", (user_id,))
    conn.commit()
    conn.close()


def remove_admin_user(user_id: int) -> None:
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("DELETE FROM admin_users WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()


def list_admin_users() -> list[int]:
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT user_id FROM admin_users ORDER BY user_id")
    rows = cur.fetchall()
    conn.close()
    return [r[0] for r in rows]


def get_santa_number_for_user(user_id: int) -> Optional[int]:
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT gift_number FROM secret_santa WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def assign_secret_santa_number(user_id: int) -> Optional[int]:
    """Assign next gift number to user. Returns number or existing if already assigned."""
    existing = get_santa_number_for_user(user_id)
    if existing:
        return existing
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO secret_santa (user_id) VALUES (?)",
        (user_id,),
    )
    conn.commit()
    cur.execute("SELECT gift_number FROM secret_santa WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def update_secret_santa_details(user_id: int, name: str, gift_photo_id: Optional[str]) -> None:
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO secret_santa (user_id) VALUES (?)", (user_id,))
    cur.execute(
        """
        UPDATE secret_santa
        SET name = ?, gift_photo_id = ?
        WHERE user_id = ?
        """,
        (name, gift_photo_id, user_id),
    )
    conn.commit()
    conn.close()


def get_santa_details_for_user(user_id: int) -> Optional[Tuple[int, Optional[str], Optional[str]]]:
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT gift_number, name, gift_photo_id FROM secret_santa WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return row


def set_santa_gift_photo(user_id: int, file_id: str) -> None:
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO secret_santa (user_id) VALUES (?)", (user_id,))
    cur.execute("UPDATE secret_santa SET gift_photo_id = ? WHERE user_id = ?", (file_id, user_id))
    conn.commit()
    conn.close()


def get_first_santa_number() -> Optional[int]:
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT gift_number FROM secret_santa ORDER BY gift_number LIMIT 1")
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def get_next_santa_number(current: int) -> Optional[int]:
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT gift_number FROM secret_santa WHERE gift_number > ? ORDER BY gift_number LIMIT 1", (current,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def get_prev_santa_number(current: int) -> Optional[int]:
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT gift_number FROM secret_santa WHERE gift_number < ? ORDER BY gift_number DESC LIMIT 1", (current,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def get_santa_by_number(gift_number: int) -> Optional[Tuple[int, int, str, str, str, str, str]]:
    """Return (gift_number, user_id, username, first_name, last_name, name, gift_photo_id)."""
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT s.gift_number, s.user_id, u.username, u.first_name, u.last_name, s.name, s.gift_photo_id
        FROM secret_santa s
        LEFT JOIN users u ON u.user_id = s.user_id
        WHERE s.gift_number = ?
        """,
        (gift_number,),
    )
    row = cur.fetchone()
    conn.close()
    return row

STAT_COLUMNS = {
    "referrals": "referrals",
    "referral_rewards": "referral_rewards",
    "boosts_credited": "boosts_credited",
    "profiles_created": "profiles_created",
    "profiles_updated": "profiles_updated",
    "votes_cast": "votes_cast",
    "likes_given": "likes_given",
    "swipes": "swipes",
    "matches": "matches",
    "purchases": "purchases",
    "gifts_sent": "gifts_sent",
    "gifts_received": "gifts_received",
    "games_played": "games_played",
    "candidates_submitted": "candidates_submitted",
}


def ensure_user_stats(user_id: int) -> None:
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO user_stats (user_id) VALUES (?)", (user_id,))
    conn.commit()
    conn.close()


def increment_stat(user_id: int, field: str, delta: int = 1) -> None:
    column = STAT_COLUMNS.get(field)
    if not column or delta == 0:
        return
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("INSERT OR IGNORE INTO user_stats (user_id) VALUES (?)", (user_id,))
    cur.execute(
        f"UPDATE user_stats SET {column} = {column} + ? WHERE user_id = ?",
        (delta, user_id),
    )
    conn.commit()
    conn.close()


def ensure_wallet(user_id: int) -> None:
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO user_wallets (user_id, balance) VALUES (?, 20)",
        (user_id,),
    )
    conn.commit()
    conn.close()


def get_balance(user_id: int) -> int:
    ensure_wallet(user_id)
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT balance FROM user_wallets WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else 0


def adjust_balance(user_id: int, delta: int) -> int:
    ensure_wallet(user_id)
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "UPDATE user_wallets SET balance = balance + ? WHERE user_id = ?",
        (delta, user_id),
    )
    conn.commit()
    cur.execute("SELECT balance FROM user_wallets WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else 0


def get_last_checkin(user_id: int) -> Optional[str]:
    ensure_wallet(user_id)
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT last_checkin FROM user_wallets WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def record_checkin(user_id: int) -> int:
    today = date.today().isoformat()
    ensure_wallet(user_id)
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "UPDATE user_wallets SET balance = balance + 5, last_checkin = ? WHERE user_id = ?",
        (today, user_id),
    )
    conn.commit()
    cur.execute("SELECT balance FROM user_wallets WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else 0


def get_user_gender(user_id: int) -> Optional[str]:
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT gender FROM user_profiles WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def get_user_language(user_id: int) -> Optional[str]:
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT language FROM user_profiles WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    conn.close()
    if row and row[0] in LANGUAGE_OPTIONS:
        return row[0]
    return None


def get_user_stats(user_id: int):
    defaults = {
        "referrals": 0,
        "referral_rewards": 0,
        "boosts_credited": 0,
        "profiles_created": 0,
        "profiles_updated": 0,
        "votes_cast": 0,
        "likes": 0,
        "swipes": 0,
        "matches": 0,
        "purchases": 0,
        "gifts_sent": 0,
        "gifts_received": 0,
        "games_played": 0,
        "candidates_submitted": 0,
    }
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT referrals, referral_rewards, boosts_credited, profiles_created, profiles_updated, votes_cast,
               likes_given, swipes, matches, purchases, gifts_sent, gifts_received,
               games_played, candidates_submitted
        FROM user_stats WHERE user_id = ?
        """,
        (user_id,),
    )
    row = cur.fetchone()
    stats = defaults.copy()
    if row:
        (
            stats["referrals"],
            stats["referral_rewards"],
            stats["boosts_credited"],
            stats["profiles_created"],
            stats["profiles_updated"],
            stats["votes_cast"],
            stats["likes"],
            stats["swipes"],
            stats["matches"],
            stats["purchases"],
            stats["gifts_sent"],
            stats["gifts_received"],
            stats["games_played"],
            stats["candidates_submitted"],
        ) = row
    cur.execute("SELECT COUNT(*) FROM votes WHERE user_id = ?", (user_id,))
    votes = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM candidates WHERE user_id = ?", (user_id,))
    candidates = cur.fetchone()[0]
    conn.close()
    stats["votes"] = votes
    stats["candidates"] = candidates
    stats["votes_cast"] = max(stats["votes_cast"], votes)
    stats["candidates_submitted"] = max(stats["candidates_submitted"], candidates)
    return stats

def get_user_basic(user_id: int):
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "SELECT username, first_name, last_name FROM users WHERE user_id = ?",
        (user_id,),
    )
    row = cur.fetchone()
    conn.close()
    return row


def user_exists(user_id: int) -> bool:
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM users WHERE user_id = ? LIMIT 1", (user_id,))
    exists = cur.fetchone() is not None
    conn.close()
    return exists


def find_user_by_username(username: str) -> Optional[int]:
    if not username:
        return None
    uname = username.lstrip("@")
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "SELECT user_id FROM users WHERE LOWER(username) = LOWER(?) LIMIT 1",
        (uname,),
    )
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def reset_swipes_for_user(user_id: int):
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("DELETE FROM tinder_swipes WHERE user_id = ?", (user_id,))
    cur.execute(
        "DELETE FROM tinder_matches WHERE user_a = ? OR user_b = ?",
        (user_id, user_id),
    )
    conn.commit()
    conn.close()


def reset_swipes_all():
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("DELETE FROM tinder_swipes")
    cur.execute("DELETE FROM tinder_matches")
    conn.commit()
    conn.close()


def wipe_tinder_profiles():
    """Remove all Tinder-style profiles, media, swipes, and matches."""
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("DELETE FROM tinder_media")
    cur.execute("DELETE FROM tinder_swipes")
    cur.execute("DELETE FROM tinder_matches")
    cur.execute("DELETE FROM tinder_profiles")
    conn.commit()
    conn.close()


# ---------- Tinder-style helpers ----------


def upsert_profile(
    user_id: int,
    age: int,
    gender: str,
    interest: str,
    city: str,
    lat,
    lon,
    name: str,
    bio: str,
    normalized_city: str,
):
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO tinder_profiles (user_id, age, gender, interest, city, normalized_city, lat, lon, name, bio, active)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
        ON CONFLICT(user_id) DO UPDATE SET
            age = excluded.age,
            gender = excluded.gender,
            interest = excluded.interest,
            city = excluded.city,
            normalized_city = excluded.normalized_city,
            lat = excluded.lat,
            lon = excluded.lon,
            name = excluded.name,
            bio = excluded.bio,
            active = 1,
            updated_at = CURRENT_TIMESTAMP
        """,
        (user_id, age, gender, interest, city, normalized_city, lat, lon, name, bio),
    )
    conn.commit()
    conn.close()


def get_profile(user_id: int):
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT user_id, age, gender, interest, city, normalized_city, lat, lon, name, bio, active, created_at, updated_at
        FROM tinder_profiles WHERE user_id = ?
        """,
        (user_id,),
    )
    row = cur.fetchone()
    conn.close()
    return row


def clear_profile_media(user_id: int):
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("DELETE FROM tinder_media WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()


def add_profile_media(user_id: int, file_id: str, kind: str, position: int):
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO tinder_media (user_id, file_id, kind, position) VALUES (?, ?, ?, ?)",
        (user_id, file_id, kind, position),
    )
    conn.commit()
    conn.close()


def list_tinder_profiles(offset: int = 0, limit: int = 20):
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT user_id, age, gender, interest, city, lat, lon, normalized_city, name, bio, active
        FROM tinder_profiles
        ORDER BY user_id
        LIMIT ? OFFSET ?
        """,
        (limit, offset),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def set_profile_active(user_id: int, active: bool):
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "UPDATE tinder_profiles SET active = ?, updated_at = CURRENT_TIMESTAMP WHERE user_id = ?",
        (1 if active else 0, user_id),
    )
    conn.commit()
    conn.close()


def get_next_profile_id(current_id: int):
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "SELECT user_id FROM tinder_profiles WHERE user_id > ? ORDER BY user_id LIMIT 1",
        (current_id,),
    )
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def get_prev_profile_id(current_id: int):
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "SELECT user_id FROM tinder_profiles WHERE user_id < ? ORDER BY user_id DESC LIMIT 1",
        (current_id,),
    )
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def list_profile_media(user_id: int):
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "SELECT file_id, kind FROM tinder_media WHERE user_id = ? ORDER BY position",
        (user_id,),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def mark_swipe(user_id: int, target_id: int, status: str):
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO tinder_swipes (user_id, target_id, status)
        VALUES (?, ?, ?)
        """
        ,
        (user_id, target_id, status),
    )
    conn.commit()
    conn.close()
    increment_stat(user_id, "swipes")
    if status == "like":
        increment_stat(user_id, "likes_given")


def has_swiped(user_id: int, target_id: int) -> bool:
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM tinder_swipes WHERE user_id = ? AND target_id = ?",
        (user_id, target_id),
    )
    exists = cur.fetchone()
    conn.close()
    return bool(exists)


def check_mutual_like(user_id: int, target_id: int) -> bool:
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT 1 FROM tinder_swipes
        WHERE user_id = ? AND target_id = ? AND status = 'like'
        """,
        (target_id, user_id),
    )
    exists = cur.fetchone()
    if exists:
        cur.execute(
            """
            INSERT OR IGNORE INTO tinder_matches (user_a, user_b)
            VALUES (?, ?)
            """,
            (min(user_id, target_id), max(user_id, target_id)),
        )
        conn.commit()
        matched = True
    else:
        matched = False
    conn.close()
    if matched:
        increment_stat(user_id, "matches")
        increment_stat(target_id, "matches")
        return True
    return False


def mark_like_and_check(user_id: int, target_id: int) -> bool:
    """Mark a like and return True if it's mutual."""
    mark_swipe(user_id, target_id, "like")
    return check_mutual_like(user_id, target_id)


def users_who_liked_me(user_id: int):
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT s.user_id
        FROM tinder_swipes s
        WHERE s.target_id = ?
          AND s.status = 'like'
          AND NOT EXISTS (
              SELECT 1 FROM tinder_swipes s2
              WHERE s2.user_id = ? AND s2.target_id = s.user_id
          )
        ORDER BY s.created_at DESC
        """,
        (user_id, user_id),
    )
    rows = cur.fetchall()
    conn.close()
    return [r[0] for r in rows]


def get_next_candidate(user_id: int, user_gender: str, user_interest: str, normalized_city: str = ""):
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    def run_query(city_filter: bool):
        params = [user_id, user_id]
        city_clause = ""
        if city_filter and normalized_city:
            city_clause = " AND p.normalized_city = ? "
            params.append(normalized_city)
        cur.execute(
            f"""
            SELECT p.user_id, p.age, p.gender, p.interest, p.city, p.name, p.bio
            FROM tinder_profiles p
            WHERE p.active = 1
              AND p.user_id != ?
              AND NOT EXISTS (
                  SELECT 1 FROM tinder_swipes s
                  WHERE s.user_id = ? AND s.target_id = p.user_id
              )
              AND (
                    ? = 'Any' OR p.gender = ?
              )
              AND (
                    p.interest = 'Any' OR p.interest = ?
              )
              {city_clause}
            ORDER BY p.updated_at DESC
            LIMIT 1
            """,
            params + [user_interest, user_interest, user_gender],
        )
        return cur.fetchone()

    row = run_query(city_filter=True)
    if not row:
        row = run_query(city_filter=False)
    conn.close()
    return row


def set_user_language(user_id: int, language: str) -> None:
    from translations import LANGUAGE_OPTIONS as LANGS  # lazy import to avoid cycles

    if language not in LANGS:
        language = "en"
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO user_profiles (user_id, language)
        VALUES (?, ?)
        ON CONFLICT(user_id) DO UPDATE SET language = excluded.language
        """,
        (user_id, language),
    )
    conn.commit()
    conn.close()


def set_user_gender(user_id: int, gender: str) -> None:
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO user_profiles (user_id, gender)
        VALUES (?, ?)
        ON CONFLICT(user_id) DO UPDATE SET gender = excluded.gender
        """,
        (user_id, gender),
    )
    conn.commit()
    conn.close()


def delete_user_profile(user_id: int) -> None:
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("DELETE FROM user_profiles WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()


def upsert_user_basic(
    user_id: int,
    username: Optional[str],
    first_name: Optional[str],
    last_name: Optional[str],
    language_code: Optional[str],
) -> None:
    ensure_user_local_ids()
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT local_id FROM users WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    if row and row[0]:
        local_id = row[0]
    else:
        cur.execute("SELECT COALESCE(MAX(local_id), 0) + 1 FROM users")
        local_id = cur.fetchone()[0]

    cur.execute(
        """
        INSERT INTO users (user_id, local_id, username, first_name, last_name, language_code)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            local_id = COALESCE(users.local_id, excluded.local_id),
            username = excluded.username,
            first_name = excluded.first_name,
            last_name = excluded.last_name,
            language_code = excluded.language_code,
            last_seen = CURRENT_TIMESTAMP
        """,
        (user_id, local_id, username, first_name, last_name, language_code),
    )
    conn.commit()
    conn.close()


def get_local_user_id(user_id: int) -> Optional[int]:
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT local_id FROM users WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def delete_candidates_for_user(user_id: int) -> None:
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("DELETE FROM candidates WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()


def get_all_user_ids() -> List[int]:
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT user_id FROM user_profiles")
    rows = cur.fetchall()
    conn.close()
    return [r[0] for r in rows]


def list_users(limit: int = 50):
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT user_id, username, first_name, last_name, last_seen
        FROM users
        ORDER BY last_seen DESC
        LIMIT ?
        """,
        (limit,),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def get_user(user_id: int):
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT user_id, local_id, username, first_name, last_name, language_code, created_at, last_seen
        FROM users WHERE user_id = ?
        """,
        (user_id,),
    )
    row = cur.fetchone()
    conn.close()
    return row


def get_next_user_id(current_id: int):
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "SELECT user_id FROM users WHERE user_id > ? ORDER BY user_id LIMIT 1",
        (current_id,),
    )
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def get_prev_user_id(current_id: int):
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "SELECT user_id FROM users WHERE user_id < ? ORDER BY user_id DESC LIMIT 1",
        (current_id,),
    )
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def reset_votes_for_user(user_id: int) -> None:
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("DELETE FROM votes WHERE user_id = ?", (user_id,))
    cur.execute("DELETE FROM user_profiles WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()


def approve_candidate(candidate_id: int, value: int) -> None:
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "UPDATE candidates SET approved = ? WHERE id = ?",
        (value, candidate_id),
    )
    conn.commit()
    conn.close()


def get_candidate_by_id(candidate_id: int) -> Optional[CandidateRow]:
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "SELECT id, name, age, gender, instagram, photo_file_id, approved "
        "FROM candidates WHERE id = ?",
        (candidate_id,),
    )
    row = cur.fetchone()
    conn.close()
    return row  # type: ignore


def get_first_candidate() -> Optional[CandidateRow]:
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "SELECT id, name, age, gender, instagram, photo_file_id, approved "
        "FROM candidates ORDER BY id LIMIT 1"
    )
    row = cur.fetchone()
    conn.close()
    return row  # type: ignore


def get_next_candidate_id(current_id: int) -> Optional[int]:
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "SELECT id FROM candidates WHERE id > ? ORDER BY id LIMIT 1",
        (current_id,),
    )
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def get_prev_candidate_id(current_id: int) -> Optional[int]:
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "SELECT id FROM candidates WHERE id < ? ORDER BY id DESC LIMIT 1",
        (current_id,),
    )
    row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def has_candidate_for_user(user_id: int) -> bool:
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM candidates WHERE user_id = ?", (user_id,))
    exists = cur.fetchone()
    conn.close()
    return bool(exists)


def get_next_unrated_candidate(user_id: int, target_gender: Optional[str]):
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    params = []
    gender_clause = ""
    if target_gender:
        gender_clause = "AND gender = ?"
        params.append(target_gender)
    params.append(user_id)

    cur.execute(
        f"""
        SELECT id, name, age, gender, instagram, photo_file_id
        FROM candidates
        WHERE approved = 1
          {gender_clause}
          AND id NOT IN (
              SELECT candidate_id FROM votes WHERE user_id = ?
          )
        ORDER BY id
        LIMIT 1
        """,
        params,
    )
    row = cur.fetchone()
    conn.close()
    return row


def get_approved_count(target_gender: Optional[str] = None) -> int:
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    if target_gender:
        cur.execute(
            "SELECT COUNT(*) FROM candidates WHERE approved = 1 AND gender = ?",
            (target_gender,),
        )
    else:
        cur.execute("SELECT COUNT(*) FROM candidates WHERE approved = 1")
    count = cur.fetchone()[0]
    conn.close()
    return count


def add_vote(user_id: int, candidate_id: int, rating: int) -> bool:
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "SELECT 1 FROM votes WHERE user_id = ? AND candidate_id = ?",
        (user_id, candidate_id),
    )
    already = cur.fetchone()
    if already:
        conn.close()
        return False
    cur.execute(
        "INSERT INTO votes (user_id, candidate_id, rating) VALUES (?, ?, ?)",
        (user_id, candidate_id, rating),
    )
    conn.commit()
    conn.close()
    increment_stat(user_id, "votes_cast")
    return True


def add_candidate(
    user_id: int,
    name: str,
    age: int,
    gender: str,
    instagram: Optional[str],
    file_id: str,
) -> None:
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO candidates (user_id, name, age, gender, instagram, photo_file_id)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (user_id, name, age, gender, instagram, file_id),
    )
    conn.commit()
    conn.close()
    increment_stat(user_id, "candidates_submitted")


def delete_candidate_for_user(user_id: int):
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("DELETE FROM candidates WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()


def upsert_candidate_from_profile(user_id: int, name: str, age: int, gender: str, file_id: str, instagram: Optional[str] = None):
    """Create or refresh candidate from profile data; resets approval to pending."""
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT id FROM candidates WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    created = False
    if row:
        cid = row[0]
        cur.execute(
            """
            UPDATE candidates
            SET name = ?, age = ?, gender = ?, instagram = ?, photo_file_id = ?, approved = 0
            WHERE id = ?
            """,
            (name, age, gender, instagram, file_id, cid),
        )
    else:
        cur.execute(
            """
            INSERT INTO candidates (user_id, name, age, gender, instagram, photo_file_id, approved)
            VALUES (?, ?, ?, ?, ?, ?, 0)
            """,
            (user_id, name, age, gender, instagram, file_id),
        )
        created = True
    conn.commit()
    conn.close()
    if created:
        increment_stat(user_id, "candidates_submitted")


def delete_full_profile(user_id: int):
    """Delete all finder/profile data for a user (profile, media, swipes, matches, candidates, votes)."""
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("DELETE FROM tinder_media WHERE user_id = ?", (user_id,))
    cur.execute("DELETE FROM tinder_swipes WHERE user_id = ? OR target_id = ?", (user_id, user_id))
    cur.execute("DELETE FROM tinder_matches WHERE user_a = ? OR user_b = ?", (user_id, user_id))
    cur.execute("DELETE FROM tinder_profiles WHERE user_id = ?", (user_id,))
    cur.execute("DELETE FROM candidates WHERE user_id = ?", (user_id,))
    cur.execute("DELETE FROM votes WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()


def delete_user_completely(user_id: int):
    """Delete all user data including user row, wallets, contacts, purchases."""
    delete_full_profile(user_id)
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("DELETE FROM user_wallets WHERE user_id = ?", (user_id,))
    cur.execute("DELETE FROM user_profiles WHERE user_id = ?", (user_id,))
    cur.execute("DELETE FROM user_contacts WHERE user_id = ?", (user_id,))
    cur.execute("DELETE FROM user_stats WHERE user_id = ?", (user_id,))
    cur.execute("DELETE FROM purchases WHERE user_id = ? OR recipient_id = ?", (user_id, user_id))
    cur.execute("DELETE FROM blackjack_sessions WHERE user_id = ?", (user_id,))
    cur.execute("DELETE FROM phantom_users WHERE user_id = ?", (user_id,))
    cur.execute("DELETE FROM referrals WHERE user_id = ?", (user_id,))
    cur.execute("DELETE FROM users WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()


def add_phantom_user(user_id: int):
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "INSERT OR IGNORE INTO phantom_users (user_id) VALUES (?)",
        (user_id,),
    )
    conn.commit()
    conn.close()


def boost_exists(boost_id: str) -> bool:
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM chat_boosts WHERE boost_id = ? LIMIT 1", (boost_id,))
    exists = cur.fetchone() is not None
    conn.close()
    return exists


def add_boost(boost_id: str, user_id: Optional[int]):
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT OR IGNORE INTO chat_boosts (boost_id, user_id, credited)
        VALUES (?, ?, 0)
        """,
        (boost_id, user_id),
    )
    conn.commit()
    conn.close()


def mark_boost_credited(boost_id: str):
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("UPDATE chat_boosts SET credited = 1 WHERE boost_id = ?", (boost_id,))
    conn.commit()
    conn.close()


def set_referrer(user_id: int, referrer_id: int):
    """Store referrer for a user if not already set and not self-referral."""
    if user_id == referrer_id:
        return
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO referrals (user_id, referrer_id, credited)
        VALUES (?, ?, 0)
        ON CONFLICT(user_id) DO NOTHING
        """,
        (user_id, referrer_id),
    )
    conn.commit()
    conn.close()


def get_referrer(user_id: int):
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT referrer_id, credited FROM referrals WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    conn.close()
    return row if row else (None, None)


def mark_referral_credited(user_id: int):
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("UPDATE referrals SET credited = 1 WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()


def delete_phantom_users():
    """Remove all phantom guest users and their data (tracked ids + usernames ending with _ph)."""
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT user_id FROM phantom_users")
    phantom_ids = {row[0] for row in cur.fetchall()}
    # ESCAPE '\' to match literal underscore in suffix
    cur.execute("SELECT user_id FROM users WHERE username LIKE ? ESCAPE '\\'", ("%\\_ph",))
    phantom_ids.update([row[0] for row in cur.fetchall()])
    conn.close()
    for uid in phantom_ids:
        delete_full_profile(uid)
        conn = sqlite3.connect(config.DB_PATH)
        cur = conn.cursor()
        cur.execute("DELETE FROM user_wallets WHERE user_id = ?", (uid,))
        cur.execute("DELETE FROM user_profiles WHERE user_id = ?", (uid,))
        cur.execute("DELETE FROM user_contacts WHERE user_id = ?", (uid,))
        cur.execute("DELETE FROM user_stats WHERE user_id = ?", (uid,))
        cur.execute("DELETE FROM purchases WHERE user_id = ? OR recipient_id = ?", (uid, uid))
        cur.execute("DELETE FROM blackjack_sessions WHERE user_id = ?", (uid,))
        cur.execute("DELETE FROM phantom_users WHERE user_id = ?", (uid,))
        cur.execute("DELETE FROM users WHERE user_id = ?", (uid,))
        conn.commit()
        conn.close()


def list_shop_items() -> List[Tuple[int, str, int]]:
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT id, name, price, code, kind FROM shop_items ORDER BY id")
    rows = cur.fetchall()
    conn.close()
    return rows


def get_shop_item(item_id: int):
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT id, code, name, kind, price FROM shop_items WHERE id = ?", (item_id,))
    row = cur.fetchone()
    conn.close()
    return row


def ensure_shop_item(code: str, name: str, kind: str, price: int):
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT id FROM shop_items WHERE code = ?", (code,))
    row = cur.fetchone()
    if row:
        cur.execute(
            "UPDATE shop_items SET name = ?, kind = ?, price = ? WHERE code = ?",
            (name, kind, price, code),
        )
    else:
        cur.execute(
            "INSERT INTO shop_items (code, name, kind, price) VALUES (?, ?, ?, ?)",
            (code, name, kind, price),
        )
    conn.commit()
    conn.close()


def create_purchase(
    buyer_id: int,
    item_id: int,
    recipient_id: Optional[int],
    recipient_username: Optional[str],
    data: str,
    status: str = "pending",
) -> int:
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO purchases (user_id, item_id, recipient_id, recipient_username, data, status)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (buyer_id, item_id, recipient_id, recipient_username, data, status),
    )
    purchase_id = cur.lastrowid
    conn.commit()
    conn.close()
    increment_stat(buyer_id, "purchases")
    if recipient_id and recipient_id != buyer_id:
        increment_stat(buyer_id, "gifts_sent")
        increment_stat(recipient_id, "gifts_received")
    return purchase_id


def set_purchase_recipient(purchase_id: int, recipient_id: int, username: Optional[str] = None):
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "UPDATE purchases SET recipient_id = ?, recipient_username = COALESCE(recipient_username, ?), notified = 0 WHERE id = ?",
        (recipient_id, username, purchase_id),
    )
    conn.commit()
    conn.close()


def mark_purchase_notified(purchase_id: int):
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("UPDATE purchases SET notified = 1 WHERE id = ?", (purchase_id,))
    conn.commit()
    conn.close()


def update_purchase_status(purchase_id: int, status: str):
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "UPDATE purchases SET status = ?, created_at = created_at WHERE id = ?",
        (status, purchase_id),
    )
    conn.commit()
    conn.close()


def list_pending_purchases(limit: int = 20):
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT p.id, p.user_id, p.item_id, p.recipient_id, p.recipient_username, p.data, p.status, p.created_at,
               si.name, si.kind, si.price
        FROM purchases p
        JOIN shop_items si ON si.id = p.item_id
        WHERE p.status = 'pending'
        ORDER BY p.created_at ASC
        LIMIT ?
        """,
        (limit,),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def pending_gifts_for_user(user_id: int, username: Optional[str]):
    """Return purchases where this user is the intended recipient and not yet notified."""
    uname = (username or "").lstrip("@")
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT p.id, p.user_id, p.item_id, p.recipient_username, p.data, si.name, si.kind
        FROM purchases p
        JOIN shop_items si ON si.id = p.item_id
        WHERE p.notified = 0
          AND p.status = 'pending'
          AND (
                p.recipient_id = ?
                OR (p.recipient_id IS NULL AND p.recipient_username IS NOT NULL AND LOWER(p.recipient_username) = LOWER(?))
              )
        """,
        (user_id, uname),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def get_purchase(purchase_id: int):
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT p.id, p.user_id, p.item_id, p.recipient_id, p.recipient_username, p.data, p.status, p.notified,
               si.name, si.kind, si.price, si.code
        FROM purchases p
        JOIN shop_items si ON si.id = p.item_id
        WHERE p.id = ?
        """,
        (purchase_id,),
    )
    row = cur.fetchone()
    conn.close()
    return row


def update_purchase_data(purchase_id: int, data: str):
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("UPDATE purchases SET data = ? WHERE id = ?", (data, purchase_id))
    conn.commit()
    conn.close()


def pending_gifts_for_user(user_id: int, username: Optional[str]):
    """Return purchases where this user is the intended recipient and not yet notified."""
    uname = (username or "").lstrip("@")
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT p.id, p.user_id, p.item_id, p.recipient_username, p.data, si.name, si.kind
        FROM purchases p
        JOIN shop_items si ON si.id = p.item_id
        WHERE p.notified = 0
          AND p.status = 'pending'
          AND (
                p.recipient_id = ?
                OR (p.recipient_id IS NULL AND p.recipient_username IS NOT NULL AND LOWER(p.recipient_username) = LOWER(?))
              )
        """,
        (user_id, uname),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def upsert_contact(user_id: int, full_name: Optional[str], email: Optional[str], address: Optional[str] = None, size: Optional[str] = None):
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS user_contacts (
            user_id INTEGER PRIMARY KEY,
            full_name TEXT,
            email TEXT,
            address TEXT,
            size TEXT
        )
        """
    )
    cur.execute(
        """
        INSERT INTO user_contacts (user_id, full_name, email, address, size)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            full_name=COALESCE(excluded.full_name, full_name),
            email=COALESCE(excluded.email, email),
            address=COALESCE(excluded.address, address),
            size=COALESCE(excluded.size, size)
        """,
        (user_id, full_name, email, address, size),
    )
    conn.commit()
    conn.close()


def get_contact(user_id: int):
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS user_contacts (
            user_id INTEGER PRIMARY KEY,
            full_name TEXT,
            email TEXT,
            address TEXT,
            size TEXT
        )
        """
    )
    cur.execute(
        """
        SELECT full_name, email, address, size FROM user_contacts WHERE user_id = ?
        """,
        (user_id,),
    )
    row = cur.fetchone()
    conn.close()
    return row


def save_blackjack_session(
    user_id: int,
    deck,
    player_hand,
    dealer_hand,
    bet: int,
    status: str = "active",
) -> None:
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO blackjack_sessions (user_id, deck, player_hand, dealer_hand, bet, status)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            deck = excluded.deck,
            player_hand = excluded.player_hand,
            dealer_hand = excluded.dealer_hand,
            bet = excluded.bet,
            status = excluded.status
        """,
        (
            user_id,
            json.dumps(deck),
            json.dumps(player_hand),
            json.dumps(dealer_hand),
            bet,
            status,
        ),
    )
    conn.commit()
    conn.close()


def set_blackjack_boost(user_id: int, boost: float):
    """Boost is a fraction, e.g., 0.2 for +20% win bias."""
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO blackjack_boosts (user_id, boost)
        VALUES (?, ?)
        ON CONFLICT(user_id) DO UPDATE SET boost = excluded.boost
        """,
        (user_id, boost),
    )
    conn.commit()
    conn.close()


def get_blackjack_boost(user_id: int) -> float:
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT boost FROM blackjack_boosts WHERE user_id = ?", (user_id,))
    row = cur.fetchone()
    conn.close()
    if row and row[0] is not None:
        return float(row[0])
    return 0.0


def load_blackjack_session(user_id: int):
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        "SELECT deck, player_hand, dealer_hand, bet, status FROM blackjack_sessions WHERE user_id = ?",
        (user_id,),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    deck, player_hand, dealer_hand, bet, status = row
    return {
        "deck": json.loads(deck),
        "player_hand": json.loads(player_hand),
        "dealer_hand": json.loads(dealer_hand),
        "bet": bet,
        "status": status,
    }


def clear_blackjack_session(user_id: int) -> None:
    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("DELETE FROM blackjack_sessions WHERE user_id = ?", (user_id,))
    conn.commit()
    conn.close()
