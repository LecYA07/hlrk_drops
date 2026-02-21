import aiosqlite
import datetime
import logging

logger = logging.getLogger("Database")

class Database:
    def __init__(self, db_path):
        self.db_path = db_path

    async def init(self):
        async with aiosqlite.connect(self.db_path) as db:
            # Active Users
            await db.execute('''
                CREATE TABLE IF NOT EXISTS active_users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    channel TEXT,
                    nickname TEXT,
                    last_active_at DATETIME
                )
            ''')

            await db.execute('''
                CREATE TABLE IF NOT EXISTS watch_time (
                    channel TEXT NOT NULL,
                    nickname TEXT NOT NULL,
                    seconds INTEGER NOT NULL DEFAULT 0,
                    last_seen_at DATETIME,
                    PRIMARY KEY(channel, nickname)
                )
            ''')

            await db.execute('''
                CREATE TABLE IF NOT EXISTS stream_sessions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    channel TEXT NOT NULL,
                    started_at DATETIME,
                    ended_at DATETIME
                )
            ''')

            await db.execute('''
                CREATE TABLE IF NOT EXISTS stream_watch_time (
                    session_id INTEGER NOT NULL,
                    nickname TEXT NOT NULL,
                    seconds INTEGER NOT NULL DEFAULT 0,
                    last_seen_at DATETIME,
                    PRIMARY KEY(session_id, nickname),
                    FOREIGN KEY(session_id) REFERENCES stream_sessions(id)
                )
            ''')
            
            # Rewards
            await db.execute('''
                CREATE TABLE IF NOT EXISTS rewards (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    channel_id INTEGER,
                    name TEXT,
                    description TEXT,
                    weight INTEGER,
                    quantity INTEGER DEFAULT 1,
                    enabled INTEGER DEFAULT 1
                )
            ''')
            
            # Draws (Updated)
            await db.execute('''
                CREATE TABLE IF NOT EXISTS draws (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    channel TEXT,
                    nickname TEXT,
                    reward_id INTEGER,
                    created_at DATETIME,
                    status TEXT DEFAULT 'pending', -- pending, claimed, expired
                    expires_at DATETIME,
                    notified_in_tg INTEGER DEFAULT 0,
                    FOREIGN KEY(reward_id) REFERENCES rewards(id)
                )
            ''')

            # Telegram Users
            await db.execute('''
                CREATE TABLE IF NOT EXISTS telegram_users (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_id INTEGER UNIQUE,
                    twitch_username TEXT,
                    verification_code TEXT,
                    created_at DATETIME
                )
            ''')

            await db.execute('''
                CREATE TABLE IF NOT EXISTS withdrawals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_id INTEGER NOT NULL,
                    telegram_username TEXT,
                    item_name TEXT NOT NULL,
                    photo_file_id TEXT NOT NULL,
                    price TEXT NOT NULL,
                    pattern TEXT NOT NULL,
                    status TEXT DEFAULT 'pending',
                    reason TEXT,
                    created_at DATETIME,
                    decided_at DATETIME,
                    admin_id INTEGER,
                    admin_chat_id INTEGER,
                    admin_message_id INTEGER
                )
            ''')

            await db.execute('''
                CREATE TABLE IF NOT EXISTS gold_balances (
                    telegram_id INTEGER PRIMARY KEY,
                    balance INTEGER NOT NULL DEFAULT 0,
                    updated_at DATETIME
                )
            ''')

            await db.execute('''
                CREATE TABLE IF NOT EXISTS gold_transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_id INTEGER NOT NULL,
                    amount INTEGER NOT NULL,
                    source_type TEXT NOT NULL,
                    source_id INTEGER NOT NULL,
                    created_at DATETIME,
                    UNIQUE(telegram_id, source_type, source_id)
                )
            ''')

            await db.execute('''
                CREATE TABLE IF NOT EXISTS check_channels (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    chat_id INTEGER UNIQUE,
                    title TEXT,
                    created_at DATETIME
                )
            ''')

            await db.execute('''
                CREATE TABLE IF NOT EXISTS gold_checks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT UNIQUE,
                    amount INTEGER NOT NULL,
                    max_activations INTEGER NOT NULL,
                    activated_count INTEGER NOT NULL DEFAULT 0,
                    status TEXT NOT NULL DEFAULT 'active',
                    created_by INTEGER,
                    created_at DATETIME,
                    channel_id INTEGER,
                    message_id INTEGER
                )
            ''')

            await db.execute('''
                CREATE TABLE IF NOT EXISTS gold_check_activations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    check_id INTEGER NOT NULL,
                    telegram_id INTEGER NOT NULL,
                    activated_at DATETIME,
                    UNIQUE(check_id, telegram_id),
                    FOREIGN KEY(check_id) REFERENCES gold_checks(id)
                )
            ''')

            await db.execute('''
                CREATE TABLE IF NOT EXISTS giveaway_triggers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    requested_by INTEGER,
                    created_at DATETIME,
                    processed_at DATETIME,
                    trigger_type TEXT DEFAULT 'random',
                    channel_id INTEGER,
                    reward_id INTEGER,
                    winners_count INTEGER,
                    planned_giveaway_id INTEGER,
                    guess_number INTEGER,
                    guess_min INTEGER,
                    guess_max INTEGER
                )
            ''')

            await db.execute('''
                CREATE TABLE IF NOT EXISTS planned_giveaways (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    channel_id INTEGER,
                    reward_id INTEGER NOT NULL,
                    title TEXT NOT NULL,
                    winners_count INTEGER NOT NULL DEFAULT 1,
                    status TEXT NOT NULL DEFAULT 'planned',
                    created_by INTEGER,
                    created_at DATETIME,
                    triggered_at DATETIME,
                    FOREIGN KEY(reward_id) REFERENCES rewards(id)
                )
            ''')

            await db.execute('''
                CREATE TABLE IF NOT EXISTS item_claims (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    draw_id INTEGER UNIQUE,
                    telegram_id INTEGER NOT NULL,
                    twitch_username TEXT,
                    reward_name TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'available',
                    claimed_at DATETIME
                )
            ''')

            await db.execute('''
                CREATE TABLE IF NOT EXISTS conversion_requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_id INTEGER NOT NULL,
                    telegram_username TEXT,
                    draw_id INTEGER NOT NULL,
                    reward_name TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'pending',
                    requested_at DATETIME,
                    decided_at DATETIME,
                    admin_id INTEGER,
                    gold_amount INTEGER,
                    reason TEXT,
                    admin_chat_id INTEGER,
                    admin_message_id INTEGER,
                    UNIQUE(draw_id)
                )
            ''')

            await db.execute('''
                CREATE TABLE IF NOT EXISTS channels (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    login TEXT UNIQUE,
                    owner_telegram_id INTEGER,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    created_at DATETIME
                )
            ''')

            await db.execute('''
                CREATE TABLE IF NOT EXISTS channel_settings (
                    channel_id INTEGER PRIMARY KEY,
                    min_interval_minutes INTEGER,
                    max_interval_minutes INTEGER,
                    active_timeout_minutes INTEGER,
                    claim_timeout_minutes INTEGER,
                    drops_enabled INTEGER DEFAULT 1,
                    updated_at DATETIME,
                    FOREIGN KEY(channel_id) REFERENCES channels(id)
                )
            ''')

            await db.execute('''
                CREATE TABLE IF NOT EXISTS channel_requests (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    telegram_id INTEGER NOT NULL,
                    telegram_username TEXT,
                    twitch_login TEXT NOT NULL,
                    contact TEXT,
                    note TEXT,
                    status TEXT NOT NULL DEFAULT 'pending',
                    created_at DATETIME,
                    admin_chat_id INTEGER,
                    admin_message_id INTEGER
                )
            ''')

            # Migrations
            await self.migrate_draws_table(db)
            await self.migrate_withdrawals_table(db)
            await self.migrate_gold_tables(db)
            await self.migrate_giveaway_triggers_table(db)
            await self.migrate_rewards_table(db)
            await self.migrate_planned_giveaways_table(db)
            
            await db.commit()
            logger.info("Database initialized.")

    async def migrate_draws_table(self, db):
        async with db.execute("PRAGMA table_info(draws)") as cursor:
            columns = [row[1] for row in await cursor.fetchall()]
            
        if 'status' not in columns:
            logger.info("Migrating draws table: Adding status column")
            await db.execute("ALTER TABLE draws ADD COLUMN status TEXT DEFAULT 'claimed'") # Old draws are claimed
        
        if 'expires_at' not in columns:
            logger.info("Migrating draws table: Adding expires_at column")
            await db.execute("ALTER TABLE draws ADD COLUMN expires_at DATETIME")

        if 'notified_in_tg' not in columns:
            logger.info("Migrating draws table: Adding notified_in_tg column")
            await db.execute("ALTER TABLE draws ADD COLUMN notified_in_tg INTEGER DEFAULT 0")

    async def migrate_withdrawals_table(self, db):
        async with db.execute("PRAGMA table_info(withdrawals)") as cursor:
            columns = [row[1] for row in await cursor.fetchall()]

        if not columns:
            return

        required = {
            "status": "ALTER TABLE withdrawals ADD COLUMN status TEXT DEFAULT 'pending'",
            "reason": "ALTER TABLE withdrawals ADD COLUMN reason TEXT",
            "admin_id": "ALTER TABLE withdrawals ADD COLUMN admin_id INTEGER",
            "admin_chat_id": "ALTER TABLE withdrawals ADD COLUMN admin_chat_id INTEGER",
            "admin_message_id": "ALTER TABLE withdrawals ADD COLUMN admin_message_id INTEGER",
            "decided_at": "ALTER TABLE withdrawals ADD COLUMN decided_at DATETIME",
            "telegram_username": "ALTER TABLE withdrawals ADD COLUMN telegram_username TEXT",
        }
        for col, sql in required.items():
            if col not in columns:
                await db.execute(sql)

    async def migrate_gold_tables(self, db):
        async with db.execute("PRAGMA table_info(gold_balances)") as cursor:
            columns = [row[1] for row in await cursor.fetchall()]
        if columns and "updated_at" not in columns:
            await db.execute("ALTER TABLE gold_balances ADD COLUMN updated_at DATETIME")

    async def migrate_giveaway_triggers_table(self, db):
        async with db.execute("PRAGMA table_info(giveaway_triggers)") as cursor:
            columns = [row[1] for row in await cursor.fetchall()]

        if not columns:
            return

        required = {
            "trigger_type": "ALTER TABLE giveaway_triggers ADD COLUMN trigger_type TEXT DEFAULT 'random'",
            "channel_id": "ALTER TABLE giveaway_triggers ADD COLUMN channel_id INTEGER",
            "reward_id": "ALTER TABLE giveaway_triggers ADD COLUMN reward_id INTEGER",
            "winners_count": "ALTER TABLE giveaway_triggers ADD COLUMN winners_count INTEGER",
            "planned_giveaway_id": "ALTER TABLE giveaway_triggers ADD COLUMN planned_giveaway_id INTEGER",
            "guess_number": "ALTER TABLE giveaway_triggers ADD COLUMN guess_number INTEGER",
            "guess_min": "ALTER TABLE giveaway_triggers ADD COLUMN guess_min INTEGER",
            "guess_max": "ALTER TABLE giveaway_triggers ADD COLUMN guess_max INTEGER",
        }
        for col, sql in required.items():
            if col not in columns:
                await db.execute(sql)

    async def migrate_rewards_table(self, db):
        async with db.execute("PRAGMA table_info(rewards)") as cursor:
            columns = [row[1] for row in await cursor.fetchall()]
        if not columns:
            return
        if "channel_id" not in columns:
            await db.execute("ALTER TABLE rewards ADD COLUMN channel_id INTEGER")

    async def migrate_planned_giveaways_table(self, db):
        async with db.execute("PRAGMA table_info(planned_giveaways)") as cursor:
            columns = [row[1] for row in await cursor.fetchall()]
        if not columns:
            return
        if "channel_id" not in columns:
            await db.execute("ALTER TABLE planned_giveaways ADD COLUMN channel_id INTEGER")

    async def get_telegram_user(self, telegram_id):
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT * FROM telegram_users WHERE telegram_id = ?", (telegram_id,)) as cursor:
                row = await cursor.fetchone()
                if row:
                    return {
                        'id': row[0],
                        'telegram_id': row[1],
                        'twitch_username': row[2],
                        'verification_code': row[3],
                        'created_at': row[4]
                    }
                return None

    async def create_telegram_verification(self, telegram_id, code):
        async with aiosqlite.connect(self.db_path) as db:
            now = datetime.datetime.now()
            # Upsert
            await db.execute("""
                INSERT INTO telegram_users (telegram_id, verification_code, created_at) 
                VALUES (?, ?, ?)
                ON CONFLICT(telegram_id) DO UPDATE SET verification_code = ?, twitch_username = NULL
            """, (telegram_id, code, now, code))
            await db.commit()

    async def verify_twitch_link(self, twitch_username, code):
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT telegram_id FROM telegram_users WHERE verification_code = ?", (code,)) as cursor:
                row = await cursor.fetchone()
                if row:
                    telegram_id = row[0]
                    await db.execute("UPDATE telegram_users SET twitch_username = ?, verification_code = NULL WHERE telegram_id = ?", (twitch_username, telegram_id))
                    await db.commit()
                    return telegram_id
                return None
    
    async def get_user_stats(self, twitch_username):
        async with aiosqlite.connect(self.db_path) as db:
            # Count wins
            async with db.execute("SELECT COUNT(*) FROM draws WHERE nickname = ? AND status = 'claimed'", (twitch_username,)) as cursor:
                wins = (await cursor.fetchone())[0]
            
            # Last win
            async with db.execute("""
                SELECT d.created_at, r.name 
                FROM draws d 
                JOIN rewards r ON d.reward_id = r.id 
                WHERE d.nickname = ? AND d.status = 'claimed' 
                ORDER BY d.created_at DESC LIMIT 1
            """, (twitch_username,)) as cursor:
                last_win = await cursor.fetchone()
            
            return {'wins': wins, 'last_win': last_win}

    async def get_pending_notifications(self):
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("""
                SELECT d.id, d.nickname, r.name, tu.telegram_id
                FROM draws d
                JOIN rewards r ON d.reward_id = r.id
                JOIN telegram_users tu ON d.nickname = tu.twitch_username
                WHERE d.status = 'claimed' AND d.notified_in_tg = 0
            """) as cursor:
                return await cursor.fetchall()

    async def mark_notified(self, draw_ids):
        if not draw_ids:
            return
        async with aiosqlite.connect(self.db_path) as db:
            placeholders = ','.join('?' * len(draw_ids))
            await db.execute(f"UPDATE draws SET notified_in_tg = 1 WHERE id IN ({placeholders})", draw_ids)
            await db.commit()

    async def get_telegram_id_by_twitch_username(self, twitch_username: str):
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT telegram_id FROM telegram_users WHERE twitch_username = ?",
                (twitch_username,),
            ) as cursor:
                row = await cursor.fetchone()
                return row[0] if row else None

    async def get_all_linked_telegram_ids(self):
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT telegram_id FROM telegram_users WHERE twitch_username IS NOT NULL"
            ) as cursor:
                rows = await cursor.fetchall()
                return [row[0] for row in rows]

    async def expire_pending_draws(self):
        now = datetime.datetime.now()
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                """
                SELECT d.id, d.nickname, r.name, tu.telegram_id
                FROM draws d
                JOIN rewards r ON r.id = d.reward_id
                LEFT JOIN telegram_users tu ON tu.twitch_username = d.nickname
                WHERE d.status = 'pending' AND d.expires_at IS NOT NULL AND d.expires_at <= ?
                """,
                (now,),
            ) as cursor:
                rows = await cursor.fetchall()

            if not rows:
                return []

            draw_ids = [row[0] for row in rows]
            placeholders = ",".join(["?"] * len(draw_ids))
            await db.execute(
                f"UPDATE draws SET status = 'expired' WHERE id IN ({placeholders})",
                draw_ids,
            )
            await db.commit()

        return [(row[1], row[2], row[3]) for row in rows]

    async def get_linked_users_count(self):
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT COUNT(*) FROM telegram_users WHERE twitch_username IS NOT NULL"
            ) as cursor:
                return (await cursor.fetchone())[0]

    async def get_total_draws_count(self):
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT COUNT(*) FROM draws") as cursor:
                return (await cursor.fetchone())[0]

    async def ensure_channel(self, login: str, owner_telegram_id: int | None = None, enabled: int = 1) -> int:
        login = (login or "").strip().lower()
        if not login:
            raise ValueError("bad channel login")
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT id, owner_telegram_id FROM channels WHERE login = ?", (login,)) as cursor:
                row = await cursor.fetchone()
            if row:
                if owner_telegram_id and not row[1]:
                    await db.execute(
                        "UPDATE channels SET owner_telegram_id = ? WHERE id = ?",
                        (int(owner_telegram_id), int(row[0])),
                    )
                    await db.commit()
                return int(row[0])
            now = datetime.datetime.now()
            cur = await db.execute(
                "INSERT INTO channels (login, owner_telegram_id, enabled, created_at) VALUES (?, ?, ?, ?)",
                (login, owner_telegram_id, int(enabled), now),
            )
            await db.commit()
            return int(cur.lastrowid)

    async def get_channel_by_login(self, login: str) -> dict | None:
        login = (login or "").strip().lower()
        if not login:
            return None
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT id, login, owner_telegram_id, enabled FROM channels WHERE login = ?",
                (login,),
            ) as cursor:
                row = await cursor.fetchone()
            if not row:
                return None
            return {
                "id": int(row[0]),
                "login": row[1],
                "owner_telegram_id": int(row[2]) if row[2] is not None else None,
                "enabled": int(row[3]) if row[3] is not None else 0,
            }

    async def get_channel_by_id(self, channel_id: int) -> dict | None:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT id, login, owner_telegram_id, enabled FROM channels WHERE id = ?",
                (int(channel_id),),
            ) as cursor:
                row = await cursor.fetchone()
            if not row:
                return None
            return {
                "id": int(row[0]),
                "login": row[1],
                "owner_telegram_id": int(row[2]) if row[2] is not None else None,
                "enabled": int(row[3]) if row[3] is not None else 0,
            }

    async def list_enabled_channels(self) -> list[dict]:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT id, login, owner_telegram_id FROM channels WHERE enabled = 1 ORDER BY id ASC"
            ) as cursor:
                rows = await cursor.fetchall()
            return [
                {
                    "id": int(r[0]),
                    "login": r[1],
                    "owner_telegram_id": int(r[2]) if r[2] is not None else None,
                }
                for r in rows
            ]

    async def list_all_channels(self) -> list[dict]:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT id, login, owner_telegram_id, enabled FROM channels ORDER BY id ASC"
            ) as cursor:
                rows = await cursor.fetchall()
            return [
                {
                    "id": int(r[0]),
                    "login": r[1],
                    "owner_telegram_id": int(r[2]) if r[2] is not None else None,
                    "enabled": int(r[3]) if r[3] is not None else 0,
                }
                for r in rows
            ]

    async def list_channels_by_owner(self, telegram_id: int) -> list[dict]:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT id, login, enabled FROM channels WHERE owner_telegram_id = ? ORDER BY id ASC",
                (int(telegram_id),),
            ) as cursor:
                rows = await cursor.fetchall()
            return [{"id": int(r[0]), "login": r[1], "enabled": int(r[2])} for r in rows]

    async def backfill_channel_data(self, channel_id: int) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE rewards SET channel_id = ? WHERE channel_id IS NULL",
                (int(channel_id),),
            )
            await db.execute(
                "UPDATE planned_giveaways SET channel_id = ? WHERE channel_id IS NULL",
                (int(channel_id),),
            )
            await db.execute(
                "UPDATE giveaway_triggers SET channel_id = ? WHERE channel_id IS NULL",
                (int(channel_id),),
            )
            await db.commit()

    async def get_channel_settings(self, channel_id: int) -> dict | None:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                """
                SELECT min_interval_minutes, max_interval_minutes, active_timeout_minutes, claim_timeout_minutes, drops_enabled
                FROM channel_settings WHERE channel_id = ?
                """,
                (int(channel_id),),
            ) as cursor:
                row = await cursor.fetchone()
            if not row:
                return None
            return {
                "min_interval_minutes": int(row[0]) if row[0] is not None else None,
                "max_interval_minutes": int(row[1]) if row[1] is not None else None,
                "active_timeout_minutes": int(row[2]) if row[2] is not None else None,
                "claim_timeout_minutes": int(row[3]) if row[3] is not None else None,
                "drops_enabled": int(row[4]) if row[4] is not None else 1,
            }

    async def upsert_channel_settings(
        self,
        channel_id: int,
        min_interval_minutes: int,
        max_interval_minutes: int,
        active_timeout_minutes: int,
        claim_timeout_minutes: int,
        drops_enabled: int = 1,
    ) -> None:
        now = datetime.datetime.now()
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                INSERT INTO channel_settings
                    (channel_id, min_interval_minutes, max_interval_minutes, active_timeout_minutes, claim_timeout_minutes, drops_enabled, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(channel_id) DO UPDATE SET
                    min_interval_minutes = excluded.min_interval_minutes,
                    max_interval_minutes = excluded.max_interval_minutes,
                    active_timeout_minutes = excluded.active_timeout_minutes,
                    claim_timeout_minutes = excluded.claim_timeout_minutes,
                    drops_enabled = excluded.drops_enabled,
                    updated_at = excluded.updated_at
                """,
                (
                    int(channel_id),
                    int(min_interval_minutes),
                    int(max_interval_minutes),
                    int(active_timeout_minutes),
                    int(claim_timeout_minutes),
                    int(drops_enabled),
                    now,
                ),
            )
            await db.commit()

    async def update_channel_settings(self, channel_id: int, **kwargs) -> None:
        fields = []
        values = []
        for key, value in kwargs.items():
            fields.append(f"{key} = ?")
            values.append(int(value) if isinstance(value, bool) else value)
        if not fields:
            return
        values.append(datetime.datetime.now())
        values.append(int(channel_id))
        sql = f"UPDATE channel_settings SET {', '.join(fields)}, updated_at = ? WHERE channel_id = ?"
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(sql, values)
            await db.commit()

    async def create_channel_request(
        self,
        telegram_id: int,
        telegram_username: str,
        twitch_login: str,
        contact: str,
        note: str = "",
        admin_chat_id: int | None = None,
        admin_message_id: int | None = None,
    ) -> int:
        now = datetime.datetime.now()
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute(
                """
                INSERT INTO channel_requests
                    (telegram_id, telegram_username, twitch_login, contact, note, status, created_at, admin_chat_id, admin_message_id)
                VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?)
                """,
                (
                    int(telegram_id),
                    telegram_username or "",
                    (twitch_login or "").strip().lower(),
                    contact or "",
                    note or "",
                    now,
                    admin_chat_id,
                    admin_message_id,
                ),
            )
            await db.commit()
            return int(cur.lastrowid)

    async def set_channel_request_admin_message(self, request_id: int, admin_chat_id: int, admin_message_id: int) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE channel_requests SET admin_chat_id = ?, admin_message_id = ? WHERE id = ?",
                (int(admin_chat_id), int(admin_message_id), int(request_id)),
            )
            await db.commit()

    async def get_channel_request(self, request_id: int) -> dict | None:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                """
                SELECT id, telegram_id, telegram_username, twitch_login, contact, note, status, admin_chat_id, admin_message_id
                FROM channel_requests WHERE id = ?
                """,
                (int(request_id),),
            ) as cursor:
                row = await cursor.fetchone()
            if not row:
                return None
            return {
                "id": int(row[0]),
                "telegram_id": int(row[1]),
                "telegram_username": row[2] or "",
                "twitch_login": row[3] or "",
                "contact": row[4] or "",
                "note": row[5] or "",
                "status": row[6] or "",
                "admin_chat_id": row[7],
                "admin_message_id": row[8],
            }

    async def set_channel_request_status(self, request_id: int, status: str) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE channel_requests SET status = ? WHERE id = ?",
                (status, int(request_id)),
            )
            await db.commit()

    async def create_reward(
        self,
        channel_id: int,
        name: str,
        description: str = "",
        weight: int = 0,
        quantity: int = 1,
        enabled: int = 0,
    ) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute(
                """
                INSERT INTO rewards (channel_id, name, description, weight, quantity, enabled)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (int(channel_id), name, description, int(weight), int(quantity), int(enabled)),
            )
            await db.commit()
            return int(cur.lastrowid)

    async def get_reward(self, reward_id: int):
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT id, channel_id, name, description, weight, quantity, enabled FROM rewards WHERE id = ?",
                (reward_id,),
            ) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return None
                return {
                    "id": int(row[0]),
                    "channel_id": int(row[1]) if row[1] is not None else None,
                    "name": row[2],
                    "description": row[3] or "",
                    "weight": int(row[4]) if row[4] is not None else 0,
                    "quantity": int(row[5]) if row[5] is not None else 1,
                    "enabled": int(row[6]) if row[6] is not None else 0,
                }

    async def list_rewards(self, channel_id: int) -> list[dict]:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                """
                SELECT id, name, description, weight, quantity, enabled
                FROM rewards
                WHERE channel_id = ?
                ORDER BY id DESC
                """,
                (int(channel_id),),
            ) as cursor:
                rows = await cursor.fetchall()
                return [
                    {
                        "id": int(r[0]),
                        "name": r[1],
                        "description": r[2] or "",
                        "weight": int(r[3]) if r[3] is not None else 0,
                        "quantity": int(r[4]) if r[4] is not None else 1,
                        "enabled": int(r[5]) if r[5] is not None else 0,
                    }
                    for r in rows
                ]

    async def set_reward_enabled(self, reward_id: int, enabled: int) -> bool:
        async with aiosqlite.connect(self.db_path) as db:
            cur = await db.execute(
                "UPDATE rewards SET enabled = ? WHERE id = ?",
                (int(enabled), int(reward_id)),
            )
            await db.commit()
            return cur.rowcount > 0

    async def create_planned_giveaway(self, channel_id: int, title: str, winners_count: int, created_by: int) -> int:
        title = (title or "").strip()
        winners_count = int(winners_count)
        if not title or winners_count <= 0:
            raise ValueError("bad planned giveaway")
        reward_id = await self.create_reward(
            channel_id=int(channel_id),
            name=title,
            description="planned_giveaway",
            weight=0,
            quantity=winners_count,
            enabled=0,
        )
        async with aiosqlite.connect(self.db_path) as db:
            now = datetime.datetime.now()
            cur = await db.execute(
                """
                INSERT INTO planned_giveaways (channel_id, reward_id, title, winners_count, status, created_by, created_at)
                VALUES (?, ?, ?, ?, 'planned', ?, ?)
                """,
                (int(channel_id), reward_id, title, winners_count, created_by, now),
            )
            await db.commit()
            return int(cur.lastrowid)

    async def list_planned_giveaways(self, channel_id: int | None = None, status: str | None = None) -> list[dict]:
        async with aiosqlite.connect(self.db_path) as db:
            if status and channel_id is not None:
                async with db.execute(
                    """
                    SELECT id, channel_id, reward_id, title, winners_count, status, created_at, triggered_at
                    FROM planned_giveaways
                    WHERE status = ? AND channel_id = ?
                    ORDER BY id DESC
                    """,
                    (status, int(channel_id)),
                ) as cursor:
                    rows = await cursor.fetchall()
            elif status:
                async with db.execute(
                    """
                    SELECT id, channel_id, reward_id, title, winners_count, status, created_at, triggered_at
                    FROM planned_giveaways
                    WHERE status = ?
                    ORDER BY id DESC
                    """,
                    (status,),
                ) as cursor:
                    rows = await cursor.fetchall()
            elif channel_id is not None:
                async with db.execute(
                    """
                    SELECT id, channel_id, reward_id, title, winners_count, status, created_at, triggered_at
                    FROM planned_giveaways
                    WHERE channel_id = ?
                    ORDER BY id DESC
                    """,
                    (int(channel_id),),
                ) as cursor:
                    rows = await cursor.fetchall()
            else:
                async with db.execute(
                    """
                    SELECT id, channel_id, reward_id, title, winners_count, status, created_at, triggered_at
                    FROM planned_giveaways
                    ORDER BY id DESC
                    """
                ) as cursor:
                    rows = await cursor.fetchall()
            return [
                {
                    "id": int(r[0]),
                    "channel_id": int(r[1]) if r[1] is not None else None,
                    "reward_id": int(r[2]),
                    "title": r[3],
                    "winners_count": int(r[4]),
                    "status": r[5],
                    "created_at": r[6],
                    "triggered_at": r[7],
                }
                for r in rows
            ]

    async def mark_planned_giveaway_triggered(self, planned_id: int) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            now = datetime.datetime.now()
            await db.execute(
                "UPDATE planned_giveaways SET status = 'triggered', triggered_at = ? WHERE id = ? AND status = 'planned'",
                (now, planned_id),
            )
            await db.commit()

    async def set_planned_giveaway_status(self, planned_id: int, status: str) -> bool:
        status = (status or "").strip()
        if status not in ("planned", "end", "triggered"):
            return False
        async with aiosqlite.connect(self.db_path) as db:
            if status == "triggered":
                now = datetime.datetime.now()
                await db.execute(
                    "UPDATE planned_giveaways SET status = 'triggered', triggered_at = ? WHERE id = ?",
                    (now, int(planned_id)),
                )
            else:
                await db.execute(
                    "UPDATE planned_giveaways SET status = ? WHERE id = ?",
                    (status, int(planned_id)),
                )
            await db.commit()
            return True

    async def create_planned_giveaway_trigger(self, planned_giveaway_id: int, requested_by: int) -> int:
        planned_giveaway_id = int(planned_giveaway_id)
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT channel_id, reward_id, winners_count, status FROM planned_giveaways WHERE id = ?",
                (planned_giveaway_id,),
            ) as cursor:
                row = await cursor.fetchone()
            if not row or (row[3] != "planned" and row[3] != "triggered"):
                raise ValueError("planned giveaway not found")
            channel_id = int(row[0]) if row[0] is not None else None
            reward_id = int(row[1])
            winners_count = int(row[2])
            now = datetime.datetime.now()
            cur = await db.execute(
                """
                INSERT INTO giveaway_triggers
                    (requested_by, created_at, trigger_type, channel_id, reward_id, winners_count, planned_giveaway_id)
                VALUES
                    (?, ?, 'planned', ?, ?, ?, ?)
                """,
                (requested_by, now, channel_id, reward_id, winners_count, planned_giveaway_id),
            )
            await db.commit()
            return int(cur.lastrowid)

    async def record_item_claim(self, draw_id: int, telegram_id: int, twitch_username: str, reward_name: str) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            now = datetime.datetime.now()
            await db.execute(
                """
                INSERT INTO item_claims (draw_id, telegram_id, twitch_username, reward_name, status, claimed_at)
                VALUES (?, ?, ?, ?, 'available', ?)
                ON CONFLICT(draw_id) DO NOTHING
                """,
                (int(draw_id), int(telegram_id), twitch_username, reward_name, now),
            )
            await db.commit()

    async def list_available_item_claims(self, telegram_id: int) -> list[dict]:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                """
                SELECT draw_id, reward_name, claimed_at
                FROM item_claims
                WHERE telegram_id = ? AND status = 'available'
                ORDER BY id DESC
                """,
                (int(telegram_id),),
            ) as cursor:
                rows = await cursor.fetchall()
                return [{"draw_id": int(r[0]), "reward_name": r[1], "claimed_at": r[2]} for r in rows]

    async def create_conversion_request(self, telegram_id: int, telegram_username: str, draw_id: int) -> int | None:
        telegram_id = int(telegram_id)
        draw_id = int(draw_id)
        async with aiosqlite.connect(self.db_path) as db:
            now = datetime.datetime.now()
            await db.execute("BEGIN IMMEDIATE")
            async with db.execute(
                "SELECT reward_name, status FROM item_claims WHERE draw_id = ? AND telegram_id = ?",
                (draw_id, telegram_id),
            ) as cursor:
                row = await cursor.fetchone()
            if not row or row[1] != "available":
                await db.execute("ROLLBACK")
                return None
            reward_name = row[0]
            await db.execute(
                "UPDATE item_claims SET status = 'conversion_pending' WHERE draw_id = ? AND telegram_id = ?",
                (draw_id, telegram_id),
            )
            try:
                cur = await db.execute(
                    """
                    INSERT INTO conversion_requests
                        (telegram_id, telegram_username, draw_id, reward_name, status, requested_at)
                    VALUES
                        (?, ?, ?, ?, 'pending', ?)
                    """,
                    (telegram_id, telegram_username, draw_id, reward_name, now),
                )
            except aiosqlite.IntegrityError:
                await db.execute("ROLLBACK")
                return None
            await db.execute("COMMIT")
            return int(cur.lastrowid)

    async def get_conversion_request(self, request_id: int):
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT * FROM conversion_requests WHERE id = ?",
                (int(request_id),),
            ) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return None
                return {
                    "id": int(row[0]),
                    "telegram_id": int(row[1]),
                    "telegram_username": row[2] or "",
                    "draw_id": int(row[3]),
                    "reward_name": row[4],
                    "status": row[5],
                    "requested_at": row[6],
                    "decided_at": row[7],
                    "admin_id": row[8],
                    "gold_amount": row[9],
                    "reason": row[10],
                    "admin_chat_id": row[11],
                    "admin_message_id": row[12],
                }

    async def set_conversion_admin_message(self, request_id: int, admin_chat_id: int, admin_message_id: int) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                UPDATE conversion_requests
                SET admin_chat_id = ?, admin_message_id = ?
                WHERE id = ?
                """,
                (int(admin_chat_id), int(admin_message_id), int(request_id)),
            )
            await db.commit()

    async def decide_conversion(
        self,
        request_id: int,
        status: str,
        admin_id: int,
        gold_amount: int | None = None,
        reason: str | None = None,
    ) -> bool:
        request_id = int(request_id)
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("BEGIN IMMEDIATE")
            async with db.execute(
                "SELECT status, draw_id, telegram_id FROM conversion_requests WHERE id = ?",
                (request_id,),
            ) as cursor:
                row = await cursor.fetchone()
            if not row or row[0] != "pending":
                await db.execute("ROLLBACK")
                return False
            draw_id = int(row[1])
            telegram_id = int(row[2])
            now = datetime.datetime.now()
            await db.execute(
                """
                UPDATE conversion_requests
                SET status = ?, decided_at = ?, admin_id = ?, gold_amount = ?, reason = ?
                WHERE id = ?
                """,
                (status, now, int(admin_id), gold_amount, reason, request_id),
            )
            if status == "credited":
                await db.execute(
                    "UPDATE item_claims SET status = 'converted' WHERE draw_id = ? AND telegram_id = ?",
                    (draw_id, telegram_id),
                )
            else:
                await db.execute(
                    "UPDATE item_claims SET status = 'available' WHERE draw_id = ? AND telegram_id = ?",
                    (draw_id, telegram_id),
                )
            await db.execute("COMMIT")
            return True

    async def credit_conversion_request(self, request_id: int, admin_id: int, gold_amount: int) -> dict:
        request_id = int(request_id)
        gold_amount = int(gold_amount)
        if gold_amount <= 0:
            return {"ok": False, "status": "bad_amount"}
        async with aiosqlite.connect(self.db_path) as db:
            now = datetime.datetime.now()
            await db.execute("BEGIN IMMEDIATE")
            async with db.execute(
                "SELECT status, draw_id, telegram_id FROM conversion_requests WHERE id = ?",
                (request_id,),
            ) as cursor:
                row = await cursor.fetchone()
            if not row:
                await db.execute("ROLLBACK")
                return {"ok": False, "status": "not_found"}
            if row[0] != "pending":
                await db.execute("ROLLBACK")
                return {"ok": False, "status": "not_pending"}
            draw_id = int(row[1])
            telegram_id = int(row[2])

            try:
                await db.execute(
                    """
                    INSERT INTO gold_transactions (telegram_id, amount, source_type, source_id, created_at)
                    VALUES (?, ?, 'conversion', ?, ?)
                    """,
                    (telegram_id, gold_amount, request_id, now),
                )
            except aiosqlite.IntegrityError:
                await db.execute("ROLLBACK")
                return {"ok": False, "status": "exists"}

            await db.execute(
                """
                INSERT INTO gold_balances (telegram_id, balance, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(telegram_id) DO UPDATE SET
                    balance = balance + excluded.balance,
                    updated_at = excluded.updated_at
                """,
                (telegram_id, gold_amount, now),
            )

            await db.execute(
                """
                UPDATE conversion_requests
                SET status = 'credited', decided_at = ?, admin_id = ?, gold_amount = ?
                WHERE id = ?
                """,
                (now, int(admin_id), gold_amount, request_id),
            )
            await db.execute(
                "UPDATE item_claims SET status = 'converted' WHERE draw_id = ? AND telegram_id = ?",
                (draw_id, telegram_id),
            )
            await db.execute("COMMIT")
            return {"ok": True, "status": "credited", "telegram_id": telegram_id, "amount": gold_amount}

    async def create_withdrawal(
        self,
        telegram_id: int,
        telegram_username: str,
        item_name: str,
        photo_file_id: str,
        price: str,
        pattern: str,
    ) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            now = datetime.datetime.now()
            cur = await db.execute(
                """
                INSERT INTO withdrawals
                    (telegram_id, telegram_username, item_name, photo_file_id, price, pattern, status, created_at)
                VALUES
                    (?, ?, ?, ?, ?, ?, 'pending', ?)
                """,
                (telegram_id, telegram_username, item_name, photo_file_id, price, pattern, now),
            )
            await db.commit()
            return int(cur.lastrowid)

    async def set_withdrawal_admin_message(
        self, withdrawal_id: int, admin_chat_id: int, admin_message_id: int
    ) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                UPDATE withdrawals
                SET admin_chat_id = ?, admin_message_id = ?
                WHERE id = ?
                """,
                (admin_chat_id, admin_message_id, withdrawal_id),
            )
            await db.commit()

    async def get_withdrawal(self, withdrawal_id: int):
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT * FROM withdrawals WHERE id = ?",
                (withdrawal_id,),
            ) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return None
                return {
                    "id": row[0],
                    "telegram_id": row[1],
                    "telegram_username": row[2],
                    "item_name": row[3],
                    "photo_file_id": row[4],
                    "price": row[5],
                    "pattern": row[6],
                    "status": row[7],
                    "reason": row[8],
                    "created_at": row[9],
                    "decided_at": row[10],
                    "admin_id": row[11],
                    "admin_chat_id": row[12],
                    "admin_message_id": row[13],
                }

    async def decide_withdrawal(
        self, withdrawal_id: int, status: str, admin_id: int, reason: str | None = None
    ) -> bool:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT status FROM withdrawals WHERE id = ?",
                (withdrawal_id,),
            ) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return False
                if row[0] != "pending":
                    return False

            now = datetime.datetime.now()
            await db.execute(
                """
                UPDATE withdrawals
                SET status = ?, reason = ?, admin_id = ?, decided_at = ?
                WHERE id = ?
                """,
                (status, reason, admin_id, now, withdrawal_id),
            )
            await db.commit()
            return True

    async def delete_withdrawal(self, withdrawal_id: int) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("DELETE FROM withdrawals WHERE id = ?", (withdrawal_id,))
            await db.commit()

    async def get_gold_balance(self, telegram_id: int) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT balance FROM gold_balances WHERE telegram_id = ?",
                (telegram_id,),
            ) as cursor:
                row = await cursor.fetchone()
                return int(row[0]) if row else 0

    async def credit_gold_once(
        self, telegram_id: int, amount: int, source_type: str, source_id: int
    ) -> bool:
        if amount <= 0:
            return False
        async with aiosqlite.connect(self.db_path) as db:
            now = datetime.datetime.now()
            try:
                await db.execute(
                    """
                    INSERT INTO gold_transactions (telegram_id, amount, source_type, source_id, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (telegram_id, amount, source_type, source_id, now),
                )
            except aiosqlite.IntegrityError:
                return False

            await db.execute(
                """
                INSERT INTO gold_balances (telegram_id, balance, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(telegram_id) DO UPDATE SET
                    balance = balance + excluded.balance,
                    updated_at = excluded.updated_at
                """,
                (telegram_id, amount, now),
            )
            await db.commit()
            return True

    async def apply_gold_delta_once(
        self, telegram_id: int, amount: int, source_type: str, source_id: int
    ) -> dict:
        if amount == 0:
            return {"ok": False, "status": "zero"}
        async with aiosqlite.connect(self.db_path) as db:
            now = datetime.datetime.now()
            await db.execute("BEGIN IMMEDIATE")
            async with db.execute(
                "SELECT balance FROM gold_balances WHERE telegram_id = ?",
                (telegram_id,),
            ) as cursor:
                row = await cursor.fetchone()
                current = int(row[0]) if row else 0

            if amount < 0 and current < (-amount):
                await db.execute("ROLLBACK")
                return {"ok": False, "status": "insufficient", "balance": current}

            try:
                await db.execute(
                    """
                    INSERT INTO gold_transactions (telegram_id, amount, source_type, source_id, created_at)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (telegram_id, amount, source_type, source_id, now),
                )
            except aiosqlite.IntegrityError:
                await db.execute("ROLLBACK")
                return {"ok": False, "status": "exists", "balance": current}

            await db.execute(
                """
                INSERT INTO gold_balances (telegram_id, balance, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(telegram_id) DO UPDATE SET
                    balance = balance + excluded.balance,
                    updated_at = excluded.updated_at
                """,
                (telegram_id, amount, now),
            )

            async with db.execute(
                "SELECT balance FROM gold_balances WHERE telegram_id = ?",
                (telegram_id,),
            ) as cursor:
                new_row = await cursor.fetchone()
                new_balance = int(new_row[0]) if new_row else (current + amount)

            await db.execute("COMMIT")
            return {"ok": True, "status": "applied", "balance": new_balance}

    async def update_watch_time(self, channel: str, nickname: str, max_gap_seconds: int = 300) -> None:
        channel = (channel or "").strip().lower()
        nickname = (nickname or "").strip().lower()
        if not channel or not nickname:
            return
        async with aiosqlite.connect(self.db_path) as db:
            now = datetime.datetime.now()
            async with db.execute(
                "SELECT seconds, last_seen_at FROM watch_time WHERE channel = ? AND nickname = ?",
                (channel, nickname),
            ) as cursor:
                row = await cursor.fetchone()

            seconds = int(row[0]) if row else 0
            last_seen_at = row[1] if row else None

            add = 0
            if last_seen_at:
                try:
                    last_dt = datetime.datetime.fromisoformat(str(last_seen_at))
                    delta = int((now - last_dt).total_seconds())
                    if delta > 0:
                        add = min(delta, int(max_gap_seconds))
                except Exception:
                    add = 0

            await db.execute(
                """
                INSERT INTO watch_time (channel, nickname, seconds, last_seen_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(channel, nickname) DO UPDATE SET
                    seconds = seconds + excluded.seconds,
                    last_seen_at = excluded.last_seen_at
                """,
                (channel, nickname, add, now),
            )
            await db.commit()

    async def get_watch_time_seconds(self, channel: str, nickname: str) -> int:
        channel = (channel or "").strip().lower()
        nickname = (nickname or "").strip().lower()
        if not channel or not nickname:
            return 0
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT seconds FROM watch_time WHERE channel = ? AND nickname = ?",
                (channel, nickname),
            ) as cursor:
                row = await cursor.fetchone()
                return int(row[0]) if row else 0

    async def start_stream_session(self, channel: str) -> int:
        channel = (channel or "").strip().lower()
        async with aiosqlite.connect(self.db_path) as db:
            now = datetime.datetime.now()
            cur = await db.execute(
                "INSERT INTO stream_sessions (channel, started_at) VALUES (?, ?)",
                (channel, now),
            )
            await db.commit()
            return int(cur.lastrowid)

    async def end_stream_session(self, session_id: int) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            now = datetime.datetime.now()
            await db.execute(
                "UPDATE stream_sessions SET ended_at = ? WHERE id = ? AND ended_at IS NULL",
                (now, int(session_id)),
            )
            await db.commit()

    async def update_stream_watch_time(self, session_id: int, nickname: str, max_gap_seconds: int = 300) -> None:
        nickname = (nickname or "").strip().lower()
        if not session_id or not nickname:
            return
        async with aiosqlite.connect(self.db_path) as db:
            now = datetime.datetime.now()
            async with db.execute(
                "SELECT seconds, last_seen_at FROM stream_watch_time WHERE session_id = ? AND nickname = ?",
                (int(session_id), nickname),
            ) as cursor:
                row = await cursor.fetchone()

            last_seen_at = row[1] if row else None
            add = 0
            if last_seen_at:
                try:
                    last_dt = datetime.datetime.fromisoformat(str(last_seen_at))
                    delta = int((now - last_dt).total_seconds())
                    if delta > 0:
                        add = min(delta, int(max_gap_seconds))
                except Exception:
                    add = 0

            await db.execute(
                """
                INSERT INTO stream_watch_time (session_id, nickname, seconds, last_seen_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(session_id, nickname) DO UPDATE SET
                    seconds = seconds + excluded.seconds,
                    last_seen_at = excluded.last_seen_at
                """,
                (int(session_id), nickname, add, now),
            )
            await db.commit()

    async def get_stream_watch_time_seconds(self, session_id: int, nickname: str) -> int:
        nickname = (nickname or "").strip().lower()
        if not session_id or not nickname:
            return 0
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT seconds FROM stream_watch_time WHERE session_id = ? AND nickname = ?",
                (int(session_id), nickname),
            ) as cursor:
                row = await cursor.fetchone()
                return int(row[0]) if row else 0

    async def get_stream_eligible_users(self, session_id: int, min_seconds: int) -> list[str]:
        if not session_id:
            return []
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                """
                SELECT nickname
                FROM stream_watch_time
                WHERE session_id = ? AND seconds >= ?
                """,
                (int(session_id), int(min_seconds)),
            ) as cursor:
                rows = await cursor.fetchall()
                return [str(r[0]) for r in rows]

    async def add_check_channel(self, chat_id: int, title: str | None = None) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            now = datetime.datetime.now()
            await db.execute(
                """
                INSERT INTO check_channels (chat_id, title, created_at)
                VALUES (?, ?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET title = excluded.title
                """,
                (chat_id, title, now),
            )
            await db.commit()

    async def remove_check_channel(self, chat_id: int) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("DELETE FROM check_channels WHERE chat_id = ?", (chat_id,))
            await db.commit()

    async def list_check_channels(self) -> list[dict]:
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT chat_id, title FROM check_channels ORDER BY id DESC"
            ) as cursor:
                rows = await cursor.fetchall()
                return [{"chat_id": int(r[0]), "title": r[1] or ""} for r in rows]

    async def create_gold_check(
        self, amount: int, max_activations: int, created_by: int, channel_id: int, code: str
    ) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            now = datetime.datetime.now()
            cur = await db.execute(
                """
                INSERT INTO gold_checks
                    (code, amount, max_activations, activated_count, status, created_by, created_at, channel_id)
                VALUES
                    (?, ?, ?, 0, 'active', ?, ?, ?)
                """,
                (code, amount, max_activations, created_by, now, channel_id),
            )
            await db.commit()
            return int(cur.lastrowid)

    async def set_gold_check_message(self, check_id: int, message_id: int) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                "UPDATE gold_checks SET message_id = ? WHERE id = ?",
                (message_id, check_id),
            )
            await db.commit()

    async def get_gold_check_by_code(self, code: str):
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                """
                SELECT id, code, amount, max_activations, activated_count, status, channel_id, message_id
                FROM gold_checks
                WHERE code = ?
                """,
                (code,),
            ) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return None
                return {
                    "id": int(row[0]),
                    "code": row[1],
                    "amount": int(row[2]),
                    "max_activations": int(row[3]),
                    "activated_count": int(row[4]),
                    "status": row[5],
                    "channel_id": int(row[6]) if row[6] is not None else None,
                    "message_id": int(row[7]) if row[7] is not None else None,
                }

    async def activate_gold_check(self, code: str, telegram_id: int) -> dict:
        async with aiosqlite.connect(self.db_path) as db:
            now = datetime.datetime.now()

            async with db.execute(
                """
                SELECT id, amount, max_activations, activated_count, status, channel_id, message_id
                FROM gold_checks
                WHERE code = ?
                """,
                (code,),
            ) as cursor:
                row = await cursor.fetchone()
                if not row:
                    return {"status": "not_found"}

            check_id = int(row[0])
            amount = int(row[1])
            max_activations = int(row[2])
            activated_count = int(row[3])
            status = row[4]
            channel_id = int(row[5]) if row[5] is not None else None
            message_id = int(row[6]) if row[6] is not None else None

            if status != "active":
                return {"status": "inactive"}
            if activated_count >= max_activations:
                return {
                    "status": "finished",
                    "check_id": check_id,
                    "amount": amount,
                    "max_activations": max_activations,
                    "activated_count": activated_count,
                    "channel_id": channel_id,
                    "message_id": message_id,
                }

            try:
                cur = await db.execute(
                    """
                    INSERT INTO gold_check_activations (check_id, telegram_id, activated_at)
                    VALUES (?, ?, ?)
                    """,
                    (check_id, telegram_id, now),
                )
            except aiosqlite.IntegrityError:
                return {
                    "status": "already",
                    "check_id": check_id,
                    "amount": amount,
                    "max_activations": max_activations,
                    "activated_count": activated_count,
                    "channel_id": channel_id,
                    "message_id": message_id,
                }

            activation_id = int(cur.lastrowid)

            await db.execute(
                """
                UPDATE gold_checks
                SET activated_count = activated_count + 1
                WHERE id = ? AND activated_count < max_activations AND status = 'active'
                """,
                (check_id,),
            )
            await db.execute(
                """
                INSERT INTO gold_transactions (telegram_id, amount, source_type, source_id, created_at)
                VALUES (?, ?, 'check_activation', ?, ?)
                """,
                (telegram_id, amount, activation_id, now),
            )
            await db.execute(
                """
                INSERT INTO gold_balances (telegram_id, balance, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(telegram_id) DO UPDATE SET
                    balance = balance + excluded.balance,
                    updated_at = excluded.updated_at
                """,
                (telegram_id, amount, now),
            )
            await db.commit()

            async with db.execute(
                "SELECT activated_count FROM gold_checks WHERE id = ?",
                (check_id,),
            ) as cursor:
                new_count_row = await cursor.fetchone()
                new_count = int(new_count_row[0]) if new_count_row else activated_count + 1

            if new_count >= max_activations:
                await db.execute(
                    "UPDATE gold_checks SET status = 'finished' WHERE id = ?",
                    (check_id,),
                )
                await db.commit()

            return {
                "status": "activated",
                "check_id": check_id,
                "amount": amount,
                "max_activations": max_activations,
                "activated_count": new_count,
                "channel_id": channel_id,
                "message_id": message_id,
            }

    async def create_giveaway_trigger(self, channel_id: int, requested_by: int) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            now = datetime.datetime.now()
            cur = await db.execute(
                """
                INSERT INTO giveaway_triggers (requested_by, created_at, trigger_type, channel_id)
                VALUES (?, ?, 'random', ?)
                """,
                (requested_by, now, int(channel_id)),
            )
            await db.commit()
            return int(cur.lastrowid)

    async def create_clip_trigger(self, channel_id: int, requested_by: int) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            now = datetime.datetime.now()
            cur = await db.execute(
                """
                INSERT INTO giveaway_triggers (requested_by, created_at, trigger_type, channel_id)
                VALUES (?, ?, 'clip', ?)
                """,
                (requested_by, now, int(channel_id)),
            )
            await db.commit()
            return int(cur.lastrowid)

    async def create_number_guess_trigger(
        self,
        channel_id: int,
        requested_by: int,
        reward_id: int,
        guess_number: int,
        guess_min: int,
        guess_max: int,
    ) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            now = datetime.datetime.now()
            cur = await db.execute(
                """
                INSERT INTO giveaway_triggers
                    (requested_by, created_at, trigger_type, channel_id, reward_id, guess_number, guess_min, guess_max)
                VALUES
                    (?, ?, 'guess', ?, ?, ?, ?, ?)
                """,
                (int(requested_by), now, int(channel_id), int(reward_id), int(guess_number), int(guess_min), int(guess_max)),
            )
            await db.commit()
            return int(cur.lastrowid)

    async def create_draw_claimed(
        self,
        channel: str,
        nickname: str,
        reward_id: int,
        notified_in_tg: int = 1,
    ) -> int:
        channel = (channel or "").strip().lower()
        nickname = (nickname or "").strip().lower()
        async with aiosqlite.connect(self.db_path) as db:
            now = datetime.datetime.now()
            cur = await db.execute(
                """
                INSERT INTO draws (channel, nickname, reward_id, created_at, status, expires_at, notified_in_tg)
                VALUES (?, ?, ?, ?, 'claimed', NULL, ?)
                """,
                (channel, nickname, int(reward_id), now, int(notified_in_tg)),
            )
            await db.commit()
            return int(cur.lastrowid)

    async def claim_giveaway_trigger(self, channel_id: int):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("BEGIN IMMEDIATE")
            async with db.execute(
                """
                SELECT
                    id,
                    requested_by,
                    trigger_type,
                    channel_id,
                    reward_id,
                    winners_count,
                    planned_giveaway_id,
                    guess_number,
                    guess_min,
                    guess_max
                FROM giveaway_triggers
                WHERE processed_at IS NULL AND channel_id = ?
                ORDER BY id ASC
                LIMIT 1
                """,
                (int(channel_id),),
            ) as cursor:
                row = await cursor.fetchone()
            if not row:
                await db.execute("COMMIT")
                return None
            trigger_id = int(row[0])
            requested_by = int(row[1]) if row[1] is not None else 0
            trigger_type = row[2] or "random"
            channel_id = int(row[3]) if row[3] is not None else None
            reward_id = int(row[4]) if row[4] is not None else None
            winners_count = int(row[5]) if row[5] is not None else None
            planned_giveaway_id = int(row[6]) if row[6] is not None else None
            guess_number = int(row[7]) if row[7] is not None else None
            guess_min = int(row[8]) if row[8] is not None else None
            guess_max = int(row[9]) if row[9] is not None else None
            now = datetime.datetime.now()
            await db.execute(
                "UPDATE giveaway_triggers SET processed_at = ? WHERE id = ? AND processed_at IS NULL",
                (now, trigger_id),
            )
            await db.execute("COMMIT")
            return {
                "id": trigger_id,
                "requested_by": requested_by,
                "trigger_type": trigger_type,
                "channel_id": channel_id,
                "reward_id": reward_id,
                "winners_count": winners_count,
                "planned_giveaway_id": planned_giveaway_id,
                "guess_number": guess_number,
                "guess_min": guess_min,
                "guess_max": guess_max,
            }

# Load config to instantiate global DB if needed, but better to pass instance
