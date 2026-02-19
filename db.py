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
            
            # Rewards
            await db.execute('''
                CREATE TABLE IF NOT EXISTS rewards (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
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
                    processed_at DATETIME
                )
            ''')

            # Migrations
            await self.migrate_draws_table(db)
            await self.migrate_withdrawals_table(db)
            await self.migrate_gold_tables(db)
            
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

    async def create_giveaway_trigger(self, requested_by: int) -> int:
        async with aiosqlite.connect(self.db_path) as db:
            now = datetime.datetime.now()
            cur = await db.execute(
                "INSERT INTO giveaway_triggers (requested_by, created_at) VALUES (?, ?)",
                (requested_by, now),
            )
            await db.commit()
            return int(cur.lastrowid)

    async def claim_giveaway_trigger(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("BEGIN IMMEDIATE")
            async with db.execute(
                """
                SELECT id, requested_by
                FROM giveaway_triggers
                WHERE processed_at IS NULL
                ORDER BY id ASC
                LIMIT 1
                """
            ) as cursor:
                row = await cursor.fetchone()
            if not row:
                await db.execute("COMMIT")
                return None
            trigger_id = int(row[0])
            requested_by = int(row[1]) if row[1] is not None else 0
            now = datetime.datetime.now()
            await db.execute(
                "UPDATE giveaway_triggers SET processed_at = ? WHERE id = ? AND processed_at IS NULL",
                (now, trigger_id),
            )
            await db.execute("COMMIT")
            return {"id": trigger_id, "requested_by": requested_by}

# Load config to instantiate global DB if needed, but better to pass instance
