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

            # Migrations
            await self.migrate_draws_table(db)
            
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

# Load config to instantiate global DB if needed, but better to pass instance
