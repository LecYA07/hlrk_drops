from twitchio.ext import commands
import yaml
import asyncio
import random
import datetime
import logging
import aiosqlite
from db import Database
from telegram_bot import notify_user
from twitch_helix import HelixClient


logger = logging.getLogger("TwitchBot")


class TwitchBot(commands.Bot):
    def __init__(self, config: dict, bot_id: str):
        self.config = config
        self.db_path = self.config["database"]["db_path"]
        self.db = Database(self.db_path)

        self.channel_name = self.config["twitch"]["channel"].replace("#", "").lower()
        self.ignore_list = [name.lower() for name in self.config.get("ignore_list", [])]

        self.active_timeout = int(self.config["giveaway"].get("active_timeout_minutes", 15))
        self.claim_timeout = int(self.config["giveaway"].get("claim_timeout_minutes", 7))
        self.stream_check_interval_seconds = int(self.config["giveaway"].get("stream_check_interval_seconds", 60))
        self.min_interval_minutes = int(self.config["giveaway"].get("min_interval_minutes", 10))
        self.max_interval_minutes = int(self.config["giveaway"].get("max_interval_minutes", 30))

        self.helix = HelixClient(
            client_id=self.config["twitch"]["client_id"],
            client_secret=self.config["twitch"]["client_secret"],
        )

        raw_token = self.config["twitch"]["bot_token"]
        token_clean = raw_token.replace("oauth:", "") if raw_token.startswith("oauth:") else raw_token
        token_value = f"oauth:{token_clean}"

        super().__init__(
            token=token_value,
            client_id=self.config["twitch"]["client_id"],
            client_secret=self.config["twitch"]["client_secret"],
            bot_id=bot_id,
            prefix="!",
            initial_channels=[self.channel_name],
        )

        self.is_stream_online = False
        self._tasks: list[asyncio.Task] = []

    @classmethod
    async def create(cls):
        with open("config.yaml", "r") as f:
            config = yaml.safe_load(f)

        bot_id = config["twitch"].get("bot_id")
        
        # Если bot_id заполнен и не дефолтный — используем его
        if bot_id and not str(bot_id).upper().startswith("YOUR_"):
            logger.info(f"Используем bot_id из конфига: {bot_id}")
            return cls(config, bot_id)

        # Иначе пробуем получить через Helix
        logger.info("bot_id не найден в конфиге, пробуем получить через Helix...")
        helix = HelixClient(
            client_id=config["twitch"]["client_id"],
            client_secret=config["twitch"]["client_secret"],
        )
        
        bot_nick = config["twitch"]["bot_nick"]
        try:
            bot_id = await helix.get_user_id(bot_nick)
        except Exception as e:
            logger.error(f"Ошибка получения bot_id: {e}")
            bot_id = None

        if not bot_id:
            logger.warning(f"Не удалось получить bot_id для {bot_nick}. Бот запустится, но некоторые функции могут не работать.")
            # Пробуем запуститься без bot_id, возможно twitchio сам справится или он не нужен для базовых функций
        else:
            logger.info(f"bot_id получен через Helix: {bot_id}")

        return cls(config, bot_id)

    async def event_ready(self):
        # Безопасное получение nick и user_id с проверкой атрибутов
        if not getattr(self, "nick", None) or not getattr(self, "user_id", None):
            try:
                # Пытаемся получить данные пользователя, если их нет
                # Используем fetch_users без аргументов для получения текущего пользователя
                users = await self.fetch_users(names=[self.config["twitch"]["bot_nick"]])
                if users:
                    self.nick = users[0].name
                    self.user_id = users[0].id
                else:
                    # Если не удалось по нику, пробуем по ID
                     users = await self.fetch_users(ids=[self.config["twitch"].get("bot_id")])
                     if users:
                        self.nick = users[0].name
                        self.user_id = users[0].id
            except Exception as e:
                logger.warning(f"Не удалось подтянуть self.nick/user_id через fetch_users: {e}")

        nick = getattr(self, "nick", None) or "Unknown"
        user_id = getattr(self, "user_id", None) or "Unknown"
        logger.info(f"Вошли как: {nick} (user_id={user_id})")
        
        # Явный джойн к каналу (иногда initial_channels не срабатывает как надо)
        try:
            # Для twitchio 2.x нужно использовать join_channels
            await self.join_channels([self.channel_name])
            logger.info(f"Присоединились к каналу: {self.channel_name}")
        except AttributeError:
             # Если join_channels нет, значит что-то странное с версией, но initial_channels уже должен был сработать
            logger.warning("Метод join_channels не найден (возможно старая версия twitchio?), надеемся на initial_channels.")
        except Exception as e:
            logger.error(f"Ошибка присоединения к каналу {self.channel_name}: {e}")

        if not self._tasks:
            self._tasks.append(asyncio.create_task(self.stream_check_loop()))
            self._tasks.append(asyncio.create_task(self.giveaway_loop()))
            self._tasks.append(asyncio.create_task(self.expire_loop()))

    @commands.command(name="ping")
    async def cmd_ping(self, ctx: commands.Context):
        await ctx.send(f"@{ctx.author.name}, Pong! Бот работает.")

    @commands.command(name="test")
    async def cmd_test(self, ctx: commands.Context):
        await ctx.send(f"@{ctx.author.name}, Тест успешен! Я тут.")

    async def close(self):
        for t in self._tasks:
            t.cancel()
        await super().close()

    async def event_message(self, message):
        if message.echo:
            return

        content = getattr(message, "content", "") or ""
        author = getattr(message, "author", None)
        author_name = author.name.lower() if author and author.name else "unknown"

        if content.startswith("!"):
            logger.info(f"Команда из чата: {author_name}: {content}")

        if author_name in self.ignore_list:
            return

        # Обновляем активность и проверяем награды
        if author_name != "unknown":
            await self.update_active_user(author_name)
            await self.claim_pending_draws(author_name)

        # Ручная обработка команд, если стандартная не работает
        lowered = content.strip().lower()
        if lowered == "!ping":
            await message.channel.send(f"@{author.name}, Pong! Бот работает.")
            return
        if lowered == "!test":
            await message.channel.send(f"@{author.name}, Тест успешен! Я тут.")
            return
        if lowered.startswith("!link"):
            # Извлекаем аргументы вручную
            parts = content.split()
            code = parts[1] if len(parts) > 1 else ""
            await self.manual_link_handler(message, code)
            return

        await self.handle_commands(message)

    async def manual_link_handler(self, message, code):
        """Ручной обработчик для !link, если commands.command не срабатывает"""
        code = (code or "").strip().upper()
        author_name = message.author.name
        
        if not code:
            await message.channel.send(f"@{author_name}, укажи код. Пример: !link ABC123")
            return

        telegram_id = await self.db.verify_twitch_link(author_name.lower(), code)
        if telegram_id:
            await message.channel.send(f"@{author_name}, аккаунт привязан.")
            await notify_user(telegram_id, f"✅ Twitch аккаунт @{author_name} привязан к Telegram.")
            return

        await message.channel.send(f"@{author_name}, код неверный или уже использован.")

    async def event_command(self, ctx: commands.Context):
        logger.info(f"Команда выполнена: {ctx.command.name} от {ctx.author.name}")

    async def event_command_error(self, ctx: commands.Context, error: Exception):
        logger.error(f"Ошибка команды {ctx.command.name} от {ctx.author.name}: {error}")

    @commands.command(name="link")
    async def cmd_link(self, ctx: commands.Context, code: str = ""):
        code = (code or "").strip().upper()
        if not code:
            await ctx.send(f"@{ctx.author.name}, укажи код. Пример: !link ABC123")
            return

        telegram_id = await self.db.verify_twitch_link(ctx.author.name.lower(), code)
        if telegram_id:
            await ctx.send(f"@{ctx.author.name}, аккаунт привязан.")
            await notify_user(telegram_id, f"✅ Twitch аккаунт @{ctx.author.name} привязан к Telegram.")
            return

        await ctx.send(f"@{ctx.author.name}, код неверный или уже использован.")

    async def update_active_user(self, username: str):
        async with aiosqlite.connect(self.db_path) as db:
            now = datetime.datetime.now()
            async with db.execute(
                "SELECT id FROM active_users WHERE nickname = ? AND channel = ?",
                (username, self.channel_name),
            ) as cursor:
                row = await cursor.fetchone()

            if row:
                await db.execute(
                    "UPDATE active_users SET last_active_at = ? WHERE id = ?",
                    (now, row[0]),
                )
            else:
                await db.execute(
                    "INSERT INTO active_users (channel, nickname, last_active_at) VALUES (?, ?, ?)",
                    (self.channel_name, username, now),
                )

            await db.commit()

    async def claim_pending_draws(self, username: str):
        now = datetime.datetime.now()
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                """
                SELECT d.id, r.name
                FROM draws d
                JOIN rewards r ON r.id = d.reward_id
                WHERE d.nickname = ? AND d.status = 'pending' AND d.expires_at > ?
                """,
                (username, now),
            ) as cursor:
                rows = await cursor.fetchall()

            if not rows:
                return

            draw_ids = [row[0] for row in rows]
            placeholders = ",".join(["?"] * len(draw_ids))
            await db.execute(
                f"UPDATE draws SET status = 'claimed' WHERE id IN ({placeholders})",
                draw_ids,
            )
            await db.commit()

        channel = self.get_channel(self.channel_name)
        if channel:
            if len(rows) == 1:
                await channel.send(f"@{username} забрал награду: {rows[0][1]}.")
            else:
                rewards_list = ", ".join([r[1] for r in rows])
                await channel.send(f"@{username} забрал награды: {rewards_list}.")

        telegram_id = await self.db.get_telegram_id_by_twitch_username(username)
        if telegram_id:
            if len(rows) == 1:
                await notify_user(telegram_id, f"🎁 Награда подтверждена: {rows[0][1]}.")
            else:
                rewards_list = "\n- " + "\n- ".join([r[1] for r in rows])
                await notify_user(telegram_id, f"🎁 Награды подтверждены:{rewards_list}")

    async def stream_check_loop(self):
        await self.wait_for_ready()

        delay = 1
        while True:
            try:
                is_online_now = await self.helix.is_stream_online(self.channel_name)

                if is_online_now and not self.is_stream_online:
                    self.is_stream_online = True
                    logger.info(f"Стрим {self.channel_name} начался.")
                    await self.send_stream_start_notifications()

                if (not is_online_now) and self.is_stream_online:
                    self.is_stream_online = False
                    logger.info(f"Стрим {self.channel_name} закончился.")
                    await self.send_stream_summary()

                delay = 1
                await asyncio.sleep(self.stream_check_interval_seconds)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Ошибка проверки онлайна стрима: {e}")
                await asyncio.sleep(min(60, delay))
                delay = min(60, delay * 2)

    async def giveaway_loop(self):
        await self.wait_for_ready()

        while True:
            try:
                next_minutes = random.randint(self.min_interval_minutes, self.max_interval_minutes)
                await asyncio.sleep(next_minutes * 60)

                if not self.is_stream_online:
                    continue

                await self.run_giveaway()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Ошибка цикла розыгрышей: {e}")
                await asyncio.sleep(5)

    async def expire_loop(self):
        await self.wait_for_ready()

        while True:
            try:
                expired = await self.db.expire_pending_draws()
                for row in expired:
                    nickname, reward_name, telegram_id = row
                    if telegram_id:
                        await notify_user(
                            telegram_id,
                            f"⏳ Награда \"{reward_name}\" сгорела, причина: афк фарм.",
                        )
                await asyncio.sleep(30)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Ошибка обработки истёкших наград: {e}")
                await asyncio.sleep(5)

    async def send_stream_start_notifications(self):
        telegram_ids = await self.db.get_all_linked_telegram_ids()
        if not telegram_ids:
            return

        text = f"🔴 Стрим начался на канале {self.channel_name}! Заходи в чат, чтобы участвовать в дропах."
        for tg_id in telegram_ids:
            await notify_user(tg_id, text)
            await asyncio.sleep(0.03)

    async def send_stream_summary(self):
        pending = await self.db.get_pending_notifications()
        if not pending:
            return

        notifications: dict[int, dict[str, list]] = {}
        for draw_id, nickname, reward_name, telegram_id in pending:
            if telegram_id not in notifications:
                notifications[telegram_id] = {"rewards": [], "draw_ids": []}
            notifications[telegram_id]["rewards"].append(reward_name)
            notifications[telegram_id]["draw_ids"].append(draw_id)

        for tg_id, data in notifications.items():
            rewards_list = "\n- " + "\n- ".join(data["rewards"])
            msg = f"🏁 Стрим закончился! Вот что ты получил за просмотр:{rewards_list}"
            await notify_user(tg_id, msg)
            await self.db.mark_notified(data["draw_ids"])

    async def run_giveaway(self):
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute("SELECT id, name, weight, quantity FROM rewards WHERE enabled = 1") as cursor:
                rewards = await cursor.fetchall()

        if not rewards:
            logger.info("Розыгрыш пропущен: нет включённых наград.")
            return

        active_users = await self.get_active_users()
        if not active_users:
            logger.info("Розыгрыш пропущен: нет активных участников.")
            return

        reward_id, reward_name, _, reward_qty = self.select_weighted_reward(rewards)
        winners_count = min(int(reward_qty), len(active_users))
        winners = random.sample(active_users, winners_count)

        for w in winners:
            await self.record_draw_pending(w, reward_id)

        channel = self.get_channel(self.channel_name)
        if channel:
            winners_mentions = " ".join([f"@{w}" for w in winners])
            await channel.send(
                f"{winners_mentions} вы выиграли \"{reward_name}\"!.",
            )

        logger.info(f"Розыгрыш: {reward_name}; победители: {', '.join(winners)}")

    async def get_active_users(self):
        async with aiosqlite.connect(self.db_path) as db:
            now = datetime.datetime.now()
            limit_time = now - datetime.timedelta(minutes=self.active_timeout)
            async with db.execute(
                "SELECT nickname FROM active_users WHERE channel = ? AND last_active_at >= ?",
                (self.channel_name, limit_time),
            ) as cursor:
                rows = await cursor.fetchall()
                return [row[0] for row in rows]

    def select_weighted_reward(self, rewards):
        total_weight = sum(int(r[2]) for r in rewards)
        pick = random.uniform(0, total_weight)
        upto = 0
        for reward in rewards:
            upto += int(reward[2])
            if upto >= pick:
                return reward
        return rewards[-1]

    async def record_draw_pending(self, winner: str, reward_id: int):
        async with aiosqlite.connect(self.db_path) as db:
            now = datetime.datetime.now()
            expires_at = now + datetime.timedelta(minutes=self.claim_timeout)
            await db.execute(
                """
                INSERT INTO draws (channel, nickname, reward_id, created_at, status, expires_at)
                VALUES (?, ?, ?, ?, 'pending', ?)
                """,
                (self.channel_name, winner, reward_id, now, expires_at),
            )
            await db.commit()
