from twitchio.ext import commands
import yaml
import asyncio
import random
import datetime
import logging
import re
import aiosqlite
from db import Database
from telegram_bot import notify_user
from twitch_helix import HelixClient


logger = logging.getLogger("TwitchBot")

GOLD_RE = re.compile(r"^\s*(\d+)\s*GOLD\s*$", re.IGNORECASE)


class TwitchBot(commands.Bot):
    def __init__(self, config: dict, bot_id: str, channel_login: str, channel_id: int | None):
        self.config = config
        self.db_path = self.config["database"]["db_path"]
        self.db = Database(self.db_path)

        self.channel_name = (channel_login or "").replace("#", "").lower()
        self.channel_id = int(channel_id) if channel_id is not None else None
        self.ignore_list = [name.lower() for name in self.config.get("ignore_list", [])]

        self.active_timeout = int(self.config["giveaway"].get("active_timeout_minutes", 15))
        self.claim_timeout = int(self.config["giveaway"].get("claim_timeout_minutes", 7))
        self.stream_check_interval_seconds = int(self.config["giveaway"].get("stream_check_interval_seconds", 60))
        self.min_interval_minutes = int(self.config["giveaway"].get("min_interval_minutes", 10))
        self.max_interval_minutes = int(self.config["giveaway"].get("max_interval_minutes", 30))
        self.drops_enabled = 1

        self.helix = HelixClient(
            client_id=self.config["twitch"]["client_id"],
            client_secret=self.config["twitch"]["client_secret"],
            user_token=self.config["twitch"].get("clip_token"),
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
        self.current_stream_session_id: int | None = None
        self.number_game: dict | None = None
        self.channel_user_id: str | None = None
        self.last_clip_at: datetime.datetime | None = None
        self.clip_cooldown_seconds = 45
        self._tasks: list[asyncio.Task] = []

    @classmethod
    async def create(cls, channel_login: str | None = None, channel_id: int | None = None):
        with open("config.yaml", "r") as f:
            config = yaml.safe_load(f)

        bot_id = await cls.resolve_bot_id(config)
        channel_login = channel_login or config["twitch"]["channel"]
        return cls(config, bot_id, channel_login, channel_id)

    @classmethod
    async def resolve_bot_id(cls, config: dict):
        bot_id = config["twitch"].get("bot_id")
        if bot_id and not str(bot_id).upper().startswith("YOUR_"):
            logger.info(f"Используем bot_id из конфига: {bot_id}")
            return bot_id

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
        else:
            logger.info(f"bot_id получен через Helix: {bot_id}")
        return bot_id

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

        try:
            self.channel_user_id = await self.helix.get_user_id(self.channel_name)
        except Exception as e:
            logger.error(f"Не удалось получить id канала {self.channel_name}: {e}")
            self.channel_user_id = None

        await self.apply_channel_settings()
        
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
            self._tasks.append(asyncio.create_task(self.instant_giveaway_loop()))

    async def apply_channel_settings(self):
        if not self.channel_id and self.channel_name:
            self.channel_id = await self.db.ensure_channel(self.channel_name, None)
        if not self.channel_id:
            return
        settings = await self.db.get_channel_settings(self.channel_id)
        if not settings:
            await self.db.upsert_channel_settings(
                self.channel_id,
                min_interval_minutes=self.min_interval_minutes,
                max_interval_minutes=self.max_interval_minutes,
                active_timeout_minutes=self.active_timeout,
                claim_timeout_minutes=self.claim_timeout,
                drops_enabled=1,
            )
            settings = await self.db.get_channel_settings(self.channel_id)
        if not settings:
            return
        self.min_interval_minutes = int(settings.get("min_interval_minutes") or self.min_interval_minutes)
        self.max_interval_minutes = int(settings.get("max_interval_minutes") or self.max_interval_minutes)
        self.active_timeout = int(settings.get("active_timeout_minutes") or self.active_timeout)
        self.claim_timeout = int(settings.get("claim_timeout_minutes") or self.claim_timeout)
        self.drops_enabled = int(settings.get("drops_enabled") or 0)

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
            await self.handle_number_game_message(message, author_name)
            await self.handle_clip_trigger(message, author_name)

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

    async def handle_clip_trigger(self, message, author_name: str):
        if not self.is_stream_online:
            return
        if not self.channel_user_id:
            return
        author = getattr(message, "author", None)
        is_broadcaster = getattr(author, "is_broadcaster", False)
        if not is_broadcaster:
            return
        content = (getattr(message, "content", "") or "").strip().lower()
        if content != "клип":
            return
        await self.create_clip_now("chat")

    async def create_clip_now(self, source: str):
        if not self.is_stream_online:
            return
        if not self.channel_user_id:
            return
        now = datetime.datetime.now()
        if self.last_clip_at and (now - self.last_clip_at).total_seconds() < self.clip_cooldown_seconds:
            return
        clip_id = await self.helix.create_clip(self.channel_user_id, has_delay=True)
        if clip_id:
            self.last_clip_at = now
            logger.info(f"Клип создан ({source}): {clip_id}")
        else:
            logger.error(f"Не удалось создать клип ({source})")

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
        await self.db.update_watch_time(self.channel_name, username)
        if self.is_stream_online and self.current_stream_session_id:
            await self.db.update_stream_watch_time(self.current_stream_session_id, username)

    async def handle_number_game_message(self, message, author_name: str):
        game = self.number_game
        if not game or not game.get("active"):
            return
        if not self.is_stream_online or not self.current_stream_session_id:
            return

        content = getattr(message, "content", "") or ""
        s = content.strip()
        if not s.isdigit():
            return
        try:
            guess = int(s)
        except Exception:
            return

        now = datetime.datetime.now()
        if guess < int(game["min"]) or guess > int(game["max"]):
            return

        eligible_seconds = await self.db.get_stream_watch_time_seconds(self.current_stream_session_id, author_name)
        is_eligible = eligible_seconds >= 600

        if guess == int(game["number"]):
            if not is_eligible:
                last = game.get("last_not_eligible_at")
                if not last or (now - last).total_seconds() >= 10:
                    game["last_not_eligible_at"] = now
                    await message.channel.send(f"@{author_name}, нужно быть на стриме минимум 10 минут.")
                return
            game["active"] = False
            self.number_game = None
            reward_id = int(game["reward_id"])
            reward_name = str(game.get("reward_name") or "")
            try:
                await message.channel.send(f"@{author_name} угадал число и выиграл \"{reward_name}\"!.")
            except Exception:
                pass
            await self.award_reward_immediately(author_name, reward_id)
            return

        last_hint_at = game.get("last_hint_at")
        if last_hint_at and (now - last_hint_at).total_seconds() < 2.5:
            return

        per_user = game.setdefault("last_hint_by_user", {})
        last_user = per_user.get(author_name)
        if last_user and (now - last_user).total_seconds() < 12:
            return

        if random.random() > 0.35:
            return

        game["last_hint_at"] = now
        per_user[author_name] = now
        direction = "больше" if guess < int(game["number"]) else "меньше"
        await message.channel.send(f"@{author_name}, загаданное число {direction}.")

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
            total_gold = 0
            items: list[str] = []
            for draw_id, reward_name in rows:
                m = GOLD_RE.match(reward_name or "")
                if m:
                    amount = int(m.group(1))
                    credited = await self.db.credit_gold_once(int(telegram_id), amount, "draw", int(draw_id))
                    if credited:
                        total_gold += amount
                else:
                    items.append(reward_name)
                    await self.db.record_item_claim(int(draw_id), int(telegram_id), username, reward_name)

            parts: list[str] = []
            if total_gold > 0:
                parts.append(f"💰 Начислено: {total_gold} GOLD")
            if items:
                if len(items) == 1:
                    parts.append(f"🎁 Награда подтверждена: {items[0]}.")
                else:
                    rewards_list = "\n- " + "\n- ".join(items)
                    parts.append(f"🎁 Награды подтверждены:{rewards_list}")
            if not parts:
                parts.append("✅ Награда подтверждена.")
            await notify_user(int(telegram_id), "\n\n".join(parts))

    async def stream_check_loop(self):
        await self.wait_for_ready()

        delay = 1
        while True:
            try:
                await self.apply_channel_settings()
                is_online_now = await self.helix.is_stream_online(self.channel_name)

                if is_online_now and not self.is_stream_online:
                    self.is_stream_online = True
                    logger.info(f"Стрим {self.channel_name} начался.")
                    try:
                        self.current_stream_session_id = await self.db.start_stream_session(self.channel_name)
                    except Exception as e:
                        logger.error(f"Не удалось создать stream session: {e}")
                        self.current_stream_session_id = None
                    await self.send_stream_start_notifications()

                if (not is_online_now) and self.is_stream_online:
                    self.is_stream_online = False
                    logger.info(f"Стрим {self.channel_name} закончился.")
                    await self.send_stream_summary()
                    planned = await self.db.list_planned_giveaways(self.channel_id, "end")
                    for g in planned:
                        try:
                            await self.run_giveaway_for_reward(int(g["reward_id"]), int(g["winners_count"]))
                            await self.db.set_planned_giveaway_status(int(g["id"]), "triggered")
                        except Exception as e:
                            logger.error(f"Ошибка розыгрыша в конце стрима: {e}")
                    if self.current_stream_session_id:
                        try:
                            await self.db.end_stream_session(self.current_stream_session_id)
                        except Exception:
                            pass
                        self.current_stream_session_id = None
                    self.number_game = None

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
                next_minutes = random.randint(15, 30)
                await asyncio.sleep(next_minutes * 60)

                if not self.is_stream_online:
                    continue

                await self.run_giveaway()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Ошибка цикла розыгрышей: {e}")
                await asyncio.sleep(5)

    async def instant_giveaway_loop(self):
        await self.wait_for_ready()
        while True:
            try:
                if not self.channel_id:
                    await asyncio.sleep(3)
                    continue
                trigger = await self.db.claim_giveaway_trigger(self.channel_id)
                if trigger:
                    t = trigger.get("trigger_type")
                    if t in ("random", "planned", "guess") and not self.drops_enabled:
                        logger.info(f"Дропы выключены для {self.channel_name}, триггер {t} пропущен.")
                    elif t == "planned" and trigger.get("reward_id"):
                        logger.info(
                            f"Запрошен плановый розыгрыш: id={trigger['id']} reward_id={trigger['reward_id']} by={trigger['requested_by']}"
                        )
                        await self.run_giveaway_for_reward(int(trigger["reward_id"]), trigger.get("winners_count"))
                    elif t == "guess" and trigger.get("reward_id") and trigger.get("guess_number") is not None:
                        await self.start_number_game(
                            reward_id=int(trigger["reward_id"]),
                            number=int(trigger["guess_number"]),
                            min_value=int(trigger.get("guess_min") or 1),
                            max_value=int(trigger.get("guess_max") or 100),
                        )
                    elif t == "clip":
                        await self.create_clip_now("telegram")
                    else:
                        logger.info(f"Мгновенный розыгрыш запрошен: id={trigger['id']} by={trigger['requested_by']}")
                        await self.run_admin_giveaway_immediate()
                await asyncio.sleep(3)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Ошибка мгновенного розыгрыша: {e}")
                await asyncio.sleep(3)

    async def get_eligible_viewers(self, min_seconds: int = 600) -> list[str]:
        if not self.is_stream_online or not self.current_stream_session_id:
            return []
        users = await self.db.get_stream_eligible_users(self.current_stream_session_id, min_seconds)
        ignore = set(self.ignore_list)
        ignore.add((getattr(self, "nick", "") or "").lower())
        out: list[str] = []
        for u in users:
            lu = (u or "").strip().lower()
            if not lu or lu in ignore:
                continue
            out.append(lu)
        return out

    async def award_reward_immediately(self, winner: str, reward_id: int):
        reward = await self.db.get_reward(int(reward_id))
        if not reward:
            return
        reward_name = reward["name"] or ""

        draw_id = await self.db.create_draw_claimed(self.channel_name, winner, int(reward_id), notified_in_tg=1)

        telegram_id = await self.db.get_telegram_id_by_twitch_username(winner)
        m = GOLD_RE.match(reward_name or "")
        if telegram_id:
            if m:
                amount = int(m.group(1))
                credited = await self.db.credit_gold_once(int(telegram_id), amount, "draw", int(draw_id))
                if credited:
                    await notify_user(int(telegram_id), f"💰 Начислено: {amount} GOLD")
            else:
                await self.db.record_item_claim(int(draw_id), int(telegram_id), winner, reward_name)
                await notify_user(int(telegram_id), f"🎁 Ты выиграл: {reward_name}")

    async def run_admin_giveaway_immediate(self):
        if not self.is_stream_online or not self.drops_enabled or not self.channel_id:
            return
        eligible = await self.get_eligible_viewers(600)
        if not eligible:
            logger.info("Админ-розыгрыш пропущен: нет участников 10+ минут.")
            return
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT id, name, weight, quantity FROM rewards WHERE enabled = 1",
            ) as cursor:
                rewards = await cursor.fetchall()
        if not rewards:
            logger.info("Админ-розыгрыш пропущен: нет включённых наград.")
            return
        reward_id, reward_name, _, reward_qty = self.select_weighted_reward(rewards)
        winners_count = min(int(reward_qty), len(eligible))
        winners = random.sample(eligible, winners_count)
        for w in winners:
            await self.award_reward_immediately(w, int(reward_id))
        channel = self.get_channel(self.channel_name)
        if channel:
            winners_mentions = " ".join([f"@{w}" for w in winners])
            await channel.send(f"{winners_mentions} вы выиграли \"{reward_name}\"!.")
        logger.info(f"Админ-розыгрыш: {reward_name}; победители: {', '.join(winners)}")

    async def run_giveaway_for_reward(self, reward_id: int, winners_count: int | None = None):
        reward = await self.db.get_reward(int(reward_id))
        if not reward:
            logger.info(f"Плановый розыгрыш пропущен: награда {reward_id} не найдена.")
            return

        if not self.drops_enabled:
            logger.info("Плановый розыгрыш пропущен: дропы выключены.")
            return

        eligible = await self.get_eligible_viewers(600)
        if not eligible:
            logger.info("Плановый розыгрыш пропущен: нет участников 10+ минут.")
            return

        reward_name = reward["name"]
        count = int(winners_count) if winners_count is not None else int(reward.get("quantity") or 1)
        count = max(1, count)
        winners_count = min(count, len(eligible))
        winners = random.sample(eligible, winners_count)
        for w in winners:
            await self.award_reward_immediately(w, int(reward_id))

        channel = self.get_channel(self.channel_name)
        if channel:
            winners_mentions = " ".join([f"@{w}" for w in winners])
            await channel.send(f"{winners_mentions} вы выиграли \"{reward_name}\"!.")

        logger.info(f"Плановый розыгрыш: {reward_name}; победители: {', '.join(winners)}")

    async def start_number_game(self, reward_id: int, number: int, min_value: int, max_value: int):
        if not self.is_stream_online or not self.current_stream_session_id:
            return
        reward = await self.db.get_reward(int(reward_id))
        if not reward:
            return
        if self.channel_id and reward.get("channel_id") not in (None, self.channel_id):
            return
        if not self.drops_enabled:
            return
        min_value = int(min_value)
        max_value = int(max_value)
        if min_value >= max_value:
            min_value, max_value = 1, 100
        number = int(number)
        if number < min_value or number > max_value:
            number = random.randint(min_value, max_value)
        self.number_game = {
            "active": True,
            "reward_id": int(reward_id),
            "reward_name": reward["name"] or "",
            "number": number,
            "min": min_value,
            "max": max_value,
            "started_at": datetime.datetime.now(),
            "last_hint_at": None,
            "last_hint_by_user": {},
        }
        channel = self.get_channel(self.channel_name)
        if channel:
            await channel.send(
                f"🎲 Угадай число от {min_value} до {max_value}! "
                f"Приз: \"{reward['name']}\". "
                "Пиши число в чат."
            )

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

        text = (
            f"🔴 Стрим начался на канале {self.channel_name}!\n"
            f"https://twitch.tv/{self.channel_name}\n\n"
            "Заходи в чат, чтобы участвовать в дропах."
        )
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
        if not self.drops_enabled or not self.channel_id:
            logger.info("Розыгрыш пропущен: дропы выключены.")
            return
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                "SELECT id, name, weight, quantity FROM rewards WHERE enabled = 1",
            ) as cursor:
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
