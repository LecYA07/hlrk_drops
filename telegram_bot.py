import asyncio
import datetime
import logging
import random
import string
import re

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import CallbackQuery, InlineKeyboardButton, Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

import yaml

from db import Database


logger = logging.getLogger("TelegramBot")


with open("config.yaml", "r") as f:
    config = yaml.safe_load(f)


TOKEN = config["telegram"]["bot_token"]
ADMIN_IDS = set(config["telegram"].get("admin_ids", []))
ADMIN_CHAT_ID = int(config["telegram"].get("admin_chat_id", -1003117136623))
TWITCH_CHANNEL = str(config.get("twitch", {}).get("channel", "")).replace("#", "").strip()
TWITCH_CHAT_URL = f"https://www.twitch.tv/popout/{TWITCH_CHANNEL}/chat?popout=" if TWITCH_CHANNEL else ""


bot = Bot(token=TOKEN)
dp = Dispatcher()
db = Database(config["database"]["db_path"])

withdraw_sessions: dict[int, dict] = {}
admin_reason_wait: dict[int, dict] = {}
admin_check_sessions: dict[int, dict] = {}
admin_giveaway_sessions: dict[int, dict] = {}
admin_conversion_wait: dict[int, dict] = {}

BOT_USERNAME: str | None = None
GOLD_RE = re.compile(r"^\s*(\d+)\s*GOLD\s*$", re.IGNORECASE)


def generate_code(length: int = 6) -> str:
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=length))

def format_watch_time(seconds: int) -> str:
    seconds = int(seconds) if seconds else 0
    minutes = seconds // 60
    hours = minutes // 60
    minutes = minutes % 60
    if hours > 0:
        return f"{hours}ч {minutes}м"
    return f"{minutes}м"

def format_dt(value) -> str:
    if not value:
        return ""
    s = str(value)
    try:
        dt = datetime.datetime.fromisoformat(s)
        return dt.strftime("%Y-%m-%d %H:%M")
    except Exception:
        return s


def menu_kb(is_admin: bool, is_linked: bool = False):
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="👤 Профиль", callback_data="profile"))
    
    if not is_linked:
        kb.row(InlineKeyboardButton(text="🔗 Привязать Twitch", callback_data="link"))
        
    kb.row(InlineKeyboardButton(text="ℹ️ Помощь", callback_data="help"))
    kb.row(InlineKeyboardButton(text="💸 Вывод", callback_data="withdraw"))
    
    if is_admin:
        kb.row(InlineKeyboardButton(text="🛡 Админ-панель", callback_data="admin"))
    return kb.as_markup()


def back_kb():
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="🔙 Назад", callback_data="menu"))
    return kb.as_markup()

def profile_kb():
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="🔄 Конвертировать предмет", callback_data="convert_menu"))
    kb.row(InlineKeyboardButton(text="🔙 Назад", callback_data="menu"))
    return kb.as_markup()


def admin_kb():
    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats"),
        InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast"),
    )
    kb.row(
        InlineKeyboardButton(text="🧾 Чеки GOLD", callback_data="admin_checks"),
        InlineKeyboardButton(text="📣 Каналы чеков", callback_data="admin_check_channels"),
    )
    kb.row(InlineKeyboardButton(text="🎁 Розыгрыши на стрим", callback_data="admin_stream_giveaways"))
    kb.row(InlineKeyboardButton(text="⚡ Мгновенный розыгрыш", callback_data="admin_instant_giveaway"))
    kb.row(InlineKeyboardButton(text="🔙 Назад", callback_data="menu"))
    return kb.as_markup()

async def get_bot_username() -> str:
    global BOT_USERNAME
    if BOT_USERNAME:
        return BOT_USERNAME
    me = await bot.get_me()
    BOT_USERNAME = me.username
    return BOT_USERNAME


@dp.callback_query(F.data == "menu")
async def cb_menu(query: CallbackQuery):
    is_admin = query.from_user.id in ADMIN_IDS
    user = await db.get_telegram_user(query.from_user.id)
    is_linked = user is not None and user.get("twitch_username") is not None

    await query.message.edit_text("🏠 Меню:", reply_markup=menu_kb(is_admin, is_linked))
    await query.answer()


@dp.callback_query(F.data == "help")
async def cb_help(query: CallbackQuery):
    text = (
        "ℹ️ <b>Как привязать Twitch:</b>\n"
        "1) Нажми «Привязать Twitch» и получи код.\n"
        "2) В чате Twitch напиши: <code>!link КОД</code>\n\n"
        "🎁 <b>Важно про дропы:</b>\n"
        "- если ты выиграл, нужно написать <b>ЛЮБОЕ</b> сообщение в чат в течение 7 минут\n"
        "- иначе награда сгорает 🔥"
    )
    await query.message.edit_text(text, reply_markup=back_kb(), parse_mode="HTML")
    await query.answer()


@dp.callback_query(F.data == "link")
async def cb_link(query: CallbackQuery):
    code = generate_code()
    await db.create_telegram_verification(query.from_user.id, code)
    text = (
        "🔗 <b>Привязка аккаунта Twitch</b>\n\n"
        f"Твой код: <code>{code}</code>\n\n"
        "Отправь в чат Twitch команду:\n"
        f"<code>!link {code}</code>"
    )
    if TWITCH_CHAT_URL:
        text += f"\n\nСсылка на чат Twitch: <a href=\"{TWITCH_CHAT_URL}\">{TWITCH_CHANNEL}</a>"
    await query.message.edit_text(text, reply_markup=back_kb(), parse_mode="HTML")
    await query.answer()


@dp.callback_query(F.data == "profile")
async def cb_profile(query: CallbackQuery):
    user = await db.get_telegram_user(query.from_user.id)
    if not user or not user.get("twitch_username"):
        kb = InlineKeyboardBuilder()
        kb.row(InlineKeyboardButton(text="🔗 Привязать Twitch", callback_data="link"))
        kb.row(InlineKeyboardButton(text="🔙 Назад", callback_data="menu"))
        await query.message.edit_text("❌ Twitch не привязан.", reply_markup=kb.as_markup())
        await query.answer()
        return

    stats = await db.get_user_stats(user["twitch_username"])
    balance = await db.get_gold_balance(query.from_user.id)
    watch_seconds = await db.get_watch_time_seconds(TWITCH_CHANNEL, user["twitch_username"])
    text = (
        f"👤 <b>Профиль</b>\n\n"
        f"🟣 Twitch: <b>{user['twitch_username']}</b>\n"
        f"🏆 Побед: <b>{stats['wins']}</b>\n"
        f"🕓 Время просмотра: <b>{format_watch_time(watch_seconds)}</b>\n"
        f"💰 GOLD: <b>{balance}</b>"
    )
    if stats.get("last_win"):
        text += f"\n🎁 Последний выигрыш: <b>{stats['last_win'][1]}</b>\n🗓 {format_dt(stats['last_win'][0])}"
    await query.message.edit_text(text, reply_markup=profile_kb(), parse_mode="HTML")
    await query.answer()

def convert_items_kb(items: list[dict]):
    kb = InlineKeyboardBuilder()
    for it in items[:20]:
        name = (it.get("reward_name") or "").strip()
        draw_id = int(it["draw_id"])
        label = name if len(name) <= 30 else (name[:27] + "…")
        kb.row(InlineKeyboardButton(text=f"🔄 {label} (#{draw_id})", callback_data=f"convert:{draw_id}"))
    kb.row(InlineKeyboardButton(text="🔙 Назад", callback_data="profile"))
    return kb.as_markup()

@dp.callback_query(F.data == "convert_menu")
async def cb_convert_menu(query: CallbackQuery):
    user = await db.get_telegram_user(query.from_user.id)
    if not user or not user.get("twitch_username"):
        await query.answer("Сначала привяжи Twitch", show_alert=True)
        return
    items = await db.list_available_item_claims(query.from_user.id)
    if not items:
        await query.message.edit_text("Пока нет предметов для конвертации.", reply_markup=back_kb())
        await query.answer()
        return
    await query.message.edit_text(
        "Выбери предмет, который нужно конвертировать в GOLD:",
        reply_markup=convert_items_kb(items),
    )
    await query.answer()

def conversion_admin_kb(request_id: int):
    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="✅ Начислить GOLD", callback_data=f"cv:credit:{request_id}"),
        InlineKeyboardButton(text="❌ Отклонить", callback_data=f"cv:rej:{request_id}"),
    )
    return kb.as_markup()

@dp.callback_query(F.data.startswith("convert:"))
async def cb_convert_pick(query: CallbackQuery):
    parts = (query.data or "").split(":")
    if len(parts) != 2:
        await query.answer("Некорректные данные", show_alert=True)
        return
    try:
        draw_id = int(parts[1])
    except Exception:
        await query.answer("Некорректный ID", show_alert=True)
        return

    request_id = await db.create_conversion_request(
        telegram_id=query.from_user.id,
        telegram_username=query.from_user.username or "",
        draw_id=draw_id,
    )
    if not request_id:
        await query.answer("Не получилось создать заявку", show_alert=True)
        return

    req = await db.get_conversion_request(int(request_id))
    text = (
        "🔄 <b>Конвертация предмета</b>\n\n"
        f"👤 TG: @{query.from_user.username or '—'} (id <code>{query.from_user.id}</code>)\n"
        f"🎁 Предмет: <b>{req['reward_name']}</b>\n"
        f"🧾 Заявка: <code>{request_id}</code>\n"
        f"📦 Draw ID: <code>{draw_id}</code>"
    )
    try:
        admin_msg = await bot.send_message(
            ADMIN_CHAT_ID,
            text,
            reply_markup=conversion_admin_kb(int(request_id)),
            parse_mode="HTML",
        )
        await db.set_conversion_admin_message(int(request_id), admin_msg.chat.id, admin_msg.message_id)
    except Exception:
        await db.decide_conversion(int(request_id), "rejected", 0, reason="admin_chat_send_failed")
        await query.answer("Не удалось отправить в админ-чат", show_alert=True)
        return

    await query.message.edit_text("Заявка на конвертацию отправлена. Ожидай решения админа.", reply_markup=back_kb())
    await query.answer("Отправлено", show_alert=True)

def stream_giveaways_kb(rows: list[dict]):
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="➕ Создать", callback_data="sg:create"))
    kb.row(InlineKeyboardButton(text="🔄 Обновить", callback_data="admin_stream_giveaways"))
    for g in rows[:10]:
        gid = int(g["id"])
        status = g.get("status") or "planned"
        title = (g.get("title") or "").strip()
        label = title if len(title) <= 24 else (title[:21] + "…")
        if status in ("planned", "end"):
            kb.row(InlineKeyboardButton(text=f"▶️ Сейчас #{gid} ({label})", callback_data=f"sg:run:{gid}"))
        if status == "planned":
            kb.row(InlineKeyboardButton(text=f"🏁 В конец #{gid}", callback_data=f"sg:end:{gid}"))
        if status == "end":
            kb.row(InlineKeyboardButton(text=f"↩️ Убрать из конца #{gid}", callback_data=f"sg:plan:{gid}"))
    kb.row(InlineKeyboardButton(text="🔙 Назад", callback_data="admin"))
    return kb.as_markup()

@dp.callback_query(F.data == "admin_stream_giveaways")
async def cb_admin_stream_giveaways(query: CallbackQuery):
    if query.from_user.id not in ADMIN_IDS:
        await query.answer("⛔️ Нет доступа", show_alert=True)
        return
    rows = await db.list_planned_giveaways()
    text = "🎁 <b>Розыгрыши на стрим</b>\n\n"
    if not rows:
        text += "Пока пусто."
    else:
        lines: list[str] = []
        for g in rows[:10]:
            status = g.get("status") or "planned"
            lines.append(f"#{g['id']} — <b>{g['title']}</b> — победителей: {g['winners_count']} — {status}")
        text += "\n".join(lines)
    await query.message.edit_text(text, reply_markup=stream_giveaways_kb(rows), parse_mode="HTML")
    await query.answer()

@dp.callback_query(F.data == "sg:create")
async def cb_sg_create(query: CallbackQuery):
    if query.from_user.id not in ADMIN_IDS:
        await query.answer("⛔️ Нет доступа", show_alert=True)
        return
    admin_giveaway_sessions[query.from_user.id] = {"stage": "create"}
    await query.message.answer(
        "Отправь одним сообщением:\n<code>Название | Кол-во победителей</code>\nПример: <code>AKR12 | 2</code>",
        parse_mode="HTML",
    )
    await query.answer("Жду параметры", show_alert=True)

@dp.callback_query(F.data.startswith("sg:"))
async def cb_sg_actions(query: CallbackQuery):
    if query.from_user.id not in ADMIN_IDS:
        await query.answer("⛔️ Нет доступа", show_alert=True)
        return
    if (query.data or "") == "sg:create":
        return
    parts = (query.data or "").split(":")
    if len(parts) != 3:
        await query.answer("Некорректные данные", show_alert=True)
        return
    action = parts[1]
    try:
        planned_id = int(parts[2])
    except Exception:
        await query.answer("Некорректный ID", show_alert=True)
        return

    if action == "run":
        try:
            await db.create_planned_giveaway_trigger(planned_id, query.from_user.id)
            await db.set_planned_giveaway_status(planned_id, "triggered")
        except Exception:
            await query.answer("Не получилось запустить", show_alert=True)
            return
        await query.answer("Запрошено", show_alert=True)
        await cb_admin_stream_giveaways(query)
        return

    if action == "end":
        await db.set_planned_giveaway_status(planned_id, "end")
        await query.answer("Добавлено в конец стрима", show_alert=True)
        await cb_admin_stream_giveaways(query)
        return

    if action == "plan":
        await db.set_planned_giveaway_status(planned_id, "planned")
        await query.answer("Убрано из конца", show_alert=True)
        await cb_admin_stream_giveaways(query)
        return

    await query.answer("Неизвестное действие", show_alert=True)


@dp.callback_query(F.data.startswith("cv:"))
async def cb_conversion_admin_action(query: CallbackQuery):
    if query.from_user.id not in ADMIN_IDS:
        await query.answer("⛔️ Нет доступа", show_alert=True)
        return
    parts = (query.data or "").split(":")
    if len(parts) != 3:
        await query.answer("Некорректные данные", show_alert=True)
        return
    action = parts[1]
    try:
        request_id = int(parts[2])
    except Exception:
        await query.answer("Некорректный ID", show_alert=True)
        return

    req = await db.get_conversion_request(request_id)
    if not req or req.get("status") != "pending":
        await query.answer("Уже обработано", show_alert=True)
        return

    if action == "credit":
        admin_conversion_wait[query.from_user.id] = {"request_id": request_id, "action": "credit"}
        await query.message.answer(f"Напиши сумму GOLD для заявки <code>{request_id}</code>.", parse_mode="HTML")
        await query.answer("Жду сумму", show_alert=True)
        return

    if action == "rej":
        admin_conversion_wait[query.from_user.id] = {"request_id": request_id, "action": "rej"}
        await query.message.answer(f"Напиши причину отказа для заявки <code>{request_id}</code>.", parse_mode="HTML")
        await query.answer("Жду причину", show_alert=True)
        return

    await query.answer("Неизвестное действие", show_alert=True)

@dp.callback_query(F.data == "admin")
async def cb_admin(query: CallbackQuery):
    if query.from_user.id not in ADMIN_IDS:
        await query.answer("⛔️ Нет доступа", show_alert=True)
        return
    await query.message.edit_text("🛡 Админ-панель:", reply_markup=admin_kb())
    await query.answer()


@dp.callback_query(F.data == "admin_instant_giveaway")
async def cb_admin_instant_giveaway(query: CallbackQuery):
    if query.from_user.id not in ADMIN_IDS:
        await query.answer("⛔️ Нет доступа", show_alert=True)
        return
    trigger_id = await db.create_giveaway_trigger(query.from_user.id)
    await query.answer("Запрос отправлен", show_alert=True)
    try:
        await query.message.edit_text(
            f"⚡ Запрошен мгновенный розыгрыш. ID: <code>{trigger_id}</code>",
            reply_markup=admin_kb(),
            parse_mode="HTML",
        )
    except Exception:
        pass


@dp.callback_query(F.data == "admin_stats")
async def cb_admin_stats(query: CallbackQuery):
    if query.from_user.id not in ADMIN_IDS:
        await query.answer("⛔️ Нет доступа", show_alert=True)
        return

    linked = await db.get_linked_users_count()
    draws = await db.get_total_draws_count()
    await query.message.edit_text(
        f"📊 <b>Статистика</b>\n\n"
        f"🔗 Привязанных пользователей: <b>{linked}</b>\n"
        f"🎁 Всего дропов в базе: <b>{draws}</b>",
        reply_markup=admin_kb(),
        parse_mode="HTML"
    )
    await query.answer()


@dp.callback_query(F.data == "admin_broadcast")
async def cb_admin_broadcast(query: CallbackQuery):
    if query.from_user.id not in ADMIN_IDS:
        await query.answer("⛔️ Нет доступа", show_alert=True)
        return

    text = (
        "📢 <b>Рассылка</b>\n\n"
        "Отправь команду:\n"
        "<code>/broadcast Текст сообщения</code>"
    )
    await query.message.edit_text(text, reply_markup=admin_kb(), parse_mode="HTML")
    await query.answer()


@dp.message(Command("broadcast"))
async def cmd_broadcast(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return

    text = message.text.replace("/broadcast", "", 1).strip()
    if not text:
        await message.answer("Пример: /broadcast Привет всем!")
        return

    telegram_ids = await db.get_all_linked_telegram_ids()
    sent = 0
    for tg_id in telegram_ids:
        try:
            await bot.send_message(tg_id, f"Объявление\n\n{text}")
            sent += 1
            await asyncio.sleep(0.05)
        except Exception:
            continue
    await message.answer(f"Отправлено: {sent}")


def withdraw_admin_kb(withdrawal_id: int):
    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="✅ Одобрить", callback_data=f"wd:ok:{withdrawal_id}"),
        InlineKeyboardButton(text="❌ Отклонить", callback_data=f"wd:rej:{withdrawal_id}"),
    )
    kb.row(InlineKeyboardButton(text="🗑 Отклонить и списать", callback_data=f"wd:rejw:{withdrawal_id}"))
    return kb.as_markup()

def withdrawal_caption(withdrawal: dict, status_line: str | None = None) -> str:
    tg_id = int(withdrawal["telegram_id"])
    username = (withdrawal.get("telegram_username") or "").strip()
    user_label = f"@{username}" if username else str(tg_id)
    base = (
        "Новая заявка на вывод\n\n"
        f"Пользователь: <a href=\"tg://user?id={tg_id}\">{user_label}</a>\n"
        f"Предмет: {withdrawal.get('item_name')}\n"
        f"Цена: {withdrawal.get('price')} GOLD\n"
        f"Паттерн: {withdrawal.get('pattern')}\n"
        f"ID: {withdrawal.get('id')}"
    )
    if status_line:
        return base + "\n\n" + status_line
    return base


@dp.message(Command("cancel"))
async def cmd_cancel(message: Message):
    withdraw_sessions.pop(message.from_user.id, None)
    admin_reason_wait.pop(message.from_user.id, None)
    admin_check_sessions.pop(message.from_user.id, None)
    admin_giveaway_sessions.pop(message.from_user.id, None)
    admin_conversion_wait.pop(message.from_user.id, None)
    await message.answer("Отменено.")


@dp.callback_query(F.data == "withdraw")
async def cb_withdraw(query: CallbackQuery):
    withdraw_sessions[query.from_user.id] = {"stage": "photo"}
    text = (
        "Заявка на вывод\n\n"
        "Отправь скриншот выставленного предмета \"G22 flock\" на рынке.\n\n"
        "После отправки заявки GOLD спишется с баланса.\n"
        "Минимум: 1000 GOLD.\n\n"
        "Отмена: /cancel"
    )
    await query.message.edit_text(text, reply_markup=back_kb())
    await query.answer()


@dp.message(F.chat.type == "private", F.photo)
async def withdraw_photo(message: Message):
    session = withdraw_sessions.get(message.from_user.id)
    if not session or session.get("stage") != "photo":
        return
    session["photo_id"] = message.photo[-1].file_id
    session["stage"] = "price"
    await message.answer("Укажи цену в GOLD (целым числом). Минимум: 1000")


@dp.message(F.chat.type == "private", F.text, ~F.text.startswith("/"))
async def private_text_router(message: Message):
    text = (message.text or "").strip()

    session = withdraw_sessions.get(message.from_user.id)
    if session:
        if session.get("stage") == "price":
            try:
                amount = int(text)
            except Exception:
                await message.answer("Цена должна быть целым числом GOLD. Пример: 1500")
                return
            if amount < 1000:
                await message.answer("Минимальная сумма вывода: 1000 GOLD")
                return
            session["price"] = str(amount)
            session["stage"] = "pattern"
            await message.answer("Укажи паттерн.")
            return

        if session.get("stage") == "pattern":
            pattern = text
            try:
                price_gold = int(session.get("price") or 0)
            except Exception:
                price_gold = 0
            if price_gold < 1000:
                await message.answer("Минимальная сумма вывода: 1000 GOLD")
                return

            withdrawal_id = await db.create_withdrawal(
                telegram_id=message.from_user.id,
                telegram_username=message.from_user.username or "",
                item_name="G22 flock",
                photo_file_id=session.get("photo_id"),
                price=str(price_gold),
                pattern=pattern,
            )

            debit = await db.apply_gold_delta_once(
                telegram_id=message.from_user.id,
                amount=-price_gold,
                source_type="withdrawal",
                source_id=withdrawal_id,
            )
            if not debit.get("ok"):
                await db.delete_withdrawal(withdrawal_id)
                if debit.get("status") == "insufficient":
                    await message.answer(
                        f"Недостаточно GOLD для вывода.\n💰 Баланс: {debit.get('balance', 0)}"
                    )
                else:
                    await message.answer("Не удалось списать GOLD. Попробуй позже.")
                withdraw_sessions.pop(message.from_user.id, None)
                return

            withdrawal = await db.get_withdrawal(withdrawal_id)
            caption = withdrawal_caption(withdrawal)
            try:
                admin_msg = await bot.send_photo(
                    ADMIN_CHAT_ID,
                    withdrawal.get("photo_file_id"),
                    caption=caption,
                    reply_markup=withdraw_admin_kb(withdrawal_id),
                    parse_mode="HTML",
                )
            except Exception:
                await db.apply_gold_delta_once(
                    telegram_id=message.from_user.id,
                    amount=price_gold,
                    source_type="withdrawal_rollback",
                    source_id=withdrawal_id,
                )
                await db.delete_withdrawal(withdrawal_id)
                await message.answer("Не удалось отправить заявку в админ-чат. Попробуй позже.")
                withdraw_sessions.pop(message.from_user.id, None)
                return

            await db.set_withdrawal_admin_message(
                withdrawal_id=withdrawal_id,
                admin_chat_id=admin_msg.chat.id,
                admin_message_id=admin_msg.message_id,
            )
            withdraw_sessions.pop(message.from_user.id, None)
            await message.answer("Заявка отправлена. GOLD списан, ожидай решения админа.")
            return

        return

    if message.from_user.id in ADMIN_IDS:
        gsess = admin_giveaway_sessions.get(message.from_user.id)
        if gsess and gsess.get("stage") == "create":
            raw = text
            if "|" in raw:
                title_part, count_part = raw.split("|", 1)
                title = title_part.strip()
                count_raw = count_part.strip()
            else:
                title = raw.strip()
                count_raw = "1"
            try:
                winners_count = int(count_raw)
            except Exception:
                await message.answer("Кол-во победителей должно быть числом. Пример: <code>AKR12 | 2</code>", parse_mode="HTML")
                return
            if not title or winners_count <= 0:
                await message.answer("Некорректные данные. Пример: <code>AKR12 | 2</code>", parse_mode="HTML")
                return
            try:
                planned_id = await db.create_planned_giveaway(title, winners_count, message.from_user.id)
            except Exception:
                await message.answer("Не удалось создать розыгрыш.")
                admin_giveaway_sessions.pop(message.from_user.id, None)
                return
            admin_giveaway_sessions.pop(message.from_user.id, None)
            await message.answer(f"Создан розыгрыш #{planned_id}: {title} (победителей: {winners_count})")
            return

        sess = admin_check_sessions.get(message.from_user.id)
        if sess and sess.get("stage") == "params":
            parts = text.strip().split()
            if len(parts) != 2:
                await message.answer(
                    "Отправь два числа: <code>N M</code>\nПример: <code>100 5</code>",
                    parse_mode="HTML",
                )
                return
            try:
                amount = int(parts[0])
                max_activations = int(parts[1])
            except Exception:
                await message.answer(
                    "N и M должны быть числами. Пример: <code>100 5</code>",
                    parse_mode="HTML",
                )
                return
            if amount <= 0 or max_activations <= 0:
                await message.answer("N и M должны быть больше 0.")
                return
            sess["amount"] = amount
            sess["max_activations"] = max_activations
            sess["stage"] = "channel"
            channels = await db.list_check_channels()
            if not channels:
                await message.answer("Сначала добавь канал: /add_check_channel CHAT_ID Название")
                admin_check_sessions.pop(message.from_user.id, None)
                return
            await message.answer(
                f"Чек: <b>{amount} GOLD</b>, активаций: <b>{max_activations}</b>\n\nВыбери канал для публикации:",
                reply_markup=check_channel_kb(channels),
                parse_mode="HTML",
            )
            return

    return


@dp.callback_query(F.data.startswith("wd:"))
async def cb_withdraw_admin_action(query: CallbackQuery):
    if query.from_user.id not in ADMIN_IDS:
        await query.answer("⛔️ Нет доступа", show_alert=True)
        return
    parts = (query.data or "").split(":")
    if len(parts) != 3:
        await query.answer("Некорректные данные", show_alert=True)
        return
    action = parts[1]
    try:
        withdrawal_id = int(parts[2])
    except Exception:
        await query.answer("Некорректный ID", show_alert=True)
        return
    withdrawal = await db.get_withdrawal(withdrawal_id)
    if not withdrawal:
        await query.answer("Заявка не найдена", show_alert=True)
        return

    if action == "ok":
        saved = await db.decide_withdrawal(withdrawal_id, "approved", query.from_user.id)
        if not saved:
            await query.answer("Уже обработано", show_alert=True)
            return
        try:
            await bot.send_message(int(withdrawal["telegram_id"]), "✅ Вывод сделан.")
        except Exception:
            pass
        try:
            await bot.edit_message_reply_markup(
                chat_id=int(withdrawal["admin_chat_id"]),
                message_id=int(withdrawal["admin_message_id"]),
                reply_markup=None,
            )
            await bot.edit_message_caption(
                chat_id=int(withdrawal["admin_chat_id"]),
                message_id=int(withdrawal["admin_message_id"]),
                caption=withdrawal_caption(withdrawal, "Статус: ✅ Одобрено"),
                parse_mode="HTML",
            )
        except Exception:
            pass
        await query.answer("Одобрено")
        return

    if action in ("rej", "rejw"):
        if withdrawal.get("status") != "pending":
            await query.answer("Уже обработано", show_alert=True)
            return
        admin_reason_wait[query.from_user.id] = {"withdrawal_id": withdrawal_id, "action": action}
        await query.message.answer(f"Напиши причину для заявки ID {withdrawal_id} одним сообщением.")
        await query.answer("Жду причину")
        return

    await query.answer("Неизвестное действие", show_alert=True)


@dp.message(F.chat.id == ADMIN_CHAT_ID, F.text)
async def withdraw_admin_reason(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    wait = admin_reason_wait.get(message.from_user.id)
    cwait = admin_conversion_wait.get(message.from_user.id)
    if not wait and not cwait:
        return

    if cwait:
        request_id = int(cwait["request_id"])
        action = cwait["action"]
        req = await db.get_conversion_request(request_id)
        if not req or req.get("status") != "pending":
            admin_conversion_wait.pop(message.from_user.id, None)
            await message.reply("Заявка не найдена или уже обработана.")
            return
        text = (message.text or "").strip()
        if action == "credit":
            try:
                amount = int(text)
            except Exception:
                await message.reply("Сумма должна быть числом. Пример: 1500")
                return
            res = await db.credit_conversion_request(request_id, message.from_user.id, amount)
            if not res.get("ok"):
                admin_conversion_wait.pop(message.from_user.id, None)
                await message.reply("Не удалось начислить.")
                return
            try:
                await bot.send_message(int(req["telegram_id"]), f"✅ Конвертация подтверждена. Начислено {amount} GOLD.")
            except Exception:
                pass
            try:
                await bot.edit_message_reply_markup(
                    chat_id=int(req["admin_chat_id"]),
                    message_id=int(req["admin_message_id"]),
                    reply_markup=None,
                )
                await bot.edit_message_text(
                    "🔄 <b>Конвертация предмета</b>\n\n"
                    f"🎁 Предмет: <b>{req['reward_name']}</b>\n"
                    f"🧾 Заявка: <code>{request_id}</code>\n"
                    f"✅ Начислено: <b>{amount} GOLD</b>",
                    chat_id=int(req["admin_chat_id"]),
                    message_id=int(req["admin_message_id"]),
                    parse_mode="HTML",
                )
            except Exception:
                pass
            admin_conversion_wait.pop(message.from_user.id, None)
            await message.reply("Готово.")
            return

        reason = text
        if not reason:
            await message.reply("Причина не должна быть пустой.")
            return
        saved = await db.decide_conversion(request_id, "rejected", message.from_user.id, reason=reason)
        if not saved:
            admin_conversion_wait.pop(message.from_user.id, None)
            await message.reply("Заявка уже обработана.")
            return
        try:
            await bot.send_message(int(req["telegram_id"]), f"❌ Конвертация отклонена.\nПричина: {reason}")
        except Exception:
            pass
        try:
            await bot.edit_message_reply_markup(
                chat_id=int(req["admin_chat_id"]),
                message_id=int(req["admin_message_id"]),
                reply_markup=None,
            )
            await bot.edit_message_text(
                "🔄 <b>Конвертация предмета</b>\n\n"
                f"🎁 Предмет: <b>{req['reward_name']}</b>\n"
                f"🧾 Заявка: <code>{request_id}</code>\n"
                f"❌ Отклонено: {reason}",
                chat_id=int(req["admin_chat_id"]),
                message_id=int(req["admin_message_id"]),
                parse_mode="HTML",
            )
        except Exception:
            pass
        admin_conversion_wait.pop(message.from_user.id, None)
        await message.reply("Готово.")
        return

    if not wait:
        return
    withdrawal_id = int(wait["withdrawal_id"])
    action = wait["action"]
    withdrawal = await db.get_withdrawal(withdrawal_id)
    if not withdrawal:
        admin_reason_wait.pop(message.from_user.id, None)
        await message.reply("Заявка не найдена или уже обработана.")
        return
    if withdrawal.get("status") != "pending":
        admin_reason_wait.pop(message.from_user.id, None)
        await message.reply("Заявка уже обработана.")
        return

    reason = (message.text or "").strip()
    if not reason:
        await message.reply("Причина не должна быть пустой.")
        return

    if action == "rej":
        saved = await db.decide_withdrawal(
            withdrawal_id,
            "rejected_refund",
            message.from_user.id,
            reason=reason,
        )
        if not saved:
            admin_reason_wait.pop(message.from_user.id, None)
            await message.reply("Заявка уже обработана.")
            return
        try:
            amount = int(withdrawal.get("price") or 0)
        except Exception:
            amount = 0
        if amount > 0:
            await db.apply_gold_delta_once(
                telegram_id=int(withdrawal["telegram_id"]),
                amount=amount,
                source_type="withdrawal_refund",
                source_id=withdrawal_id,
            )
        user_text = (
            "❌ Заявка на вывод отклонена.\n"
            f"Причина: {reason}\n"
            "GOLD возвращён."
        )
        status = f"Статус: ❌ Отклонено\nПричина: {reason}\nДействие: GOLD возвращён"
    else:
        saved = await db.decide_withdrawal(
            withdrawal_id,
            "rejected_writeoff",
            message.from_user.id,
            reason=reason,
        )
        if not saved:
            admin_reason_wait.pop(message.from_user.id, None)
            await message.reply("Заявка уже обработана.")
            return
        user_text = (
            "❌ Заявка на вывод отклонена.\n"
            f"Причина: {reason}\n"
            "GOLD не возвращается."
        )
        status = f"Статус: 🗑 Отклонено и списано\nПричина: {reason}"

    try:
        await bot.send_message(int(withdrawal["telegram_id"]), user_text)
    except Exception:
        pass

    try:
        await bot.edit_message_reply_markup(
            chat_id=int(withdrawal["admin_chat_id"]),
            message_id=int(withdrawal["admin_message_id"]),
            reply_markup=None,
        )
        await bot.edit_message_caption(
            chat_id=int(withdrawal["admin_chat_id"]),
            message_id=int(withdrawal["admin_message_id"]),
            caption=withdrawal_caption(withdrawal, status),
            parse_mode="HTML",
        )
    except Exception:
        pass

    admin_reason_wait.pop(message.from_user.id, None)
    await message.reply("Готово.")


def check_channel_kb(channels: list[dict]):
    kb = InlineKeyboardBuilder()
    for ch in channels:
        title = (ch.get("title") or "").strip()
        label = title if title else str(ch["chat_id"])
        kb.row(InlineKeyboardButton(text=label, callback_data=f"check_post:{ch['chat_id']}"))
    kb.row(InlineKeyboardButton(text="🔙 Назад", callback_data="admin"))
    return kb.as_markup()


def check_admin_menu_kb():
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="➕ Создать чек", callback_data="check_create"))
    kb.row(InlineKeyboardButton(text="🔙 Назад", callback_data="admin"))
    return kb.as_markup()


def check_channels_menu_kb():
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="🔙 Назад", callback_data="admin"))
    return kb.as_markup()


def check_message_text(amount: int, max_activations: int, activated_count: int) -> str:
    return (
        f"🧾 Новый чек на {amount} GOLD\n"
        f"🔁 Активаций: {max_activations}\n"
        f"✅ Активировано: {activated_count}"
    )


def check_activate_kb(bot_username: str, code: str):
    url = f"https://t.me/{bot_username}?start=check_{code}"
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="✅ Активировать", url=url))
    return kb.as_markup()


@dp.callback_query(F.data == "admin_checks")
async def cb_admin_checks(query: CallbackQuery):
    if query.from_user.id not in ADMIN_IDS:
        await query.answer("⛔️ Нет доступа", show_alert=True)
        return
    text = (
        "🧾 <b>Чеки GOLD</b>\n\n"
        "Создание: нажми «Создать чек», затем отправь: <code>N M</code>\n"
        "где N — сумма GOLD, M — кол-во активаций.\n\n"
        "Отмена: /cancel"
    )
    await query.message.edit_text(text, reply_markup=check_admin_menu_kb(), parse_mode="HTML")
    await query.answer()


@dp.callback_query(F.data == "check_create")
async def cb_check_create(query: CallbackQuery):
    if query.from_user.id not in ADMIN_IDS:
        await query.answer("⛔️ Нет доступа", show_alert=True)
        return
    admin_check_sessions[query.from_user.id] = {"stage": "params"}
    await query.message.edit_text(
        "🧾 <b>Создание чека</b>\n\n"
        "Отправь одним сообщением:\n"
        "<code>N M</code>\n\n"
        "N — сумма GOLD\n"
        "M — количество активаций\n\n"
        "Пример: <code>100 5</code>\n\n"
        "Отмена: /cancel",
        reply_markup=InlineKeyboardBuilder().row(InlineKeyboardButton(text="🔙 Назад", callback_data="admin_checks")).as_markup(),
        parse_mode="HTML",
    )
    await query.answer()


@dp.callback_query(F.data == "admin_check_channels")
async def cb_admin_check_channels(query: CallbackQuery):
    if query.from_user.id not in ADMIN_IDS:
        await query.answer("⛔️ Нет доступа", show_alert=True)
        return
    channels = await db.list_check_channels()
    lines = ["📣 <b>Каналы для чеков</b>\n"]
    if not channels:
        lines.append("Список пуст.\n")
    else:
        for ch in channels:
            title = (ch.get("title") or "").strip()
            label = title if title else str(ch["chat_id"])
            lines.append(f"- {label} (<code>{ch['chat_id']}</code>)")
        lines.append("")
    lines.append("Добавить: <code>/add_check_channel CHAT_ID Название</code>")
    lines.append("Удалить: <code>/del_check_channel CHAT_ID</code>")
    await query.message.edit_text("\n".join(lines), reply_markup=check_channels_menu_kb(), parse_mode="HTML")
    await query.answer()


@dp.message(Command("add_check_channel"))
async def cmd_add_check_channel(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    parts = (message.text or "").split(maxsplit=2)
    if len(parts) < 2:
        await message.answer("Пример: /add_check_channel -1001234567890 Мой канал")
        return
    try:
        chat_id = int(parts[1])
    except Exception:
        await message.answer("CHAT_ID должен быть числом. Пример: -1001234567890")
        return
    title = parts[2].strip() if len(parts) >= 3 else ""
    await db.add_check_channel(chat_id, title)
    await message.answer("Канал добавлен.")


@dp.message(Command("del_check_channel"))
async def cmd_del_check_channel(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Пример: /del_check_channel -1001234567890")
        return
    try:
        chat_id = int(parts[1])
    except Exception:
        await message.answer("CHAT_ID должен быть числом.")
        return
    await db.remove_check_channel(chat_id)
    await message.answer("Канал удалён.")


@dp.callback_query(F.data.startswith("check_post:"))
async def cb_check_post(query: CallbackQuery):
    if query.from_user.id not in ADMIN_IDS:
        await query.answer("⛔️ Нет доступа", show_alert=True)
        return
    sess = admin_check_sessions.get(query.from_user.id)
    if not sess or sess.get("stage") != "channel":
        await query.answer("Сначала создай чек", show_alert=True)
        return
    try:
        channel_id = int((query.data or "").split(":", 1)[1])
    except Exception:
        await query.answer("Некорректный канал", show_alert=True)
        return

    amount = int(sess["amount"])
    max_activations = int(sess["max_activations"])
    code = generate_code(16)
    bot_username = await get_bot_username()

    check_id = await db.create_gold_check(amount, max_activations, query.from_user.id, channel_id, code)
    text = check_message_text(amount, max_activations, 0)
    try:
        msg = await bot.send_message(
            channel_id,
            text,
            reply_markup=check_activate_kb(bot_username, code),
        )
    except Exception:
        await query.message.answer("Не удалось отправить сообщение в канал. Проверь права бота в канале.")
        admin_check_sessions.pop(query.from_user.id, None)
        await query.answer()
        return

    await db.set_gold_check_message(check_id, msg.message_id)
    admin_check_sessions.pop(query.from_user.id, None)
    await query.message.answer(f"Чек создан и опубликован в канале {channel_id}. ID: {check_id}")
    await query.answer("Опубликовано")


@dp.message(Command("start"))
async def cmd_start(message: Message):
    payload = ""
    if message.text and " " in message.text:
        payload = message.text.split(" ", 1)[1].strip()
    if payload.startswith("check_"):
        code = payload.replace("check_", "", 1).strip()
        user = await db.get_telegram_user(message.from_user.id)
        if not user or not user.get("twitch_username"):
            kb = InlineKeyboardBuilder()
            kb.row(InlineKeyboardButton(text="🔗 Привязать Twitch", callback_data="link"))
            if TWITCH_CHAT_URL:
                text = (
                    "Чтобы активировать чек, сначала привяжи Twitch.\n\n"
                    f"Ссылка на чат Twitch: <a href=\"{TWITCH_CHAT_URL}\">{TWITCH_CHANNEL}</a>"
                )
                await message.answer(text, reply_markup=kb.as_markup(), parse_mode="HTML")
            else:
                await message.answer("Чтобы активировать чек, сначала привяжи Twitch.", reply_markup=kb.as_markup())
            return
        result = await db.activate_gold_check(code, message.from_user.id)
        if result.get("status") == "activated":
            balance = await db.get_gold_balance(message.from_user.id)
            await message.answer(f"✅ Чек активирован: +{result['amount']} GOLD\n💰 Баланс: {balance}")
            try:
                bot_username = await get_bot_username()
                await bot.edit_message_text(
                    chat_id=int(result["channel_id"]),
                    message_id=int(result["message_id"]),
                    text=check_message_text(
                        int(result["amount"]),
                        int(result["max_activations"]),
                        int(result["activated_count"]),
                    ),
                    reply_markup=check_activate_kb(bot_username, code),
                )
            except Exception:
                pass
            return
        if result.get("status") == "already":
            await message.answer("Ты уже активировал этот чек.")
            return
        if result.get("status") == "finished":
            await message.answer("Этот чек уже закончился.")
            return
        if result.get("status") == "inactive":
            await message.answer("Этот чек больше не активен.")
            return
        await message.answer("Чек не найден.")
        return

    is_admin = message.from_user.id in ADMIN_IDS
    user = await db.get_telegram_user(message.from_user.id)
    is_linked = user is not None and user.get("twitch_username") is not None

    text = (
        "👋 Привет! Я Telegram-бот для дропов на Twitch.\n\n"
        "🤖 <b>Что умею:</b>\n"
        "- 🔗 привязка Twitch к Telegram\n"
        "- 📊 профиль и статистика\n"
        "- 🔔 уведомления о старте/конце стрима и наградах\n\n"
        "Нажми кнопку ниже."
    )
    await message.answer(text, reply_markup=menu_kb(is_admin, is_linked), parse_mode="HTML")


async def start_telegram_bot():
    await db.init()
    await dp.start_polling(bot)


async def notify_user(telegram_id: int, text: str):
    try:
        await bot.send_message(telegram_id, text)
    except Exception as e:
        logger.error(f"Не удалось отправить сообщение в TG {telegram_id}: {e}")
