import asyncio
import logging
import random
import string

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


bot = Bot(token=TOKEN)
dp = Dispatcher()
db = Database(config["database"]["db_path"])


def generate_code(length: int = 6) -> str:
    return "".join(random.choices(string.ascii_uppercase + string.digits, k=length))


def menu_kb(is_admin: bool, is_linked: bool = False):
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="👤 Профиль", callback_data="profile"))
    
    if not is_linked:
        kb.row(InlineKeyboardButton(text="🔗 Привязать Twitch", callback_data="link"))
        
    kb.row(InlineKeyboardButton(text="ℹ️ Помощь", callback_data="help"))
    
    if is_admin:
        kb.row(InlineKeyboardButton(text="🛡 Админ-панель", callback_data="admin"))
    return kb.as_markup()


def back_kb():
    kb = InlineKeyboardBuilder()
    kb.row(InlineKeyboardButton(text="🔙 Назад", callback_data="menu"))
    return kb.as_markup()


def admin_kb():
    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text="📊 Статистика", callback_data="admin_stats"),
        InlineKeyboardButton(text="📢 Рассылка", callback_data="admin_broadcast"),
    )
    kb.row(InlineKeyboardButton(text="🔙 Назад", callback_data="menu"))
    return kb.as_markup()


@dp.message(Command("start"))
async def cmd_start(message: Message):
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
    text = (
        f"👤 <b>Профиль</b>\n\n"
        f"🟣 Twitch: <b>{user['twitch_username']}</b>\n"
        f"🏆 Побед: <b>{stats['wins']}</b>"
    )
    if stats.get("last_win"):
        text += f"\n🕒 Последний выигрыш: {stats['last_win'][1]}"
    await query.message.edit_text(text, reply_markup=back_kb(), parse_mode="HTML")
    await query.answer()


@dp.callback_query(F.data == "admin")
async def cb_admin(query: CallbackQuery):
    if query.from_user.id not in ADMIN_IDS:
        await query.answer("⛔️ Нет доступа", show_alert=True)
        return
    await query.message.edit_text("🛡 Админ-панель:", reply_markup=admin_kb())
    await query.answer()


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


async def start_telegram_bot():
    await db.init()
    await dp.start_polling(bot)


async def notify_user(telegram_id: int, text: str):
    try:
        await bot.send_message(telegram_id, text)
    except Exception as e:
        logger.error(f"Не удалось отправить сообщение в TG {telegram_id}: {e}")

