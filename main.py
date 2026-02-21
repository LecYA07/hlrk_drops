import asyncio
import logging
from bot import TwitchBot
from telegram_bot import start_telegram_bot
from db import Database
import yaml

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("bot.log"),
        logging.StreamHandler()
    ]
)

async def manage_twitch_bots(config: dict):
    db = Database(config["database"]["db_path"])
    await db.init()

    default_channel = (config["twitch"]["channel"] or "").replace("#", "").lower()
    default_channel_id = await db.ensure_channel(default_channel, None)
    await db.backfill_channel_data(default_channel_id)

    bot_id = await TwitchBot.resolve_bot_id(config)
    active_bots: dict[int, asyncio.Task] = {}

    while True:
        try:
            channels = await db.list_enabled_channels()
            for ch in channels:
                ch_id = int(ch["id"])
                if ch_id in active_bots:
                    continue
                bot = TwitchBot(config, bot_id, ch["login"], ch_id)
                active_bots[ch_id] = asyncio.create_task(bot.start())
                logging.info(f"Twitch бот запущен для канала {ch['login']}")
            await asyncio.sleep(20)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logging.error(f"Ошибка менеджера Twitch-ботов: {e}")
            await asyncio.sleep(5)


async def main():
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)

    try:
        await asyncio.gather(
            manage_twitch_bots(config),
            start_telegram_bot(),
        )
    except KeyboardInterrupt:
        logging.info("Stopping bots...")
    except Exception as e:
        logging.error(f"Error in main loop: {e}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Bot stopped by user.")
