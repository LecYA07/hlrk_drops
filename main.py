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

async def main():
    # Load config for DB
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)
    
    # Init Database
    db = Database(config['database']['db_path'])
    await db.init()
    
    # Init Twitch Bot
    twitch_bot = await TwitchBot.create()
    
    # Run both bots
    try:
        await asyncio.gather(
            twitch_bot.start(),
            start_telegram_bot()
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
