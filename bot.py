import logging
import os

from telegram.ext import ApplicationBuilder

# Token is provided at runtime via BOT_TOKEN environment variable.
import config
import db
from handlers import register_handlers, set_persistent_commands
from helpers import repair_tinder_profiles

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


def main() -> None:
    db.init_db()
    repair_tinder_profiles()

    token = os.environ.get("BOT_TOKEN")
    if not token:
        raise RuntimeError("BOT_TOKEN environment variable is not set.")

    builder = ApplicationBuilder().token(token).post_init(set_persistent_commands)
    if config.API_BASE_URL:
        builder = builder.base_url(config.API_BASE_URL)
    application = builder.build()

    register_handlers(application)
    application.run_polling(close_loop=False)


if __name__ == "__main__":
    main()
