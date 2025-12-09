import os
from pathlib import Path

# Allow overriding DB location via environment (use a persistent volume path in production)
DB_PATH = Path(os.environ.get("DB_PATH", "ballet_bot.db"))
TELEGRAM_PROMO_URL = "https://t.me/+DHe6FlkUmeAwZWJi"
ADMIN_ID = 276784395
ADMIN_GUEST_MODE = False
GUEST_PHANTOM_ID = None
CHANNEL_ID = -1002375876800  # TODO: set your channel ID here for boost checks
BOOST_BONUS = 50
API_BASE_URL = None  # Use official Telegram API; set to e.g., "http://localhost:8081/bot" for self-hosted

# Conversation states
ENTER_NAME, ENTER_AGE, ENTER_GENDER, ENTER_INSTAGRAM, ENTER_PHOTO = range(5)

# Support contact (Telegram username without @). If set, /help will show a contact button.
SUPPORT_USERNAME = ""

def toggle_guest_mode() -> bool:
    """Flip admin guest mode and return the new value."""
    global ADMIN_GUEST_MODE
    ADMIN_GUEST_MODE = not ADMIN_GUEST_MODE
    return ADMIN_GUEST_MODE


def set_guest_mode(enabled: bool) -> None:
    global ADMIN_GUEST_MODE
    ADMIN_GUEST_MODE = enabled
    if not enabled:
        global GUEST_PHANTOM_ID
        GUEST_PHANTOM_ID = None

# Tinder-style profile flow states
TINDER_AGE, TINDER_GENDER, TINDER_INTEREST, TINDER_CITY, TINDER_NAME, TINDER_BIO, TINDER_MEDIA, TINDER_CONFIRM = range(5, 13)
# Admin edit for finder profiles
TINDER_ADMIN_EDIT = 13

# Shop flow states
SHOP_KEEP_OR_GIFT, SHOP_RECIPIENT, SHOP_FILL_WHO, SHOP_FILL_CONTACT, SHOP_NAME, SHOP_EMAIL, SHOP_SIZE, SHOP_ADDRESS = range(20, 28)

# Referral rewards
REFERRAL_BONUS = 50

# Secret Santa flow
SANTA_NAME, SANTA_GIFT = range(40, 42)
