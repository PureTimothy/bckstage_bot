from telegram import InlineKeyboardButton, InlineKeyboardMarkup

import config
import db
from translations import t


def language_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(t("en", "language_button_en"), callback_data="lang:en"),
            ],
            [
                InlineKeyboardButton(t("en", "language_button_uk"), callback_data="lang:uk"),
            ],
            [
                InlineKeyboardButton(t("en", "language_button_ru"), callback_data="lang:ru"),
            ],
        ]
    )


def start_voting_keyboard(lang: str) -> InlineKeyboardMarkup:
    rows = []
    if db.is_feature_enabled("voting"):
        rows.append(
            [
                InlineKeyboardButton(
                    f"⭐ {t(lang, 'start_voting_button')}",
                    callback_data="start:vote",
                )
            ]
        )
    if db.is_feature_enabled("santa"):
        rows.append([InlineKeyboardButton(t(lang, "santa_menu_button"), callback_data="santa:start")])
    if db.is_feature_enabled("finder"):
        rows.append([InlineKeyboardButton(t(lang, "tinder_find_button"), callback_data="tinder:start")])
    if db.is_feature_enabled("fun"):
        rows.append([InlineKeyboardButton(t(lang, "play_game_button"), callback_data="game:menu")])
    if config.TELEGRAM_PROMO_URL:
        rows.append(
            [InlineKeyboardButton(t(lang, "promo_button"), url=config.TELEGRAM_PROMO_URL)]
        )
    return InlineKeyboardMarkup(rows)


def paused_keyboard(lang: str) -> InlineKeyboardMarkup:
    buttons = []
    if db.is_feature_enabled("santa"):
        buttons.append([InlineKeyboardButton(t(lang, "santa_menu_button"), callback_data="santa:start")])
    if db.is_feature_enabled("fun"):
        buttons.append([InlineKeyboardButton(t(lang, "play_game_button"), callback_data="game:menu")])
    if db.is_feature_enabled("finder"):
        buttons.append([InlineKeyboardButton(t(lang, "tinder_find_button"), callback_data="tinder:start")])
    if config.TELEGRAM_PROMO_URL:
        buttons.append(
            [InlineKeyboardButton(t(lang, "promo_button"), url=config.TELEGRAM_PROMO_URL)]
        )
    return InlineKeyboardMarkup(buttons)


def game_promo_keyboard(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton(t(lang, "play_game_button"), callback_data="game:menu")]]
    )
