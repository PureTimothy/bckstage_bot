import logging
import sqlite3
import json
from datetime import date, datetime, timezone, timedelta
import random
from typing import Optional
import time

from telegram import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaPhoto,
    InputMediaVideo,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    KeyboardButton,
    Update,
)
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    ConversationHandler,
    MessageHandler,
    filters,
)

import config
import db
import game
from helpers import is_admin, lang_for_user, record_user, split_name, target_gender_for_voter
from helpers import normalize_city, reverse_geocode_city, looks_like_coord, parse_lat_lon
from keyboards import game_promo_keyboard, language_keyboard, paused_keyboard, start_voting_keyboard
from translations import LANGUAGE_OPTIONS, t

logger = logging.getLogger(__name__)


def _fmt_santa_number(number: int) -> str:
    """Format Secret Santa numbers as three digits (e.g., 001)."""
    try:
        return str(int(number)).zfill(3)
    except Exception:
        return str(number)


async def send_language_prompt(update_or_query):
    user_id = (
        update_or_query.effective_user.id
        if isinstance(update_or_query, Update)
        else update_or_query.from_user.id
    )
    lang = lang_for_user(user_id)
    prompt = t(lang, "language_prompt")
    await update_or_query.message.reply_text(prompt, reply_markup=language_keyboard())


async def _remove_reply_keyboard(user_id: int, bot) -> None:
    """Send a blank message with ReplyKeyboardRemove to clear custom keyboards."""
    try:
        await bot.send_message(
            chat_id=user_id,
            text=" ",
            reply_markup=ReplyKeyboardRemove(),
            disable_notification=True,
        )
    except Exception:
        pass


async def _send_help_keyboard(user_id: int, lang: str, bot) -> None:
    """Show quick-help and menu buttons below the input field."""
    try:
        await bot.send_message(
            chat_id=user_id,
            text=" ",
            reply_markup=ReplyKeyboardMarkup(
                [[t(lang, "quick_help_btn"), t(lang, "quick_menu_btn")]],
                resize_keyboard=True,
                one_time_keyboard=False,
            ),
            disable_notification=True,
        )
    except Exception:
        pass


async def send_start_message(update_or_query, lang: str, user_id: int):
    if not lang:
        await send_language_prompt(update_or_query)
        return
    bot = update_or_query.get_bot() if hasattr(update_or_query, "get_bot") else None
    if bot:
        await _remove_reply_keyboard(user_id, bot)
        await _send_help_keyboard(user_id, lang, bot)
    mode = db.get_mode()
    voting_on = db.is_feature_enabled("voting")
    if mode == "collect":
        text = t(lang, "start_collect")
        rows = [
            [
                InlineKeyboardButton(
                    t(lang, "start_application_button"),
                    callback_data="start:submit",
                )
            ],
        ]
        if db.is_feature_enabled("santa"):
            rows.append([InlineKeyboardButton(t(lang, "santa_menu_button"), callback_data="santa:start")])
        if db.is_feature_enabled("finder"):
            rows.append([InlineKeyboardButton(t(lang, "tinder_find_button"), callback_data="tinder:start")])
        if db.is_feature_enabled("fun"):
            rows.append([InlineKeyboardButton(t(lang, "play_game_button"), callback_data="game:menu")])
        if config.TELEGRAM_PROMO_URL:
            rows.append([InlineKeyboardButton(t(lang, "promo_button"), url=config.TELEGRAM_PROMO_URL)])
        keyboard = InlineKeyboardMarkup(rows)
    elif mode == "vote":
        text = t(lang, "start_vote") if voting_on else t(lang, "start_paused")
        keyboard = start_voting_keyboard(lang)
    elif mode == "disabled":
        text = t(lang, "start_disabled")
        rows = []
        if db.is_feature_enabled("santa"):
            rows.append([InlineKeyboardButton(t(lang, "santa_menu_button"), callback_data="santa:start")])
        if db.is_feature_enabled("finder"):
            rows.append([InlineKeyboardButton(t(lang, "tinder_find_button"), callback_data="tinder:start")])
        if db.is_feature_enabled("fun"):
            rows.append([InlineKeyboardButton(t(lang, "play_game_button"), callback_data="game:menu")])
        if config.TELEGRAM_PROMO_URL:
            rows.append([InlineKeyboardButton(t(lang, "promo_button"), url=config.TELEGRAM_PROMO_URL)])
        keyboard = InlineKeyboardMarkup(rows) if rows else None
    else:
        # paused or any other
        text = t(lang, "start_paused")
        keyboard = paused_keyboard(lang)

    await update_or_query.message.reply_text(text, reply_markup=keyboard)


def clear_santa_state(context) -> None:
    context.user_data.pop("santa_active", None)
    context.user_data.pop("santa_name", None)
    context.user_data.pop("santa_insta", None)
    context.user_data.pop("santa_existing_number", None)


def clear_transient_state(context) -> None:
    if context is None:
        return
    # Clear any pending blackjack bet prompt or Santa flow flags
    context.user_data.pop("bj_pending", None)
    clear_santa_state(context)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = ensure_user_context(update)
    clear_transient_state(context)
    # Referral tracking via /start ref<id>
    if context.args:
        arg = context.args[0]
        if arg.startswith("ref"):
            try:
                referrer_id = int(arg.replace("ref", ""))
                db.set_referrer(user_id, referrer_id)
            except ValueError:
                pass
    lang = db.get_user_language(user_id)
    if not lang:
        await send_language_prompt(update)
        return
    await deliver_pending_gifts(user_id, update.effective_user.username if update.effective_user else None, context)
    await send_start_message(update, lang, user_id)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = ensure_user_context(update)
    lang = lang_for_user(user_id)

    if is_admin(user_id):
        text = t(lang, "help_admin")
    else:
        text = t(lang, "help_user")
    await update.message.reply_text(text)
    if config.SUPPORT_USERNAME:
        btn = InlineKeyboardMarkup(
            [[InlineKeyboardButton(t(lang, "contact_admin_button"), url=f"https://t.me/{config.SUPPORT_USERNAME}")]]
        )
        await update.message.reply_text(" ", reply_markup=btn)


async def language_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = ensure_user_context(update)
    await deliver_pending_gifts(user_id, update.effective_user.username if update.effective_user else None, context)
    await send_language_prompt(update)


async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = ensure_user_context(update)
    clear_transient_state(context)
    lang = lang_for_user(user_id)
    if not lang:
        await send_language_prompt(update)
        return
    await deliver_pending_gifts(user_id, update.effective_user.username if update.effective_user else None, context)
    try:
        if update.message:
            await update.message.reply_text(" ", reply_markup=ReplyKeyboardRemove())
    except Exception:
        pass
    await send_start_message(update, lang, user_id)


async def submit_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    clear_transient_state(context)
    user_id = ensure_user_context(update)
    lang = lang_for_user(user_id)
    mode = db.get_mode()
    if mode != "collect":
        if mode == "vote":
            keyboard = start_voting_keyboard(lang)
            await update.message.reply_text(
                t(lang, "applications_closed"), reply_markup=keyboard
            )
        else:
            keyboard = paused_keyboard(lang)
            await update.message.reply_text(
                t(lang, "applications_closed_review"), reply_markup=keyboard
            )
        return ConversationHandler.END

    profile = db.get_profile(user_id)
    if not profile:
        btn = InlineKeyboardMarkup(
            [[InlineKeyboardButton(t(lang, "tinder_find_button"), callback_data="tinder:start")]]
        )
        await update.message.reply_text(t(lang, "submit_need_profile"), reply_markup=btn)
        return ConversationHandler.END
    if db.has_candidate_for_user(user_id):
        buttons = [
            [InlineKeyboardButton(t(lang, "submit_menu"), callback_data="submit:menu")],
            [InlineKeyboardButton(t(lang, "submit_withdraw"), callback_data="submit:withdraw")],
        ]
        await update.message.reply_text(t(lang, "submit_already"), reply_markup=InlineKeyboardMarkup(buttons))
        return ConversationHandler.END

    buttons = [
        [InlineKeyboardButton(t(lang, "submit_use_profile"), callback_data="submit:useprofile")],
        [InlineKeyboardButton(t(lang, "submit_edit_profile"), callback_data="submit:editprofile")],
    ]
    await update.message.reply_text(t(lang, "submit_choose_profile"), reply_markup=InlineKeyboardMarkup(buttons))
    return ConversationHandler.END


async def submit_start_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    clear_transient_state(context)
    user_id = ensure_user_context(query)
    lang = lang_for_user(user_id)
    mode = db.get_mode()
    if mode != "collect":
        if mode == "vote":
            keyboard = start_voting_keyboard(lang)
            await query.message.reply_text(
                t(lang, "applications_closed"), reply_markup=keyboard
            )
        else:
            keyboard = paused_keyboard(lang)
            await query.message.reply_text(
                t(lang, "applications_closed_review"), reply_markup=keyboard
            )
        return ConversationHandler.END

    profile = db.get_profile(user_id)
    if not profile:
        btn = InlineKeyboardMarkup(
            [[InlineKeyboardButton(t(lang, "tinder_find_button"), callback_data="tinder:start")]]
        )
        await query.message.reply_text(t(lang, "submit_need_profile"), reply_markup=btn)
        return ConversationHandler.END
    if db.has_candidate_for_user(user_id):
        buttons = [
            [InlineKeyboardButton(t(lang, "submit_menu"), callback_data="submit:menu")],
            [InlineKeyboardButton(t(lang, "submit_withdraw"), callback_data="submit:withdraw")],
        ]
        await query.message.reply_text(t(lang, "submit_already"), reply_markup=InlineKeyboardMarkup(buttons))
        return ConversationHandler.END

    buttons = [
        [InlineKeyboardButton(t(lang, "submit_use_profile"), callback_data="submit:useprofile")],
        [InlineKeyboardButton(t(lang, "submit_edit_profile"), callback_data="submit:editprofile")],
    ]
    await query.message.reply_text(t(lang, "submit_choose_profile"), reply_markup=InlineKeyboardMarkup(buttons))
    return ConversationHandler.END


async def submit_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = lang_for_user(update.effective_user.id)
    full_name = update.message.text.strip()
    first, last = split_name(full_name)
    if not last:
        await update.message.reply_text(t(lang, "name_need_last"))
        return config.ENTER_NAME
    context.user_data["name"] = f"{first} {last}"
    context.user_data["first_name_only"] = first
    await update.message.reply_text(t(lang, "ask_age"))
    return config.ENTER_AGE


async def submit_age(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = lang_for_user(update.effective_user.id)
    text = update.message.text.strip()
    if not text.isdigit():
        await update.message.reply_text(t(lang, "age_not_number"))
        return config.ENTER_AGE
    age = int(text)
    if age < 5 or age > 120:
        await update.message.reply_text(t(lang, "age_invalid"))
        return config.ENTER_AGE
    context.user_data["age"] = age

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    f"👦 {t(lang, 'gender_male')}", callback_data="gender:Male"
                ),
                InlineKeyboardButton(
                    f"👧 {t(lang, 'gender_female')}", callback_data="gender:Female"
                ),
            ],
        ]
    )
    await update.message.reply_text(
        t(lang, "ask_gender"),
        reply_markup=keyboard,
    )
    return config.ENTER_GENDER


async def gender_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    _, gender_value = query.data.split(":", 1)
    context.user_data["gender"] = gender_value
    lang = lang_for_user(update.effective_user.id)

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    f"✏️ {t(lang, 'insta_type_button')}", callback_data="insta:type"
                ),
            ],
            [
                InlineKeyboardButton(
                    f"❌ {t(lang, 'insta_skip_button')}", callback_data="insta:skip"
                ),
            ],
        ]
    )
    await query.message.reply_text(
        t(lang, "ask_instagram"),
        reply_markup=keyboard,
    )
    return config.ENTER_INSTAGRAM


async def submit_gender_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = lang_for_user(update.effective_user.id)
    gender_text = update.message.text.strip().lower()
    if gender_text in ["male", "мужчина", "m", "man", "чоловік"]:
        context.user_data["gender"] = "Male"
    elif gender_text in ["female", "женщина", "f", "woman", "жінка"]:
        context.user_data["gender"] = "Female"
    else:
        await update.message.reply_text(t(lang, "gender_invalid_choice"))
        return config.ENTER_GENDER
    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    f"✏️ {t(lang, 'insta_type_button')}", callback_data="insta:type"
                ),
            ],
            [
                InlineKeyboardButton(
                    f"❌ {t(lang, 'insta_skip_button')}", callback_data="insta:skip"
                ),
            ],
        ]
    )
    await update.message.reply_text(
        t(lang, "ask_instagram"),
        reply_markup=keyboard,
    )
    return config.ENTER_INSTAGRAM


async def insta_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data
    lang = lang_for_user(update.effective_user.id)

    if data == "insta:skip":
        context.user_data["instagram"] = None
        await query.message.reply_text(t(lang, "ask_photo"))
        return config.ENTER_PHOTO

    if data == "insta:type":
        await query.message.reply_text(t(lang, "insta_type_prompt"))
        return config.ENTER_INSTAGRAM


async def submit_instagram(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = lang_for_user(update.effective_user.id)
    text = update.message.text.strip()
    if text.lower() in ["skip", "no", "none", "❌"]:
        context.user_data["instagram"] = None
    else:
        context.user_data["instagram"] = text

    await update.message.reply_text(t(lang, "ask_photo"))
    return config.ENTER_PHOTO


async def submit_photo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = lang_for_user(update.effective_user.id)
    if not update.message.photo:
        await update.message.reply_text(t(lang, "not_photo"))
        return config.ENTER_PHOTO

    photo = update.message.photo[-1]
    file_id = photo.file_id

    user_id = update.effective_user.id
    name = context.user_data.get("name")
    age = context.user_data.get("age")
    gender = context.user_data.get("gender")
    instagram = context.user_data.get("instagram")

    db.add_candidate(user_id, name, age, gender, instagram, file_id)

    await update.message.reply_text(t(lang, "thank_you"))
    await send_start_message(update, lang, user_id)
    context.user_data.clear()
    return ConversationHandler.END


async def submit_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    lang = lang_for_user(update.effective_user.id)
    await update.message.reply_text(t(lang, "submission_cancel"))
    return ConversationHandler.END


async def ensure_candidate_from_profile(update_or_query, user_id: int, lang: str) -> bool:
    """Create/update candidate from finder profile; returns True if queued."""
    profile = db.get_profile(user_id)
    if not profile:
        return False
    _, age, gender, interest, city, norm_city, lat, lon, name, bio, active, *_ = profile
    media = db.list_profile_media(user_id)
    if not media:
        await _respond(update_or_query, t(lang, "submit_profile_missing_media"))
        return False
    file_id, kind = media[0]
    db.upsert_candidate_from_profile(user_id, name or "—", age or 0, gender or "", file_id)
    return True


async def ensure_candidate_from_contest(user_id: int, context: ContextTypes.DEFAULT_TYPE, lang: str, update_or_query) -> bool:
    draft = context.user_data.get("contest_profile")
    if not draft:
        return False
    # Draft may include extra timestamps; ignore tail
    _, age, gender, interest, city, norm_city, lat, lon, name, bio, active, *rest = draft
    media = context.user_data.get("contest_media") or db.list_profile_media(user_id)
    if not media:
        await _respond(update_or_query, t(lang, "submit_profile_missing_media"))
        return False
    file_id, kind = media[0]
    db.upsert_candidate_from_profile(user_id, name or "—", age or 0, gender or "", file_id)
    return True


async def wipe_profile_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_user_context(update)
    if not is_admin(update.effective_user.id):
        return
    lang = lang_for_user(update.effective_user.id)
    target_id = update.effective_user.id
    if context.args:
        try:
            target_id = int(context.args[0])
        except ValueError:
            await update.message.reply_text(t(lang, "user_id_must_be_number"))
            return
    db.delete_user_completely(target_id)
    await update.message.reply_text(t(lang, "profile_wiped", user_id=target_id))


async def mode_collect(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_user_context(update)
    if not is_admin(update.effective_user.id):
        return
    db.set_mode("collect")
    lang = lang_for_user(update.effective_user.id)
    await update.message.reply_text(t(lang, "mode_collect_set"))


async def mode_vote(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_user_context(update)
    if not is_admin(update.effective_user.id):
        return
    db.set_mode("vote")
    lang = lang_for_user(update.effective_user.id)
    await update.message.reply_text(t(lang, "mode_vote_set"))


async def mode_pause(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_user_context(update)
    if not is_admin(update.effective_user.id):
        return
    db.set_mode("paused")
    lang = lang_for_user(update.effective_user.id)
    await update.message.reply_text(t(lang, "mode_pause_set"))


async def mode_disable(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_user_context(update)
    if not is_admin(update.effective_user.id):
        return
    db.set_mode("disabled")
    lang = lang_for_user(update.effective_user.id)
    await update.message.reply_text(t(lang, "mode_disable_set"))


async def set_persistent_commands(application: Application) -> None:
    try:
        from telegram import BotCommandScopeAllChatAdministrators
    except Exception:
        BotCommandScopeAllChatAdministrators = None

    for lang in LANGUAGE_OPTIONS:
        default_commands = [
            ("menu", t(lang, "cmd_menu")),     # single entry point
            ("fun", t(lang, "cmd_fun")),       # mini games near top
            ("find", t(lang, "cmd_find")),     # finder flow
            ("likes", t(lang, "cmd_likes")),   # check likes
            ("profile", t(lang, "cmd_profile")),  # quick my profile
            ("help", t(lang, "cmd_help")),
            ("language", t(lang, "cmd_language")),
        ]
        admin_commands = default_commands + [
            ("mode_disable", t(lang, "cmd_disable")),
            ("mode_pause", t(lang, "cmd_pause")),
            ("say", t(lang, "cmd_say")),
            ("edit", t(lang, "cmd_edit")),
            ("givepoints", t(lang, "cmd_givepoints")),
            ("list_users", t(lang, "cmd_list_users")),
            ("user", t(lang, "cmd_user")),
            ("reset_shows", t(lang, "cmd_reset_shows")),
            ("reset_shows_all", t(lang, "cmd_reset_shows")),
            ("review_profiles", t(lang, "cmd_review_profiles")),
            ("wipe_profiles", t(lang, "cmd_wipe_profiles")),
            ("wipe_profile", t(lang, "cmd_wipe_profile")),
            ("sync_boosts", t(lang, "cmd_sync_boosts")),
        ]
        try:
            await application.bot.set_my_commands(
                commands=[(cmd, desc) for cmd, desc in default_commands],
                language_code=lang,
            )
            if BotCommandScopeAllChatAdministrators:
                await application.bot.set_my_commands(
                    commands=[(cmd, desc) for cmd, desc in admin_commands],
                    scope=BotCommandScopeAllChatAdministrators(),
                    language_code=lang,
                )
        except Exception as e:
            logger.warning("Failed to set commands for lang %s: %s", lang, e)


async def list_candidates(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_user_context(update)
    if not is_admin(update.effective_user.id):
        return
    lang = lang_for_user(update.effective_user.id)

    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT id, name, approved FROM candidates ORDER BY id")
    rows = cur.fetchall()
    conn.close()

    if not rows:
        await update.message.reply_text(t(lang, "no_candidates"))
        return

    def status_text(v: int) -> str:
        if v == 1:
            return t(lang, "status_approved")
        if v == -1:
            return t(lang, "status_rejected")
        return t(lang, "status_pending")

    lines = [f"{cid}. {name} ({status_text(approved)})" for cid, name, approved in rows]
    await update.message.reply_text("\n".join(lines))


def user_nav_keyboard(uid: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("⬅️", callback_data=f"usernav:{uid}:prev"),
                InlineKeyboardButton("➡️", callback_data=f"usernav:{uid}:next"),
            ]
        ]
    )


def user_action_keyboard(uid: int, lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(t(lang, "user_btn_plus1"), callback_data=f"useract:{uid}:bal:+1"),
                InlineKeyboardButton(t(lang, "user_btn_minus1"), callback_data=f"useract:{uid}:bal:-1"),
            ],
            [
                InlineKeyboardButton(t(lang, "user_btn_set_male"), callback_data=f"useract:{uid}:gender:Male"),
                InlineKeyboardButton(t(lang, "user_btn_set_female"), callback_data=f"useract:{uid}:gender:Female"),
            ],
            [
                InlineKeyboardButton(t(lang, "user_btn_reset_votes"), callback_data=f"useract:{uid}:resetvotes"),
            ],
            [
                InlineKeyboardButton("⬅️", callback_data=f"usernav:{uid}:prev"),
                InlineKeyboardButton("➡️", callback_data=f"usernav:{uid}:next"),
            ],
        ]
    )


def render_user_profile(uid: int, lang: str):
    user_row = db.get_user(uid)
    if not user_row:
        return t(lang, "user_not_found", uid=uid), None
    user_id, local_id, username, first_name, last_name, lang_code, created, last_seen = user_row
    stats = db.get_user_stats(uid)
    balance = db.get_balance(uid)
    gender = db.get_user_gender(uid) or "—"
    name = " ".join([p for p in [first_name, last_name] if p]) or "—"
    username_display = username if username else "—"
    lines = [
        t(lang, "user_profile_title", uid=user_id),
        t(lang, "user_internal_id", local_id=local_id or "—"),
        t(lang, "user_username", username=username_display),
        t(lang, "user_name", name=name),
        t(lang, "user_lang", lang_value=lang_code or "—"),
        t(lang, "user_wallet", balance=balance),
        t(lang, "user_gender", gender=gender),
        t(lang, "user_votes", votes=stats["votes"]),
        t(lang, "user_candidates", candidates=stats["candidates"]),
        t(lang, "user_referrals", referrals=stats.get("referrals", 0)),
        t(lang, "user_referral_rewards", rewards=stats.get("referral_rewards", 0)),
        t(lang, "user_boosts", boosts=stats.get("boosts_credited", 0)),
        t(
            lang,
            "user_profiles_stats",
            created=stats.get("profiles_created", 0),
            updated=stats.get("profiles_updated", 0),
        ),
        t(
            lang,
            "user_swipe_stats",
            swipes=stats.get("swipes", 0),
            likes=stats.get("likes", 0),
            matches=stats.get("matches", 0),
        ),
        t(
            lang,
            "user_shop_stats",
            purchases=stats.get("purchases", 0),
            gifts_sent=stats.get("gifts_sent", 0),
            gifts_received=stats.get("gifts_received", 0),
        ),
        t(lang, "user_games_played", games=stats.get("games_played", 0)),
        t(lang, "user_created", created=format_mt(created)),
        t(lang, "user_last_seen", last_seen=format_mt(last_seen)),
    ]
    return "\n".join(lines), user_action_keyboard(uid, lang)


async def list_users_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_user_context(update)
    if not is_admin(update.effective_user.id):
        return
    lang = lang_for_user(update.effective_user.id)
    rows = db.list_users(limit=50)
    if not rows:
        await update.message.reply_text(t(lang, "user_not_found", uid="—"))
        return
    lines = [t(lang, "user_list_title")]
    for uid, username, *_rest, last_seen in rows:
        lines.append(
            t(
                lang,
                "user_line",
                uid=uid,
                username=username or "—",
                last_seen=format_mt(last_seen),
            )
        )
    await update.message.reply_text("\n".join(lines))


async def user_info_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_user_context(update)
    if not is_admin(update.effective_user.id):
        return
    lang = lang_for_user(update.effective_user.id)
    if not context.args:
        await update.message.reply_text(t(lang, "user_usage"))
        return
    try:
        uid = int(context.args[0])
    except ValueError:
        await update.message.reply_text(t(lang, "user_usage"))
        return
    text, kb = render_user_profile(uid, lang)
    await update.message.reply_text(text, reply_markup=kb)


def build_admin_keyboard(cid: int, approved: int) -> InlineKeyboardMarkup:
    keyboard = [
        [
            InlineKeyboardButton("⬅️", callback_data=f"admin:{cid}:prev"),
            InlineKeyboardButton(
                "✅" if approved != 1 else "✅ (approved)",
                callback_data=f"admin:{cid}:approve",
            ),
            InlineKeyboardButton(
                "❌" if approved != -1 else "❌ (rejected)",
                callback_data=f"admin:{cid}:reject",
            ),
            InlineKeyboardButton("➡️", callback_data=f"admin:{cid}:next"),
        ]
    ]
    return InlineKeyboardMarkup(keyboard)


async def send_admin_candidate(update_or_query, candidate_row):
    user_id = (
        update_or_query.effective_user.id
        if isinstance(update_or_query, Update)
        else update_or_query.from_user.id
    )
    lang = lang_for_user(user_id)
    cid, name, age, gender, instagram, photo_id, approved = candidate_row
    status_text = (
        f"✅ {t(lang, 'status_approved')}"
        if approved == 1
        else f"❌ {t(lang, 'status_rejected')}"
        if approved == -1
        else f"⏳ {t(lang, 'status_pending')}"
    )
    caption_lines = [
        f"ID: {cid}",
        t(lang, "label_name", name=name),
        t(lang, "label_age", age=age),
        t(lang, "label_gender", gender=gender),
        t(lang, "label_instagram", instagram=instagram if instagram else "—"),
        t(lang, "label_status", status=status_text),
    ]
    keyboard = build_admin_keyboard(cid, approved)

    if isinstance(update_or_query, Update):
        await update_or_query.message.reply_photo(
            photo=photo_id,
            caption="\n".join(caption_lines),
            reply_markup=keyboard,
        )
    else:
        await update_or_query.edit_message_media(
            media=InputMediaPhoto(
                media=photo_id,
                caption="\n".join(caption_lines),
            ),
            reply_markup=keyboard,
        )


async def review(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_user_context(update)
    if not is_admin(update.effective_user.id):
        return
    lang = lang_for_user(update.effective_user.id)
    row = db.get_first_candidate()
    if not row:
        await update.message.reply_text(t(lang, "no_candidates_review"))
        return
    await send_admin_candidate(update, row)


async def show_candidate_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_user_context(update)
    if not is_admin(update.effective_user.id):
        return
    lang = lang_for_user(update.effective_user.id)
    if not context.args:
        await update.message.reply_text(t(lang, "usage_show"))
        return
    try:
        cid = int(context.args[0])
    except ValueError:
        await update.message.reply_text(t(lang, "candidate_id_must_be_number"))
        return
    row = db.get_candidate_by_id(cid)
    if not row:
        await update.message.reply_text(t(lang, "no_candidate_with_id", cid=cid))
        return
    await send_admin_candidate(update, row)


async def approve_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_user_context(update)
    if not is_admin(update.effective_user.id):
        return
    lang = lang_for_user(update.effective_user.id)
    if not context.args:
        await update.message.reply_text(t(lang, "usage_approve"))
        return
    try:
        cid = int(context.args[0])
    except ValueError:
        await update.message.reply_text(t(lang, "candidate_id_must_be_number"))
        return
    db.approve_candidate(cid, 1)
    await update.message.reply_text(t(lang, "candidate_approved", cid=cid))


async def reject_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_user_context(update)
    if not is_admin(update.effective_user.id):
        return
    lang = lang_for_user(update.effective_user.id)
    if not context.args:
        await update.message.reply_text(t(lang, "usage_reject"))
        return
    try:
        cid = int(context.args[0])
    except ValueError:
        await update.message.reply_text(t(lang, "candidate_id_must_be_number"))
        return
    db.approve_candidate(cid, -1)
    await update.message.reply_text(t(lang, "candidate_rejected", cid=cid))


async def reset_votes_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_user_context(update)
    if not is_admin(update.effective_user.id):
        return

    target_user_id = update.effective_user.id
    if context.args:
        try:
            target_user_id = int(context.args[0])
        except ValueError:
            lang = lang_for_user(update.effective_user.id)
            await update.message.reply_text(t(lang, "user_id_must_be_number"))
            return

    db.reset_votes_for_user(target_user_id)
    lang = lang_for_user(update.effective_user.id)
    await update.message.reply_text(t(lang, "votes_reset_user", user_id=target_user_id))


async def reset_votes_admin_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_user_context(update)
    if not is_admin(update.effective_user.id):
        return
    db.reset_votes_for_user(update.effective_user.id)
    lang = lang_for_user(update.effective_user.id)
    await update.message.reply_text(t(lang, "admin_votes_reset"))


async def guest_mode_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin: toggle guest mode as a phantom user (fresh experience without deleting real data)."""
    ensure_user_context(update)
    if update.effective_user.id != config.ADMIN_ID:
        return
    # Toggle off if already in guest mode
    if config.ADMIN_GUEST_MODE:
        config.ADMIN_GUEST_MODE = False
        config.GUEST_PHANTOM_ID = None
        lang = lang_for_user(update.effective_user.id)
        await update.message.reply_text(t(lang, "guest_disabled"))
        return

    # Enter guest mode with fresh phantom id
    config.ADMIN_GUEST_MODE = True
    phantom_id = int(time.time())
    config.GUEST_PHANTOM_ID = phantom_id
    db.delete_full_profile(phantom_id)
    db.upsert_user_basic(
        user_id=phantom_id,
        username=f"{update.effective_user.username or 'guest'}_ph",
        first_name=update.effective_user.first_name,
        last_name=update.effective_user.last_name,
        language_code=update.effective_user.language_code,
    )
    db.add_phantom_user(phantom_id)
    lang = lang_for_user(phantom_id)
    await update.message.reply_text(t(lang, "guest_enabled"))
    await send_language_prompt(update)


async def wipe_phantom_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_user_context(update)
    if update.effective_user.id != config.ADMIN_ID:
        return
    lang = lang_for_user(update.effective_user.id)
    if not context.args or context.args[0].upper() != "CONFIRM":
        await update.message.reply_text(t(lang, "wipe_phantom_confirm"))
        return
    db.delete_phantom_users()
    config.GUEST_PHANTOM_ID = None
    config.ADMIN_GUEST_MODE = False
    await update.message.reply_text(t(lang, "profile_wiped", user_id="phantom"))


def game_menu_markup(lang: str, user_id: int) -> InlineKeyboardMarkup:
    buttons = []
    if db.get_last_checkin(user_id) != date.today().isoformat():
        buttons.append([InlineKeyboardButton(t(lang, "checkin_button"), callback_data="game:checkin")])
    buttons.append([InlineKeyboardButton(t(lang, "earn_button"), callback_data="game:earn")])
    buttons.append([InlineKeyboardButton(t(lang, "shop_button"), callback_data="game:shop")])
    buttons.append([InlineKeyboardButton(t(lang, "balance_button"), callback_data="game:balance")])
    buttons.append([InlineKeyboardButton(t(lang, "back_main_menu"), callback_data="game:home")])
    return InlineKeyboardMarkup(buttons)


async def send_game_menu(update_or_query) -> None:
    user_id = ensure_user_context(update_or_query)
    lang = lang_for_user(user_id)
    if not db.is_feature_enabled("fun"):
        await _respond(update_or_query, t(lang, "feature_disabled"))
        return
    bot = update_or_query.get_bot() if hasattr(update_or_query, "get_bot") else None
    if bot:
        await _remove_reply_keyboard(user_id, bot)
        await _send_help_keyboard(user_id, lang, bot)
    await _respond(
        update_or_query, t(lang, "game_menu_title"), reply_markup=game_menu_markup(lang, user_id)
    )


async def fun_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not db.is_feature_enabled("fun"):
        await _respond(update, t(lang_for_user(update.effective_user.id), "feature_disabled"))
        return
    clear_transient_state(context)
    await send_game_menu(update)


async def _respond(update_or_query, text: str, reply_markup=None):
    """Reply helper that works for messages or callback queries; prefers sending a new message."""
    if isinstance(update_or_query, Update):
        if update_or_query.callback_query:
            q = update_or_query.callback_query
            try:
                await q.answer()
            except Exception:
                pass
            if q.message:
                return await q.message.reply_text(text, reply_markup=reply_markup)
        if update_or_query.message:
            return await update_or_query.message.reply_text(text, reply_markup=reply_markup)
        if update_or_query.effective_user:
            return await update_or_query.get_bot().send_message(
                chat_id=update_or_query.effective_user.id, text=text, reply_markup=reply_markup
            )
    else:
        # CallbackQuery passed directly
        try:
            await update_or_query.answer()
        except Exception:
            pass
        if update_or_query.message:
            return await update_or_query.message.reply_text(text, reply_markup=reply_markup)
        if update_or_query.from_user:
            return await update_or_query.get_bot().send_message(
                chat_id=update_or_query.from_user.id, text=text, reply_markup=reply_markup
            )


def ensure_user_context(update_or_query) -> int:
    """Ensure user is recorded; return user_id."""
    user = update_or_query.effective_user if isinstance(update_or_query, Update) else update_or_query.from_user
    if not user:
        return 0
    mapped_id = user.id
    if user.id == config.ADMIN_ID and config.ADMIN_GUEST_MODE and config.GUEST_PHANTOM_ID:
        mapped_id = config.GUEST_PHANTOM_ID
    db.upsert_user_basic(
        user_id=mapped_id,
        username=user.username,
        first_name=user.first_name,
        last_name=user.last_name,
        language_code=user.language_code,
    )
    if not db.get_user_language(mapped_id) and user.language_code in LANGUAGE_OPTIONS:
        db.set_user_language(mapped_id, user.language_code)
    return mapped_id


# ---------- Tinder-style profiles ----------


def interest_label(value: str, lang: str) -> str:
    if value == "Male":
        return t(lang, "tinder_interest_male")
    if value == "Female":
        return t(lang, "tinder_interest_female")
    return t(lang, "tinder_interest_any")


def render_profile_text(profile_row, lang: str, show_interest: bool = False) -> str:
    user_id, age, gender, interest, city, normalized_city, lat, lon, name, bio, active, *_ = profile_row

    def city_label(city_val, lat_val, lon_val):
        if city_val and any(ch.isalpha() for ch in city_val):
            return city_val
        if city_val and isinstance(city_val, str) and city_val.startswith("geo:"):
            parts = city_val.split(":")
            if len(parts) >= 3:
                try:
                    lat_val = float(parts[1])
                    lon_val = float(parts[2])
                except Exception:
                    pass
        if (lat_val is None or lon_val is None) and city_val and looks_like_coord(str(city_val)):
            parsed_lat, parsed_lon = parse_lat_lon(str(city_val))
            if parsed_lat is not None and parsed_lon is not None:
                lat_val, lon_val = parsed_lat, parsed_lon
        if lat_val is not None and lon_val is not None:
            label, _ = reverse_geocode_city(lat_val, lon_val)
            if label:
                return label
        return city_val or "—"

    # Fix name for legacy records that stored coords/empty
    if not name or looks_like_coord(str(name)):
        basic = db.get_user_basic(user_id)
        if basic:
            username, first_name, last_name = basic
            if first_name or last_name:
                name = " ".join([p for p in [first_name, last_name] if p])
            elif username:
                name = f"@{username}" if not str(username).startswith("@") else str(username)
            else:
                name = "—"
    # Hide bad bio coords
    if bio and looks_like_coord(str(bio)):
        bio = "—"

    city_display = city_label(city, lat, lon)

    # Persist repairs if something changed
    repaired = False
    if city_display != (city or "—"):
        city = city_display
        normalized_city = normalize_city(city_display)
        repaired = True
    if not name or looks_like_coord(str(name)):
        repaired = True
    if bio and looks_like_coord(str(bio)):
        repaired = True
    if repaired:
        db.upsert_profile(
            user_id,
            age,
            gender,
            interest,
            city,
            lat,
            lon,
            name,
            bio,
            normalized_city or normalize_city(city_display),
        )

    base = t(
        lang,
        "tinder_profile_line",
        name=name or "—",
        age=age,
        gender=gender,
        city=city_display,
        interest=interest_label(interest, lang),
        bio=bio or "—",
    )
    if show_interest:
        base += "\n" + t(lang, "tinder_interest_label", interest=interest_label(interest, lang))
    return base


def format_username(username, fallback_id):
    if not username:
        return str(fallback_id)
    return username if username.startswith("@") else f"@{username}"


# ----- Time helpers -----
MT_TZ = timezone(timedelta(hours=-7))


def format_mt(ts: str) -> str:
    """Format ISO timestamp string into Mountain Time readable label."""
    if not ts:
        return "—"
    try:
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(MT_TZ).strftime("%Y-%m-%d %H:%M MT")
    except Exception:
        return ts


async def send_own_profile(update_or_query, user_id: int, lang: str):
    profile = db.get_profile(user_id)
    if not profile:
        await _respond(update_or_query, t(lang, "tinder_profile_needed"))
        return False
    media = db.list_profile_media(user_id)
    text = render_profile_text(profile, lang, show_interest=True)
    buttons = [
        [InlineKeyboardButton(t(lang, "btn_browse"), callback_data="tinder:browse")],
        [InlineKeyboardButton(t(lang, "btn_edit"), callback_data="tinder:edit")],
        [InlineKeyboardButton(t(lang, "back_main_menu"), callback_data="game:home")],
    ]
    if media:
        fid, kind = media[0]
        try:
            if isinstance(update_or_query, Update) and update_or_query.message:
                if kind == "video":
                    await update_or_query.message.reply_video(video=fid, caption=text, reply_markup=InlineKeyboardMarkup(buttons))
                else:
                    await update_or_query.message.reply_photo(photo=fid, caption=text, reply_markup=InlineKeyboardMarkup(buttons))
            else:
                bot = update_or_query.get_bot()
                if kind == "video":
                    await bot.send_video(chat_id=user_id, video=fid, caption=text, reply_markup=InlineKeyboardMarkup(buttons))
                else:
                    await bot.send_photo(chat_id=user_id, photo=fid, caption=text, reply_markup=InlineKeyboardMarkup(buttons))
        except Exception:
            await _respond(update_or_query, text, reply_markup=InlineKeyboardMarkup(buttons))
    else:
        await _respond(update_or_query, text, reply_markup=InlineKeyboardMarkup(buttons))
    return True


async def send_admin_profile(update_or_query, profile_row, lang: str):
    if not profile_row:
        return
    uid, age, gender, interest, city, norm_city, lat, lon, name, bio, active, *_ = profile_row
    media = db.list_profile_media(uid)
    status = "✅ Visible" if active else "🙈 Hidden"
    caption = t(
        lang,
        "tinder_admin_profile_caption",
        uid=uid,
        name=name,
        age=age,
        gender=gender,
        city=city or "—",
        interest=interest_label(interest, lang),
        bio=bio or "—",
    )
    caption = f"{caption}\n{status}"
    kb = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("⬅️", callback_data=f"tadmin:{uid}:prev"),
                InlineKeyboardButton("✏️", callback_data=f"tadmineditmenu:{uid}"),
                InlineKeyboardButton("🙈" if active else "👁️", callback_data=f"tadmin:{uid}:toggle"),
                InlineKeyboardButton("➡️", callback_data=f"tadmin:{uid}:next"),
            ]
        ]
    )
    chat_id = (
        update_or_query.effective_chat.id
        if isinstance(update_or_query, Update)
        else update_or_query.message.chat_id if update_or_query.message else update_or_query.from_user.id
    )
    if media:
        fid, kind = media[0]
        try:
            if update_or_query.message and (update_or_query.message.photo or update_or_query.message.video):
                if kind == "video":
                    await update_or_query.edit_message_media(
                        media=InputMediaVideo(media=fid, caption=caption),
                        reply_markup=kb,
                    )
                else:
                    await update_or_query.edit_message_media(
                        media=InputMediaPhoto(media=fid, caption=caption),
                        reply_markup=kb,
                    )
                return
        except Exception:
            pass
        bot = update_or_query.get_bot() if not isinstance(update_or_query, Update) else update_or_query.get_bot()
        if kind == "video":
            await bot.send_video(chat_id=chat_id, video=fid, caption=caption, reply_markup=kb)
        else:
            await bot.send_photo(chat_id=chat_id, photo=fid, caption=caption, reply_markup=kb)
    else:
        # No media; try to edit caption if message exists, else send text
        try:
            if hasattr(update_or_query, "edit_message_caption"):
                await update_or_query.edit_message_caption(caption=caption, reply_markup=kb)
                return
        except Exception:
            pass
        await _respond(update_or_query, caption, reply_markup=kb)


async def admin_edit_menu(update_or_query, target_id: int, lang: str):
    kb = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🎂", callback_data=f"tadminedit:{target_id}:age"),
                InlineKeyboardButton("🚻", callback_data=f"tadminedit:{target_id}:gender"),
                InlineKeyboardButton("🧭", callback_data=f"tadminedit:{target_id}:interest"),
            ],
            [
                InlineKeyboardButton("📍", callback_data=f"tadminedit:{target_id}:city"),
                InlineKeyboardButton("📝", callback_data=f"tadminedit:{target_id}:name"),
                InlineKeyboardButton("✨", callback_data=f"tadminedit:{target_id}:bio"),
            ],
            [
                InlineKeyboardButton("⬅️", callback_data=f"tadmin:{target_id}:show"),
            ],
        ]
    )
    await _respond(update_or_query, t(lang, "tinder_edit_prompt"), reply_markup=kb)


async def tinder_admin_review(update_or_query, context: ContextTypes.DEFAULT_TYPE):
    user_id = ensure_user_context(update_or_query)
    if not is_admin(user_id):
        return
    lang = lang_for_user(user_id)
    rows = db.list_tinder_profiles(limit=1)
    if not rows:
        await _respond(update_or_query, t(lang, "tinder_admin_no_profiles"))
        return
    uid, age, gender, interest, city, lat, lon, norm_city, name, bio, active = rows[0]
    row = db.get_profile(uid)
    await send_admin_profile(update_or_query, row, lang)


async def admin_edit_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Capture admin edits for finder profiles."""
    user_id = ensure_user_context(update)
    if not is_admin(user_id):
        return ConversationHandler.END
    target_id = context.user_data.get("admin_edit_target")
    field = context.user_data.get("admin_edit_field")
    if not target_id or not field:
        return ConversationHandler.END
    lang = lang_for_user(user_id)
    text = update.message.text.strip()
    profile = db.get_profile(target_id)
    if not profile:
        await update.message.reply_text(t(lang, "tinder_admin_no_profiles"))
        return ConversationHandler.END
    _, age, gender, interest, city, norm_city, lat, lon, name, bio, active, *_ = profile

    try:
        if field == "age":
            age = int(text)
        elif field == "gender":
            low = text.lower()
            if "ж" in low or low.startswith("f"):
                gender = "Female"
            elif "м" in low or low.startswith("m"):
                gender = "Male"
        elif field == "interest":
            low = text.lower()
            if "ж" in low or low.startswith("f"):
                interest = "Female"
            elif "м" in low or low.startswith("m"):
                interest = "Male"
            else:
                interest = "Any"
        elif field == "city":
            city = text
            norm_city = normalize_city(city)
        elif field == "name":
            name = text
        elif field == "bio":
            bio = text
    except Exception:
        pass

    db.upsert_profile(target_id, age, gender, interest, city, lat, lon, name, bio, norm_city or normalize_city(city or ""))
    await update.message.reply_text(t(lang, "tinder_saved"))
    context.user_data.pop("admin_edit_target", None)
    context.user_data.pop("admin_edit_field", None)
    updated = db.get_profile(target_id)
    await send_admin_profile(update, updated, lang)
    return ConversationHandler.END


async def tinder_start_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Entry from inline button: show own profile if exists, else start flow."""
    user_id = ensure_user_context(update)
    lang = lang_for_user(user_id)
    profile = db.get_profile(user_id)
    if profile:
        await send_own_profile(update, user_id, lang)
        return ConversationHandler.END
    return await start_tinder_flow(update, context)


def load_profile_into_context(user_id: int, context: ContextTypes.DEFAULT_TYPE):
    profile = None
    if context.user_data.get("edit_for_submit") and context.user_data.get("contest_profile"):
        profile = context.user_data["contest_profile"]
    else:
        profile = db.get_profile(user_id)
    if not profile:
        return
    _, age, gender, interest, city, normalized_city, lat, lon, name, bio, *_ = profile
    context.user_data["tinder_age"] = age
    context.user_data["tinder_gender"] = gender
    context.user_data["tinder_interest"] = interest
    context.user_data["tinder_city"] = city
    context.user_data["tinder_normalized_city"] = normalized_city
    context.user_data["tinder_lat"] = lat
    context.user_data["tinder_lon"] = lon
    context.user_data["tinder_name"] = name
    context.user_data["tinder_bio"] = bio


def save_profile_from_context(user_id: int, context: ContextTypes.DEFAULT_TYPE):
    profile = None
    if context.user_data.get("edit_for_submit") and context.user_data.get("contest_profile"):
        profile = context.user_data.get("contest_profile")
    else:
        profile = db.get_profile(user_id)
    existing = {
        "age": profile[1] if profile else None,
        "gender": profile[2] if profile else None,
        "interest": profile[3] if profile else None,
        "city": profile[4] if profile else None,
        "norm_city": profile[5] if profile else None,
        "lat": profile[6] if profile else None,
        "lon": profile[7] if profile else None,
        "name": profile[8] if profile else None,
        "bio": profile[9] if profile else None,
    }
    age = context.user_data.get("tinder_age") or existing.get("age")
    gender = context.user_data.get("tinder_gender") or existing.get("gender")
    interest = context.user_data.get("tinder_interest") or existing.get("interest")
    city = context.user_data.get("tinder_city") or existing.get("city")
    norm_city = context.user_data.get("tinder_normalized_city") or existing.get("norm_city") or normalize_city(city or "")
    lat = context.user_data.get("tinder_lat") if context.user_data.get("tinder_lat") is not None else existing.get("lat")
    lon = context.user_data.get("tinder_lon") if context.user_data.get("tinder_lon") is not None else existing.get("lon")
    name = context.user_data.get("tinder_name") or existing.get("name")
    bio = context.user_data.get("tinder_bio") or existing.get("bio")
    # If editing for submit, store in contest_profile draft only
    if context.user_data.get("edit_for_submit"):
        context.user_data["contest_profile"] = (
            user_id,
            age,
            gender,
            interest,
            city,
            norm_city,
            lat,
            lon,
            name,
            bio,
            1,
        )
        return
    db.upsert_profile(user_id, age, gender, interest, city, lat, lon, name, bio, norm_city)


async def myprofile_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = ensure_user_context(update)
    lang = lang_for_user(user_id)
    profile = db.get_profile(user_id)
    if not profile:
        return await start_tinder_flow(update, context)
    # Reuse the standard sender so photo/video is included
    await send_own_profile(update, user_id, lang)


async def start_tinder_flow(update_or_query, context: ContextTypes.DEFAULT_TYPE):
    user_id = ensure_user_context(update_or_query)
    lang = lang_for_user(user_id)
    clear_transient_state(context)
    await _respond(update_or_query, t(lang, "tinder_age_prompt"))
    if context:
        context.user_data["tinder_media"] = []
    return config.TINDER_AGE


async def tinder_age(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = ensure_user_context(update)
    lang = lang_for_user(user_id)
    text = update.message.text.strip()
    if not text.isdigit():
        await update.message.reply_text(t(lang, "age_not_number"))
        return config.TINDER_AGE
    age = int(text)
    if age < 18 or age > 120:
        await update.message.reply_text(t(lang, "age_invalid"))
        return config.TINDER_AGE
    context.user_data["tinder_age"] = age
    keyboard = ReplyKeyboardMarkup(
        [
            [f"👦 {t(lang, 'gender_male')}", f"👧 {t(lang, 'gender_female')}"],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    await update.message.reply_text(t(lang, "tinder_gender_prompt"), reply_markup=keyboard)
    return config.TINDER_GENDER


async def tinder_gender_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = ensure_user_context(query)
    lang = lang_for_user(user_id)
    _, gender_value = query.data.split(":")
    context.user_data["tinder_gender"] = gender_value
    keyboard = ReplyKeyboardMarkup(
        [
            [t(lang, "tinder_interest_male"), t(lang, "tinder_interest_female")],
            [t(lang, "tinder_interest_any")],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    await query.message.reply_text(t(lang, "tinder_interest_prompt"), reply_markup=keyboard)
    if context.user_data.get("tinder_edit_mode"):
        save_profile_from_context(user_id, context)
        context.user_data["tinder_edit_mode"] = False
        await query.message.reply_text(t(lang, "tinder_saved"), reply_markup=ReplyKeyboardRemove())
        return await tinder_edit_menu(query, context)
    return config.TINDER_INTEREST


async def tinder_interest_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = ensure_user_context(query)
    lang = lang_for_user(user_id)
    _, interest_value = query.data.split(":")
    context.user_data["tinder_interest"] = interest_value
    existing_profile = db.get_profile(user_id)
    last_city = context.user_data.get("tinder_city") or (existing_profile[4] if existing_profile else None)
    context.user_data["tinder_last_city_option"] = last_city
    if context.user_data.get("tinder_edit_mode"):
        save_profile_from_context(user_id, context)
        context.user_data["tinder_edit_mode"] = False
        await query.message.reply_text(t(lang, "tinder_saved"), reply_markup=ReplyKeyboardRemove())
        return await tinder_edit_menu(query, context)
    buttons = []
    if last_city:
        buttons.append([t(lang, "tinder_city_button_last", city=last_city)])
    buttons.append([KeyboardButton(t(lang, "tinder_city_button_location"), request_location=True)])
    kb = ReplyKeyboardMarkup(buttons, resize_keyboard=True, one_time_keyboard=True)
    await query.message.reply_text(t(lang, "tinder_city_prompt"), reply_markup=kb)
    return config.TINDER_CITY


async def tinder_gender_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = ensure_user_context(update)
    lang = lang_for_user(user_id)
    text = update.message.text.lower()
    if "м" in text or "male" in text or "man" in text or "ч" in text:
        gender_value = "Male"
    elif "f" in text or "жен" in text or "female" in text or "girl" in text or "ж" in text:
        gender_value = "Female"
    else:
        await update.message.reply_text(t(lang, "gender_invalid_choice"))
        return config.TINDER_GENDER
    context.user_data["tinder_gender"] = gender_value
    if context.user_data.get("tinder_edit_mode"):
        save_profile_from_context(user_id, context)
        context.user_data["tinder_edit_mode"] = False
        await update.message.reply_text(t(lang, "tinder_saved"), reply_markup=ReplyKeyboardRemove())
        return await tinder_edit_menu(update, context)
    keyboard = ReplyKeyboardMarkup(
        [
            [t(lang, "tinder_interest_male"), t(lang, "tinder_interest_female")],
            [t(lang, "tinder_interest_any")],
        ],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    await update.message.reply_text(t(lang, "tinder_interest_prompt"), reply_markup=keyboard)
    if context.user_data.get("tinder_edit_mode"):
        save_profile_from_context(user_id, context)
        context.user_data["tinder_edit_mode"] = False
        await update.message.reply_text(t(lang, "tinder_saved"), reply_markup=ReplyKeyboardRemove())
        return await tinder_edit_menu(update, context)
    return config.TINDER_INTEREST


async def tinder_interest_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = ensure_user_context(update)
    lang = lang_for_user(user_id)
    text = update.message.text.lower()
    if "муж" in text or "man" in text or "мужчины" in text:
        interest_value = "Male"
    elif "жен" in text or "girl" in text or "women" in text or "female" in text:
        interest_value = "Female"
    else:
        interest_value = "Any"
    context.user_data["tinder_interest"] = interest_value
    existing_profile = db.get_profile(user_id)
    last_city = context.user_data.get("tinder_city") or (existing_profile[4] if existing_profile else None)
    context.user_data["tinder_last_city_option"] = last_city
    if context.user_data.get("tinder_edit_mode"):
        save_profile_from_context(user_id, context)
        context.user_data["tinder_edit_mode"] = False
        await update.message.reply_text(t(lang, "tinder_saved"), reply_markup=ReplyKeyboardRemove())
        return await tinder_edit_menu(update, context)
    buttons = []
    if last_city:
        buttons.append([t(lang, "tinder_city_button_last", city=last_city)])
    buttons.append([KeyboardButton(t(lang, "tinder_city_button_location"), request_location=True)])
    kb = ReplyKeyboardMarkup(buttons, resize_keyboard=True, one_time_keyboard=True)
    await update.message.reply_text(t(lang, "tinder_city_prompt"), reply_markup=kb)
    return config.TINDER_CITY


async def tinder_city(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = ensure_user_context(update)
    lang = lang_for_user(user_id)
    if update.message.location:
        lat = update.message.location.latitude
        lon = update.message.location.longitude
        context.user_data["tinder_lat"] = lat
        context.user_data["tinder_lon"] = lon
        city_label, norm_label = reverse_geocode_city(lat, lon)
        context.user_data["tinder_city"] = city_label
        context.user_data["tinder_normalized_city"] = norm_label
    else:
        text = update.message.text.strip()
        last_city = context.user_data.get("tinder_last_city_option")
        if last_city and text == t(lang, "tinder_city_button_last", city=last_city):
            context.user_data["tinder_city"] = last_city
            context.user_data["tinder_normalized_city"] = normalize_city(last_city)
        else:
            context.user_data["tinder_city"] = text
            context.user_data["tinder_normalized_city"] = normalize_city(text)
        context.user_data["tinder_lat"] = None
        context.user_data["tinder_lon"] = None
    if context.user_data.get("tinder_edit_mode"):
        save_profile_from_context(user_id, context)
        context.user_data["tinder_edit_mode"] = False
        await update.message.reply_text(t(lang, "tinder_saved"), reply_markup=ReplyKeyboardRemove())
        return await tinder_edit_menu(update, context)
    await update.message.reply_text(t(lang, "tinder_name_prompt"), reply_markup=ReplyKeyboardRemove())
    return config.TINDER_NAME


async def tinder_name(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = ensure_user_context(update)
    lang = lang_for_user(user_id)
    context.user_data["tinder_name"] = update.message.text.strip()
    if context.user_data.get("tinder_edit_mode"):
        save_profile_from_context(user_id, context)
        context.user_data["tinder_edit_mode"] = False
        await update.message.reply_text(t(lang, "tinder_saved"), reply_markup=ReplyKeyboardRemove())
        return await tinder_edit_menu(update, context)
    await update.message.reply_text(t(lang, "tinder_bio_prompt"))
    return config.TINDER_BIO


async def tinder_bio(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = ensure_user_context(update)
    lang = lang_for_user(user_id)
    context.user_data["tinder_bio"] = update.message.text.strip()
    if context.user_data.get("tinder_edit_mode"):
        save_profile_from_context(user_id, context)
        context.user_data["tinder_edit_mode"] = False
        await update.message.reply_text(t(lang, "tinder_saved"), reply_markup=ReplyKeyboardRemove())
        return await tinder_edit_menu(update, context)
    await update.message.reply_text(t(lang, "tinder_media_prompt"))
    return config.TINDER_MEDIA


def _store_media(context, file_id: str, kind: str):
    media = context.user_data.get("tinder_media", [])
    if len(media) >= 3:
        return False
    media.append((file_id, kind))
    context.user_data["tinder_media"] = media
    return True


async def tinder_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = ensure_user_context(update)
    lang = lang_for_user(user_id)
    if update.message.photo:
        fid = update.message.photo[-1].file_id
        ok = _store_media(context, fid, "photo")
    elif update.message.video:
        fid = update.message.video.file_id
        ok = _store_media(context, fid, "video")
    else:
        await update.message.reply_text(t(lang, "not_photo"))
        return config.TINDER_MEDIA
    if not ok:
        await update.message.reply_text("Limit reached (3).")
        return await tinder_confirm(update, context)
    if len(context.user_data["tinder_media"]) >= 3:
        return await tinder_confirm(update, context)
    kb = ReplyKeyboardMarkup(
        [[t(lang, "tinder_done_button")]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    await update.message.reply_text(t(lang, "tinder_media_more"), reply_markup=kb)
    return config.TINDER_MEDIA


async def tinder_media_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = ensure_user_context(update)
    lang = lang_for_user(user_id)
    text = update.message.text.strip().lower()
    done_label = t(lang, "tinder_done_button").lower()
    if done_label.lower() in text or text in ["done", "готово", "готово✅", "готово ✅", "готово✅ "]:
        media = context.user_data.get("tinder_media", [])
        if not media:
            await update.message.reply_text(t(lang, "tinder_media_prompt"))
            return config.TINDER_MEDIA
        await update.message.reply_text("👍", reply_markup=ReplyKeyboardRemove())
        return await tinder_confirm(update, context)
    # Any other text: remind to send media or tap Done
    kb = ReplyKeyboardMarkup(
        [[t(lang, "tinder_done_button")]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )
    await update.message.reply_text(t(lang, "tinder_media_more"), reply_markup=kb)
    return config.TINDER_MEDIA


async def tinder_confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = ensure_user_context(update)
    lang = lang_for_user(user_id)
    media = context.user_data.get("tinder_media", [])
    text = t(
        lang,
        "tinder_profile_line",
        name=context.user_data.get("tinder_name"),
        age=context.user_data.get("tinder_age"),
        gender=context.user_data.get("tinder_gender"),
        city=context.user_data.get("tinder_city"),
        interest=interest_label(context.user_data.get("tinder_interest"), lang),
        bio=context.user_data.get("tinder_bio"),
    )
    buttons = [
        [InlineKeyboardButton(t(lang, "btn_save"), callback_data="tinder:save")],
        [InlineKeyboardButton(t(lang, "btn_edit"), callback_data="tinder:edit")],
    ]
    if media:
        fid, kind = media[0]
        if kind == "video":
            await update.message.reply_video(video=fid, caption=text, reply_markup=InlineKeyboardMarkup(buttons))
        else:
            await update.message.reply_photo(photo=fid, caption=text, reply_markup=InlineKeyboardMarkup(buttons))
    else:
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(buttons))
    return config.TINDER_CONFIRM


async def tinder_save_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = ensure_user_context(query)
    lang = lang_for_user(user_id)
    existing = db.get_profile(user_id)
    is_new_profile = existing is None
    age = context.user_data.get("tinder_age") or (existing[1] if existing else None)
    gender = context.user_data.get("tinder_gender") or (existing[2] if existing else None)
    interest = context.user_data.get("tinder_interest") or (existing[3] if existing else None)
    city = context.user_data.get("tinder_city") or (existing[4] if existing else None)
    norm_city = context.user_data.get("tinder_normalized_city") or (existing[5] if existing else normalize_city(city or ""))
    lat = context.user_data.get("tinder_lat") if context.user_data.get("tinder_lat") is not None else (existing[6] if existing else None)
    lon = context.user_data.get("tinder_lon") if context.user_data.get("tinder_lon") is not None else (existing[7] if existing else None)
    name = context.user_data.get("tinder_name") or (existing[8] if existing else query.from_user.full_name)
    bio = context.user_data.get("tinder_bio") or (existing[9] if existing else None)
    if context.user_data.get("edit_for_submit"):
        # Update draft only, do not overwrite main profile
        context.user_data["contest_profile"] = (
            user_id,
            age,
            gender,
            interest,
            city,
            norm_city,
            lat,
            lon,
            name,
            bio,
            1,
        )
        if context.user_data.get("tinder_media"):
            context.user_data["contest_media"] = context.user_data.get("tinder_media", [])
        context.user_data.pop("tinder_media", None)
        context.user_data["tinder_edit_mode"] = False
        await query.message.reply_text(t(lang, "tinder_saved"))
        return await tinder_edit_menu(query, context)

    db.upsert_profile(user_id, age, gender, interest, city, lat, lon, name, bio, norm_city)
    if context.user_data.get("tinder_media"):
        db.clear_profile_media(user_id)
        for idx, (fid, kind) in enumerate(context.user_data.get("tinder_media", [])):
            db.add_profile_media(user_id, fid, kind, idx)
    context.user_data.pop("tinder_media", None)
    if is_new_profile:
        db.increment_stat(user_id, "profiles_created")
    else:
        db.increment_stat(user_id, "profiles_updated")
    # Referral credit on first profile creation
    if is_new_profile:
        referrer_id, credited = db.get_referrer(user_id)
        if referrer_id and credited == 0:
            db.adjust_balance(user_id, config.REFERRAL_BONUS)
            db.adjust_balance(referrer_id, config.REFERRAL_BONUS)
            db.mark_referral_credited(user_id)
            db.increment_stat(referrer_id, "referrals")
            db.increment_stat(user_id, "referral_rewards")
            try:
                await query.answer(
                    t(lang, "referral_bonus_claimed", points=config.REFERRAL_BONUS),
                    show_alert=True,
                )
            except Exception:
                pass
            try:
                await query.message.reply_text(t(lang, "referral_bonus_claimed", points=config.REFERRAL_BONUS))
            except Exception:
                pass
    # If this was triggered from edit flow, return to edit menu
    if context.user_data.get("tinder_edit_mode"):
        context.user_data["tinder_edit_mode"] = False
        await query.message.reply_text(t(lang, "tinder_saved"))
        return await tinder_edit_menu(query, context)

    mode = db.get_mode()
    if mode == "vote":
        # Jump straight into browsing in vote mode
        sent = await send_next_candidate_card(query, user_id, lang, context)
        if not sent:
            await query.message.reply_text(t(lang, "tinder_browse_empty_after_save"))
        return ConversationHandler.END

    # Collect/paused: show options to go to contest or browse or home
    buttons = []
    if mode == "collect":
        buttons.append([InlineKeyboardButton(t(lang, "start_application_button"), callback_data="start:submit")])
    buttons.append([InlineKeyboardButton(t(lang, "tinder_find_button"), callback_data="tinder:browse")])
    buttons.append([InlineKeyboardButton(t(lang, "back_main_menu"), callback_data="game:home")])
    await query.message.reply_text(t(lang, "tinder_saved"), reply_markup=InlineKeyboardMarkup(buttons))
    return ConversationHandler.END


async def tinder_edit_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    return await tinder_edit_menu(update, context)


async def tinder_edit_menu(update_or_query, context: ContextTypes.DEFAULT_TYPE):
    user_id = ensure_user_context(update_or_query)
    lang = lang_for_user(user_id)
    profile = db.get_profile(user_id)
    if not profile:
        return await start_tinder_flow(update_or_query, context)
    if context.user_data.get("edit_for_submit"):
        buttons = [
            [InlineKeyboardButton(t(lang, "btn_age"), callback_data="tedit:age"), InlineKeyboardButton(t(lang, "btn_gender"), callback_data="tedit:gender")],
            [InlineKeyboardButton(t(lang, "btn_name"), callback_data="tedit:name")],
            [InlineKeyboardButton(t(lang, "btn_media"), callback_data="tedit:media")],
            [InlineKeyboardButton(t(lang, "tinder_done_button"), callback_data="submit:saveprofile")],
        ]
    else:
        buttons = [
            [InlineKeyboardButton(t(lang, "btn_age"), callback_data="tedit:age"), InlineKeyboardButton(t(lang, "btn_gender"), callback_data="tedit:gender")],
            [InlineKeyboardButton(t(lang, "btn_interest"), callback_data="tedit:interest"), InlineKeyboardButton(t(lang, "btn_city"), callback_data="tedit:city")],
            [InlineKeyboardButton(t(lang, "btn_name"), callback_data="tedit:name"), InlineKeyboardButton(t(lang, "btn_bio"), callback_data="tedit:bio")],
            [InlineKeyboardButton(t(lang, "btn_media"), callback_data="tedit:media")],
            [InlineKeyboardButton(t(lang, "btn_browse"), callback_data="tedit:browse")],
        ]
    await _respond(update_or_query, t(lang, "tinder_edit_prompt"), reply_markup=InlineKeyboardMarkup(buttons))
    return ConversationHandler.END


async def tinder_edit_dispatch(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = ensure_user_context(query)
    lang = lang_for_user(user_id)
    parts = query.data.split(":")
    field = parts[1] if len(parts) > 1 else ""
    load_profile_into_context(user_id, context)
    context.user_data["tinder_edit_mode"] = True
    if context.user_data.get("edit_for_submit"):
        # Seed draft media list
        if not context.user_data.get("contest_media"):
            context.user_data["contest_media"] = db.list_profile_media(user_id)
    if field == "age":
        await query.message.reply_text(t(lang, "tinder_age_prompt"), reply_markup=ReplyKeyboardRemove())
        return config.TINDER_AGE
    if field == "gender":
        keyboard = ReplyKeyboardMarkup(
            [
                [f"👦 {t(lang, 'gender_male')}", f"👧 {t(lang, 'gender_female')}"],
            ],
            resize_keyboard=True,
            one_time_keyboard=True,
        )
        await query.message.reply_text(t(lang, "tinder_gender_prompt"), reply_markup=keyboard)
        return config.TINDER_GENDER
    if field == "interest":
        keyboard = ReplyKeyboardMarkup(
            [
                [t(lang, "tinder_interest_male"), t(lang, "tinder_interest_female")],
                [t(lang, "tinder_interest_any")],
            ],
            resize_keyboard=True,
            one_time_keyboard=True,
        )
        await query.message.reply_text(t(lang, "tinder_interest_prompt"), reply_markup=keyboard)
        return config.TINDER_INTEREST
    if field == "city":
        last_city = context.user_data.get("tinder_city")
        context.user_data["tinder_last_city_option"] = last_city
        buttons = []
        if last_city:
            buttons.append([t(lang, "tinder_city_button_last", city=last_city)])
        buttons.append([KeyboardButton(t(lang, "tinder_city_button_location"), request_location=True)])
        kb = ReplyKeyboardMarkup(buttons, resize_keyboard=True, one_time_keyboard=True)
        await query.message.reply_text(t(lang, "tinder_city_prompt"), reply_markup=kb)
        return config.TINDER_CITY
    if field == "name":
        await query.message.reply_text(t(lang, "tinder_name_prompt"), reply_markup=ReplyKeyboardRemove())
        return config.TINDER_NAME
    if field == "bio":
        await query.message.reply_text(t(lang, "tinder_bio_prompt"), reply_markup=ReplyKeyboardRemove())
        return config.TINDER_BIO
    if field == "media":
        context.user_data["tinder_media"] = []
        kb = ReplyKeyboardMarkup([[t(lang, "tinder_done_button")]], resize_keyboard=True, one_time_keyboard=True)
        await query.message.reply_text(t(lang, "tinder_media_prompt"), reply_markup=kb)
        return config.TINDER_MEDIA
    if field == "browse":
        context.user_data["tinder_edit_mode"] = False
        await send_next_candidate_card(query, user_id, lang, context)
        return ConversationHandler.END
    return ConversationHandler.END


async def find_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = ensure_user_context(update)
    lang = lang_for_user(user_id)
    if not db.is_feature_enabled("finder"):
        await _respond(update, t(lang, "feature_disabled"))
        return ConversationHandler.END
    profile = db.get_profile(user_id)
    if not profile:
        # if we have partial data from earlier edit, seed it
        if context.user_data.get("tinder_age"):
            return await tinder_confirm(update, context)
        return await start_tinder_flow(update, context)
    await send_own_profile(update, user_id, lang)
    return ConversationHandler.END


async def likes_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = ensure_user_context(update)
    lang = lang_for_user(user_id)
    if not db.is_feature_enabled("finder"):
        await _respond(update, t(lang, "feature_disabled"))
        return ConversationHandler.END
    await send_next_liker_card(update, user_id, lang, context)


def build_swipe_keyboard(lang: str, target_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("❤️", callback_data=f"swipe:{target_id}:like"),
                InlineKeyboardButton("💔", callback_data=f"swipe:{target_id}:pass"),
                InlineKeyboardButton("😴", callback_data="tinder:snooze"),
            ]
        ]
    )


async def send_next_candidate_card(update_or_query, user_id: int, lang: str, context: ContextTypes.DEFAULT_TYPE = None):
    profile = db.get_profile(user_id)
    if not profile:
        await _respond(update_or_query, t(lang, "tinder_profile_needed"))
        return False
    _, _, user_gender, user_interest, _, norm_city, *_ = profile
    row = db.get_next_candidate(user_id, user_gender, user_interest, norm_city or "")
    if not row:
        kb = ReplyKeyboardMarkup([["🔍", "📥"], ["😴"]], resize_keyboard=True, one_time_keyboard=True)
        await _respond(update_or_query, t(lang, "tinder_no_more_profiles"), reply_markup=kb)
        return False
    target_id, age, gender, interest, city, name, bio = row
    if context:
        context.user_data["tinder_current_target"] = target_id
    media = db.list_profile_media(target_id)
    caption = t(
        lang,
        "tinder_profile_line",
        name=name,
        age=age,
        gender=gender,
        city=city or "—",
        interest=interest_label(interest, lang),
        bio=bio or "—",
    )
    kb = ReplyKeyboardMarkup([["❤️", "💔", "😴"]], resize_keyboard=True, one_time_keyboard=True)
    if media:
        fid, kind = media[0]
        if isinstance(update_or_query, Update) and update_or_query.message:
            if kind == "video":
                await update_or_query.message.reply_video(video=fid, caption=caption, reply_markup=kb)
            else:
                await update_or_query.message.reply_photo(photo=fid, caption=caption, reply_markup=kb)
        else:
            bot = update_or_query.get_bot()
            try:
                if kind == "video":
                    await bot.send_video(chat_id=user_id, video=fid, caption=caption, reply_markup=kb)
                else:
                    await bot.send_photo(chat_id=user_id, photo=fid, caption=caption, reply_markup=kb)
            except Exception:
                # fallback to text if chat resolution fails
                await _respond(update_or_query, caption, reply_markup=kb)
    else:
        await _respond(update_or_query, caption, reply_markup=kb)
    return True


async def send_next_liker_card(update_or_query, user_id: int, lang: str, context: ContextTypes.DEFAULT_TYPE = None):
    likers = db.users_who_liked_me(user_id)
    if not likers:
        kb_empty = ReplyKeyboardMarkup([["🔍", "📥", "😴"]], resize_keyboard=True, one_time_keyboard=True)
        await _respond(update_or_query, t(lang, "tinder_likes_empty"), reply_markup=kb_empty)
        return
    target_id = likers[0]
    row = db.get_profile(target_id)
    if not row:
        kb_empty = ReplyKeyboardMarkup([["🔍", "📥", "😴"]], resize_keyboard=True, one_time_keyboard=True)
        await _respond(update_or_query, t(lang, "tinder_likes_empty"), reply_markup=kb_empty)
        return
    _, age, gender, interest, city, norm_city, lat, lon, name, bio, *_ = row
    if context:
        context.user_data["tinder_current_target"] = target_id
    media = db.list_profile_media(target_id)
    caption = t(
        lang,
        "tinder_profile_line",
        name=name,
        age=age,
        gender=gender,
        city=city or "—",
        interest=interest_label(interest, lang),
        bio=bio or "—",
    )
    kb = ReplyKeyboardMarkup([["❤️", "💔", "😴"]], resize_keyboard=True, one_time_keyboard=True)
    if media:
        fid, kind = media[0]
        if kind == "video":
            await update_or_query.message.reply_video(video=fid, caption=caption, reply_markup=kb)
        else:
            await update_or_query.message.reply_photo(photo=fid, caption=caption, reply_markup=kb)
    else:
        await _respond(update_or_query, caption, reply_markup=kb)


async def tinder_swipe_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = ensure_user_context(update)
    lang = lang_for_user(user_id)
    target_id = context.user_data.get("tinder_current_target")
    txt = update.message.text.strip()
    kb = ReplyKeyboardMarkup([["🔍", "📥"], ["😴"]], resize_keyboard=True, one_time_keyboard=True)
    if "🔍" in txt:
        await send_next_candidate_card(update, user_id, lang, context)
        return
    if "📥" in txt:
        await send_next_liker_card(update, user_id, lang, context)
        return
    if "😴" in txt:
        context.user_data.pop("tinder_current_target", None)
        await send_start_message(update, lang, user_id)
        return
    if not target_id:
        await update.message.reply_text(t(lang, "tinder_no_more_profiles"), reply_markup=kb)
        return
    if "❤️" in txt:
        mutual = db.mark_like_and_check(user_id, target_id)
        if mutual:
            try:
                chat = await context.bot.get_chat(target_id)
                username = format_username(chat.username, target_id)
            except Exception:
                username = format_username(None, target_id)
            await update.message.reply_text(t(lang, "tinder_match", username=username))
            target_lang = lang_for_user(target_id)
            me_un = update.effective_user.username
            me_username = format_username(me_un, user_id)
            await context.bot.send_message(
                chat_id=target_id,
                text=t(target_lang, "tinder_match", username=me_username),
            )
        else:
            target_lang = lang_for_user(target_id)
            try:
                await context.bot.send_message(
                    chat_id=target_id,
                    text=t(target_lang, "tinder_new_like"),
                )
            except Exception:
                pass
    elif "💔" in txt:
        db.mark_swipe(user_id, target_id, "dislike")
    elif "😴" in txt:
        context.user_data.pop("tinder_current_target", None)
        await send_start_message(update, lang, user_id)
        return
    await send_next_candidate_card(update, user_id, lang, context)


async def handle_checkin(update_or_query) -> None:
    user_id = ensure_user_context(update_or_query)
    lang = lang_for_user(user_id)
    last = db.get_last_checkin(user_id)
    today = date.today().isoformat()
    if last == today:
        await _respond(update_or_query, t(lang, "checkin_already"))
        return
    new_balance = db.record_checkin(user_id)
    await _respond(
        update_or_query,
        t(lang, "checkin_success", points=new_balance),
        reply_markup=game_menu_markup(lang, user_id),
    )


async def show_balance(update_or_query) -> None:
    user_id = ensure_user_context(update_or_query)
    lang = lang_for_user(user_id)
    balance = db.get_balance(user_id)
    await _respond(
        update_or_query,
        t(lang, "balance_label", points=balance),
        reply_markup=game_menu_markup(lang, user_id),
    )


def feature_enabled_or_reply(feature: str, update_or_query, lang: str):
    if db.is_feature_enabled(feature):
        return True
    _respond(update_or_query, t(lang, "feature_disabled"))
    return False


async def balance_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_balance(update)


async def shop_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    clear_transient_state(context)
    await show_shop(update)


async def blackjack_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start_blackjack(update, context)


async def blackjack_boost_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin: set blackjack win bias percentage for a user."""
    ensure_user_context(update)
    if not is_admin(update.effective_user.id):
        return
    lang = lang_for_user(update.effective_user.id)
    if len(context.args) != 2:
        await update.message.reply_text(t(lang, "bj_boost_usage"))
        return
    try:
        target_id = int(context.args[0])
        pct = float(context.args[1])
    except ValueError:
        await update.message.reply_text(t(lang, "bj_boost_usage"))
        return
    pct = max(0.0, min(pct, 100.0))
    db.set_blackjack_boost(target_id, pct / 100.0)
    await update.message.reply_text(t(lang, "bj_boost_set", pct=pct, uid=target_id))


async def grant_boost_points_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin: manually credit boost rewards and track stats."""
    ensure_user_context(update)
    if not is_admin(update.effective_user.id):
        return
    lang = lang_for_user(update.effective_user.id)
    if len(context.args) != 2:
        await update.message.reply_text(t(lang, "grant_boost_usage"))
        return
    try:
        target_id = int(context.args[0])
        boosts = int(context.args[1])
    except ValueError:
        await update.message.reply_text(t(lang, "grant_boost_usage"))
        return
    if boosts <= 0:
        await update.message.reply_text(t(lang, "grant_boost_usage"))
        return
    points = boosts * config.BOOST_BONUS
    new_balance = db.adjust_balance(target_id, points)
    db.increment_stat(target_id, "boosts_credited", boosts)
    await update.message.reply_text(
        t(lang, "grant_boost_ok", uid=target_id, boosts=boosts, points=points, balance=new_balance)
    )
    # Notify recipient if possible
    try:
        target_lang = lang_for_user(target_id)
        await context.bot.send_message(
            chat_id=target_id,
            text=t(target_lang, "boost_granted_user", boosts=boosts, points=points, balance=new_balance),
        )
    except Exception:
        pass


async def santa_start(update_or_query, context: ContextTypes.DEFAULT_TYPE, *, edit_mode: bool = False):
    """Begin Secret Santa flow: collect name + Instagram then assign number."""
    user_id = ensure_user_context(update_or_query)
    lang = lang_for_user(user_id)
    if not db.is_feature_enabled("santa"):
        await _respond(update_or_query, t(lang, "santa_disabled"))
        return ConversationHandler.END
    clear_transient_state(context)
    context.user_data["santa_active"] = True
    context.user_data.pop("santa_name", None)
    context.user_data.pop("santa_insta", None)
    existing_number = db.get_santa_number_for_user(user_id)
    details = db.get_santa_details_for_user(user_id)
    has_details = details and details[1] and details[2]
    if existing_number and has_details and not edit_mode:
        kb = InlineKeyboardMarkup(
            [[InlineKeyboardButton(t(lang, "santa_edit_button"), callback_data="santa:edit")]]
        )
        await _respond(update_or_query, t(lang, "santa_complete", number=_fmt_santa_number(existing_number)), reply_markup=kb)
        return ConversationHandler.END
    context.user_data["santa_existing_number"] = existing_number
    await _respond(update_or_query, t(lang, "santa_name_prompt"), reply_markup=ReplyKeyboardRemove())
    return config.SANTA_NAME


async def santa_name_step(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = ensure_user_context(update)
    lang = lang_for_user(user_id)
    if not context.user_data.get("santa_active"):
        return ConversationHandler.END
    name = (update.message.text or "").strip()
    if not name:
        await update.message.reply_text(t(lang, "santa_name_prompt"))
        return config.SANTA_NAME
    context.user_data["santa_name"] = name[:120]
    await update.message.reply_text(
        t(lang, "santa_gift_prompt"),
        reply_markup=ReplyKeyboardRemove(),
    )
    return config.SANTA_GIFT


async def santa_insta_step(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = ensure_user_context(update)
    lang = lang_for_user(user_id)
    if not context.user_data.get("santa_active"):
        return ConversationHandler.END
    if not update.message or not update.message.photo:
        await update.message.reply_text(t(lang, "santa_gift_prompt"))
        return config.SANTA_GIFT
    photo = update.message.photo[-1]
    file_id = photo.file_id
    name = context.user_data.get("santa_name")
    existing_number = context.user_data.get("santa_existing_number")
    number = existing_number or db.assign_secret_santa_number(user_id)
    if name:
        db.update_secret_santa_details(user_id, name=name, gift_photo_id=file_id)
    else:
        db.set_santa_gift_photo(user_id, file_id)
    if not number:
        await update.message.reply_text(t(lang, "santa_disabled"), reply_markup=ReplyKeyboardRemove())
        return ConversationHandler.END
    msg_key = "santa_details_updated" if existing_number else "santa_assigned"
    await update.message.reply_text(
        t(lang, msg_key, number=_fmt_santa_number(number)),
        reply_markup=ReplyKeyboardRemove(),
    )
    # Return user to main menu after finishing
    try:
        await send_start_message(update, lang, user_id)
    except Exception:
        pass
    clear_santa_state(context)
    return ConversationHandler.END


async def santa_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    lang = lang_for_user(ensure_user_context(update))
    await update.message.reply_text(t(lang, "submission_cancel"), reply_markup=ReplyKeyboardRemove())
    clear_santa_state(context)
    return ConversationHandler.END


async def santa_edit_start(update_or_query, context: ContextTypes.DEFAULT_TYPE):
    return await santa_start(update_or_query, context, edit_mode=True)


async def santa_lookup_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_user_context(update)
    if not is_admin(update.effective_user.id):
        return
    lang = lang_for_user(update.effective_user.id)
    if not context.args:
        await update.message.reply_text(t(lang, "santa_usage"))
        return
    if context.args[0].lower() == "all":
        first = db.get_first_santa_number()
        if not first:
            await update.message.reply_text(t(lang, "santa_lookup_not_found", number="—"))
            return
        await send_santa_card(update, first, lang)
        return
    try:
        number = int(context.args[0])
    except ValueError:
        await update.message.reply_text(t(lang, "santa_usage"))
        return
    await send_santa_card(update, number, lang)


async def send_santa_card(update_or_query, gift_number: int, lang: str):
    row = db.get_santa_by_number(gift_number)
    if not row:
        await _respond(update_or_query, t(lang, "santa_lookup_not_found", number=gift_number))
        return
    gift_number, uid, username, first_name, last_name, saved_name, gift_photo_id = row
    name_parts = [n for n in [saved_name, first_name, last_name] if n]
    name = " ".join(name_parts) if name_parts else "—"
    uname = username if username else "—"
    caption = t(lang, "santa_lookup", number=_fmt_santa_number(gift_number), uid=uid, username=uname, name=name)
    kb = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("⬅️", callback_data=f"santa:nav:{gift_number}:prev"),
                InlineKeyboardButton("➡️", callback_data=f"santa:nav:{gift_number}:next"),
                InlineKeyboardButton("⏹️", callback_data=f"santa:nav:{gift_number}:stop"),
            ]
        ]
    )
    bot = update_or_query.get_bot() if hasattr(update_or_query, "get_bot") else None
    chat_id = (
        update_or_query.effective_user.id
        if isinstance(update_or_query, Update) and update_or_query.effective_user
        else update_or_query.from_user.id
    )
    if gift_photo_id and bot:
        try:
            await bot.send_photo(chat_id=chat_id, photo=gift_photo_id, caption=caption, reply_markup=kb)
            return
        except Exception:
            # Fall back to text if the stored file_id is invalid
            caption = caption + f"\n\n{t(lang, 'photo_unavailable')}"
    await _respond(update_or_query, caption, reply_markup=kb)


async def feature_toggle_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin: /feature <name> <on/off> or /feature list."""
    ensure_user_context(update)
    if not is_admin(update.effective_user.id):
        return
    lang = lang_for_user(update.effective_user.id)
    if not context.args:
        await update.message.reply_text(t(lang, "feature_usage"))
        return
    if context.args[0].lower() == "list":
        flags = [
            ("finder", db.is_feature_enabled("finder")),
            ("shop", db.is_feature_enabled("shop")),
            ("blackjack", db.is_feature_enabled("blackjack")),
            ("earn", db.is_feature_enabled("earn")),
            ("referral", db.is_feature_enabled("referral")),
            ("boost", db.is_feature_enabled("boost")),
            ("santa", db.is_feature_enabled("santa")),
            ("fun", db.is_feature_enabled("fun")),
            ("voting", db.is_feature_enabled("voting")),
        ]
        lines = [t(lang, "feature_status_line", name=name, status=("on" if val else "off")) for name, val in flags]
        await update.message.reply_text("\n".join(lines))
        return
    if len(context.args) != 2:
        await update.message.reply_text(t(lang, "feature_usage"))
        return
    name = context.args[0].lower()
    value = context.args[1].lower()
    if name not in {"finder", "shop", "blackjack", "earn", "referral", "boost", "santa", "fun", "voting"} or value not in {"on", "off"}:
        await update.message.reply_text(t(lang, "feature_usage"))
        return
    db.set_feature_enabled(name, value == "on")
    await update.message.reply_text(t(lang, "feature_set", name=name, status=value))


async def quick_reply_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle taps on the always-visible quick keyboard."""
    user_id = ensure_user_context(update)
    lang = lang_for_user(user_id)
    text = (update.message.text or "").strip()
    if not text:
        return
    if context.user_data.get("bj_pending"):
        if text.isdigit():
            await handle_bet_selection(update, context, int(text))
        else:
            await update.message.reply_text(t(lang, "bet_invalid"))
        return
    if text == t(lang, "back_game_menu"):
        await send_game_menu(update)
        return
    if text == t(lang, "back_main_menu"):
        await send_start_message(update, lang, user_id)
        return
    if text == t(lang, "quick_help_btn"):
        await help_command(update, context)


async def send_earn_points(update_or_query, context: ContextTypes.DEFAULT_TYPE):
    user_id = ensure_user_context(update_or_query)
    lang = lang_for_user(user_id)
    clear_transient_state(context)
    if not db.is_feature_enabled("earn"):
        await _respond(update_or_query, t(lang, "feature_disabled"))
        return
    # Build buttons based on enabled sub-features
    rows = []
    if db.is_feature_enabled("boost"):
        rows.append([InlineKeyboardButton(t(lang, "earn_boost_button"), callback_data="earn:boost")])
    if db.is_feature_enabled("referral"):
        rows.append([InlineKeyboardButton(t(lang, "earn_ref_button"), callback_data="earn:ref")])
    rows.append([InlineKeyboardButton(t(lang, "blackjack_button"), callback_data="game:blackjack")])
    if not rows or (not db.is_feature_enabled("boost") and not db.is_feature_enabled("referral")):
        await _respond(update_or_query, t(lang, "feature_disabled"))
        return
    text = t(lang, "earn_title") + "\n\n" + t(lang, "earn_choose")
    bot = update_or_query.get_bot() if hasattr(update_or_query, "get_bot") else None
    kb_inline = InlineKeyboardMarkup(rows)
    kb_reply = ReplyKeyboardMarkup(
        [[t(lang, "back_game_menu"), t(lang, "back_main_menu")]],
        resize_keyboard=True,
    )
    # Always push our nav keyboard to override any lingering "Help" keyboard
    if bot:
        try:
            await bot.send_message(
                chat_id=user_id,
                text=" ",
                reply_markup=kb_reply,
                disable_notification=True,
            )
        except Exception:
            pass
    if isinstance(update_or_query, Update) and update_or_query.callback_query and update_or_query.callback_query.message:
        try:
            await update_or_query.callback_query.edit_message_text(text=text, reply_markup=kb_inline, disable_web_page_preview=False)
            return
        except Exception:
            pass
    await _respond(update_or_query, text, reply_markup=kb_inline)


def blackjack_actions_keyboard(lang: str, allow_double: bool) -> InlineKeyboardMarkup:
    rows = [
        [
            InlineKeyboardButton(t(lang, "action_hit"), callback_data="bj:hit"),
            InlineKeyboardButton(t(lang, "action_stand"), callback_data="bj:stand"),
        ]
    ]
    if allow_double:
        rows.append(
            [InlineKeyboardButton(t(lang, "action_double"), callback_data="bj:double")]
        )
    return InlineKeyboardMarkup(rows)


def post_round_keyboard(lang: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton(t(lang, "play_again"), callback_data="game:blackjack")],
            [InlineKeyboardButton(t(lang, "back_to_menu"), callback_data="game:menu")],
        ]
    )


async def start_blackjack(update_or_query, context: ContextTypes.DEFAULT_TYPE) -> None:
    user_id = ensure_user_context(update_or_query)
    lang = lang_for_user(user_id)
    clear_santa_state(context)
    if not db.is_feature_enabled("blackjack"):
        await _respond(update_or_query, t(lang, "feature_disabled"))
        return
    balance = db.get_balance(user_id)
    if balance < 1:
        await _respond(update_or_query, t(lang, "not_enough_points"), reply_markup=game_menu_markup(lang, user_id))
        return
    bets = [b for b in [1, 2, 5, 10] if b <= balance]
    if not bets:
        bets = [balance]
    if context:
        context.user_data["bj_pending"] = True
    kb = ReplyKeyboardMarkup(
        [list(map(str, bets))],
        resize_keyboard=True,
        one_time_keyboard=True,
        input_field_placeholder=t(lang, "bet_prompt_type"),
    )
    await _respond(
        update_or_query,
        f"{t(lang, 'bet_prompt')} (balance {balance})",
        reply_markup=kb,
    )


async def send_blackjack_state(update_or_query, player_hand, dealer_hand, bet, allow_double=True):
    user_id = ensure_user_context(update_or_query)
    lang = lang_for_user(user_id)
    player_total, _ = game.hand_value(player_hand)
    dealer_show = game.format_card(dealer_hand[0])
    text = "\n\n".join(
        [
            t(lang, "blackjack_title"),
            t(lang, "player_hand", cards=game.format_hand(player_hand), total=player_total),
            t(lang, "dealer_shows", card=dealer_show),
            t(lang, "bet_set", bet=bet),
        ]
    )
    bot = update_or_query.get_bot() if hasattr(update_or_query, "get_bot") else None
    if bot:
        await _remove_reply_keyboard(user_id, bot)
    await _respond(update_or_query, text, reply_markup=blackjack_actions_keyboard(lang, allow_double))


async def resolve_blackjack_outcome(update_or_query, player_hand, dealer_hand, bet):
    user_id = ensure_user_context(update_or_query)
    lang = lang_for_user(user_id)
    player_total, _ = game.hand_value(player_hand)
    dealer_total, _ = game.hand_value(dealer_hand)
    dealer_total_orig = dealer_total

    # Apply admin-set boost: chance to flip a dealer win/tie into a bust
    boost = db.get_blackjack_boost(user_id)
    forced_dealer_bust = False
    if (
        boost > 0
        and player_total <= 21
        and dealer_total >= player_total
        and dealer_total <= 21
        and random.random() < boost
    ):
        forced_dealer_bust = True

    message_lines = [
        t(lang, "blackjack_title"),
        t(lang, "player_hand", cards=game.format_hand(player_hand), total=player_total),
        t(lang, "dealer_hand", cards=game.format_hand(dealer_hand), total=dealer_total_orig),
        "",
    ]
    outcome_text = ""
    payout = 0
    if player_total > 21:
        outcome_text = t(lang, "blackjack_bust", bet=bet)
    elif forced_dealer_bust or dealer_total > 21:
        payout = bet * 2
        db.adjust_balance(user_id, payout)
        outcome_text = t(lang, "blackjack_dealer_bust", payout=payout)
    elif player_total > dealer_total:
        payout = bet * 2
        db.adjust_balance(user_id, payout)
        outcome_text = t(lang, "blackjack_player_win", payout=payout, profit=payout - bet)
    elif dealer_total > player_total:
        outcome_text = t(lang, "blackjack_dealer_win", bet=bet)
    else:
        payout = bet
        db.adjust_balance(user_id, payout)
        outcome_text = t(lang, "blackjack_push")
    message_lines.append(outcome_text)
    balance = db.get_balance(user_id)
    message_lines.append("")
    message_lines.append(t(lang, "balance_label", points=balance))
    await _respond(
        update_or_query,
        "\n".join(message_lines),
        reply_markup=post_round_keyboard(lang),
    )
    db.clear_blackjack_session(user_id)


async def handle_bet_selection(update_or_query, context: ContextTypes.DEFAULT_TYPE, bet: int) -> None:
    user_id = ensure_user_context(update_or_query)
    lang = lang_for_user(user_id)
    balance = db.get_balance(user_id)
    if bet > balance:
        await _respond(update_or_query, t(lang, "bet_too_high"), reply_markup=game_menu_markup(lang, user_id))
        return

    if context:
        context.user_data["bj_pending"] = False
    db.increment_stat(user_id, "games_played")
    deck = game.build_deck()
    player_hand = [game.draw_card(deck), game.draw_card(deck)]
    dealer_hand = [game.draw_card(deck), game.draw_card(deck)]
    db.adjust_balance(user_id, -bet)
    db.save_blackjack_session(user_id, deck, player_hand, dealer_hand, bet)

    player_bj = game.is_blackjack(player_hand)
    dealer_bj = game.is_blackjack(dealer_hand)
    if player_bj or dealer_bj:
        outcome_lines = [
            t(lang, "blackjack_title"),
            t(lang, "player_hand", cards=game.format_hand(player_hand), total=game.hand_value(player_hand)[0]),
            t(lang, "dealer_hand", cards=game.format_hand(dealer_hand), total=game.hand_value(dealer_hand)[0]),
            "",
        ]
        if player_bj and dealer_bj:
            db.adjust_balance(user_id, bet)
            outcome_lines.append(t(lang, "blackjack_push"))
        elif player_bj:
            payout = bet * 2
            db.adjust_balance(user_id, payout)
            outcome_lines.append(t(lang, "blackjack_player_blackjack", payout=payout))
        else:
            outcome_lines.append(t(lang, "blackjack_dealer_blackjack", bet=bet))
        balance = db.get_balance(user_id)
        outcome_lines.append("")
        outcome_lines.append(t(lang, "balance_label", points=balance))
        await _respond(update_or_query, "\n".join(outcome_lines), reply_markup=post_round_keyboard(lang))
        db.clear_blackjack_session(user_id)
        return

    await send_blackjack_state(update_or_query, player_hand, dealer_hand, bet, allow_double=True)


async def blackjack_hit(update_or_query) -> None:
    user_id = ensure_user_context(update_or_query)
    lang = lang_for_user(user_id)
    session = db.load_blackjack_session(user_id)
    if not session:
        await _respond(update_or_query, t(lang, "no_active_game"), reply_markup=game_menu_markup(lang, user_id))
        return
    deck = session["deck"]
    player_hand = session["player_hand"]
    dealer_hand = session["dealer_hand"]
    bet = session["bet"]

    player_hand.append(game.draw_card(deck))
    db.save_blackjack_session(user_id, deck, player_hand, dealer_hand, bet)

    total, _ = game.hand_value(player_hand)
    if total > 21:
        db.clear_blackjack_session(user_id)
        balance = db.get_balance(user_id)
        text = "\n".join(
            [
                t(lang, "blackjack_bust", bet=bet),
                t(lang, "player_hand", cards=game.format_hand(player_hand), total=total),
                "",
                t(lang, "balance_label", points=balance),
            ]
        )
        await _respond(update_or_query, text, reply_markup=post_round_keyboard(lang))
        return

    await send_blackjack_state(
        update_or_query, player_hand, dealer_hand, bet, allow_double=False
    )


async def blackjack_stand(update_or_query) -> None:
    user_id = ensure_user_context(update_or_query)
    lang = lang_for_user(user_id)
    session = db.load_blackjack_session(user_id)
    if not session:
        await _respond(update_or_query, t(lang, "no_active_game"), reply_markup=game_menu_markup(lang, user_id))
        return
    deck = session["deck"]
    player_hand = session["player_hand"]
    dealer_hand = game.dealer_play(session["dealer_hand"], deck)
    bet = session["bet"]
    await resolve_blackjack_outcome(update_or_query, player_hand, dealer_hand, bet)


async def blackjack_double(update_or_query) -> None:
    user_id = ensure_user_context(update_or_query)
    lang = lang_for_user(user_id)
    session = db.load_blackjack_session(user_id)
    if not session:
        await _respond(update_or_query, t(lang, "no_active_game"), reply_markup=game_menu_markup(lang, user_id))
        return

    player_hand = session["player_hand"]
    if len(player_hand) != 2:
        await _respond(update_or_query, t(lang, "double_not_allowed"), reply_markup=blackjack_actions_keyboard(lang, False))
        return
    bet = session["bet"]
    balance = db.get_balance(user_id)
    if balance < bet:
        await _respond(update_or_query, t(lang, "not_enough_points"), reply_markup=blackjack_actions_keyboard(lang, False))
        return

    db.adjust_balance(user_id, -bet)
    bet *= 2
    deck = session["deck"]
    dealer_hand = session["dealer_hand"]
    player_hand.append(game.draw_card(deck))
    total, _ = game.hand_value(player_hand)
    if total > 21:
        db.clear_blackjack_session(user_id)
        balance = db.get_balance(user_id)
        text = "\n".join(
            [
                t(lang, "blackjack_bust", bet=bet),
                t(lang, "player_hand", cards=game.format_hand(player_hand), total=total),
                "",
                t(lang, "balance_label", points=balance),
            ]
        )
        await _respond(update_or_query, text, reply_markup=post_round_keyboard(lang))
        return

    dealer_hand = game.dealer_play(dealer_hand, deck)
    db.clear_blackjack_session(user_id)
    await resolve_blackjack_outcome(update_or_query, player_hand, dealer_hand, bet)


async def show_shop(update_or_query) -> None:
    user_id = ensure_user_context(update_or_query)
    lang = lang_for_user(user_id)
    if not db.is_feature_enabled("shop"):
        await _respond(update_or_query, t(lang, "feature_disabled"))
        return
    items = db.list_shop_items()
    if not items:
        await _respond(update_or_query, t(lang, "shop_empty"), reply_markup=game_menu_markup(lang, user_id))
        return
    lines = [t(lang, "shop_title")]
    buttons = []
    for item_id, name, price, code, kind in items:
        label = shop_item_label(lang, code, kind, name)
        lines.append(t(lang, "shop_item_line", name=label, price=price))
        buttons.append([InlineKeyboardButton(f"{label} ({price})", callback_data=f"shopbuy:{item_id}")])
    buttons.append([InlineKeyboardButton(t(lang, "back_main_menu"), callback_data="game:home")])
    await _respond(update_or_query, "\n".join(lines), reply_markup=InlineKeyboardMarkup(buttons))


# ---------- Shop Conversation ----------


def parse_username(text: str) -> Optional[str]:
    txt = text.strip()
    if txt.startswith("@"):
        return txt[1:]
    if " " in txt:
        return None
    return txt


def shop_contact_from_saved(user_id: int):
    contact = db.get_contact(user_id)
    if contact:
        full_name, email, address, size = contact
        return full_name, email, address, size
    return None, None, None, None


def shop_item_label(lang: str, code: str, kind: str, fallback: str) -> str:
    """Return localized/emoji label for shop items."""
    if kind == "ticket":
        return t(lang, "shop_item_ticket_label")
    if kind == "hoodie":
        return t(lang, "shop_item_hoodie_label")
    if kind == "bottle":
        return t(lang, "shop_item_bottle_label")
    return fallback


async def deliver_pending_gifts(user_id: int, username: Optional[str], context: ContextTypes.DEFAULT_TYPE):
    """Notify user about any gifts waiting for them."""
    pending = db.pending_gifts_for_user(user_id, username)
    if not pending:
        return
    lang = lang_for_user(user_id)
    for pid, buyer_id, item_id, recipient_username, data_json, item_name, item_kind in pending:
        buyer_basic = db.get_user_basic(buyer_id)
        buyer_username = buyer_basic[0] if buyer_basic else None
        sender_label = format_username(buyer_username, buyer_id)
        kb = InlineKeyboardMarkup(
            [[InlineKeyboardButton(t(lang, "shop_fill_button"), callback_data=f"giftfill:{pid}")]]
        )
        try:
            await context.bot.send_message(
                chat_id=user_id,
                text=t(lang, "shop_gift_received", item=item_name, sender=sender_label),
                reply_markup=kb,
            )
            db.set_purchase_recipient(pid, user_id, username)
            db.mark_purchase_notified(pid)
        except Exception as e:
            logger.warning("Failed to notify gift recipient %s for purchase %s: %s", user_id, pid, e)


async def shop_buy_start(update_or_query, context: ContextTypes.DEFAULT_TYPE, item_id: int):
    user_id = ensure_user_context(update_or_query)
    lang = lang_for_user(user_id)
    item = db.get_shop_item(item_id)
    if not item:
        await _respond(update_or_query, t(lang, "shop_empty"), reply_markup=game_menu_markup(lang, user_id))
        return ConversationHandler.END
    _, code, name, kind, price = item
    label = shop_item_label(lang, code, kind, name)
    balance = db.get_balance(user_id)
    if balance < price:
        await _respond(update_or_query, t(lang, "shop_no_points", name=label, price=price, points=balance), reply_markup=game_menu_markup(lang, user_id))
        return ConversationHandler.END

    context.user_data["shop_item"] = (item[0], code, label, kind, price)
    context.user_data["shop_data"] = {"buyer": user_id}
    buttons = [
        [InlineKeyboardButton(t(lang, "shop_keep"), callback_data="shop:keep")],
        [InlineKeyboardButton(t(lang, "shop_gift"), callback_data="shop:gift")],
    ]
    await _respond(update_or_query, t(lang, "shop_choose_keep_gift"), reply_markup=InlineKeyboardMarkup(buttons))
    return config.SHOP_KEEP_OR_GIFT


async def shop_buy_entry(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    _, item_id = query.data.split(":")
    return await shop_buy_start(query, context, int(item_id))


async def shop_keep_gift_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = ensure_user_context(query)
    lang = lang_for_user(user_id)
    choice = query.data.split(":")[1]
    item = context.user_data.get("shop_item")
    if not item:
        await query.message.reply_text(t(lang, "shop_empty"))
        return ConversationHandler.END
    _, code, name, kind, price = item
    context.user_data["shop_recipient_id"] = None
    context.user_data["shop_recipient_username"] = None
    context.user_data["shop_skip_details"] = False
    if choice == "gift":
        await query.message.reply_text(t(lang, "shop_enter_recipient"))
        return config.SHOP_RECIPIENT
    return await shop_collect_details(query, context, user_id, kind)


async def shop_recipient_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = ensure_user_context(update)
    lang = lang_for_user(user_id)
    username = parse_username(update.message.text or "")
    if not username:
        await update.message.reply_text(t(lang, "shop_recipient_invalid"))
        return config.SHOP_RECIPIENT
    context.user_data["shop_recipient_username"] = username
    context.user_data["shop_recipient_id"] = None  # we don't resolve id reliably
    await update.message.reply_text(t(lang, "shop_recipient_saved", username=f"@{username}"))
    item = context.user_data.get("shop_item")
    if not item:
        return ConversationHandler.END
    _, code, name, kind, price = item
    # Ask who will fill details
    buttons = [
        [InlineKeyboardButton(t(lang, "shop_fill_self"), callback_data="shopfill:self")],
        [InlineKeyboardButton(t(lang, "shop_fill_recipient"), callback_data="shopfill:recipient")],
    ]
    await update.message.reply_text(t(lang, "shop_fill_who"), reply_markup=InlineKeyboardMarkup(buttons))
    return config.SHOP_FILL_WHO


async def shop_fill_who_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = ensure_user_context(query)
    lang = lang_for_user(user_id)
    choice = query.data.split(":")[1]
    item = context.user_data.get("shop_item")
    if not item:
        return ConversationHandler.END
    _, code, name, kind, price = item
    if choice == "recipient":
        context.user_data["shop_skip_details"] = True
        return await shop_finalize(query, context, user_id)
    context.user_data["shop_skip_details"] = False
    return await shop_collect_details(query, context, user_id, kind)


async def shop_collect_details(update_or_query, context: ContextTypes.DEFAULT_TYPE, user_id: int, kind: str):
    lang = lang_for_user(user_id)
    saved_name, saved_email, saved_address, saved_size = shop_contact_from_saved(user_id)
    context.user_data["shop_contact"] = {"full_name": None, "email": None, "address": None, "size": None}

    # Tickets need name/email
    if kind == "ticket":
        if saved_name and saved_email:
            buttons = [
                [InlineKeyboardButton(t(lang, "shop_yes"), callback_data="shop:usecontact")],
                [InlineKeyboardButton(t(lang, "shop_no"), callback_data="shop:nocontact")],
            ]
            await _respond(update_or_query, t(lang, "shop_use_saved_contact", name=saved_name, email=saved_email), reply_markup=InlineKeyboardMarkup(buttons))
            return config.SHOP_NAME
        await _respond(update_or_query, t(lang, "shop_ticket_name"))
        return config.SHOP_NAME

    # Hoodie needs size/address; reuse name/email if present
    if kind == "hoodie":
        if saved_name and saved_email:
            context.user_data["shop_contact"]["full_name"] = saved_name
            context.user_data["shop_contact"]["email"] = saved_email
        await _respond(update_or_query, t(lang, "shop_hoodie_size"))
        return config.SHOP_SIZE

    # Bottle: name/email
    if kind == "bottle":
        if saved_name and saved_email:
            buttons = [
                [InlineKeyboardButton(t(lang, "shop_yes"), callback_data="shop:usecontact")],
                [InlineKeyboardButton(t(lang, "shop_no"), callback_data="shop:nocontact")],
            ]
            await _respond(update_or_query, t(lang, "shop_use_saved_contact", name=saved_name, email=saved_email), reply_markup=InlineKeyboardMarkup(buttons))
            return config.SHOP_NAME
        await _respond(update_or_query, t(lang, "shop_ticket_name"))
        return config.SHOP_NAME

    await _respond(update_or_query, t(lang, "shop_empty"))
    return ConversationHandler.END


async def shop_use_contact_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = ensure_user_context(query)
    lang = lang_for_user(user_id)
    choice = query.data.split(":")[1]
    saved_name, saved_email, saved_address, saved_size = shop_contact_from_saved(user_id)
    if choice == "usecontact" and saved_name and saved_email:
        context.user_data["shop_contact"] = {"full_name": saved_name, "email": saved_email, "address": saved_address, "size": saved_size}
        item = context.user_data.get("shop_item")
        if not item:
            return ConversationHandler.END
        _, code, name, kind, price = item
        if kind == "ticket":
            # done
            return await shop_finalize(query, context, user_id)
        if kind == "bottle":
            return await shop_finalize(query, context, user_id)
    # proceed to ask name
    await query.message.reply_text(t(lang, "shop_ticket_name"))
    return config.SHOP_NAME


async def shop_name_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = ensure_user_context(update)
    lang = lang_for_user(user_id)
    context.user_data.setdefault("shop_contact", {})
    context.user_data["shop_contact"]["full_name"] = update.message.text.strip()
    # If email already present (e.g., from saved contact), skip asking
    if context.user_data["shop_contact"].get("email"):
        return await shop_email_text(update, context)
    await update.message.reply_text(t(lang, "shop_ticket_email"))
    return config.SHOP_EMAIL


async def gift_contact_new_flow(update_or_query, context: ContextTypes.DEFAULT_TYPE, user_id: int, item_kind: str, item_name: str, fresh_prompt: bool = False):
    lang = lang_for_user(user_id)
    contact = context.user_data.get("shop_contact", {})
    if item_kind in ("ticket", "bottle"):
        if fresh_prompt:
            await _respond(update_or_query, t(lang, "shop_fill_prompt", item=item_name))
        if contact.get("full_name") and contact.get("email"):
            return await shop_finalize(update_or_query, context, user_id)
        await _respond(update_or_query, t(lang, "shop_ticket_name"))
        return config.SHOP_NAME
    if item_kind == "hoodie":
        need_name = not contact.get("full_name") or not contact.get("email")
        need_size = not contact.get("size")
        need_addr = not contact.get("address")
        if fresh_prompt:
            await _respond(update_or_query, t(lang, "shop_fill_prompt", item=item_name))
        if need_name:
            await _respond(update_or_query, t(lang, "shop_ticket_name"))
            return config.SHOP_NAME
        if need_size:
            await _respond(update_or_query, t(lang, "shop_hoodie_size"))
            return config.SHOP_SIZE
        if need_addr:
            await _respond(update_or_query, t(lang, "shop_hoodie_address"))
            return config.SHOP_ADDRESS
        return await shop_finalize(update_or_query, context, user_id)
    return ConversationHandler.END


async def gift_contact_choice_cb(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = ensure_user_context(query)
    lang = lang_for_user(user_id)
    choice = query.data.split(":")[1]
    item = context.user_data.get("shop_item")
    if not item:
        return ConversationHandler.END
    _, code, name, kind, price = item
    saved = db.get_contact(user_id)
    if choice == "use" and saved:
        full_name_saved, email_saved, address_saved, size_saved = saved
        context.user_data["shop_contact"] = {
            "full_name": full_name_saved,
            "email": email_saved,
            "address": address_saved,
            "size": size_saved,
        }
        return await gift_contact_new_flow(query, context, user_id, kind, name)
    # new info: clear contact so they enter fresh
    context.user_data["shop_contact"] = {"full_name": None, "email": None, "address": None, "size": None}
    return await gift_contact_new_flow(query, context, user_id, kind, name, fresh_prompt=True)


async def shop_email_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = ensure_user_context(update)
    lang = lang_for_user(user_id)
    context.user_data.setdefault("shop_contact", {})
    context.user_data["shop_contact"]["email"] = update.message.text.strip()
    item = context.user_data.get("shop_item")
    if not item:
        return ConversationHandler.END
    _, code, name, kind, price = item
    if kind == "ticket":
        return await shop_finalize(update, context, user_id)
    if kind == "bottle":
        return await shop_finalize(update, context, user_id)
    if kind == "hoodie":
        await update.message.reply_text(t(lang, "shop_hoodie_size"))
        return config.SHOP_SIZE
    return ConversationHandler.END


async def shop_size_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = ensure_user_context(update)
    lang = lang_for_user(user_id)
    context.user_data.setdefault("shop_contact", {})
    context.user_data["shop_contact"]["size"] = update.message.text.strip()
    await update.message.reply_text(t(lang, "shop_hoodie_address"))
    return config.SHOP_ADDRESS


async def shop_address_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = ensure_user_context(update)
    lang = lang_for_user(user_id)
    context.user_data.setdefault("shop_contact", {})
    context.user_data["shop_contact"]["address"] = update.message.text.strip()
    return await shop_finalize(update, context, user_id)


async def gift_fill_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = ensure_user_context(query)
    lang = lang_for_user(user_id)
    _, pid_str = query.data.split(":")
    try:
        pid = int(pid_str)
    except ValueError:
        return ConversationHandler.END
    row = db.get_purchase(pid)
    if not row:
        await query.message.reply_text("Not found.")
        return ConversationHandler.END
    pid, buyer_id, item_id, recipient_id, recipient_username, data_json, status, notified, item_name, item_kind, price, item_code = row
    # authorize
    if recipient_id and recipient_id != user_id:
        await query.message.reply_text("Not for you.")
        return ConversationHandler.END
    if recipient_username and recipient_id is None and recipient_username.lower().lstrip("@") != (query.from_user.username or "").lower():
        await query.message.reply_text("Not for you.")
        return ConversationHandler.END
    # persist recipient assignment when filling
    db.set_purchase_recipient(pid, user_id, query.from_user.username)
    # load data + saved contact to avoid retyping
    try:
        data = json.loads(data_json) if data_json else {}
    except Exception:
        data = {}
    saved_contact = db.get_contact(user_id)
    full_name_saved, email_saved, address_saved, size_saved = saved_contact if saved_contact else (None, None, None, None)
    context.user_data["shop_item"] = (item_id, item_code, shop_item_label(lang, item_code, item_kind, item_name), item_kind, price)
    context.user_data["shop_purchase_id"] = pid
    context.user_data["shop_contact"] = {
        "full_name": data.get("full_name") or full_name_saved,
        "email": data.get("email") or email_saved,
        "address": data.get("address") or address_saved,
        "size": data.get("size") or size_saved,
    }
    context.user_data["shop_recipient_id"] = user_id
    context.user_data["shop_recipient_username"] = query.from_user.username
    context.user_data["shop_skip_details"] = False
    context.user_data["shop_purchase_id"] = pid
    contact = context.user_data["shop_contact"]
    # If we have saved contact, offer reuse
    if full_name_saved or email_saved or address_saved or size_saved:
        buttons = [
            [InlineKeyboardButton(t(lang, "shop_yes"), callback_data="giftcontact:use")],
            [InlineKeyboardButton(t(lang, "shop_no"), callback_data="giftcontact:new")],
        ]
        await query.message.reply_text(t(lang, "shop_use_saved_contact", name=full_name_saved or "—", email=email_saved or "—"), reply_markup=InlineKeyboardMarkup(buttons))
        return config.SHOP_FILL_CONTACT
    # otherwise proceed to ask missing
    return await gift_contact_new_flow(query, context, user_id, item_kind, item_name)


async def shop_finalize(update_or_query, context: ContextTypes.DEFAULT_TYPE, user_id: int):
    lang = lang_for_user(user_id)
    item = context.user_data.get("shop_item")
    if not item:
        return ConversationHandler.END
    item_id, code, name, kind, price = item
    contact = context.user_data.get("shop_contact", {})
    recipient_id = context.user_data.get("shop_recipient_id")
    recipient_username = context.user_data.get("shop_recipient_username")
    skip_details = context.user_data.get("shop_skip_details")
    existing_purchase_id = context.user_data.get("shop_purchase_id")
    new_purchase = existing_purchase_id is None
    buyer_id_orig = user_id
    if existing_purchase_id:
        meta = db.get_purchase(existing_purchase_id)
        if meta:
            _, buyer_id_orig, _, _, _, _, _, _, _, _, price_db, _ = meta
            if price_db:
                price = price_db

    # Try to resolve recipient id by username if not set
    if not recipient_id and recipient_username:
        resolved = db.find_user_by_username(recipient_username)
        if resolved:
            recipient_id = resolved
            context.user_data["shop_recipient_id"] = resolved

    # charge balance only for new purchases
    if new_purchase:
        balance = db.get_balance(user_id)
        if balance < price:
            await _respond(update_or_query, t(lang, "shop_no_points", name=name, price=price, points=balance), reply_markup=game_menu_markup(lang, user_id))
            return ConversationHandler.END
        db.adjust_balance(user_id, -price)

    # persist contact
    if not skip_details:
        db.upsert_contact(user_id, contact.get("full_name"), contact.get("email"), contact.get("address"), contact.get("size"))

    data = {
        "full_name": contact.get("full_name"),
        "email": contact.get("email"),
        "address": contact.get("address"),
        "size": contact.get("size"),
        "kind": kind,
        "filled_by": "recipient" if skip_details else "buyer",
    }
    if new_purchase:
        pid = db.create_purchase(
            buyer_id=user_id,
            item_id=item_id,
            recipient_id=recipient_id,
            recipient_username=recipient_username,
            data=json.dumps(data, ensure_ascii=False),
        )
    else:
        pid = existing_purchase_id
        db.update_purchase_data(pid, json.dumps(data, ensure_ascii=False))

    # Notify buyer
    if not new_purchase:
        db.mark_purchase_notified(pid)
        await _respond(update_or_query, t(lang, "shop_fill_saved"))
        # Notify admin with updated data
        admin_id = config.ADMIN_ID
        admin_lang = lang_for_user(admin_id)
        buyer_basic = db.get_user_basic(buyer_id_orig)
        buyer_label = format_username(buyer_basic[0] if buyer_basic else None, buyer_id_orig)
        recipient_label = format_username(recipient_username, recipient_id or recipient_username or buyer_id_orig)
        try:
            await update_or_query.get_bot().send_message(
                chat_id=admin_id,
                text=t(admin_lang, "shop_admin_new_purchase", pid=pid, item=name, kind=kind, buyer=buyer_label, recipient=recipient_label, data=json.dumps(data, ensure_ascii=False, indent=2)),
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(t(admin_lang, "shop_admin_mark_done"), callback_data=f"pdone:{pid}")]]),
            )
        except Exception:
            pass
        await send_start_message(update_or_query, lang, user_id)
        return ConversationHandler.END
    else:
        if kind == "ticket":
            await _respond(update_or_query, t(lang, "shop_ticket_done") + "\n" + t(lang, "shop_balance_after", points=db.get_balance(user_id)), reply_markup=game_menu_markup(lang, user_id))
        elif kind == "hoodie":
            await _respond(update_or_query, t(lang, "shop_hoodie_done") + "\n" + t(lang, "shop_balance_after", points=db.get_balance(user_id)), reply_markup=game_menu_markup(lang, user_id))
        elif kind == "bottle":
            await _respond(update_or_query, t(lang, "shop_bottle_done") + "\n" + t(lang, "shop_balance_after", points=db.get_balance(user_id)), reply_markup=game_menu_markup(lang, user_id))

    # Notify recipient if this is a gift and this is a new purchase (not when recipient is filling)
    if new_purchase and (recipient_username or (recipient_id and recipient_id != user_id)):
        bot_username = getattr(context.bot, "username", None) if context and context.bot else None
        recipient_label = format_username(recipient_username, recipient_id or recipient_username or "")
        notified = False
        target_id = recipient_id if recipient_id else None
        if target_id and db.user_exists(target_id):
            try:
                buyer_basic = db.get_user_basic(user_id)
                buyer_username = buyer_basic[0] if buyer_basic else None
                sender_label = format_username(buyer_username, user_id)
                lang_rec = lang_for_user(target_id)
                kb = InlineKeyboardMarkup(
                    [[InlineKeyboardButton(t(lang_rec, "shop_fill_button"), callback_data=f"giftfill:{pid}")]]
                )
                await context.bot.send_message(
                    chat_id=target_id,
                    text=t(lang_rec, "shop_gift_received", item=name, sender=sender_label),
                    reply_markup=kb,
                )
                db.mark_purchase_notified(pid)
                notified = True
            except Exception as e:
                logger.warning("Failed to notify gift recipient %s for purchase %s: %s", target_id, pid, e)
    if notified:
        await _respond(update_or_query, t(lang, "shop_gift_notified_sender", recipient=recipient_label))
    else:
        invite_link = f"https://t.me/{bot_username}" if bot_username else (f"@{recipient_username}" if recipient_username else "")
        rec_display = f"@{recipient_username}" if recipient_username else recipient_label
        await _respond(update_or_query, t(lang, "shop_gift_pending_sender", recipient=rec_display, link=invite_link))

    # Notify admin (buyer id)
    admin_id = config.ADMIN_ID
    try:
        buyer = f"@{update_or_query.effective_user.username}" if update_or_query.effective_user and update_or_query.effective_user.username else str(user_id)
    except Exception:
        buyer = str(user_id)
    recipient_label = f"@{recipient_username}" if recipient_username else (str(recipient_id) if recipient_id else "self")
    admin_lang = lang_for_user(admin_id)
    try:
        await update_or_query.get_bot().send_message(
            chat_id=admin_id,
            text=t(admin_lang, "shop_admin_new_purchase", pid=pid, item=name, kind=kind, buyer=buyer, recipient=recipient_label, data=json.dumps(data, ensure_ascii=False, indent=2)),
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(t(admin_lang, "shop_admin_mark_done"), callback_data=f"pdone:{pid}")]]),
        )
    except Exception:
        pass

    # Clear context
    for key in ["shop_item", "shop_data", "shop_recipient_id", "shop_recipient_username", "shop_contact"]:
        context.user_data.pop(key, None)
    return ConversationHandler.END


async def admin_purchases(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = ensure_user_context(update)
    if not is_admin(user_id):
        return
    lang = lang_for_user(user_id)
    rows = db.list_pending_purchases()
    if not rows:
        await update.message.reply_text(t(lang, "shop_purchases_empty"))
        return
    for pid, buyer_id, item_id, recipient_id, recipient_username, data, status, created_at, name, kind, price in rows:
        text = t(
            lang,
            "shop_admin_new_purchase",
            pid=pid,
            item=name,
            kind=kind,
            buyer=buyer_id,
            recipient=recipient_username or recipient_id or "self",
            data=data,
        )
        kb = InlineKeyboardMarkup([[InlineKeyboardButton(t(lang, "shop_admin_mark_done"), callback_data=f"pdone:{pid}")]])
        await update.message.reply_text(text, reply_markup=kb)


async def admin_purchase_done(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    user_id = ensure_user_context(query)
    if not is_admin(user_id):
        return
    _, pid_str = query.data.split(":")
    pid = int(pid_str)
    db.update_purchase_status(pid, "done")
    lang = lang_for_user(user_id)
    await query.message.reply_text(t(lang, "shop_admin_done", pid=pid))



async def buy_item(update_or_query, item_id: int) -> None:
    # Deprecated: replaced by new shop flow
    await _respond(update_or_query, "Use the new shop buttons.", reply_markup=game_menu_markup(lang_for_user(
        update_or_query.effective_user.id if isinstance(update_or_query, Update) else update_or_query.from_user.id
    )))


def build_rating_keyboard(cid: int) -> InlineKeyboardMarkup:
    row1 = [
        InlineKeyboardButton(str(i), callback_data=f"rate:{cid}:{i}") for i in range(1, 6)
    ]
    row2 = [
        InlineKeyboardButton(str(i), callback_data=f"rate:{cid}:{i}") for i in range(6, 11)
    ]
    return InlineKeyboardMarkup([row1, row2])


async def send_next_rating_candidate(update_or_query, user_id: int, target_gender: Optional[str]):
    lang = lang_for_user(user_id)
    row = db.get_next_unrated_candidate(user_id, target_gender)
    if not row:
        approved_count = db.get_approved_count(target_gender)
        text = (
            t(lang, "no_opposite_candidates")
            if approved_count == 0
            else t(lang, "all_rated")
        )
        if isinstance(update_or_query, Update):
            await update_or_query.message.reply_text(
                text, reply_markup=game_promo_keyboard(lang)
            )
        else:
            if update_or_query.message and update_or_query.message.photo:
                await update_or_query.edit_message_caption(
                    caption=text,
                    reply_markup=game_promo_keyboard(lang),
                )
            else:
                await update_or_query.message.reply_text(
                    text, reply_markup=game_promo_keyboard(lang)
                )
        return

    cid, name, age, gender, instagram, photo_id = row
    first_name, _ = split_name(name)
    caption_lines = [
        t(lang, "participant_number", cid=cid),
        t(lang, "label_name", name=first_name),
        t(lang, "label_age", age=age),
        t(lang, "label_gender", gender=gender),
        t(lang, "label_instagram", instagram=instagram if instagram else "—"),
        "",
        t(lang, "rate_prompt"),
    ]
    keyboard = build_rating_keyboard(cid)

    if isinstance(update_or_query, Update):
        await update_or_query.message.reply_photo(
            photo=photo_id,
            caption="\n".join(caption_lines),
            reply_markup=keyboard,
        )
    else:
        if update_or_query.message and update_or_query.message.photo:
            await update_or_query.edit_message_media(
                media=InputMediaPhoto(
                    media=photo_id,
                    caption="\n".join(caption_lines),
                ),
                reply_markup=keyboard,
            )
        else:
            await update_or_query.message.reply_photo(
                photo=photo_id,
                caption="\n".join(caption_lines),
                reply_markup=keyboard,
            )


async def vote_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = ensure_user_context(update)
    mode = db.get_mode()
    lang = lang_for_user(user_id)
    if mode != "vote" or not db.is_feature_enabled("voting"):
        await update.message.reply_text(t(lang, "voting_not_open_yet"))
        await send_start_message(update, lang, user_id)
        return
    voter_gender = db.get_user_gender(user_id)
    if not voter_gender:
        profile = db.get_profile(user_id)
        if profile and profile[2]:
            voter_gender = profile[2]
            db.set_user_gender(user_id, voter_gender)
    if not voter_gender:
        keyboard = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        f"👦 {t(lang, 'gender_button_prompt')}",
                        callback_data="votergender:Male",
                    ),
                    InlineKeyboardButton(
                        f"👧 {t(lang, 'gender_button_prompt_female')}",
                        callback_data="votergender:Female",
                    ),
                ]
            ]
        )
        await update.message.reply_text(
            t(lang, "gender_prompt_vote"),
            reply_markup=keyboard,
        )
        return

    target_gender = target_gender_for_voter(voter_gender)
    if not target_gender:
        keyboard = InlineKeyboardMarkup(
            [
                [
                    InlineKeyboardButton(
                        f"👦 {t(lang, 'gender_button_prompt')}",
                        callback_data="votergender:Male",
                    ),
                    InlineKeyboardButton(
                        f"👧 {t(lang, 'gender_button_prompt_female')}",
                        callback_data="votergender:Female",
                    ),
                ]
            ]
        )
        await update.message.reply_text(t(lang, "gender_choose_valid"), reply_markup=keyboard)
        return

    await send_next_rating_candidate(update, user_id, target_gender)


async def results(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_user_context(update)
    if not is_admin(update.effective_user.id):
        return
    lang = lang_for_user(update.effective_user.id)

    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(
        """
        SELECT c.id, c.name,
               COUNT(v.id) as votes,
               AVG(v.rating) as avg_rating
        FROM candidates c
        LEFT JOIN votes v ON c.id = v.candidate_id
        WHERE c.approved = 1
        GROUP BY c.id, c.name
        ORDER BY (avg_rating IS NULL), avg_rating DESC, votes DESC
        LIMIT 10
        """
    )
    rows = cur.fetchall()
    conn.close()

    if not rows:
        await update.message.reply_text(t(lang, "no_votes_yet"))
        return

    lines = []
    for cid, name, votes, avg in rows:
        if votes == 0 or avg is None:
            lines.append(t(lang, "results_line_no_ratings", cid=cid, name=name))
        else:
            lines.append(t(lang, "results_line_rated", cid=cid, name=name, avg=avg, votes=votes))

    await update.message.reply_text("\n".join(lines))


async def broadcast_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_user_context(update)
    if not is_admin(update.effective_user.id):
        return
    lang = lang_for_user(update.effective_user.id)
    if not context.args:
        await update.message.reply_text(t(lang, "broadcast_usage"))
        return
    message = " ".join(context.args)
    user_ids = db.get_all_user_ids()
    sent = 0
    failed = 0
    for uid in user_ids:
        try:
            await context.application.bot.send_message(chat_id=uid, text=message)
            sent += 1
        except Exception as e:
            failed += 1
            logger.warning("Broadcast failed to %s: %s", uid, e)
    summary = t(lang, "broadcast_sent", count=sent)
    if failed:
        summary += " " + t(lang, "broadcast_failed", failed=failed)
    await update.message.reply_text(summary)


async def edit_candidate_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    ensure_user_context(update)
    if not is_admin(update.effective_user.id):
        return
    lang = lang_for_user(update.effective_user.id)
    if len(context.args) < 3:
        await update.message.reply_text(t(lang, "edit_usage"))
        return
    try:
        cid = int(context.args[0])
    except ValueError:
        await update.message.reply_text(t(lang, "candidate_id_must_be_number"))
        return
    field = context.args[1].lower()
    value = " ".join(context.args[2:])

    allowed_fields = {"name", "age", "gender", "instagram"}
    if field not in allowed_fields:
        await update.message.reply_text(t(lang, "edit_invalid_field"))
        return

    if field == "age":
        if not value.isdigit():
            await update.message.reply_text(t(lang, "age_not_number"))
            return
        value = int(value)

    if field == "gender":
        if value.lower() in ["male", "мужчина", "чоловік", "m"]:
            value = "Male"
        elif value.lower() in ["female", "женщина", "жінка", "f"]:
            value = "Female"
        else:
            await update.message.reply_text(t(lang, "gender_invalid_choice"))
            return

    conn = sqlite3.connect(config.DB_PATH)
    cur = conn.cursor()
    cur.execute(f"UPDATE candidates SET {field} = ? WHERE id = ?", (value, cid))
    conn.commit()
    conn.close()
    await update.message.reply_text(t(lang, "edit_ok", cid=cid))


async def give_points_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin: adjust a user's wallet balance (can be negative)."""
    ensure_user_context(update)
    if not is_admin(update.effective_user.id):
        return
    lang = lang_for_user(update.effective_user.id)
    if len(context.args) < 2:
        await update.message.reply_text("Usage: /givepoints <user_id> <amount>")
        return
    try:
        target_id = int(context.args[0])
        delta = int(context.args[1])
    except ValueError:
        await update.message.reply_text(t(lang, "user_id_must_be_number"))
        return
    new_balance = db.adjust_balance(target_id, delta)
    await update.message.reply_text(
        t(lang, "admin_points_given", user_id=target_id, balance=new_balance, delta=delta)
    )


async def sync_boosts_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin: sync channel boosts and credit bonuses."""
    ensure_user_context(update)
    if not is_admin(update.effective_user.id):
        return
    lang = lang_for_user(update.effective_user.id)
    # Short-circuit if we already learned this Bot API doesn't support getChatBoosts
    if context.bot_data.get("boosts_not_supported"):
        await _respond(update, t(lang, "boost_not_supported"))
        return
    if config.CHANNEL_ID in (0, -1000000000000):
        await _respond(update, "Set CHANNEL_ID in config.py to your channel chat id first.")
        return
    try:
        await context.bot.get_chat(config.CHANNEL_ID)
    except Exception as e:
        logger.warning("Boost sync: cannot access channel %s: %s", config.CHANNEL_ID, e)
        await _respond(
            update,
            t(lang, "boost_sync_error")
            + "\nCheck CHANNEL_ID and ensure the bot is an admin in that channel.",
        )
        return
    async def fetch_boosts():
        # PTB 20.7+ has get_chat_boosts; older versions need a raw call.
        if hasattr(context.bot, "get_chat_boosts"):
            boosts_obj = await context.bot.get_chat_boosts(config.CHANNEL_ID)
            return getattr(boosts_obj, "boosts", boosts_obj) or []
        return await context.bot._post("getChatBoosts", data={"chat_id": config.CHANNEL_ID})

    try:
        raw_boosts = await fetch_boosts() or []
    except Exception as e:
        logger.warning("Boost sync failed: %s", e)
        msg = t(lang, "boost_sync_error")
        if "Not Found" in str(e):
            msg += "\nThe Bot API server may not support getChatBoosts yet (needs Bot API 6.9+), or CHANNEL_ID is wrong."
        await _respond(update, msg)
        # If method is missing, remember to avoid spamming next time
        if "method not found" in str(e).lower():
            context.bot_data["boosts_not_supported"] = True
        return

    # Normalize different return shapes
    if isinstance(raw_boosts, dict):
        boosts = raw_boosts.get("result", []) or []
    else:
        boosts = list(raw_boosts)

    credited = 0
    skipped = 0
    for boost in boosts:
        boost_id = (
            getattr(boost, "boost_id", None)
            or getattr(boost, "id", None)
            or (boost.get("boost_id") if isinstance(boost, dict) else None)
            or (boost.get("id") if isinstance(boost, dict) else None)
        )
        if not boost_id:
            continue
        if db.boost_exists(boost_id):
            skipped += 1
            continue
        source = getattr(boost, "source", None) if not isinstance(boost, dict) else boost.get("source")
        user_obj = getattr(source, "user", None) if source and not isinstance(source, dict) else (
            source.get("user") if isinstance(source, dict) else None
        )
        user_id = (
            user_obj.id
            if user_obj and hasattr(user_obj, "id")
            else (user_obj.get("id") if isinstance(user_obj, dict) else None)
        )
        db.add_boost(boost_id, user_id)
        if user_id:
            db.adjust_balance(user_id, config.BOOST_BONUS)
            try:
                user_lang = lang_for_user(user_id)
                await context.bot.send_message(
                    chat_id=user_id,
                    text=t(user_lang, "boost_reward_user", bonus=config.BOOST_BONUS),
                )
            except Exception as notify_err:
                logger.warning("Failed to notify booster %s: %s", user_id, notify_err)
            db.mark_boost_credited(boost_id)
            credited += 1
        else:
            skipped += 1

    await _respond(update, t(lang, "boost_sync_ok", credited=credited, skipped=skipped))

# ---------- Admin ops ----------


async def op_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Primary admin: /op <user_id> [remove|list]"""
    ensure_user_context(update)
    if update.effective_user.id != config.ADMIN_ID:
        return
    lang = lang_for_user(update.effective_user.id)
    if not context.args:
        await update.message.reply_text(t(lang, "op_usage"))
        return
    if context.args[0].lower() == "list":
        ids = db.list_admin_users()
        await update.message.reply_text(t(lang, "op_list", ids=", ".join(map(str, ids)) or "—"))
        return
    try:
        target_id = int(context.args[0])
    except ValueError:
        await update.message.reply_text(t(lang, "op_usage"))
        return
    if len(context.args) >= 2 and context.args[1].lower() == "remove":
        db.remove_admin_user(target_id)
        await update.message.reply_text(t(lang, "op_removed", user_id=target_id))
    else:
        db.add_admin_user(target_id)
        await update.message.reply_text(t(lang, "op_added", user_id=target_id))


async def reset_shows_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin: clear dating swipes for a user so they can re-browse."""
    ensure_user_context(update)
    if not is_admin(update.effective_user.id):
        return
    lang = lang_for_user(update.effective_user.id)
    target_user_id = update.effective_user.id
    if context.args:
        try:
            target_user_id = int(context.args[0])
        except ValueError:
            await update.message.reply_text(t(lang, "user_id_must_be_number"))
            return
    if len(context.args) < 2 or context.args[1].upper() != "CONFIRM":
        await update.message.reply_text(t(lang, "reset_shows_confirm", user_id=target_user_id))
        return
    db.reset_swipes_for_user(target_user_id)
    await update.message.reply_text(t(lang, "swipes_reset_user", user_id=target_user_id))


async def reset_shows_all_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin: clear all dating swipes for everyone."""
    ensure_user_context(update)
    if not is_admin(update.effective_user.id):
        return
    lang = lang_for_user(update.effective_user.id)
    if not context.args or context.args[0].upper() != "CONFIRM":
        await update.message.reply_text(t(lang, "reset_shows_all_confirm"))
        return
    db.reset_swipes_all()
    await update.message.reply_text(t(lang, "swipes_reset_all"))


async def wipe_profiles_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Admin: wipe all finder profiles/media/swipes/matches (test reset) – guarded."""
    ensure_user_context(update)
    if not is_admin(update.effective_user.id):
        return
    lang = lang_for_user(update.effective_user.id)
    # Require explicit ALL to avoid accidental wipes
    if not context.args or context.args[0].upper() != "ALL":
        await update.message.reply_text(
            t(lang, "wipe_profiles_confirm", cid="ALL")
        )
        return
    db.wipe_tinder_profiles()
    await update.message.reply_text(t(lang, "tinder_wiped"))


async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    try:
        await query.answer()
    except Exception:
        pass
    data = query.data
    user_id = ensure_user_context(query)

    try:
        # --- game menu shortcuts ---
        if data == "game:menu":
            await send_game_menu(query)
            return
        if data == "game:checkin":
            await handle_checkin(query)
            return
        if data == "game:balance":
            await show_balance(query)
            return
        if data == "game:earn":
            await send_earn_points(query, context)
            return
        if data == "santa:start":
            await santa_start(query, context)
            return
        if data == "santa:edit":
            await santa_edit_start(query, context)
            return
        if data == "earn:ref":
            lang = lang_for_user(user_id)
            if not db.is_feature_enabled("referral"):
                await _respond(query, t(lang, "feature_disabled"))
                return
            bot_username = getattr(context.bot, "username", None)
            if bot_username:
                link = f"https://t.me/{bot_username}?start=ref{user_id}"
            else:
                link = "/start ref<your_id>"
            stats = db.get_user_stats(user_id)
            ref_text = (
                t(lang, "earn_referral", bonus=config.REFERRAL_BONUS, link=link)
                + "\n"
                + t(lang, "earn_referral_note")
                + "\n\n"
                + t(lang, "earn_referrals_count", count=stats.get("referrals", 0))
            )
            await _respond(query, ref_text)
            return
        if data == "earn:boost":
            lang = lang_for_user(user_id)
            if not db.is_feature_enabled("boost"):
                await _respond(query, t(lang, "feature_disabled"))
                return
            channel = config.TELEGRAM_PROMO_URL or ""
            stats = db.get_user_stats(user_id)
            boost_text = t(
                lang,
                "earn_boost_info",
                channel=channel,
                boosts=stats.get("boosts_credited", 0),
            )
            await _respond(query, boost_text)
            return
        if data == "game:blackjack":
            await start_blackjack(query, context)
            return
        if data == "game:shop":
            await show_shop(query)
            return
        if data == "game:home":
            lang = lang_for_user(user_id)
            await send_start_message(query, lang, user_id)
            return

        # --- contest profile actions ---
        if data == "submit:useprofile":
            lang = lang_for_user(user_id)
            if db.get_mode() != "collect":
                await query.message.reply_text(t(lang, "applications_closed"))
                return
            if await ensure_candidate_from_profile(query, user_id, lang):
                await query.message.reply_text(t(lang, "submit_profile_used"))
                await send_start_message(query, lang, user_id)
            return
        if data == "submit:editprofile":
            lang = lang_for_user(user_id)
            profile = db.get_profile(user_id)
            if not profile:
                await query.message.reply_text(t(lang, "submit_need_profile"))
                return
            context.user_data["edit_for_submit"] = True
            context.user_data["contest_profile"] = profile
            context.user_data["contest_media"] = db.list_profile_media(user_id)
            await tinder_edit_menu(query, context)
            return
        if data == "submit:saveprofile":
            lang = lang_for_user(user_id)
            if await ensure_candidate_from_contest(user_id, context, lang, query):
                await query.message.reply_text(t(lang, "submit_profile_used"))
                await send_start_message(query, lang, user_id)
            context.user_data.pop("edit_for_submit", None)
            context.user_data.pop("contest_profile", None)
            context.user_data.pop("contest_media", None)
            return
        if data == "submit:withdraw":
            lang = lang_for_user(user_id)
            db.delete_candidate_for_user(user_id)
            await query.message.reply_text(t(lang, "submission_cancel"))
            await send_start_message(query, lang, user_id)
            return
        if data == "submit:menu":
            lang = lang_for_user(user_id)
            await send_start_message(query, lang, user_id)
            return

        # --- tinder-style browsing/edit ---
        if data.startswith("tinder:"):
            _, action = data.split(":")
            lang = lang_for_user(user_id)
            if action == "browse":
                await send_next_candidate_card(query, user_id, lang, context)
            elif action == "edit":
                await tinder_edit_menu(query, context)
            elif action == "save":
                await tinder_save_cb(update, context)
            elif action == "snooze":
                await send_start_message(query, lang, user_id)
                return ConversationHandler.END
            elif action == "viewmedia":
                media = db.list_profile_media(user_id)
                if media:
                    fid, kind = media[0]
                    caption = "Your media"
                    if kind == "video":
                        await query.message.reply_video(video=fid, caption=caption)
                    else:
                        await query.message.reply_photo(photo=fid, caption=caption)
            elif action == "start":
                profile = db.get_profile(user_id)
                if profile:
                    await send_own_profile(query, user_id, lang)
                else:
                    await start_tinder_flow(query, context)
            elif action == "adminreview":
                await tinder_admin_review(query, context)
            return

        # --- swipes ---
        if data.startswith("swipe:"):
            _, target_str, action = data.split(":")
            target_id = int(target_str)
            lang = lang_for_user(user_id)
            if action == "like":
                mutual = db.mark_like_and_check(user_id, target_id)
                if mutual:
                    chat = await context.bot.get_chat(target_id)
                    username = format_username(chat.username, target_id)
                    await query.message.reply_text(t(lang, "tinder_match", username=username))
                    target_lang = lang_for_user(target_id)
                    me_un = query.from_user.username
                    me_username = format_username(me_un, user_id)
                    await context.bot.send_message(
                        chat_id=target_id,
                        text=t(target_lang, "tinder_match", username=me_username),
                    )
                else:
                    target_lang = lang_for_user(target_id)
                    try:
                        await context.bot.send_message(
                            chat_id=target_id,
                            text=t(target_lang, "tinder_new_like"),
                        )
                    except Exception:
                        pass
            elif action == "pass":
                db.mark_swipe(user_id, target_id, "dislike")
            await send_next_candidate_card(query, user_id, lang, context)
            return

        # --- blackjack bets ---
        if data.startswith("bjbet:"):
            _, bet_str = data.split(":")
            await handle_bet_selection(query, context, int(bet_str))
            return

        # --- tinder admin edit menu ---
        if data.startswith("tadmineditmenu:"):
            if not is_admin(user_id):
                return
            _, uid_str = data.split(":")
            target_id = int(uid_str)
            lang = lang_for_user(user_id)
            await admin_edit_menu(query, target_id, lang)
            return

        # --- tinder admin nav ---
        if data.startswith("tadmin:"):
            if not is_admin(user_id):
                return
            _, uid_str, action = data.split(":")
            current_id = int(uid_str)
            lang = lang_for_user(user_id)
            if action == "next":
                next_id = db.get_next_profile_id(current_id)
                if not next_id:
                    await query.answer(t(lang, "tinder_no_more_profiles"), show_alert=True)
                    return
                next_row = db.get_profile(next_id)
                if not next_row:
                    await query.answer(t(lang, "tinder_admin_no_profiles"), show_alert=True)
                    return
                await send_admin_profile(query, next_row, lang)
            elif action == "prev":
                prev_id = db.get_prev_profile_id(current_id)
                if not prev_id:
                    await query.answer(t(lang, "tinder_no_more_profiles"), show_alert=True)
                    return
                prev_row = db.get_profile(prev_id)
                if not prev_row:
                    await query.answer(t(lang, "tinder_admin_no_profiles"), show_alert=True)
                    return
                await send_admin_profile(query, prev_row, lang)
            elif action == "toggle":
                cur = db.get_profile(current_id)
                if not cur:
                    await query.answer(t(lang, "tinder_admin_no_profiles"), show_alert=True)
                    return
                active = cur[10]
                db.set_profile_active(current_id, not active)
                updated = db.get_profile(current_id)
                await send_admin_profile(query, updated, lang)
            elif action == "edit":
                await query.answer()
                await admin_edit_menu(query, current_id, lang)
            elif action == "show":
                row = db.get_profile(current_id)
                await send_admin_profile(query, row, lang)
            elif action == "purchases":
                await admin_purchases(query, context)
            return

        # --- tinder admin field edit ---
        if data.startswith("tadminedit:"):
            if not is_admin(user_id):
                return
            _, uid_str, field = data.split(":")
            target_id = int(uid_str)
            lang = lang_for_user(user_id)
            context.user_data["admin_edit_target"] = target_id
            context.user_data["admin_edit_field"] = field
            prompt_map = {
                "age": "tinder_age_prompt",
                "gender": "tinder_gender_prompt",
                "interest": "tinder_interest_prompt",
                "city": "tinder_city_prompt",
                "name": "tinder_name_prompt",
                "bio": "tinder_bio_prompt",
            }
            key = prompt_map.get(field, "tinder_edit_prompt")
            await query.message.reply_text(t(lang, key))
            return config.TINDER_ADMIN_EDIT

        # --- blackjack actions ---
        if data == "bj:hit":
            await blackjack_hit(query)
            return
        if data == "bj:stand":
            await blackjack_stand(query)
            return
        if data == "bj:double":
            await blackjack_double(query)
            return

        # --- shop buy entry ---
        if data.startswith("shopbuy:"):
            _, item_id = data.split(":")
            return await shop_buy_start(query, context, int(item_id))
        # --- Santa navigation ---
        if data.startswith("santa:nav:"):
            parts = data.split(":")
            if len(parts) != 4:
                return
            _, _, num_str, action = parts
            current = int(num_str)
            lang = lang_for_user(user_id)
            if action == "next":
                nxt = db.get_next_santa_number(current)
                if nxt:
                    await send_santa_card(query, nxt, lang)
                else:
                    await query.answer(t(lang, "last_candidate_alert"), show_alert=True)
            elif action == "prev":
                prv = db.get_prev_santa_number(current)
                if prv:
                    await send_santa_card(query, prv, lang)
                else:
                    await query.answer(t(lang, "first_candidate_alert"), show_alert=True)
            elif action == "stop":
                await query.message.reply_text(t(lang, "submission_cancel"))
            return

        # --- language change ---
        if data.startswith("lang:"):
            _, lang_code = data.split(":", 1)
            db.set_user_language(user_id, lang_code)
            lang = lang_for_user(user_id)
            await query.message.reply_text(t(lang, "language_set"))
            await send_start_message(query, lang, user_id)
            return

        # --- voting start ---
        if data == "start:vote":
            if db.get_mode() != "vote":
                lang = lang_for_user(user_id)
                await query.message.reply_text(
                    t(lang, "voting_not_open_yet"), reply_markup=start_voting_keyboard(lang)
                )
                return
            lang = lang_for_user(user_id)
            voter_gender = db.get_user_gender(user_id)
            if not voter_gender:
                profile = db.get_profile(user_id)
                if profile and profile[2]:
                    voter_gender = profile[2]
                    db.set_user_gender(user_id, voter_gender)
            if not voter_gender:
                keyboard = InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                f"👦 {t(lang, 'gender_button_prompt')}", callback_data="votergender:Male"
                            ),
                            InlineKeyboardButton(
                                f"👧 {t(lang, 'gender_button_prompt_female')}", callback_data="votergender:Female"
                            ),
                        ]
                    ]
                )
                await query.message.reply_text(
                    t(lang, "gender_prompt_vote"),
                    reply_markup=keyboard,
                )
                return
            target_gender = target_gender_for_voter(voter_gender)
            if not target_gender:
                keyboard = InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                f"👦 {t(lang, 'gender_button_prompt')}", callback_data="votergender:Male"
                            ),
                            InlineKeyboardButton(
                                f"👧 {t(lang, 'gender_button_prompt_female')}", callback_data="votergender:Female"
                            ),
                        ]
                    ]
                )
                await query.message.reply_text(t(lang, "gender_choose_valid"), reply_markup=keyboard)
                return
            await send_next_rating_candidate(query, user_id, target_gender)
            return

        # --- voting gender selection ---
        if data.startswith("votergender:"):
            _, gender_value = data.split(":", 1)
            lang = lang_for_user(user_id)
            existing_gender = db.get_user_gender(user_id)
            if existing_gender and not is_admin(user_id):
                target_gender = target_gender_for_voter(existing_gender)
                lock_msg = t(lang, "gender_locked", existing_gender=existing_gender)
                await query.message.reply_text(lock_msg)
                if target_gender:
                    await send_next_rating_candidate(query, user_id, target_gender)
                return
            db.set_user_gender(user_id, gender_value)
            target_gender = target_gender_for_voter(gender_value)
            if not target_gender:
                await query.message.reply_text(t(lang, "gender_choose_valid"))
                return
            lang = lang_for_user(user_id)
            await query.message.reply_text(t(lang, "gender_saved"))
            await send_next_rating_candidate(query, user_id, target_gender)
            return

        # --- voting rate ---
        if data.startswith("rate:"):
            _, cid_str, rating_str = data.split(":")
            cid = int(cid_str)
            rating = int(rating_str)
            voter_gender = db.get_user_gender(user_id)
            target_gender = target_gender_for_voter(voter_gender)
            if not target_gender:
                lang = lang_for_user(user_id)
                keyboard = InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                f"👦 {t(lang, 'gender_button_prompt')}", callback_data="votergender:Male"
                            ),
                            InlineKeyboardButton(
                                f"👧 {t(lang, 'gender_button_prompt_female')}", callback_data="votergender:Female"
                            ),
                        ]
                    ]
                )
                await query.message.reply_text(
                    t(lang, "gender_prompt_vote"),
                    reply_markup=keyboard,
                )
                return
            db.add_vote(user_id, cid, rating)
            await send_next_rating_candidate(query, user_id, target_gender)
            return

        # --- admin candidate nav ---
        if data.startswith("admin:"):
            _, cid_str, action = data.split(":")
            cid = int(cid_str)
            if not is_admin(user_id):
                return
            lang = lang_for_user(user_id)
            if action == "approve":
                db.approve_candidate(cid, 1)
                row = db.get_candidate_by_id(cid)
                if row:
                    await send_admin_candidate(query, row)
            elif action == "reject":
                db.approve_candidate(cid, -1)
                row = db.get_candidate_by_id(cid)
                if row:
                    await send_admin_candidate(query, row)
            elif action == "next":
                next_id = db.get_next_candidate_id(cid)
                if not next_id:
                    await query.answer(t(lang, "last_candidate_alert"), show_alert=True)
                    return
                row = db.get_candidate_by_id(next_id)
                if row:
                    await send_admin_candidate(query, row)
            elif action == "prev":
                prev_id = db.get_prev_candidate_id(cid)
                if not prev_id:
                    await query.answer(t(lang, "first_candidate_alert"), show_alert=True)
                    return
                row = db.get_candidate_by_id(prev_id)
                if row:
                    await send_admin_candidate(query, row)
            return

    except Exception:
        logger.exception("Unhandled callback %s", data)
        try:
            await query.message.reply_text("Oops, something went wrong. Please try again.")
        except Exception:
            pass

    if data.startswith("pdone:"):
        await admin_purchase_done(update, context)
        return

    if data.startswith("usernav:"):
        _, uid_str, action = data.split(":")
        uid = int(uid_str)
        if not is_admin(user_id):
            return
        lang = lang_for_user(user_id)
        target_uid = None
        if action == "next":
            target_uid = db.get_next_user_id(uid)
        elif action == "prev":
            target_uid = db.get_prev_user_id(uid)
        if not target_uid:
            await query.answer(t(lang, "user_not_found", uid=uid), show_alert=True)
            return
        text, kb = render_user_profile(target_uid, lang)
        await _respond(query, text, reply_markup=kb)
        return

    if data.startswith("useract:"):
        if not is_admin(user_id):
            return
        parts = data.split(":")
        uid = int(parts[1])
        action = parts[2]
        lang = lang_for_user(user_id)
        if action == "bal":
            delta = int(parts[3])
            db.adjust_balance(uid, delta)
            await query.answer(t(lang, "user_balance_updated", balance=db.get_balance(uid)))
        elif action == "gender":
            gender_value = parts[3]
            db.set_user_gender(uid, gender_value)
            await query.answer(t(lang, "user_gender_updated", gender=gender_value))
        elif action == "resetvotes":
            db.reset_votes_for_user(uid)
            await query.answer(t(lang, "user_votes_cleared"))
        text, kb = render_user_profile(uid, lang)
        await _respond(query, text, reply_markup=kb)
        return


def register_handlers(application: Application) -> None:
    conv_handler = ConversationHandler(
        entry_points=[
            CommandHandler("submit", submit_start),
            CallbackQueryHandler(submit_start_button, pattern=r"^start:submit$"),
        ],
        per_message=False,
        states={
            config.ENTER_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, submit_name)],
            config.ENTER_AGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, submit_age)],
            config.ENTER_GENDER: [
                CallbackQueryHandler(gender_button, pattern=r"^gender:"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, submit_gender_text),
            ],
            config.ENTER_INSTAGRAM: [
                CallbackQueryHandler(insta_button, pattern=r"^insta:"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, submit_instagram),
            ],
            config.ENTER_PHOTO: [
                MessageHandler(filters.PHOTO, submit_photo),
                MessageHandler(filters.TEXT & ~filters.COMMAND, submit_photo),
            ],
        },
        fallbacks=[CommandHandler("cancel", submit_cancel)],
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(conv_handler)
    application.add_handler(CommandHandler("help", help_command))
    application.add_handler(CommandHandler("language", language_command))
    application.add_handler(CommandHandler("menu", menu_command))
    application.add_handler(CommandHandler("fun", fun_command))

    application.add_handler(CommandHandler("mode_collect", mode_collect))
    application.add_handler(CommandHandler("mode_vote", mode_vote))
    application.add_handler(CommandHandler("mode_pause", mode_pause))
    application.add_handler(CommandHandler("mode_disable", mode_disable))
    application.add_handler(CommandHandler("list_candidates", list_candidates))
    application.add_handler(CommandHandler("review", review))
    application.add_handler(CommandHandler("show", show_candidate_cmd))
    application.add_handler(CommandHandler("approve", approve_cmd))
    application.add_handler(CommandHandler("reject", reject_cmd))
    application.add_handler(CommandHandler("reset_votes", reset_votes_cmd))
    application.add_handler(CommandHandler("reset_vote_admin", reset_votes_admin_cmd))
    application.add_handler(CommandHandler("guest", guest_mode_cmd))
    application.add_handler(CommandHandler("wipe_phantom", wipe_phantom_cmd))
    application.add_handler(CommandHandler("say", broadcast_cmd))
    application.add_handler(CommandHandler("edit", edit_candidate_cmd))
    application.add_handler(CommandHandler("wipe_profile", wipe_profile_cmd))
    application.add_handler(CommandHandler("blackjack", blackjack_command))
    application.add_handler(CommandHandler("grant_boost", grant_boost_points_cmd))
    application.add_handler(CommandHandler("bjboost", blackjack_boost_cmd))
    application.add_handler(CommandHandler("feature", feature_toggle_cmd))
    application.add_handler(CommandHandler("op", op_command))
    santa_conv = ConversationHandler(
        entry_points=[
            CommandHandler("santa", santa_start),
            CallbackQueryHandler(santa_start, pattern=r"^santa:start$"),
            CallbackQueryHandler(santa_edit_start, pattern=r"^santa:edit$"),
        ],
        per_message=False,
        states={
            config.SANTA_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, santa_name_step)],
            config.SANTA_GIFT: [MessageHandler(filters.PHOTO, santa_insta_step), MessageHandler(filters.TEXT & ~filters.COMMAND, santa_insta_step)],
        },
        fallbacks=[CommandHandler("cancel", santa_cancel)],
    )
    application.add_handler(santa_conv)
    application.add_handler(CommandHandler("lookup", santa_lookup_cmd))
    application.add_handler(CommandHandler("shop", shop_command))
    application.add_handler(CommandHandler("balance", balance_command))
    application.add_handler(CommandHandler("review_profiles", tinder_admin_review))
    application.add_handler(CommandHandler("purchases", admin_purchases))
    application.add_handler(CommandHandler("givepoints", give_points_cmd))
    application.add_handler(CommandHandler("sync_boosts", sync_boosts_cmd))
    application.add_handler(CommandHandler("reset_shows", reset_shows_cmd))
    application.add_handler(CommandHandler("reset_shows_all", reset_shows_all_cmd))
    application.add_handler(CommandHandler("wipe_profiles", wipe_profiles_cmd))
    application.add_handler(CommandHandler("lookup", santa_lookup_cmd))
    application.add_handler(CommandHandler("profile", myprofile_command))
    application.add_handler(CommandHandler("list_users", list_users_cmd))
    application.add_handler(CommandHandler("user", user_info_cmd))
    application.add_handler(CommandHandler("results", results))
    shop_conv = ConversationHandler(
        entry_points=[
            CallbackQueryHandler(shop_buy_entry, pattern=r"^shopbuy:"),
            CallbackQueryHandler(gift_fill_start, pattern=r"^giftfill:"),
        ],
        per_message=False,
        states={
            config.SHOP_KEEP_OR_GIFT: [
                CallbackQueryHandler(shop_keep_gift_cb, pattern=r"^shop:(keep|gift)$"),
                CallbackQueryHandler(shop_use_contact_cb, pattern=r"^shop:(usecontact|nocontact)$"),
            ],
            config.SHOP_RECIPIENT: [MessageHandler(filters.TEXT & ~filters.COMMAND, shop_recipient_text)],
            config.SHOP_FILL_WHO: [
                CallbackQueryHandler(shop_fill_who_cb, pattern=r"^shopfill:(self|recipient)$"),
            ],
            config.SHOP_FILL_CONTACT: [
                CallbackQueryHandler(gift_contact_choice_cb, pattern=r"^giftcontact:(use|new)$"),
            ],
            config.SHOP_NAME: [
                CallbackQueryHandler(shop_use_contact_cb, pattern=r"^shop:(usecontact|nocontact)$"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, shop_name_text),
            ],
            config.SHOP_EMAIL: [MessageHandler(filters.TEXT & ~filters.COMMAND, shop_email_text)],
            config.SHOP_SIZE: [MessageHandler(filters.TEXT & ~filters.COMMAND, shop_size_text)],
            config.SHOP_ADDRESS: [MessageHandler(filters.TEXT & ~filters.COMMAND, shop_address_text)],
        },
        fallbacks=[CommandHandler("cancel", submit_cancel)],
    )
    application.add_handler(shop_conv)
    # Tinder-style profile flow
    tinder_conv = ConversationHandler(
        entry_points=[
            CommandHandler("myprofile", myprofile_command),
            CommandHandler("find", find_command),
            CallbackQueryHandler(tinder_start_cb, pattern=r"^tinder:start$"),
            CallbackQueryHandler(tinder_edit_dispatch, pattern=r"^tedit:"),
        ],
        per_message=False,
        states={
            config.TINDER_AGE: [MessageHandler(filters.TEXT & ~filters.COMMAND, tinder_age)],
            config.TINDER_GENDER: [
                CallbackQueryHandler(tinder_gender_cb, pattern=r"^tinder_gender:"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, tinder_gender_text),
            ],
            config.TINDER_INTEREST: [
                CallbackQueryHandler(tinder_interest_cb, pattern=r"^tinder_interest:"),
                MessageHandler(filters.TEXT & ~filters.COMMAND, tinder_interest_text),
            ],
            config.TINDER_CITY: [
                MessageHandler(filters.LOCATION, tinder_city),
                MessageHandler(filters.TEXT & ~filters.COMMAND, tinder_city),
            ],
            config.TINDER_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, tinder_name)],
            config.TINDER_BIO: [MessageHandler(filters.TEXT & ~filters.COMMAND, tinder_bio)],
            config.TINDER_MEDIA: [
                MessageHandler(filters.PHOTO | filters.VIDEO, tinder_media),
                MessageHandler(filters.TEXT & ~filters.COMMAND, tinder_media_text),
                CommandHandler("done", tinder_confirm),
            ],
            config.TINDER_CONFIRM: [
                CallbackQueryHandler(tinder_save_cb, pattern=r"^tinder:save$"),
                CallbackQueryHandler(tinder_edit_menu, pattern=r"^tinder:edit$"),
            ],
        },
        fallbacks=[CommandHandler("cancel", submit_cancel)],
    )
    application.add_handler(tinder_conv)
    # Admin edit (finder profiles)
    admin_edit_conv = ConversationHandler(
        entry_points=[CallbackQueryHandler(button, pattern=r"^tadminedit:")],
        per_message=False,
        states={
            config.TINDER_ADMIN_EDIT: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, admin_edit_text),
            ],
        },
        fallbacks=[],
    )
    application.add_handler(admin_edit_conv)
    application.add_handler(MessageHandler(filters.Regex("^(❤️|💔|😴|🔍|📥)$"), tinder_swipe_text))
    application.add_handler(CommandHandler("find", find_command))
    application.add_handler(CommandHandler("likes", likes_command))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, quick_reply_handler))

    application.add_handler(CommandHandler("vote", vote_command))

    application.add_handler(CallbackQueryHandler(button))
