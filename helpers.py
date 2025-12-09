import config
import db
from translations import LANGUAGE_OPTIONS
import requests
import sqlite3
from functools import lru_cache


def is_admin(user_id: int) -> bool:
    if config.ADMIN_GUEST_MODE:
        return False
    if user_id == config.ADMIN_ID:
        return True
    return db.is_admin_user(user_id)


def lang_for_user(user_id: int) -> str:
    return db.get_user_language(user_id) if user_id else "en"


def target_gender_for_voter(gender):
    if gender == "Male":
        return "Female"
    if gender == "Female":
        return "Male"
    return None


def split_name(full_name: str) -> tuple[str, str]:
    parts = full_name.split()
    if len(parts) >= 2:
        return parts[0], " ".join(parts[1:])
    return parts[0], ""


def normalize_city(text: str) -> str:
    """Basic city normalization: lower, strip, remove extra spaces, simple Cyrillic->Latin."""
    if not text:
        return ""
    txt = text.strip().lower()
    # Replace common Cyrillic letters with Latin lookalikes for coarse matching
    translit_map = {
        "а": "a",
        "б": "b",
        "в": "v",
        "г": "g",
        "ґ": "g",
        "д": "d",
        "е": "e",
        "є": "e",
        "ё": "e",
        "ж": "zh",
        "з": "z",
        "и": "i",
        "і": "i",
        "ї": "i",
        "й": "i",
        "к": "k",
        "л": "l",
        "м": "m",
        "н": "n",
        "о": "o",
        "п": "p",
        "р": "r",
        "с": "s",
        "т": "t",
        "у": "u",
        "ф": "f",
        "х": "h",
        "ц": "c",
        "ч": "ch",
        "ш": "sh",
        "щ": "sh",
        "ы": "y",
        "э": "e",
        "ю": "u",
        "я": "ya",
    }
    normalized = []
    for ch in txt:
        normalized.append(translit_map.get(ch, ch))
    txt = "".join(normalized)
    # Collapse multiple spaces and punctuation to single spaces
    out = []
    prev_space = False
    for ch in txt:
        if ch.isalnum():
            out.append(ch)
            prev_space = False
        else:
            if not prev_space:
                out.append(" ")
            prev_space = True
    collapsed = " ".join("".join(out).split())
    return collapsed


def looks_like_coord(value: str) -> bool:
    if not value:
        return False
    v = value.strip().lower()
    if v.startswith("geo:"):
        return True
    digit_ratio = sum(ch.isdigit() for ch in v) / max(len(v), 1)
    return digit_ratio > 0.3 or any(ch in v for ch in [",", ".", ":"])


_GEOCODE_CACHE: dict[tuple[float, float], tuple[str, str]] = {}


def reverse_geocode_city(lat: float, lon: float) -> tuple[str, str]:
    """Best-effort reverse geocode; returns (city_label, normalized_city)."""
    try:
        lat_f = float(lat)
        lon_f = float(lon)
    except Exception:
        return str(lat), normalize_city(str(lat))
    key = (round(lat_f, 4), round(lon_f, 4))
    if key in _GEOCODE_CACHE:
        return _GEOCODE_CACHE[key]
    try:
        resp = requests.get(
            "https://nominatim.openstreetmap.org/reverse",
            params={"format": "json", "lat": lat_f, "lon": lon_f, "zoom": 10, "addressdetails": 1},
            headers={"User-Agent": "winter-ballet-bot/1.0"},
            timeout=5,
        )
        if resp.ok:
            data = resp.json()
            addr = data.get("address", {})
            city = addr.get("city") or addr.get("town") or addr.get("village") or addr.get("municipality")
            state = addr.get("state")
            country = addr.get("country")
            label = ", ".join([p for p in [city, state, country] if p])
            if label:
                result = (label, normalize_city(city or label))
                _GEOCODE_CACHE[key] = result
                return result
    except Exception:
        pass
    # Fallback to coords text
    label = f"{lat_f:.4f},{lon_f:.4f}"
    result = (label, f"geo:{round(lat_f,1)}:{round(lon_f,1)}")
    _GEOCODE_CACHE[key] = result
    return result


def parse_lat_lon(text: str):
    """Extract lat/lon from common string patterns; returns (lat, lon) or (None, None)."""
    if not text:
        return None, None
    txt = text.strip().lower().replace("geo:", "")
    for sep in [",", ":", " "]:
        if sep in txt:
            parts = txt.split(sep)
            if len(parts) >= 2:
                try:
                    lat = float(parts[0])
                    lon = float(parts[1])
                    return lat, lon
                except Exception:
                    continue
    return None, None


def record_user(user) -> None:
    """Persist basic user info and seed language if missing."""
    if not user:
        return
    db.upsert_user_basic(
        user_id=user.id,
        username=user.username,
        first_name=user.first_name,
        last_name=user.last_name,
        language_code=user.language_code,
    )
    # Seed language preference if not set and Telegram language_code is supported.
    existing_lang = db.get_user_language(user.id)
    if not existing_lang and user.language_code in LANGUAGE_OPTIONS:
        db.set_user_language(user.id, user.language_code)


def repair_tinder_profiles():
    """Fix legacy Tinder profiles that were saved with mis-ordered fields."""
    try:
        conn = sqlite3.connect(config.DB_PATH)
        cur = conn.cursor()
        rows = cur.execute(
            """
            SELECT user_id, age, gender, interest, city, lat, lon, name, bio, normalized_city
            FROM tinder_profiles
            """
        ).fetchall()
    except sqlite3.OperationalError:
        return

    def extract_coords(values):
        """Return (lat, lon) from any mix of geo strings or raw numbers."""
        # First pass: geo:lat:lon formatted strings
        for v in values:
            if v is None:
                continue
            lat_p, lon_p = parse_lat_lon(str(v))
            if lat_p is not None and lon_p is not None:
                return lat_p, lon_p

        nums = []
        for v in values:
            if v is None:
                continue
            try:
                val = float(str(v))
                if val not in nums:
                    nums.append(val)
            except Exception:
                continue
        if len(nums) >= 2:
            return nums[0], nums[1]
        if len(nums) == 1:
            return nums[0], None
        return None, None

    repaired = 0
    for row in rows:
        user_id, age, gender, interest, city_raw, lat_raw, lon_raw, name_raw, bio_raw, norm_raw = row

        # Restore coordinates (lat was saved into name, lon into bio in legacy bug)
        lat_val, lon_val = extract_coords([lat_raw, lon_raw, name_raw, bio_raw, city_raw])
        # If only one coordinate is available, prefer explicit lon_raw/lat_raw to avoid swapping.
        if lat_val is not None and lon_val is None and lon_raw:
            try:
                lon_val = float(lon_raw)
            except Exception:
                pass
        if lon_val is not None and lat_val is None and lat_raw:
            try:
                lat_val = float(lat_raw)
            except Exception:
                pass

        # Restore city: prefer existing city, else any non-coordinate text, else reverse geocode
        city_val = city_raw
        if not city_val or looks_like_coord(str(city_val)):
            for candidate in [lon_raw, norm_raw, lat_raw, bio_raw, name_raw]:
                if candidate and not looks_like_coord(str(candidate)):
                    city_val = str(candidate)
                    break
        if (not city_val or looks_like_coord(str(city_val))) and lat_val is not None and lon_val is not None:
            rev_label, rev_norm = reverse_geocode_city(lat_val, lon_val)
            city_val = rev_label
            norm_raw = rev_norm
        norm_city = normalize_city(city_val or "")

        # Restore name: pick non-coordinate text; fall back to stored user name
        name_val = None
        for candidate in [name_raw, lat_raw]:
            if candidate and not looks_like_coord(str(candidate)):
                name_val = str(candidate)
                break
        if not name_val:
            basic = db.get_user_basic(user_id)
            if basic:
                username, first_name, last_name = basic
                name_val = " ".join([p for p in [first_name, last_name] if p]) or (f"@{username}" if username else "")
        name_val = name_val or "—"

        # Restore bio: prioritize original bio column, then legacy normalized_city when it wasn't a city
        normalized_of_city = normalize_city(city_raw or "")
        bio_val = None
        if bio_raw and not looks_like_coord(str(bio_raw)) and normalize_city(str(bio_raw)) != normalized_of_city:
            bio_val = str(bio_raw)
        if not bio_val and norm_raw and not looks_like_coord(str(norm_raw)) and normalize_city(str(norm_raw)) != normalized_of_city:
            bio_val = str(norm_raw)
        bio_val = bio_val or "—"

        db.upsert_profile(user_id, age, gender, interest, city_val or "—", lat_val, lon_val, name_val, bio_val, norm_city)
        repaired += 1

    conn.close()
    if repaired:
        print(f"[repair] Tinder profiles repaired: {repaired}")
