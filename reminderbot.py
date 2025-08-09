import os
import json
import uuid
import logging
import random
import statistics
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import asyncio

import pytz
from contextlib import suppress
import gspread
import dateparser
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)


# ============================
# Configuration & Constants
# ============================

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger("reminderbot")

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_IDS = {
    int(x.strip())
    for x in os.getenv("ADMIN_IDS", "").split(",")
    if x.strip().isdigit()
}

DATA_DIR = Path("data")
USERS_FILE = DATA_DIR / "users.json"
REMINDERS_FILE = DATA_DIR / "reminders.json"
CODES_FILE = DATA_DIR / "codes.json"
SETTINGS_FILE = DATA_DIR / "settings.json"
TEMPLATES_FILE = DATA_DIR / "templates.json"

# Reminder Templates
DEFAULT_TEMPLATES = {
    "medicine": {
        "id": "medicine",
        "name": "💊 Take Medicine",
        "text": "Time to take your medicine",
        "category": "health",
        "suggested_times": ["08:00", "12:00", "18:00", "22:00"],
        "icon": "💊"
    },
    "workout": {
        "id": "workout",
        "name": "🏋️ Workout Session",
        "text": "Time for your workout!",
        "category": "health",
        "suggested_times": ["06:00", "07:00", "17:00", "18:00"],
        "icon": "🏋️"
    },
    "meeting": {
        "id": "meeting",
        "name": "📅 Meeting Reminder",
        "text": "You have a meeting coming up",
        "category": "work",
        "suggested_times": ["09:00", "10:00", "14:00", "15:00"],
        "icon": "📅"
    },
    "water": {
        "id": "water",
        "name": "💧 Drink Water",
        "text": "Stay hydrated! Time to drink water",
        "category": "health",
        "suggested_times": ["09:00", "11:00", "13:00", "15:00", "17:00"],
        "icon": "💧"
    },
    "break": {
        "id": "break",
        "name": "☕ Take a Break",
        "text": "Time for a well-deserved break!",
        "category": "work",
        "suggested_times": ["10:00", "15:00"],
        "icon": "☕"
    },
    "sleep": {
        "id": "sleep",
        "name": "🌙 Bedtime",
        "text": "Time to wind down and get ready for bed",
        "category": "personal",
        "suggested_times": ["21:00", "22:00", "23:00"],
        "icon": "🌙"
    }
}

# Reminder Categories
REMINDER_CATEGORIES = {
    "work": {"name": "💼 Work", "color": "#3498db", "icon": "💼"},
    "personal": {"name": "🏠 Personal", "color": "#9b59b6", "icon": "🏠"},
    "health": {"name": "🏥 Health", "color": "#e74c3c", "icon": "🏥"},
    "finance": {"name": "💰 Finance", "color": "#f39c12", "icon": "💰"},
    "social": {"name": "👥 Social", "color": "#2ecc71", "icon": "👥"},
    "learning": {"name": "📚 Learning", "color": "#e67e22", "icon": "📚"},
    "other": {"name": "📌 Other", "color": "#95a5a6", "icon": "📌"}
}

# AI Suggestion Patterns
AI_PATTERNS = {
    "habit_tracking": {
        "keywords": ["daily", "every day", "habit", "routine"],
        "suggestions": {
            "optimal_times": ["07:00", "21:00"],
            "category": "personal",
            "recurring": {"type": "daily", "interval": 1}
        }
    },
    "medication": {
        "keywords": ["medicine", "pill", "medication", "dose", "tablet"],
        "suggestions": {
            "optimal_times": ["08:00", "12:00", "18:00", "22:00"],
            "category": "health",
            "priority": 4
        }
    },
    "meetings": {
        "keywords": ["meeting", "call", "conference", "interview"],
        "suggestions": {
            "optimal_times": ["09:00", "10:00", "14:00", "15:00"],
            "category": "work",
            "priority": 3
        }
    },
    "deadlines": {
        "keywords": ["deadline", "due", "submit", "project"],
        "suggestions": {
            "optimal_times": ["09:00", "14:00"],
            "category": "work",
            "priority": 5
        }
    }
}

# Export formats for backup
EXPORT_FORMATS = {
    "json": {"name": "JSON Format", "extension": ".json"},
    "csv": {"name": "CSV Spreadsheet", "extension": ".csv"},
    "txt": {"name": "Plain Text", "extension": ".txt"}
}


# Defaults and policy
DEFAULT_TIMEZONE = "UTC"
FREE_TIER_MAX_ACTIVE = 20
FREE_TIER_SNOOZE_MIN = 1  # free users can snooze once per reminder
SILVER_TIER_MAX_ACTIVE = 100
SILVER_TIER_SNOOZE_MIN = 3
GOLD_TIER_MAX_ACTIVE = 300
GOLD_TIER_SNOOZE_MIN = 5
PLATINUM_TIER_MAX_ACTIVE = 1000
PLATINUM_TIER_SNOOZE_MIN = 10
INITIAL_FREE_CREDITS = 15

# Premium Tier Definitions
PREMIUM_TIERS = {
    "FREE": {"max_active": 20, "snooze_limit": 1, "features": ["basic_reminders", "simple_recurring"]},
    "SILVER": {"max_active": 100, "snooze_limit": 3, "features": ["basic_reminders", "simple_recurring", "categories", "templates"]},
    "GOLD": {"max_active": 300, "snooze_limit": 5, "features": ["basic_reminders", "simple_recurring", "categories", "templates", "smart_scheduling", "location_reminders"]},
    "PLATINUM": {"max_active": 1000, "snooze_limit": 10, "features": ["basic_reminders", "simple_recurring", "categories", "templates", "smart_scheduling", "location_reminders", "ai_suggestions", "team_sharing", "priority_support"]}
}


# ============================
# Models
# ============================


@dataclass
class UserProfile:
    user_id: int
    timezone: str = DEFAULT_TIMEZONE
    is_premium: bool = False
    premium_tier: str = "FREE"  # FREE, SILVER, GOLD, PLATINUM
    credits: int = INITIAL_FREE_CREDITS
    name: Optional[str] = None
    username: Optional[str] = None
    first_seen: Optional[str] = None  # ISO
    last_seen: Optional[str] = None   # ISO
    preferences: Optional[Dict[str, Any]] = None  # User customization preferences
    usage_stats: Optional[Dict[str, Any]] = None  # Usage analytics


@dataclass
class Reminder:
    id: str
    chat_id: int
    user_id: int
    text: str
    when_iso: str  # timezone-aware ISO
    timezone: str
    created_at: str
    snoozes_used: int = 0
    recurring: Optional[Dict[str, Any]] = None  # {type: daily|weekly, interval: int, dow: Optional[int], time: "HH:MM"}
    done: bool = False
    category: Optional[str] = None  # work, personal, health, etc.
    priority: int = 1  # 1-5, where 5 is highest priority
    tags: Optional[List[str]] = None  # Custom tags for organization
    location: Optional[Dict[str, Any]] = None  # {name: str, lat: float, lng: float, radius: int}
    template_id: Optional[str] = None  # If created from template


@dataclass
class RedeemCode:
    code: str
    kind: str  # "credits" | "premium" | "plan"
    amount: int  # credits or days depending on kind (for plan/premium, optional)
    expires_at: Optional[str] = None  # ISO string
    max_uses: int = 1
    used: int = 0
    plan_name: Optional[str] = None  # for kind == "plan"


# ============================
# Storage
# ============================


class JSONStore:
    def __init__(self, path: Path, default: Any):
        self.path = path
        self.default = default
        DATA_DIR.mkdir(exist_ok=True)
        if not self.path.exists():
            self._write(default)

    def _read(self) -> Any:
        try:
            return json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return self.default

    def _write(self, data: Any):
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
        tmp.replace(self.path)

    def get(self) -> Any:
        return self._read()

    def set(self, value: Any):
        self._write(value)


class UserStore:
    def __init__(self):
        self.store = JSONStore(USERS_FILE, {})

    def get_user(self, user_id: int) -> UserProfile:
        data = self.store.get()
        u = data.get(str(user_id))
        if not u:
            profile = UserProfile(user_id=user_id, first_seen=to_iso(now_utc()))
            data[str(user_id)] = asdict(profile)
            self.store.set(data)
            return profile
        return UserProfile(**u)

    def put_user(self, profile: UserProfile):
        data = self.store.get()
        data[str(profile.user_id)] = asdict(profile)
        self.store.set(data)


class ReminderStore:
    def __init__(self):
        self.store = JSONStore(REMINDERS_FILE, {})

    def list_by_user(self, user_id: int) -> List[Reminder]:
        data = self.store.get()
        reminders = [Reminder(**r) for r in data.values() if r.get("user_id") == user_id]
        reminders.sort(key=lambda r: r.when_iso)
        return reminders

    def all(self) -> List[Reminder]:
        data = self.store.get()
        return [Reminder(**r) for r in data.values()]

    def get(self, reminder_id: str) -> Optional[Reminder]:
        data = self.store.get()
        r = data.get(reminder_id)
        return Reminder(**r) if r else None

    def put(self, reminder: Reminder):
        data = self.store.get()
        data[reminder.id] = asdict(reminder)
        self.store.set(data)

    def delete(self, reminder_id: str):
        data = self.store.get()
        if reminder_id in data:
            del data[reminder_id]
            self.store.set(data)


class CodesStore:
    def __init__(self):
        self.store = JSONStore(CODES_FILE, {})

    def get(self, code: str) -> Optional[RedeemCode]:
        data = self.store.get()
        c = data.get(code)
        return RedeemCode(**c) if c else None

    def put(self, code: RedeemCode):
        data = self.store.get()
        data[code.code] = asdict(code)
        self.store.set(data)

    def inc_used(self, code: str):
        data = self.store.get()
        if code in data:
            data[code]["used"] = int(data[code].get("used", 0)) + 1
            self.store.set(data)


class SettingsStore:
    def __init__(self):
        default = {
            "spreadsheet": {
                "enabled": True,
                "sheet_id": "1IjDWEcxxQUcb6y_Ha_bgCpp8vjOyV3Az-nOW2OSDxbQ",
                "credentials_file": None
            }
        }
        self.store = JSONStore(SETTINGS_FILE, default)

    def get(self) -> Dict[str, Any]:
        return self.store.get()

    def set(self, settings: Dict[str, Any]):
        self.store.set(settings)


# ============================
# Spreadsheet sync (optional)
# ============================


async def push_stats_if_enabled():
    s = settings_store.get().get("spreadsheet", {})
    logger.info(f"📊 Push stats called - enabled: {s.get('enabled', False)}")
    
    if not s.get("enabled"):
        logger.info("📊 Spreadsheet sync is disabled")
        return
    
    try:
        logger.info("📊 Building stats tables...")
        combined, summary, users, reminders = build_stats_tables()
        write_local_stats_csv(combined)
        logger.info(f"📊 Local stats written - {len(summary)} summary rows, {len(users)} user rows, {len(reminders)} reminder rows")
        
        # allow creds from settings or env GOOGLE_APPLICATION_CREDENTIALS
        creds_path = s.get("credentials_file") or os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        sheet_id = s.get("sheet_id")
        
        logger.info(f"📊 Checking Google Sheets config - sheet_id: {sheet_id}, creds_path: {creds_path}")
        
        if not sheet_id:
            logger.warning("📊 No sheet_id configured for Google Sheets sync")
            return
            
        if not creds_path:
            logger.warning("📊 No credentials_file or GOOGLE_APPLICATION_CREDENTIALS found")
            return
            
        if not os.path.exists(creds_path):
            logger.warning(f"📊 Credentials file not found: {creds_path}")
            return
            
        logger.info("📊 Pushing to Google Sheets...")
        await push_to_google_sheets_async({
            "Summary": summary,
            "Users": users,
            "Reminders": reminders,
        }, creds_path, sheet_id)
        logger.info("📊 Successfully pushed to Google Sheets!")
        
    except Exception as e:
        logger.error(f"📊 Stats sync failed: {e}")
        import traceback
        logger.error(f"📊 Full error: {traceback.format_exc()}")


def build_stats_tables() -> Tuple[List[List[str]], List[List[str]], List[List[str]], List[List[str]]]:
    users_data = user_store.store.get()
    reminders = reminder_store.all()
    # Summary sheet rows
    total_users = len(users_data)
    premium_users = sum(1 for u in users_data.values() if u.get("is_premium"))
    total_reminders = len(reminders)
    active_reminders = sum(1 for r in reminders if not r.done)
    summary = [
        ["Metric", "Value"],
        ["Users", str(total_users)],
        ["Premium Users", str(premium_users)],
        ["Total Reminders", str(total_reminders)],
        ["Active Reminders", str(active_reminders)],
    ]

    # Users sheet rows
    user_rows = [["User ID", "Name", "Username", "Timezone", "Plan", "Credits", "First Seen", "Last Seen", "Active Reminders"]]
    for uid, u in users_data.items():
        active_count = len([r for r in reminders if r.user_id == int(uid) and not r.done])
        user_rows.append([
            uid,
            (u.get("name") or ""),
            (u.get("username") or ""),
            (u.get("timezone") or ""),
            ("Premium" if u.get("is_premium") else "Free"),
            str(u.get("credits", 0)),
            (u.get("first_seen") or ""),
            (u.get("last_seen") or ""),
            str(active_count),
        ])

    # Reminders sheet rows
    rem_rows = [["Reminder ID", "User ID", "Text", "When (ISO)", "Timezone", "Created At", "Recurring", "Done"]]
    for r in reminders:
        rem_rows.append([
            r.id,
            str(r.user_id),
            r.text,
            r.when_iso,
            r.timezone,
            r.created_at,
            json.dumps(r.recurring) if r.recurring else "",
            "Yes" if r.done else "No",
        ])

    # Combine all sections into one rows list with separators for CSV
    combined: List[List[str]] = []
    combined.extend([["=== Summary ==="]])
    combined.extend(summary)
    combined.extend([[""],["=== Users ==="]])
    combined.extend(user_rows)
    combined.extend([[""],["=== Reminders ==="]])
    combined.extend(rem_rows)
    return combined, summary, user_rows, rem_rows


def write_local_stats_csv(rows: List[List[str]]):
    csv_path = DATA_DIR / "stats.csv"
    try:
        with csv_path.open("w", encoding="utf-8") as f:
            for row in rows:
                f.write(",".join([str(c).replace(",", " ") for c in row]) + "\n")
    except Exception as e:
        logger.warning(f"Failed to write local stats csv: {e}")


async def push_to_google_sheets_async(tables: Dict[str, List[List[str]]], credentials_file: str, sheet_id: str):
    loop = asyncio.get_event_loop()
    def _push():
        import json as _json
        from google.oauth2.service_account import Credentials
        
        logger.info(f"🔑 Loading credentials from: {credentials_file}")
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_file(credentials_file, scopes=scopes)
        logger.info("🔑 Credentials loaded successfully")
        
        client = gspread.authorize(creds)
        logger.info(f"📝 Opening spreadsheet: {sheet_id}")
        sh = client.open_by_key(sheet_id)
        logger.info(f"📝 Spreadsheet opened: {sh.title}")
        
        for title, rows in tables.items():
            logger.info(f"📋 Processing worksheet: {title} ({len(rows)} rows)")
            try:
                ws = sh.worksheet(title)
                logger.info(f"📋 Found existing worksheet: {title}")
            except gspread.WorksheetNotFound:
                logger.info(f"📋 Creating new worksheet: {title}")
                ws = sh.add_worksheet(title=title, rows=max(len(rows), 100), cols=max(len(rows[0]) if rows else 1, 10))
                logger.info(f"📋 Created worksheet: {title}")
            
            logger.info(f"📋 Clearing worksheet: {title}")
            ws.clear()
            
            if rows:
                # Resize sheet to fit data if possible
                try:
                    target_rows = len(rows)
                    target_cols = len(rows[0]) if rows else 1
                    logger.info(f"📋 Resizing worksheet {title} to {target_rows}x{target_cols}")
                    ws.resize(rows=target_rows, cols=target_cols)
                except Exception as e:
                    logger.warning(f"📋 Could not resize worksheet {title}: {e}")
                
                # Use batch update values for reliability
                logger.info(f"📋 Updating data in worksheet {title}")
                ws.update("A1", rows, value_input_option="RAW")
                logger.info(f"📋 Successfully updated {title} with {len(rows)} rows")
            else:
                logger.info(f"📋 No data to update for worksheet {title}")
        
        logger.info("📝 All worksheets updated successfully")
    
    await loop.run_in_executor(None, _push)


# ============================
# Text input flow handler
# ============================


async def on_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    session = get_session(user.id)
    profile = user_store.get_user(user.id)
    text = (update.message.text or "").strip()

    # Clean up user message to keep chat clean
    try:
        await update.message.delete()
    except Exception:
        pass

    if session.mode == "create_when":
        dt = parse_when(text, profile.timezone)
        if not dt:
            await _edit_anchor_or_send(
                update,
                context,
                session,
                "⚠️ **Oops! I couldn't understand that time**\n\n"
                "📝 Please try a different format:\n"
                "• `in 2 hours`\n"
                "• `tomorrow at 9am`\n"
                "• `next Monday 10:00`\n"
                "• `December 25th 3pm`\n\n"
                "✨ Be as natural as you want!",
                InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:main"), InlineKeyboardButton("❌ Cancel", callback_data="flow:cancel")]]),
            )
            return
        session.temp_when_dt = dt
        session.temp_when_text = text
        session.mode = "create_text"
        await _edit_anchor_or_send(
            update,
            context,
            session,
            f"✅ **Perfect! I've got the time:**\n"
            f"📅 {human_dt(dt, profile.timezone)}\n\n"
            f"💭 **Now, what should I remind you about?**\n\n"
            f"📝 Be specific - what do you want to remember?\n"
            f"✨ Example: *Take your medicine* or *Call mom*",
            InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:main"), InlineKeyboardButton("❌ Cancel", callback_data="flow:cancel")]]),
        )
        return

    if session.mode == "smart_create_text":
        what = text
        # Use AI to analyze the text and suggest optimal settings
        suggestion = SmartScheduler.suggest_smart_time(what, profile)
        
        session.temp_text = what
        session.temp_suggestions = suggestion
        session.mode = "smart_create_time"
        
        suggested_times = suggestion['suggested_times']
        category = suggestion['detected_category']
        confidence = suggestion['confidence']
        
        time_buttons = []
        for i in range(0, len(suggested_times), 2):
            row = []
            for j in range(2):
                if i + j < len(suggested_times):
                    time = suggested_times[i + j]
                    row.append(InlineKeyboardButton(f"🕐 {time}", callback_data=f"smart_time:{time}"))
            time_buttons.append(row)
        
        time_buttons.append([InlineKeyboardButton("⌨️ Custom Time", callback_data="smart_time:custom")])
        time_buttons.append([InlineKeyboardButton("⬅️ Back", callback_data="menu:new")])
        
        cat_info = REMINDER_CATEGORIES.get(category, {"name": "Other", "icon": "📌"})
        confidence_text = "High" if confidence > 0.7 else "Medium" if confidence > 0.4 else "Low"
        
        text = (
            f"🧠 **Smart Analysis Complete**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"💭 **Reminder:** {what}\n"
            f"📂 **Suggested Category:** {cat_info['icon']} {cat_info['name']}\n"
            f"🎯 **Confidence:** {confidence_text}\n\n"
            f"⏰ **Recommended Times:**\n"
            f"Choose when you'd like to be reminded:"
        )
        
        await _edit_anchor_or_send(update, context, session, text, InlineKeyboardMarkup(time_buttons))
        return
    
    if session.mode == "manual_create_text":
        what = text
        session.temp_text = what
        session.mode = "manual_create_category"
        
        # Show category selection
        buttons = []
        categories = list(REMINDER_CATEGORIES.items())
        
        for i in range(0, len(categories), 2):
            row = []
            for j in range(2):
                if i + j < len(categories):
                    cat_key, cat_info = categories[i + j]
                    row.append(InlineKeyboardButton(
                        f"{cat_info['icon']} {cat_info['name']}",
                        callback_data=f"manual_cat:{cat_key}"
                    ))
            buttons.append(row)
        
        buttons.append([InlineKeyboardButton("⬅️ Back", callback_data="menu:new")])
        
        text = (
            f"📂 **Choose Category**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"💭 **Reminder:** {what}\n\n"
            f"🎯 **Select the best category for organization:**"
        )
        
        await _edit_anchor_or_send(update, context, session, text, InlineKeyboardMarkup(buttons))
        return

    if session.mode == "create_text":
        what = text
        active_count = len([r for r in reminder_store.list_by_user(user.id) if not r.done])
        ok, reason = CreditPolicy.can_create(profile, active_count)
        if not ok:
            await _edit_anchor_or_send(update, context, session, reason, InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:main")]]))
            session.mode = "idle"
            return
        cost = CreditPolicy.consume_on_create(profile)
        if cost:
            profile.credits -= cost
            user_store.put_user(profile)
        reminder = Reminder(
            id=uuid.uuid4().hex[:10],
            chat_id=update.effective_chat.id,
            user_id=user.id,
            text=what,
            when_iso=to_iso(session.temp_when_dt or now_utc()),
            timezone=profile.timezone,
            created_at=to_iso(now_utc()),
        )
        reminder_store.put(reminder)
        # schedule
        scheduler.schedule_once(from_iso(reminder.when_iso), reminder.id)
        session.mode = "idle"
        await _edit_anchor_or_send(
            update,
            context,
            session,
            f"🎉 **Reminder Created Successfully!**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"💭 **What:** {reminder.text}\n"
            f"📅 **When:** {human_dt(from_iso(reminder.when_iso), profile.timezone)}\n"
            f"🆔 **ID:** `{reminder.id}`\n\n"
            f"✅ I'll remind you at the perfect time!\n"
            f"💎 **Credits used:** {cost if cost else 0}",
            InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Main", callback_data="menu:main")]]),
        )
        return

    if session.mode == "settings_timezone":
        tz_name = text
        if tz_name not in pytz.all_timezones:
            await _edit_anchor_or_send(
                update,
                context,
                session,
                "Invalid timezone. Example: Europe/Berlin",
                InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:settings")]]),
            )
            return
        profile.timezone = tz_name
        user_store.put_user(profile)
        session.mode = "idle"
        await _edit_anchor_or_send(
            update,
            context,
            session,
            f"Timezone updated to {tz_name}.",
            InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:main")]]),
        )
        return

    if session.mode == "redeem_code":
        code_str = text.upper()
        code = codes_store.get(code_str)
        if not code:
            await _edit_anchor_or_send(update, context, session, "Invalid code.", InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:main")]]))
            return
        if code.expires_at and from_iso(code.expires_at) < now_utc():
            await _edit_anchor_or_send(update, context, session, "This code has expired.", InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:main")]]))
            return
        if code.used >= code.max_uses:
            await _edit_anchor_or_send(update, context, session, "This code has been fully redeemed.", InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:main")]]))
            return
        if code.kind == "credits":
            profile.credits += code.amount
            user_store.put_user(profile)
            codes_store.inc_used(code.code)
            session.mode = "idle"
            await _edit_anchor_or_send(update, context, session, f"Added {code.amount} credits. New balance: {profile.credits}", InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:main")]]))
            # User feedback already shown via anchor; also notify via DM for visibility
            try:
                await context.bot.send_message(chat_id=user.id, text=f"✅ Redeemed {code.amount} credits. Balance: {profile.credits}")
            except Exception:
                pass
            return
        if code.kind == "premium":
            profile.is_premium = True
            profile.premium_tier = "PLATINUM"  # Legacy premium codes get platinum
            user_store.put_user(profile)
            codes_store.inc_used(code.code)
            session.mode = "idle"
            
            text = (
                "🎉 **Premium Activated!**\n\n"
                "✨ You now have PLATINUM access!\n"
                "🚀 Enjoy all premium features!"
            )
            
            await _edit_anchor_or_send(update, context, session, text, InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Main", callback_data="menu:main")]]))
            try:
                await context.bot.send_message(chat_id=user.id, text="🎉 Premium activated! You now have PLATINUM tier access.")
            except Exception:
                pass
            return
        
        if code.kind == "plan":
            profile.is_premium = True
            profile.premium_tier = code.plan_name or "PREMIUM"
            user_store.put_user(profile)
            codes_store.inc_used(code.code)
            session.mode = "idle"
            
            tier_info = get_user_tier_info(profile)
            tier_name = profile.premium_tier
            
            text = (
                f"🎉 **{tier_name} Plan Activated!**\n\n"
                f"✨ **Your new benefits:**\n"
                f"⏰ {tier_info['max_active']} active reminders\n"
                f"🔄 {tier_info['snooze_limit']} snoozes per reminder\n"
                f"🎯 Premium features unlocked\n\n"
                f"🚀 **Welcome to {tier_name}!**"
            )
            
            await _edit_anchor_or_send(update, context, session, text, InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Main", callback_data="menu:main")]]))
            try:
                await context.bot.send_message(chat_id=user.id, text=f"🎉 {tier_name} plan activated! Enjoy your premium features.")
            except Exception:
                pass
            return
        await _edit_anchor_or_send(update, context, session, "Unknown code type.", InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:main")]]))
        return

    # Repeating flow text steps
    if session.mode == "repeat_time":
        # validate HH:MM
        try:
            hour, minute = [int(x) for x in text.split(":", 1)]
            if not (0 <= hour <= 23 and 0 <= minute <= 59):
                raise ValueError
        except Exception:
            await _edit_anchor_or_send(update, context, session, "Time must be HH:MM, e.g., 07:30", InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:repeat")]]))
            return
        session.repeat_time = text
        session.mode = "repeat_text"
        await _edit_anchor_or_send(update, context, session, "What should I remind you?", InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:repeat")]]))
        return

    if session.mode == "repeat_text":
        what = text
        # compute first occurrence
        tz = pytz.timezone(profile.timezone)
        now_local = now_utc().astimezone(tz)
        hour, minute = [int(x) for x in (session.repeat_time or "00:00").split(":", 1)]
        if session.repeat_kind == "daily":
            target_local = now_local.replace(hour=hour, minute=minute, second=0, microsecond=0)
            if target_local <= now_local:
                target_local += timedelta(days=session.repeat_interval or 1)
            rec = {"type": "daily", "interval": session.repeat_interval or 1}
        else:
            target_local = now_local.replace(hour=hour, minute=minute, second=0, microsecond=0)
            desired = session.repeat_dow if session.repeat_dow is not None else target_local.weekday()
            while target_local.weekday() != desired or target_local <= now_local:
                target_local += timedelta(days=1)
            rec = {"type": "weekly", "interval": session.repeat_interval or 1, "dow": desired}

        active_count = len([r for r in reminder_store.list_by_user(user.id) if not r.done])
        ok, reason = CreditPolicy.can_create(profile, active_count)
        if not ok:
            await _edit_anchor_or_send(update, context, session, reason, InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:main")]]))
            session.mode = "idle"
            return
        cost = CreditPolicy.consume_on_create(profile)
        if cost:
            profile.credits -= cost
            user_store.put_user(profile)
        reminder = Reminder(
            id=uuid.uuid4().hex[:10],
            chat_id=update.effective_chat.id,
            user_id=user.id,
            text=what,
            when_iso=to_iso(target_local.astimezone(pytz.UTC)),
            timezone=profile.timezone,
            created_at=to_iso(now_utc()),
            recurring=rec,
        )
        reminder_store.put(reminder)
        scheduler.schedule_once(from_iso(reminder.when_iso), reminder.id)
        session.mode = "idle"
        await _edit_anchor_or_send(update, context, session, f"Repeating set: {what}\nFirst: {human_dt(from_iso(reminder.when_iso), profile.timezone)}\nKind: {rec['type']}", InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:main")]]))
        return

    if session.mode == "admin_gen_credits":
        parts = text.split()
        if len(parts) < 2:
            await _edit_anchor_or_send(update, context, session, "Format: amount count [days_valid]", InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:admin")]]))
            return
        try:
            amount = int(parts[0])
            count = int(parts[1])
            days_valid = int(parts[2]) if len(parts) >= 3 else 30
        except Exception:
            await _edit_anchor_or_send(update, context, session, "Numbers only. Example: 100 3 30", InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:admin")]]))
            return
        expires_at = to_iso(now_utc() + timedelta(days=days_valid))
        codes: List[str] = []
        for _ in range(count):
            ccode = generate_credit_code(amount)
            c = RedeemCode(code=ccode, kind="credits", amount=amount, expires_at=expires_at)
            codes_store.put(c)
            codes.append(ccode)
        session.mode = "idle"
        await _edit_anchor_or_send(update, context, session, "Generated codes:\n" + "\n".join(codes), InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:admin")]]))
        return

    if session.mode == "admin_gen_plans":
        parts = text.split()
        if len(parts) < 2:
            await _edit_anchor_or_send(update, context, session, "Format: PLAN_NAME count [days_valid]", InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:admin")]]))
            return
        plan_name = parts[0]
        try:
            count = int(parts[1])
            days_valid = int(parts[2]) if len(parts) >= 3 else 30
        except Exception:
            await _edit_anchor_or_send(update, context, session, "Example: PREMIUM 2 60", InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:admin")]]))
            return
        expires_at = to_iso(now_utc() + timedelta(days=days_valid))
        codes: List[str] = []
        for _ in range(count):
            pcode = generate_plan_code(plan_name)
            c = RedeemCode(code=pcode, kind="plan", amount=0, expires_at=expires_at, plan_name=plan_name.upper())
            codes_store.put(c)
            codes.append(pcode)
        session.mode = "idle"
        await _edit_anchor_or_send(update, context, session, "Generated plan codes:\n" + "\n".join(codes), InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:admin")]]))
        return

    if session.mode == "admin_grant":
        parts = text.split()
        if len(parts) < 2:
            await _edit_anchor_or_send(update, context, session, "Format: user_id credits <amount> | user_id premium", InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:admin")]]))
            return
        try:
            target = int(parts[0])
        except Exception:
            await _edit_anchor_or_send(update, context, session, "First argument must be a user_id.", InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:admin")]]))
            return
        action = parts[1].lower()
        profile_t = user_store.get_user(target)
        if action == "credits" and len(parts) >= 3:
            try:
                amt = int(parts[2])
            except Exception:
                await _edit_anchor_or_send(update, context, session, "Amount must be a number.", InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:admin")]]))
                return
            profile_t.credits += amt
            user_store.put_user(profile_t)
            # Notify target user
            try:
                await context.bot.send_message(chat_id=target, text=f"🎁 You received {amt} credits! New balance: {profile_t.credits}")
            except Exception:
                pass
            await _edit_anchor_or_send(update, context, session, f"Granted {amt} credits to {target}.", InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:admin")]]))
            return
        if action == "premium":
            profile_t.is_premium = True
            user_store.put_user(profile_t)
            # Notify target user
            try:
                await context.bot.send_message(chat_id=target, text="🌟 Your account was upgraded to Premium! Enjoy enhanced features.")
            except Exception:
                pass
            await _edit_anchor_or_send(update, context, session, f"Granted premium to {target}.", InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:admin")]]))
            return
        await _edit_anchor_or_send(update, context, session, "Invalid. Use: user_id credits <amount> | user_id premium", InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:admin")]]))
        return

    if session.mode == "admin_user_lookup":
        lookup_input = text.strip()
        try:
            # Try to find user by ID or username
            user_found = None
            if lookup_input.isdigit():
                user_id = int(lookup_input)
                user_data = user_store.store.get().get(str(user_id))
                if user_data:
                    user_found = UserProfile(**user_data)
            else:
                # Search by username
                users_data = user_store.store.get()
                for uid, udata in users_data.items():
                    if udata.get('username', '').lower() == lookup_input.lower():
                        user_found = UserProfile(**udata)
                        break
            
            if not user_found:
                await _edit_anchor_or_send(update, context, session, f"🚫 User not found: {lookup_input}", InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin:users_menu")]]))
                return
            
            # Get user analytics
            user_analytics = AnalyticsEngine.get_user_analytics(user_found.user_id)
            tier_info = get_user_tier_info(user_found)
            
            text = (
                f"👤 **User Profile: {user_found.name or 'Unknown'}**\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"🆔 **ID:** {user_found.user_id}\n"
                f"📝 **Name:** {user_found.name or 'Not set'}\n"
                f"📱 **Username:** @{user_found.username or 'Not set'}\n"
                f"💎 **Tier:** {user_found.premium_tier}\n"
                f"💳 **Credits:** {user_found.credits:,}\n"
                f"🌍 **Timezone:** {user_found.timezone}\n\n"
                f"📊 **Activity Stats:**\n"
                f"• Total Reminders: {user_analytics['total_reminders']:,}\n"
                f"• Active Reminders: {user_analytics['active_reminders']:,}\n"
                f"• Completion Rate: {user_analytics['completion_rate']}%\n"
                f"• Productivity Score: {user_analytics['productivity_score']}/100\n\n"
                f"📅 **Account Info:**\n"
                f"• Member Since: {user_found.first_seen[:10] if user_found.first_seen else 'Unknown'}\n"
                f"• Last Active: {user_found.last_seen[:10] if user_found.last_seen else 'Unknown'}"
            )
            
            await _edit_anchor_or_send(update, context, session, text, InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Users", callback_data="admin:users_menu")]]))
            session.mode = "idle"
            
        except Exception as e:
            await _edit_anchor_or_send(update, context, session, f"❌ Error looking up user: {e}", InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin:users_menu")]]))
        return

    if session.mode == "admin_broadcast":
        payload = text
        users = list(user_store.store.get().keys())
        sent = 0
        failed = 0
        
        # Enhanced broadcast with better formatting
        broadcast_message = (
            f"📢 **System Announcement**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"{payload}\n\n"
            f"🤖 *Sent by ReminderBot Administration*"
        )
        
        for uid in users:
            try:
                await context.bot.send_message(chat_id=int(uid), text=broadcast_message)
                sent += 1
            except Exception:
                failed += 1
        
        result_text = (
            f"📢 **Broadcast Complete**\n\n"
            f"✅ **Successfully sent:** {sent:,} users\n"
            f"❌ **Failed deliveries:** {failed:,} users\n"
            f"🎯 **Success rate:** {(sent/(sent+failed)*100):.1f}%" if (sent+failed) > 0 else "0%"
        )
        
        await _edit_anchor_or_send(update, context, session, result_text, InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Admin", callback_data="menu:admin")]]))
        session.mode = "idle"
        return

    if session.mode == "smart_create_custom_time":
        suggestion = getattr(session, 'temp_suggestions', {})
        category = suggestion.get('detected_category', 'other')
        
        dt = parse_when(text, profile.timezone)
        if not dt:
            await _edit_anchor_or_send(
                update, context, session,
                "⚠️ **Couldn't understand that time**\n\n"
                "📝 Please try a different format:\n"
                "• `in 2 hours`\n"
                "• `tomorrow at 9am`\n"
                "• `Friday 3pm`\n\n"
                "✨ Use natural language!",
                InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:new")]])
            )
            return
        
        # Create smart reminder with custom time
        active_count = len([r for r in reminder_store.list_by_user(user.id) if not r.done])
        ok, reason = CreditPolicy.can_create(profile, active_count)
        if not ok:
            await _edit_anchor_or_send(update, context, session, reason, InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:main")]]))
            return
        
        cost = CreditPolicy.consume_on_create(profile)
        if cost:
            profile.credits -= cost
            user_store.put_user(profile)
        
        reminder = Reminder(
            id=uuid.uuid4().hex[:10],
            chat_id=update.effective_chat.id,
            user_id=user.id,
            text=session.temp_text,
            when_iso=to_iso(dt),
            timezone=profile.timezone,
            created_at=to_iso(now_utc()),
            category=category,
            priority=2
        )
        reminder_store.put(reminder)
        scheduler.schedule_once(dt, reminder.id)
        session.mode = "idle"
        
        cat_info = REMINDER_CATEGORIES.get(category, {"name": "Other", "icon": "📌"})
        
        text = (
            f"🎉 **Smart Reminder Created!**\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"💭 **What:** {reminder.text}\n"
            f"📅 **When:** {human_dt(dt, profile.timezone)}\n"
            f"📂 **Category:** {cat_info['icon']} {cat_info['name']}\n"
            f"⭐ **Priority:** {'★' * reminder.priority}\n"
            f"🆔 **ID:** `{reminder.id}`\n\n"
            "🧠 **AI-enhanced with custom timing!**"
        )
        
        await _edit_anchor_or_send(update, context, session, text, InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Main", callback_data="menu:main")]]))
        return
    
    if session.mode == "manual_create_custom_time":
        dt = parse_when(text, profile.timezone)
        if not dt:
            await _edit_anchor_or_send(
                update, context, session,
                "⚠️ **Couldn't understand that time**\n\n"
                "📝 Please try a different format:\n"
                "• `in 2 hours`\n"
                "• `tomorrow at 9am`\n"
                "• `Friday 3pm`\n\n"
                "✨ Use natural language!",
                InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:new")]])
            )
            return
        
        # Create manual reminder with custom time
        active_count = len([r for r in reminder_store.list_by_user(user.id) if not r.done])
        ok, reason = CreditPolicy.can_create(profile, active_count)
        if not ok:
            await _edit_anchor_or_send(update, context, session, reason, InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:main")]]))
            return
        
        cost = CreditPolicy.consume_on_create(profile)
        if cost:
            profile.credits -= cost
            user_store.put_user(profile)
        
        reminder = Reminder(
            id=uuid.uuid4().hex[:10],
            chat_id=update.effective_chat.id,
            user_id=user.id,
            text=session.temp_text,
            when_iso=to_iso(dt),
            timezone=profile.timezone,
            created_at=to_iso(now_utc()),
            category=session.temp_category,
            priority=session.temp_priority
        )
        reminder_store.put(reminder)
        scheduler.schedule_once(dt, reminder.id)
        session.mode = "idle"
        
        cat_info = REMINDER_CATEGORIES.get(session.temp_category, {"name": "Other", "icon": "📌"})
        
        text = (
            f"🎉 **Manual Reminder Created!**\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"💭 **What:** {reminder.text}\n"
            f"📅 **When:** {human_dt(dt, profile.timezone)}\n"
            f"📂 **Category:** {cat_info['icon']} {cat_info['name']}\n"
            f"⭐ **Priority:** {'★' * reminder.priority}\n"
            f"🆔 **ID:** `{reminder.id}`\n\n"
            "🎯 **Fully customized to your specifications!**"
        )
        
        await _edit_anchor_or_send(update, context, session, text, InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Main", callback_data="menu:main")]]))
        return
    
    if session.mode == "template_custom_time":
        template = getattr(session, 'temp_template', None)
        if not template:
            await _edit_anchor_or_send(update, context, session, "Template session expired.", InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:templates")]]))
            return
        
        dt = parse_when(text, profile.timezone)
        if not dt:
            await _edit_anchor_or_send(
                update, context, session,
                "⚠️ **Couldn't understand that time**\n\n"
                "📝 Please try a different format:\n"
                "• `in 2 hours`\n"
                "• `tomorrow at 9am`\n"
                "• `Friday 3pm`\n\n"
                "✨ Use natural language!",
                InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Templates", callback_data="menu:templates")]])
            )
            return
        
        # Create reminder from template
        active_count = len([r for r in reminder_store.list_by_user(user.id) if not r.done])
        ok, reason = CreditPolicy.can_create(profile, active_count)
        if not ok:
            await _edit_anchor_or_send(update, context, session, reason, InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:main")]]))
            return
        
        cost = CreditPolicy.consume_on_create(profile)
        if cost:
            profile.credits -= cost
            user_store.put_user(profile)
        
        reminder = Reminder(
            id=uuid.uuid4().hex[:10],
            chat_id=update.effective_chat.id,
            user_id=user.id,
            text=template['text'],
            when_iso=to_iso(dt),
            timezone=profile.timezone,
            created_at=to_iso(now_utc()),
            category=template['category'],
            template_id=template['id']
        )
        reminder_store.put(reminder)
        scheduler.schedule_once(dt, reminder.id)
        session.mode = "idle"
        
        text = (
            f"✅ **Template Reminder Created!**\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"{template['icon']} **{template['name']}**\n"
            f"📅 **When:** {human_dt(dt, profile.timezone)}\n"
            f"📂 **Category:** {template['category']}\n"
            f"🆔 **ID:** `{reminder.id}`\n\n"
            "🎯 Perfect! I'll remind you at the right time."
        )
        
        await _edit_anchor_or_send(update, context, session, text, InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Main", callback_data="menu:main")]]))
        return
    
    if session.mode == "admin_gen_plans":
        # Handle the new plan generation with tier
        tier = getattr(session, 'temp_plan_tier', 'PREMIUM')
        parts = text.split()
        if len(parts) < 1:
            await _edit_anchor_or_send(update, context, session, 
                "📝 **Format:** `count [days_valid]`\n"
                "📋 **Example:** `5 90`", 
                InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin:plans_menu")]]))
            return
        try:
            count = int(parts[0])
            days_valid = int(parts[1]) if len(parts) >= 2 else 30
        except ValueError:
            await _edit_anchor_or_send(update, context, session, 
                "❌ **Invalid input.** Numbers only.\n"
                "📋 **Example:** `5 90`", 
                InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin:plans_menu")]]))
            return
        
        expires_at = to_iso(now_utc() + timedelta(days=days_valid))
        codes: List[str] = []
        
        for _ in range(count):
            pcode = generate_plan_code(tier)
            c = RedeemCode(
                code=pcode,
                kind="plan",
                amount=0,
                expires_at=expires_at,
                plan_name=tier,
                max_uses=1
            )
            codes_store.put(c)
            codes.append(pcode)
        
        session.mode = "idle"
        
        text = (
            f"✅ **{tier} Plan Codes Generated**\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📊 **Generated:** {count} codes\n"
            f"⏰ **Valid for:** {days_valid} days\n"
            f"💎 **Tier:** {tier}\n\n"
            "🎫 **Codes:**\n" + "\n".join(f"`{code}`" for code in codes)
        )
        
        await _edit_anchor_or_send(update, context, session, text, InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Admin", callback_data="menu:admin")]]))
        return

    if session.mode == "admin_sheet_settings":
        parts = text.split(maxsplit=1)
        s = settings_store.get()
        ss = s.get("spreadsheet", {})
        if not parts:
            await _edit_anchor_or_send(update, context, session, 
                "🔧 **Spreadsheet Configuration**\n\n"
                "📝 **Commands:**\n"
                "• `on` - Enable sync\n"
                "• `off` - Disable sync\n"
                "• `set <SHEET_ID>` - Set spreadsheet ID\n"
                "• `creds <path>` - Set credentials file path", 
                InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Admin", callback_data="menu:admin")]]))
            return
        cmd = parts[0].lower()
        arg = parts[1] if len(parts) > 1 else None
        if cmd == "on":
            ss["enabled"] = True
        elif cmd == "off":
            ss["enabled"] = False
        elif cmd == "set" and arg:
            ss["sheet_id"] = arg
        elif cmd == "creds" and arg:
            ss["credentials_file"] = arg
        else:
            await _edit_anchor_or_send(update, context, session, 
                "❌ **Unknown command**\n\n"
                "📝 **Valid commands:**\n"
                "• `on` | `off` | `set <SHEET_ID>` | `creds <path>`", 
                InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Admin", callback_data="menu:admin")]]))
            return
        s["spreadsheet"] = ss
        settings_store.set(s)
        await _edit_anchor_or_send(update, context, session, 
            "✅ **Configuration saved successfully!**", 
            InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Admin", callback_data="menu:admin")]]))
        return

    # default: bounce to main menu
    await show_main_menu(update, context)


# ============================
# Utilities
# ============================


def now_utc() -> datetime:
    return datetime.now(tz=pytz.UTC)


def to_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = pytz.UTC.localize(dt)
    return dt.isoformat()


def from_iso(s: str) -> datetime:
    return datetime.fromisoformat(s).astimezone(pytz.UTC)


def parse_when(text: str, user_tz: str) -> Optional[datetime]:
    settings = {
        "TIMEZONE": user_tz,
        "RETURN_AS_TIMEZONE_AWARE": True,
        "PREFER_DATES_FROM": "future",
    }
    dt = dateparser.parse(text, settings=settings)
    if not dt:
        return None
    # Normalize to UTC for storage
    return dt.astimezone(pytz.UTC)


def human_dt(dt: datetime, tz_name: str) -> str:
    tz = pytz.timezone(tz_name)
    local = dt.astimezone(tz)
    return local.strftime("%a, %d %b %Y %H:%M %Z")


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def generate_code(prefix: str = "REM") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8].upper()}"


def generate_credit_code(amount: int) -> str:
    # e.g., MIKU-CR100-ABC123
    import random
    import string
    suffix = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
    return f"MIKU-CR{amount}-{suffix}"


def generate_plan_code(plan_name: str) -> str:
    # e.g., MIKU-SIL-ABC123, MIKU-GLD-ABC123, MIKU-PLT-ABC123
    import random
    import string
    tier_codes = {"SILVER": "SIL", "GOLD": "GLD", "PLATINUM": "PLT"}
    code = tier_codes.get(plan_name.upper(), plan_name.upper()[:3])
    suffix = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
    return f"MIKU-{code}-{suffix}"


def get_user_tier_info(profile: UserProfile) -> Dict[str, Any]:
    """Get tier information for a user"""
    tier = profile.premium_tier if profile.is_premium else "FREE"
    return PREMIUM_TIERS.get(tier, PREMIUM_TIERS["FREE"])


def has_feature(profile: UserProfile, feature: str) -> bool:
    """Check if user has access to a specific feature"""
    tier_info = get_user_tier_info(profile)
    return feature in tier_info.get("features", [])


class SmartScheduler:
    """AI-powered smart scheduling suggestions"""
    
    @staticmethod
    def get_optimal_times(profile: UserProfile, category: str = None) -> List[str]:
        """Get optimal reminder times based on user patterns and category"""
        # Analyze user's historical reminder patterns
        user_reminders = reminder_store.list_by_user(profile.user_id)
        
        if not user_reminders:
            # Default suggestions based on category
            return SmartScheduler._get_category_defaults(category)
        
        # Extract hour patterns from user's existing reminders
        hours = []
        for reminder in user_reminders:
            try:
                dt = from_iso(reminder.when_iso)
                local_dt = dt.astimezone(pytz.timezone(profile.timezone))
                hours.append(local_dt.hour)
            except Exception:
                continue
        
        if not hours:
            return SmartScheduler._get_category_defaults(category)
        
        # Find user's preferred time patterns
        preferred_hours = list(set(hours))
        preferred_hours.sort()
        
        # Suggest times around user's patterns
        suggestions = []
        for hour in preferred_hours[:3]:  # Top 3 preferred hours
            suggestions.append(f"{hour:02d}:00")
            if hour < 23:
                suggestions.append(f"{hour+1:02d}:00")
        
        # Add category-specific suggestions
        category_suggestions = SmartScheduler._get_category_defaults(category)
        suggestions.extend(category_suggestions)
        
        # Remove duplicates and limit to 6 suggestions
        seen = set()
        unique_suggestions = []
        for suggestion in suggestions:
            if suggestion not in seen:
                seen.add(suggestion)
                unique_suggestions.append(suggestion)
                if len(unique_suggestions) >= 6:
                    break
        
        return unique_suggestions
    
    @staticmethod
    def _get_category_defaults(category: str) -> List[str]:
        """Get default time suggestions based on category"""
        defaults = {
            "work": ["09:00", "10:00", "14:00", "15:00", "16:00"],
            "health": ["08:00", "12:00", "18:00", "20:00"],
            "personal": ["07:00", "19:00", "21:00"],
            "finance": ["09:00", "17:00", "20:00"],
            "social": ["12:00", "18:00", "19:00"],
            "learning": ["09:00", "14:00", "20:00"]
        }
        return defaults.get(category, ["09:00", "12:00", "15:00", "18:00"])
    
    @staticmethod
    def suggest_smart_time(text: str, profile: UserProfile) -> Dict[str, Any]:
        """AI-powered time suggestion based on reminder text"""
        text_lower = text.lower()
        
        # Time-based keywords
        time_suggestions = {
            "morning": ["07:00", "08:00", "09:00"],
            "afternoon": ["13:00", "14:00", "15:00"],
            "evening": ["18:00", "19:00", "20:00"],
            "night": ["21:00", "22:00"],
            "breakfast": ["07:00", "08:00"],
            "lunch": ["12:00", "13:00"],
            "dinner": ["18:00", "19:00"],
            "bedtime": ["21:00", "22:00", "23:00"],
            "wake up": ["06:00", "07:00", "08:00"],
            "work": ["09:00", "10:00", "14:00"],
            "meeting": ["09:00", "10:00", "14:00", "15:00"],
            "exercise": ["06:00", "07:00", "17:00", "18:00"],
            "medicine": ["08:00", "12:00", "18:00", "22:00"]
        }
        
        # Find matching keywords
        suggested_times = []
        detected_category = "other"
        
        for keyword, times in time_suggestions.items():
            if keyword in text_lower:
                suggested_times.extend(times)
                if keyword in ["work", "meeting"]:
                    detected_category = "work"
                elif keyword in ["medicine", "exercise"]:
                    detected_category = "health"
                elif keyword in ["breakfast", "lunch", "dinner"]:
                    detected_category = "personal"
        
        # Remove duplicates and limit
        suggested_times = list(set(suggested_times))[:4]
        
        if not suggested_times:
            suggested_times = SmartScheduler.get_optimal_times(profile, detected_category)[:4]
        
        return {
            "suggested_times": suggested_times,
            "detected_category": detected_category,
            "confidence": 0.8 if suggested_times else 0.3
        }


class AIEngine:
    """AI-powered suggestions and optimization"""
    
    @staticmethod
    def analyze_reminder_text(text: str) -> Dict[str, Any]:
        """Analyze reminder text and provide AI suggestions"""
        text_lower = text.lower()
        suggestions = {
            "category": "other",
            "priority": 1,
            "optimal_times": ["09:00", "15:00"],
            "recurring_suggestion": None,
            "confidence": 0.3
        }
        
        highest_confidence = 0
        
        for pattern_name, pattern_data in AI_PATTERNS.items():
            match_count = sum(1 for keyword in pattern_data["keywords"] if keyword in text_lower)
            confidence = match_count / len(pattern_data["keywords"])
            
            if confidence > highest_confidence:
                highest_confidence = confidence
                pattern_suggestions = pattern_data["suggestions"]
                
                suggestions.update({
                    "category": pattern_suggestions.get("category", "other"),
                    "priority": pattern_suggestions.get("priority", 1),
                    "optimal_times": pattern_suggestions.get("optimal_times", ["09:00", "15:00"]),
                    "recurring_suggestion": pattern_suggestions.get("recurring"),
                    "confidence": confidence,
                    "pattern_matched": pattern_name
                })
        
        return suggestions
    
    @staticmethod
    def suggest_optimization(profile: UserProfile) -> Dict[str, Any]:
        """Suggest optimizations based on user behavior"""
        user_reminders = reminder_store.list_by_user(profile.user_id)
        
        if len(user_reminders) < 5:
            return {"message": "Create more reminders to get personalized suggestions!"}
        
        # Analyze completion patterns
        completed_reminders = [r for r in user_reminders if r.done]
        completion_rate = len(completed_reminders) / len(user_reminders) if user_reminders else 0
        
        # Analyze snooze patterns
        high_snooze_reminders = [r for r in user_reminders if r.snoozes_used > 2]
        snooze_rate = len(high_snooze_reminders) / len(user_reminders) if user_reminders else 0
        
        suggestions = []
        
        if completion_rate < 0.7:
            suggestions.append("🎯 Consider reducing the number of active reminders for better focus")
        
        if snooze_rate > 0.3:
            suggestions.append("⏰ Try scheduling reminders at more convenient times")
        
        # Category analysis
        categories = {}
        for r in user_reminders:
            cat = r.category or "other"
            categories[cat] = categories.get(cat, 0) + 1
        
        most_used_category = max(categories.items(), key=lambda x: x[1])[0] if categories else "other"
        
        if categories.get(most_used_category, 0) > len(user_reminders) * 0.6:
            suggestions.append(f"📂 Consider diversifying beyond {REMINDER_CATEGORIES.get(most_used_category, {}).get('name', most_used_category)} reminders")
        
        return {
            "completion_rate": round(completion_rate * 100, 1),
            "snooze_rate": round(snooze_rate * 100, 1),
            "most_used_category": most_used_category,
            "suggestions": suggestions[:3],  # Limit to top 3 suggestions
            "optimization_score": round((completion_rate * 0.7 + (1 - snooze_rate) * 0.3) * 100, 1)
        }


class ExportEngine:
    """Data export and backup functionality"""
    
    @staticmethod
    def export_user_data(user_id: int, format_type: str = "json") -> Dict[str, Any]:
        """Export user data in specified format"""
        profile = user_store.get_user(user_id)
        reminders = reminder_store.list_by_user(user_id)
        analytics = AnalyticsEngine.get_user_analytics(user_id)
        
        export_data = {
            "export_info": {
                "user_id": user_id,
                "export_date": to_iso(now_utc()),
                "format": format_type,
                "version": "2.0"
            },
            "profile": asdict(profile),
            "reminders": [asdict(r) for r in reminders],
            "analytics": analytics
        }
        
        if format_type == "json":
            return export_data
        elif format_type == "csv":
            return ExportEngine._convert_to_csv(export_data)
        elif format_type == "txt":
            return ExportEngine._convert_to_text(export_data)
        else:
            return export_data
    
    @staticmethod
    def _convert_to_csv(data: Dict[str, Any]) -> str:
        """Convert data to CSV format"""
        csv_content = "# ReminderBot Data Export\n\n"
        
        # Profile section
        csv_content += "[PROFILE]\n"
        csv_content += "Field,Value\n"
        for key, value in data["profile"].items():
            csv_content += f"{key},{value}\n"
        
        # Reminders section
        csv_content += "\n[REMINDERS]\n"
        if data["reminders"]:
            headers = list(data["reminders"][0].keys())
            csv_content += ",".join(headers) + "\n"
            for reminder in data["reminders"]:
                values = [str(reminder.get(h, "")).replace(",", ";") for h in headers]
                csv_content += ",".join(values) + "\n"
        
        return csv_content
    
    @staticmethod
    def _convert_to_text(data: Dict[str, Any]) -> str:
        """Convert data to readable text format"""
        profile = data["profile"]
        reminders = data["reminders"]
        analytics = data["analytics"]
        
        text_content = f"""
=== REMINDERBOT DATA EXPORT ===
Exported: {data['export_info']['export_date'][:10]}
User ID: {data['export_info']['user_id']}

=== PROFILE ===
Name: {profile.get('name', 'Not set')}
Username: {profile.get('username', 'Not set')}
Tier: {profile.get('premium_tier', 'FREE')}
Credits: {profile.get('credits', 0)}
Timezone: {profile.get('timezone', 'UTC')}
Member Since: {profile.get('first_seen', 'Unknown')[:10]}

=== ANALYTICS ===
Total Reminders: {analytics.get('total_reminders', 0)}
Active Reminders: {analytics.get('active_reminders', 0)}
Completion Rate: {analytics.get('completion_rate', 0)}%
Productivity Score: {analytics.get('productivity_score', 0)}/100

=== REMINDERS ===
"""
        
        for i, reminder in enumerate(reminders, 1):
            status = "Done" if reminder.get('done') else "Active"
            text_content += f"""
{i}. {reminder.get('text', 'No description')}
   When: {reminder.get('when_iso', 'Unknown')}
   Category: {reminder.get('category', 'other')}
   Priority: {'★' * reminder.get('priority', 1)}
   Status: {status}
   Snoozes Used: {reminder.get('snoozes_used', 0)}
"""
        
        return text_content


class AnalyticsEngine:
    """Advanced analytics and insights engine"""
    
    @staticmethod
    def get_user_analytics(user_id: int) -> Dict[str, Any]:
        """Get comprehensive user analytics"""
        reminders = reminder_store.list_by_user(user_id)
        profile = user_store.get_user(user_id)
        
        if not reminders:
            return {
                "total_reminders": 0,
                "completion_rate": 0,
                "average_snoozes": 0,
                "most_active_hour": None,
                "category_breakdown": {},
                "productivity_score": 0
            }
        
        # Basic statistics
        total_reminders = len(reminders)
        completed_reminders = len([r for r in reminders if r.done])
        completion_rate = (completed_reminders / total_reminders) * 100 if total_reminders > 0 else 0
        
        # Snooze analysis
        snooze_counts = [r.snoozes_used for r in reminders]
        average_snoozes = statistics.mean(snooze_counts) if snooze_counts else 0
        
        # Time pattern analysis
        hours = []
        for reminder in reminders:
            try:
                dt = from_iso(reminder.when_iso)
                local_dt = dt.astimezone(pytz.timezone(profile.timezone))
                hours.append(local_dt.hour)
            except Exception:
                continue
        
        most_active_hour = statistics.mode(hours) if hours else None
        
        # Category breakdown
        categories = {}
        for reminder in reminders:
            cat = reminder.category or "other"
            categories[cat] = categories.get(cat, 0) + 1
        
        # Productivity score (0-100)
        score_factors = [
            completion_rate * 0.4,  # 40% completion rate
            max(0, 100 - (average_snoozes * 20)) * 0.3,  # 30% snooze efficiency
            min(100, len(reminders) * 5) * 0.3  # 30% activity level
        ]
        productivity_score = sum(score_factors)
        
        return {
            "total_reminders": total_reminders,
            "active_reminders": len([r for r in reminders if not r.done]),
            "completion_rate": round(completion_rate, 1),
            "average_snoozes": round(average_snoozes, 1),
            "most_active_hour": most_active_hour,
            "category_breakdown": categories,
            "productivity_score": round(productivity_score, 1),
            "tier": profile.premium_tier,
            "member_since": profile.first_seen
        }
    
    @staticmethod
    def get_system_analytics() -> Dict[str, Any]:
        """Get system-wide analytics for admins"""
        users_data = user_store.store.get()
        all_reminders = reminder_store.all()
        
        # User statistics
        total_users = len(users_data)
        premium_users = sum(1 for u in users_data.values() if u.get("is_premium"))
        
        # Tier breakdown
        tier_breakdown = {"FREE": 0, "SILVER": 0, "GOLD": 0, "PLATINUM": 0}
        for user_data in users_data.values():
            tier = user_data.get("premium_tier", "FREE")
            tier_breakdown[tier] = tier_breakdown.get(tier, 0) + 1
        
        # Reminder statistics
        total_reminders = len(all_reminders)
        active_reminders = len([r for r in all_reminders if not r.done])
        completed_reminders = len([r for r in all_reminders if r.done])
        
        # Category analysis
        category_stats = {}
        for reminder in all_reminders:
            cat = reminder.category or "other"
            category_stats[cat] = category_stats.get(cat, 0) + 1
        
        # Activity trends (last 7 days)
        now = now_utc()
        recent_activity = []
        for i in range(7):
            day_start = now - timedelta(days=i)
            day_end = day_start + timedelta(days=1)
            
            day_reminders = [
                r for r in all_reminders 
                if day_start <= from_iso(r.created_at) < day_end
            ]
            
            recent_activity.append({
                "date": day_start.strftime("%Y-%m-%d"),
                "reminders_created": len(day_reminders),
                "day_name": day_start.strftime("%A")
            })
        
        return {
            "users": {
                "total": total_users,
                "premium": premium_users,
                "free": total_users - premium_users,
                "tier_breakdown": tier_breakdown
            },
            "reminders": {
                "total": total_reminders,
                "active": active_reminders,
                "completed": completed_reminders,
                "completion_rate": round((completed_reminders / total_reminders) * 100, 1) if total_reminders > 0 else 0
            },
            "categories": category_stats,
            "recent_activity": recent_activity,
            "growth_metrics": {
                "avg_reminders_per_user": round(total_reminders / total_users, 1) if total_users > 0 else 0,
                "premium_conversion_rate": round((premium_users / total_users) * 100, 1) if total_users > 0 else 0
            }
        }


def build_reminder_keyboard(reminder: Reminder, profile: UserProfile) -> InlineKeyboardMarkup:
    tier_name = profile.premium_tier if profile.is_premium else "FREE"
    
    # Tier-specific snooze options
    snooze_options = {
        "FREE": [5, 15],
        "SILVER": [5, 15, 30],
        "GOLD": [5, 15, 30, 60],
        "PLATINUM": [5, 15, 30, 60, 120, 240]
    }
    
    options = snooze_options.get(tier_name, [5, 15])
    
    # Create snooze buttons (max 3 per row)
    snooze_rows = []
    for i in range(0, len(options), 3):
        row = []
        for j in range(3):
            if i + j < len(options):
                minutes = options[i + j]
                if minutes >= 60:
                    label = f"{minutes//60}h" if minutes % 60 == 0 else f"{minutes}m"
                else:
                    label = f"{minutes}m"
                row.append(InlineKeyboardButton(f"💤 {label}", callback_data=f"snooze:{reminder.id}:{minutes}"))
        snooze_rows.append(row)
    
    # Action buttons
    action_row = [
        InlineKeyboardButton("✅ Done", callback_data=f"done:{reminder.id}"),
        InlineKeyboardButton("🗑️ Delete", callback_data=f"del:{reminder.id}"),
    ]
    
    # Add edit button for premium users
    if has_feature(profile, "smart_scheduling"):
        action_row.insert(1, InlineKeyboardButton("✏️ Edit", callback_data=f"edit:{reminder.id}"))
    
    snooze_rows.append(action_row)
    return InlineKeyboardMarkup(snooze_rows)


# ============================
# Managers
# ============================


class CreditPolicy:
    @staticmethod
    def can_create(profile: UserProfile, active_count: int) -> Tuple[bool, Optional[str]]:
        tier_info = get_user_tier_info(profile)
        limit = tier_info["max_active"]
        
        if active_count >= limit:
            return False, (
                f"🚫 **Reminder Limit Reached**\n\n"
                f"📊 **Current:** {active_count}/{limit} active reminders\n"
                f"🎯 **Tier:** {profile.premium_tier}\n\n"
                f"💎 **Upgrade your plan for more reminders!**"
            )
        
        if not profile.is_premium and profile.credits <= 0:
            return False, (
                "💳 **No Credits Remaining**\n\n"
                "🎫 **Options:**\n"
                "• Use `/redeem` with a code\n"
                "• Upgrade to a premium plan\n"
                "• Contact support for assistance\n\n"
                "✨ Premium users get unlimited reminders!"
            )
        return True, None

    @staticmethod
    def consume_on_create(profile: UserProfile) -> int:
        if profile.is_premium:
            return 0
        return 1
    
    @staticmethod
    def get_snooze_limit(profile: UserProfile) -> int:
        tier_info = get_user_tier_info(profile)
        return tier_info["snooze_limit"]


class ReminderScheduler:
    def __init__(self, app: Application, rstore: ReminderStore, ustore: UserStore):
        self.app = app
        self.rstore = rstore
        self.ustore = ustore

    async def schedule_all_on_startup(self):
        for r in self.rstore.all():
            if r.done:
                continue
            when = from_iso(r.when_iso)
            if when < now_utc():
                # Skip past reminders (or nudge soon)
                when = now_utc() + timedelta(seconds=2)
            self.schedule_once(when, r.id)

    def schedule_once(self, when: datetime, reminder_id: str):
        jq = getattr(self.app, "job_queue", None)
        if jq is None:
            logger.warning(
                "JobQueue not available. Install python-telegram-bot[job-queue] to enable scheduling. Skipping %s",
                reminder_id,
            )
            return
        jq.run_once(self._run_reminder_job, when, name=reminder_id, data=reminder_id)

    async def _run_reminder_job(self, context: ContextTypes.DEFAULT_TYPE):
        reminder_id = context.job.data
        r = self.rstore.get(reminder_id)
        if not r or r.done:
            return
        profile = self.ustore.get_user(r.user_id)
        try:
            keyboard = build_reminder_keyboard(r, profile)
            await context.bot.send_message(
                chat_id=r.chat_id,
                text=f"⏰ Reminder\n\n{r.text}\n\nTime: {human_dt(from_iso(r.when_iso), r.timezone)}",
                reply_markup=keyboard,
                disable_notification=False,
            )
            # Also push a lightweight event to spreadsheet if enabled (non-blocking)
            try:
                await push_stats_if_enabled()
            except Exception:
                pass
        finally:
            if r.recurring:
                next_when = self._next_occurrence(r)
                if next_when:
                    r.when_iso = to_iso(next_when)
                    self.rstore.put(r)
                    self.schedule_once(next_when, r.id)
            else:
                r.done = True
                self.rstore.put(r)

    def _next_occurrence(self, r: Reminder) -> Optional[datetime]:
        rec = r.recurring or {}
        kind = rec.get("type")
        tz = pytz.timezone(r.timezone)
        base = from_iso(r.when_iso).astimezone(tz)
        if kind == "daily":
            interval = int(rec.get("interval", 1))
            next_local = base + timedelta(days=interval)
            return next_local.astimezone(pytz.UTC)
        if kind == "weekly":
            interval = int(rec.get("interval", 1))
            dow = int(rec.get("dow", base.weekday()))  # 0=Mon
            next_local = base + timedelta(weeks=interval)
            # Move to requested weekday
            while next_local.weekday() != dow:
                next_local += timedelta(days=1)
            return next_local.astimezone(pytz.UTC)
        return None


# ============================
# Handlers
# ============================


user_store = UserStore()
reminder_store = ReminderStore()
codes_store = CodesStore()
settings_store = SettingsStore()


########################################
# Inline UI (menus + sessions)
########################################


class SessionState:
    def __init__(self):
        self.anchor_chat_id: Optional[int] = None
        self.anchor_message_id: Optional[int] = None
        self.mode: str = "idle"  # idle | create_when | create_text | settings_timezone | redeem_code | repeat_kind | repeat_interval | repeat_dow | repeat_time | repeat_text | admin_gen_credits | admin_gen_plans
        self.temp_when_text: Optional[str] = None
        self.temp_when_dt: Optional[datetime] = None
        self.temp_text: Optional[str] = None
        self.temp_suggestions: Optional[Dict[str, Any]] = None
        self.temp_category: Optional[str] = None
        self.temp_priority: int = 1
        self.temp_template: Optional[Dict[str, Any]] = None
        # repeating temp
        self.repeat_kind: Optional[str] = None  # daily|weekly
        self.repeat_interval: Optional[int] = None
        self.repeat_dow: Optional[int] = None  # 0=Mon
        self.repeat_time: Optional[str] = None  # HH:MM


sessions: Dict[int, SessionState] = {}


def get_session(user_id: int) -> SessionState:
    s = sessions.get(user_id)
    if not s:
        s = SessionState()
        sessions[user_id] = s
    return s


def build_main_menu(profile: UserProfile) -> InlineKeyboardMarkup:
    buttons = [
            [
                InlineKeyboardButton("✨ Create Reminder", callback_data="menu:new"),
                InlineKeyboardButton("📝 My Reminders", callback_data="menu:list"),
            ],
            [
            InlineKeyboardButton("📋 Quick Templates", callback_data="menu:templates"),
                InlineKeyboardButton("🔄 Repeating Tasks", callback_data="menu:repeat"),
        ],
        [
            InlineKeyboardButton("🤖 AI Suggestions", callback_data="menu:ai_suggestions"),
            InlineKeyboardButton("💾 Export Data", callback_data="menu:export_data"),
        ],
    ]
    
    # Add premium features for eligible users
    if has_feature(profile, "categories"):
        buttons.append([
            InlineKeyboardButton("📂 Categories", callback_data="menu:categories"),
            InlineKeyboardButton("📊 Analytics", callback_data="menu:user_analytics"),
        ])
    
    buttons.extend([
        [
                InlineKeyboardButton("🎫 Redeem Code", callback_data="menu:redeem"),
            InlineKeyboardButton("💎 Upgrade Plan", callback_data="menu:upgrade"),
            ],
            [
                InlineKeyboardButton("⚙️ Settings", callback_data="menu:settings"),
                InlineKeyboardButton("👤 My Profile", callback_data="menu:profile"),
            ],
    ])
    
    if is_admin(profile.user_id):
        buttons.append([InlineKeyboardButton("🛠️ Admin Panel", callback_data="menu:admin")])
    
    return InlineKeyboardMarkup(buttons)


def build_templates_menu() -> InlineKeyboardMarkup:
    buttons = []
    templates = list(DEFAULT_TEMPLATES.values())
    
    # Create rows of 2 templates each
    for i in range(0, len(templates), 2):
        row = []
        for j in range(2):
            if i + j < len(templates):
                template = templates[i + j]
                row.append(InlineKeyboardButton(
                    f"{template['icon']} {template['name']}",
                    callback_data=f"template:{template['id']}"
                ))
        buttons.append(row)
    
    buttons.append([InlineKeyboardButton("⬅️ Back to Main", callback_data="menu:main")])
    return InlineKeyboardMarkup(buttons)


def build_categories_menu() -> InlineKeyboardMarkup:
    buttons = []
    categories = list(REMINDER_CATEGORIES.values())
    
    # Create rows of 2 categories each
    for i in range(0, len(categories), 2):
        row = []
        for j in range(2):
            if i + j < len(categories):
                category = categories[i + j]
                row.append(InlineKeyboardButton(
                    category['name'],
                    callback_data=f"category:{list(REMINDER_CATEGORIES.keys())[i + j]}"
                ))
        buttons.append(row)
    
    buttons.append([InlineKeyboardButton("⬅️ Back to Main", callback_data="menu:main")])
    return InlineKeyboardMarkup(buttons)


def build_settings_menu(profile: UserProfile) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🌍 Set Timezone", callback_data="settings:tz"),
            ],
            [
                InlineKeyboardButton("🔔 Notification Settings", callback_data="settings:notifications"),
            ],
            [
                InlineKeyboardButton("🎨 Appearance", callback_data="settings:appearance"),
            ],
            [
                InlineKeyboardButton("⬅️ Back to Main", callback_data="menu:main"),
            ],
        ]
    )


def build_admin_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("💳 Credit Management", callback_data="admin:credits_menu"),
                InlineKeyboardButton("👑 Premium Plans", callback_data="admin:plans_menu"),
            ],
            [
                InlineKeyboardButton("👤 User Management", callback_data="admin:users_menu"),
                InlineKeyboardButton("📢 Communications", callback_data="admin:comms_menu"),
            ],
            [
                InlineKeyboardButton("📊 Analytics & Reports", callback_data="admin:analytics_menu"),
                InlineKeyboardButton("🔧 System Settings", callback_data="admin:system_menu"),
            ],
            [
                InlineKeyboardButton("🛡️ Security & Monitoring", callback_data="admin:security_menu"),
                InlineKeyboardButton("📚 Template Management", callback_data="admin:templates_menu"),
            ],
            [
                InlineKeyboardButton("⬅️ Return to Dashboard", callback_data="menu:main"),
            ],
        ]
    )


def build_admin_credits_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🎯 Generate Credit Codes", callback_data="admin:gen_credits"),
                InlineKeyboardButton("📊 Credit Usage Stats", callback_data="admin:credit_stats"),
            ],
            [
                InlineKeyboardButton("🎁 Grant Credits Direct", callback_data="admin:grant_credits"),
                InlineKeyboardButton("🔍 Audit Credit History", callback_data="admin:credit_audit"),
            ],
            [
                InlineKeyboardButton("⬅️ Back to Admin Panel", callback_data="menu:admin"),
            ],
        ]
    )


def build_admin_plans_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🥈 Generate Silver Codes", callback_data="admin:gen_silver"),
                InlineKeyboardButton("🥇 Generate Gold Codes", callback_data="admin:gen_gold"),
            ],
            [
                InlineKeyboardButton("💎 Generate Platinum Codes", callback_data="admin:gen_platinum"),
                InlineKeyboardButton("📈 Plan Usage Analytics", callback_data="admin:plan_stats"),
            ],
            [
                InlineKeyboardButton("👤 Manage User Plans", callback_data="admin:manage_plans"),
                InlineKeyboardButton("💰 Revenue Analytics", callback_data="admin:revenue_stats"),
            ],
            [
                InlineKeyboardButton("⬅️ Back to Admin Panel", callback_data="menu:admin"),
            ],
        ]
    )


def build_dow_keyboard() -> InlineKeyboardMarkup:
    days = [
        ("📅 Mon", 0), ("📅 Tue", 1), ("📅 Wed", 2), ("📅 Thu", 3), 
        ("📅 Fri", 4), ("🌴 Sat", 5), ("🌞 Sun", 6)
    ]
    row1 = [InlineKeyboardButton(label, callback_data=f"repeat:set_dow:{val}") for label, val in days[:4]]
    row2 = [InlineKeyboardButton(label, callback_data=f"repeat:set_dow:{val}") for label, val in days[4:]]
    return InlineKeyboardMarkup([row1, row2, [InlineKeyboardButton("⬅️ Back to Repeat", callback_data="menu:repeat")]])


async def _edit_anchor_or_send(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    session: SessionState,
    text: str,
    markup: Optional[InlineKeyboardMarkup] = None,
):
    # prefer editing anchor message
    if session.anchor_message_id and session.anchor_chat_id:
        try:
            await context.bot.edit_message_text(
                chat_id=session.anchor_chat_id,
                message_id=session.anchor_message_id,
                text=text,
                reply_markup=markup,
            )
            return
        except Exception:
            pass
    # fallback: send a new anchor and capture ids
    msg_source = update.message or update.callback_query.message
    msg = await msg_source.reply_text(text, reply_markup=markup)
    session.anchor_chat_id = msg.chat_id
    session.anchor_message_id = msg.message_id


async def show_settings_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    profile = user_store.get_user(user.id)
    session = get_session(user.id)
    text = "Settings"
    markup = build_settings_menu(profile)
    await _edit_anchor_or_send(update, context, session, text, markup)


async def show_main_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    profile = user_store.get_user(user.id)
    # update profile display info
    profile.name = f"{user.first_name or ''} {user.last_name or ''}".strip() or profile.name
    profile.username = user.username or profile.username
    profile.last_seen = to_iso(now_utc())
    user_store.put_user(profile)
    session = get_session(user.id)
    tier_info = get_user_tier_info(profile)
    tier_name = profile.premium_tier if profile.is_premium else "FREE"
    
    tier_emojis = {"FREE": "🆓", "SILVER": "🥈", "GOLD": "🥇", "PLATINUM": "💎"}
    plan_emoji = tier_emojis.get(tier_name, "🆓")
    
    active_reminders = len([r for r in reminder_store.list_by_user(user.id) if not r.done])
    max_reminders = tier_info['max_active']
    
    text = (
        f"🌟 **Welcome back, {profile.name or 'there'}!**\n"
        "━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "🤖 **Your AI-Powered Reminder Assistant**\n"
        "*Never forget what matters most* ✨\n\n"
        f"{plan_emoji} **Plan:** {tier_name}\n"
        f"💎 **Credits:** {profile.credits:,}\n"
        f"⏰ **Active:** {active_reminders:,}/{max_reminders:,} reminders\n"
        f"🌍 **Timezone:** {profile.timezone}\n\n"
        "🚀 **Ready to stay organized? Let's go!**"
    )
    markup = build_main_menu(profile)
    await _edit_anchor_or_send(update, context, session, text, markup)
    session.mode = "idle"
    # Background sync stats if enabled
    with suppress(Exception):
        await push_stats_if_enabled()


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_main_menu(update, context)


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_main_menu(update, context)


def _extract_after_pipe(text: str) -> Tuple[str, str]:
    if "|" not in text:
        return text.strip(), ""
    left, right = text.split("|", 1)
    return left.strip(), right.strip()


async def cmd_remind(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Redirect to inline flow
    await show_main_menu(update, context)
    await on_callback(
        type("Q", (), {"callback_query": type("CQ", (), {"data": "menu:new", "answer": (lambda *a, **k: None), "message": update.message})(), "effective_user": update.effective_user})(),
        context,
    )


async def cmd_repeat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message
    user = update.effective_user
    profile = user_store.get_user(user.id)
    args = message.text.split(maxsplit=1)
    if len(args) == 1:
        await message.reply_text(
            "Usage examples:\n"
            "/repeat daily | Meditate at 07:30\n"
            "/repeat weekly 1 0 | Review at 09:00  (0=Mon ... 6=Sun)"
        )
        return
    left, what = _extract_after_pipe(args[1])
    parts = left.split()
    if len(parts) < 1 or not what:
        await message.reply_text("Invalid format. See /help for /repeat usage.")
        return

    kind = parts[0].lower()
    interval = 1
    dow = None
    time_at = None
    # Expect last token(s) contain 'at HH:MM' in message text; we’ll parse from 'what'
    # Allow '... at HH:MM' at the end of what
    if " at " in what:
        what_text, time_part = what.rsplit(" at ", 1)
        what = what_text.strip()
        time_at = time_part.strip()
    else:
        await message.reply_text("Please specify time using 'at HH:MM'.")
        return

    if kind not in {"daily", "weekly"}:
        await message.reply_text("Only daily or weekly are supported.")
        return

    if kind == "weekly":
        if len(parts) >= 2 and parts[1].isdigit():
            interval = int(parts[1])
        if len(parts) >= 3 and parts[2].isdigit():
            dow = int(parts[2])

    if kind == "daily" and len(parts) >= 2 and parts[1].isdigit():
        interval = int(parts[1])

    # Build initial datetime from next occurrence at specified local time
    tz = pytz.timezone(profile.timezone)
    now_local = now_utc().astimezone(tz)
    try:
        hour, minute = [int(x) for x in time_at.split(":", 1)]
    except Exception:
        await message.reply_text("Time must be HH:MM, e.g., 07:30")
        return

    if kind == "daily":
        target_local = now_local.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if target_local <= now_local:
            target_local += timedelta(days=1)
        rec = {"type": "daily", "interval": interval}
    else:
        # weekly
        target_local = now_local.replace(hour=hour, minute=minute, second=0, microsecond=0)
        desired_dow = dow if dow is not None else target_local.weekday()
        while target_local.weekday() != desired_dow or target_local <= now_local:
            target_local += timedelta(days=1)
        rec = {"type": "weekly", "interval": interval, "dow": desired_dow}

    active_count = len([r for r in reminder_store.list_by_user(user.id) if not r.done])
    ok, reason = CreditPolicy.can_create(profile, active_count)
    if not ok:
        await message.reply_text(reason)
        return
    cost = CreditPolicy.consume_on_create(profile)
    if cost:
        profile.credits -= cost
        user_store.put_user(profile)

    reminder = Reminder(
        id=uuid.uuid4().hex[:10],
        chat_id=message.chat_id,
        user_id=user.id,
        text=what,
        when_iso=to_iso(target_local.astimezone(pytz.UTC)),
        timezone=profile.timezone,
        created_at=to_iso(now_utc()),
        recurring=rec,
    )
    reminder_store.put(reminder)

    app: Application = context.application
    scheduler.schedule_once(from_iso(reminder.when_iso), reminder.id)

    await message.reply_text(
        f"Repeating set: {what}\nFirst: {human_dt(from_iso(reminder.when_iso), profile.timezone)}\nKind: {rec['type']}"
    )


async def cmd_list(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_main_menu(update, context)
    user = update.effective_user
    profile = user_store.get_user(user.id)
    reminders = [r for r in reminder_store.list_by_user(user.id) if not r.done]
    session = get_session(user.id)
    if not reminders:
        await _edit_anchor_or_send(update, context, session, "No active reminders.", InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:main")]]))
        return
    lines = [f"• {r.id}: {r.text} — {human_dt(from_iso(r.when_iso), profile.timezone)}" for r in reminders[:30]]
    more = "" if len(reminders) <= 30 else f"\n(+{len(reminders)-30} more)"
    await _edit_anchor_or_send(update, context, session, "\n".join(lines) + more, InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:main")]]))


async def cmd_delete(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = update.message.text.split(maxsplit=1)
    if len(args) == 1:
        await show_main_menu(update, context)
        session = get_session(update.effective_user.id)
        # list to help user pick an ID
        reminders = [r for r in reminder_store.list_by_user(update.effective_user.id) if not r.done]
        if not reminders:
            text = "No active reminders."
        else:
            profile = user_store.get_user(update.effective_user.id)
            lines = [f"• {r.id}: {r.text} — {human_dt(from_iso(r.when_iso), profile.timezone)}" for r in reminders[:30]]
            text = "\n".join(lines)
        await _edit_anchor_or_send(update, context, session, text, InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:main")]]))
        return
    rid = args[1].strip()
    r = reminder_store.get(rid)
    if not r:
        await update.message.reply_text("Not found.")
        return
    reminder_store.delete(rid)
    await update.message.reply_text("Deleted.")


async def cmd_timezone(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # route to settings flow
    await show_main_menu(update, context)
    session = get_session(update.effective_user.id)
    session.mode = "settings_timezone"
    await _edit_anchor_or_send(
        update,
        context,
        session,
        "Send your timezone (e.g., Europe/Berlin).",
        InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:settings")]]),
    )


async def cmd_profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await show_main_menu(update, context)
    user = update.effective_user
    profile = user_store.get_user(user.id)
    tier = "Premium" if profile.is_premium else "Free"
    session = get_session(user.id)
    await _edit_anchor_or_send(
        update,
        context,
        session,
        f"Plan: {tier}\nCredits: {profile.credits}\nTimezone: {profile.timezone}",
        InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:main")]]),
    )


async def cmd_redeem(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    profile = user_store.get_user(user.id)
    args = update.message.text.split(maxsplit=1)
    if len(args) == 1:
        # Switch to inline redeem flow to keep UI consistent
        await show_main_menu(update, context)
        session = get_session(user.id)
        session.mode = "redeem_code"
        await _edit_anchor_or_send(
            update,
            context,
            session,
            "Enter your redeem code:",
            InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:main")]]),
        )
        return
    code_str = args[1].strip().upper()
    code = codes_store.get(code_str)
    if not code:
        await update.message.reply_text("Invalid code.")
        return
    if code.expires_at and from_iso(code.expires_at) < now_utc():
        await update.message.reply_text("This code has expired.")
        return
    if code.used >= code.max_uses:
        await update.message.reply_text("This code has been fully redeemed.")
        return

    if code.kind == "credits":
        profile.credits += code.amount
        user_store.put_user(profile)
        codes_store.inc_used(code.code)
        await update.message.reply_text(f"Added {code.amount} credits. New balance: {profile.credits}")
    elif code.kind == "premium":
        # Simple: flip premium on; for fuller impl track expiry
        profile.is_premium = True
        user_store.put_user(profile)
        codes_store.inc_used(code.code)
        await update.message.reply_text("Premium activated! 🎉")
    elif code.kind == "plan":
        # Activate premium tier
        profile.is_premium = True
        profile.premium_tier = code.plan_name or "PREMIUM"
        user_store.put_user(profile)
        codes_store.inc_used(code.code)
        
        tier_info = get_user_tier_info(profile)
        tier_name = profile.premium_tier
        
        text = (
            f"🎉 **{tier_name} Plan Activated!**\n\n"
            f"✨ **Your new benefits:**\n"
            f"⏰ {tier_info['max_active']} active reminders\n"
            f"🔄 {tier_info['snooze_limit']} snoozes per reminder\n"
            f"🎯 Premium features unlocked\n\n"
            f"🚀 **Welcome to {tier_name}!**"
        )
        await update.message.reply_text(text)
    else:
        await update.message.reply_text("Unknown code type.")


# -------- Inline callbacks --------


async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = update.effective_user
    profile = user_store.get_user(user.id)
    session = get_session(user.id)
    await query.answer()
    data = (query.data or "").strip()

    # Navigation & settings
    if data.startswith("menu:"):
        _, name = data.split(":", 1)
        if name == "main":
            await show_main_menu(update, context)
            return
        if name == "new":
            session.mode = "create_reminder_type"
            session.temp_when_dt = None
            session.temp_when_text = None
            session.temp_text = None
            
            text = (
                "✨ **Create New Reminder**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "🎯 **Choose creation method:**\n\n"
                "⚡ **Quick Create** - Fast & simple\n"
                "🧠 **Smart Create** - AI-powered suggestions\n"
                "📝 **Manual Create** - Full control\n\n"
                "💡 Smart Create analyzes your text for optimal timing!"
            )
            
            buttons = [
                [InlineKeyboardButton("⚡ Quick Create", callback_data="create:quick")],
            ]
            
            if has_feature(profile, "smart_scheduling"):
                buttons.insert(0, [InlineKeyboardButton("🧠 Smart Create", callback_data="create:smart")])
            
            buttons.extend([
                [InlineKeyboardButton("📝 Manual Create", callback_data="create:manual")],
                [InlineKeyboardButton("⬅️ Back to Main", callback_data="menu:main")]
            ])
            
            await _edit_anchor_or_send(update, context, session, text, InlineKeyboardMarkup(buttons))
            return
        if name == "list":
            reminders = [r for r in reminder_store.list_by_user(user.id) if not r.done]
            if not reminders:
                text = (
                    "📝 **Your Reminders**\n"
                    "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    "🌟 No active reminders yet!\n\n"
                    "Ready to create your first one? ✨"
                )
            else:
                profile = user_store.get_user(user.id)
                lines = [f"⏰ **{r.id}:** {r.text}\n   📅 {human_dt(from_iso(r.when_iso), profile.timezone)}" for r in reminders[:30]]
                more = "" if len(reminders) <= 30 else f"\n\n💫 (+{len(reminders)-30} more reminders)"
                text = (
                    f"📝 **Your Reminders** ({len(reminders)} active)\n"
                    "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    + "\n\n".join(lines) + more
                )
            markup = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Main", callback_data="menu:main")]])
            await _edit_anchor_or_send(update, context, session, text, markup)
            return
        if name == "repeat":
            session.mode = "repeat_kind"
            session.repeat_kind = None
            session.repeat_interval = None
            session.repeat_dow = None
            session.repeat_time = None
            text = (
                "🔄 **Create Repeating Task**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "⏱️ Let's build your recurring reminder!\n\n"
                "📅 **Choose frequency:**\n"
                "• Daily - Every day at the same time\n"
                "• Weekly - Same day each week\n\n"
                "🎯 Perfect for habits and routines!"
            )
            markup = InlineKeyboardMarkup([
                [InlineKeyboardButton("🌅 Daily", callback_data="repeat:kind:daily"), InlineKeyboardButton("📆 Weekly", callback_data="repeat:kind:weekly")],
                [InlineKeyboardButton("⬅️ Back to Main", callback_data="menu:main")],
            ])
            await _edit_anchor_or_send(update, context, session, text, markup)
            return
        if name == "settings":
            text = (
                "⚙️ **Settings & Preferences**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "🔧 Customize your reminder experience:\n\n"
                f"🌍 **Current Timezone:** {profile.timezone}\n"
                f"🔔 **Notifications:** Enabled\n"
                f"🎨 **Theme:** Modern\n\n"
                "Choose what you'd like to configure:"
            )
            markup = build_settings_menu(profile)
            await _edit_anchor_or_send(update, context, session, text, markup)
            return
        if name == "redeem":
            session.mode = "redeem_code"
            text = (
                "🎫 **Redeem Your Code**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "💎 Enter your redemption code below:\n\n"
                "📝 Examples:\n"
                "• `MIKU-CR100-XXXXXX` (Credits)\n"
                "• `MIKU-PREMIUM-XXXXXX` (Premium Plan)\n\n"
                "✨ Ready to unlock rewards?"
            )
            markup = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Main", callback_data="menu:main")]])
            await _edit_anchor_or_send(update, context, session, text, markup)
            return
        if name == "profile":
            plan_emoji = "👑" if profile.is_premium else "🆓"
            tier = "Premium" if profile.is_premium else "Free"
            join_date = profile.first_seen or "Unknown"
            reminders_count = len([r for r in reminder_store.list_by_user(user.id) if not r.done])
            
            text = (
                f"👤 **Your Profile**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"🏷️ **Name:** {profile.name or 'Not set'}\n"
                f"📱 **Username:** @{profile.username or 'Not set'}\n"
                f"{plan_emoji} **Plan:** {tier}\n"
                f"💎 **Credits:** {profile.credits}\n"
                f"🌍 **Timezone:** {profile.timezone}\n"
                f"📅 **Member since:** {join_date[:10] if join_date else 'Unknown'}\n"
                f"⏰ **Active reminders:** {reminders_count}\n\n"
                "🌟 Thank you for using our bot!"
            )
            markup = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Main", callback_data="menu:main")]])
            await _edit_anchor_or_send(update, context, session, text, markup)
            return
        if name == "templates":
            if not has_feature(profile, "templates"):
                await _edit_anchor_or_send(update, context, session,
                    "🔒 **Premium Feature**\n\n"
                    "📋 Templates are available for Silver tier and above!\n\n"
                    "✨ **Upgrade Benefits:**\n"
                    "• Quick reminder templates\n"
                    "• Smart time suggestions\n"
                    "• Category organization\n\n"
                    "💎 Ready to upgrade?",
                    InlineKeyboardMarkup([
                        [InlineKeyboardButton("💎 Upgrade Now", callback_data="menu:upgrade")],
                        [InlineKeyboardButton("⬅️ Back to Main", callback_data="menu:main")]
                    ]))
                return
            
            text = (
                "📋 **Quick Templates**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "🚀 **Create reminders instantly!**\n\n"
                "Choose from our smart templates:\n"
                "• Pre-configured timing\n"
                "• Optimized for your routine\n"
                "• Categories included\n\n"
                "✨ Select a template below:"
            )
            await _edit_anchor_or_send(update, context, session, text, build_templates_menu())
            return
        
        if name == "categories":
            if not has_feature(profile, "categories"):
                await _edit_anchor_or_send(update, context, session,
                    "🔒 **Premium Feature**\n\n"
                    "📂 Categories are available for Silver tier and above!\n\n"
                    "💎 Ready to upgrade?",
                    InlineKeyboardMarkup([
                        [InlineKeyboardButton("💎 Upgrade Now", callback_data="menu:upgrade")],
                        [InlineKeyboardButton("⬅️ Back to Main", callback_data="menu:main")]
                    ]))
                return
            
            text = (
                "📂 **Reminder Categories**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "🎯 **Organize your reminders:**\n\n"
                "View reminders by category to stay focused\n"
                "and organized. Choose a category below:"
            )
            await _edit_anchor_or_send(update, context, session, text, build_categories_menu())
            return
        
        if name == "upgrade":
            current_tier = profile.premium_tier
            text = (
                "💎 **Upgrade Your Plan**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"📊 **Current Plan:** {current_tier}\n\n"
                "🥈 **SILVER** - Enhanced Productivity\n"
                "• 100 active reminders\n"
                "• 3 snoozes per reminder\n"
                "• Templates & Categories\n\n"
                "🥇 **GOLD** - Smart Organization\n"
                "• 300 active reminders\n"
                "• 5 snoozes per reminder\n"
                "• Smart scheduling & Location reminders\n\n"
                "💎 **PLATINUM** - Ultimate Experience\n"
                "• 1000 active reminders\n"
                "• 10 snoozes per reminder\n"
                "• AI suggestions & Team sharing\n\n"
                "🎫 **Have a code? Use /redeem**"
            )
            markup = InlineKeyboardMarkup([
                [InlineKeyboardButton("🎫 Redeem Code", callback_data="menu:redeem")],
                [InlineKeyboardButton("📞 Contact Support", url="https://t.me/your_support")],
                [InlineKeyboardButton("⬅️ Back to Main", callback_data="menu:main")]
            ])
            await _edit_anchor_or_send(update, context, session, text, markup)
            return
        
        if name == "admin":
            if not is_admin(user.id):
                await _edit_anchor_or_send(update, context, session, 
                    "🔒 **Access Denied**\n\n"
                    "❌ This area is restricted to administrators only.\n\n"
                    "🤝 If you need assistance, please contact support.",
                    InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Main", callback_data="menu:main")]]))
                return
            
            # Get system statistics for admin dashboard
            users_data = user_store.store.get()
            all_reminders = reminder_store.all()
            total_users = len(users_data)
            active_reminders = len([r for r in all_reminders if not r.done])
            premium_users = sum(1 for u in users_data.values() if u.get("is_premium"))
            
            text = (
                "🛠️ **Administrative Control Center**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "⚡ **System Overview:**\n"
                f"👥 Total Users: {total_users:,}\n"
                f"💎 Premium Users: {premium_users:,}\n"
                f"⏰ Active Reminders: {active_reminders:,}\n\n"
                "🎛️ **Management Tools:**\n"
                "• Revenue & Analytics\n"
                "• User & Plan Management\n"
                "• System Configuration\n"
                "• Security & Monitoring\n\n"
                "🚀 **Select management area:**"
            )
            await _edit_anchor_or_send(update, context, session, text, build_admin_menu())
            return

    if data.startswith("settings:"):
        _, name = data.split(":", 1)
        if name == "tz":
            session.mode = "settings_timezone"
            text = (
                "🌍 **Set Your Timezone**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "🕐 Please enter your timezone for accurate reminders:\n\n"
                "📍 **Examples:**\n"
                "• `Europe/Berlin`\n"
                "• `America/New_York`\n"
                "• `Asia/Tokyo`\n"
                "• `Australia/Sydney`\n\n"
                "✨ This ensures I remind you at the right time!"
            )
            markup = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Settings", callback_data="menu:settings")]])
            await _edit_anchor_or_send(update, context, session, text, markup)
            return
        if name == "notifications":
            text = (
                "🔔 **Notification Settings**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "✅ **Current Status:** Enabled\n\n"
                "📱 All notifications are currently active!\n"
                "This feature is coming soon with more options.\n\n"
                "🎯 Stay tuned for customizable alerts!"
            )
            markup = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Settings", callback_data="menu:settings")]])
            await _edit_anchor_or_send(update, context, session, text, markup)
            return
        if name == "appearance":
            text = (
                "🎨 **Appearance Settings**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "✨ **Current Theme:** Modern Dark\n\n"
                "🌟 You're already using our sleek design!\n"
                "Custom themes coming in future updates.\n\n"
                "💫 This bot will look modern even in 2050!"
            )
            markup = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Settings", callback_data="menu:settings")]])
            await _edit_anchor_or_send(update, context, session, text, markup)
            return

    # Template selection
    if data.startswith("template:"):
        _, template_id = data.split(":", 1)
        if not has_feature(profile, "templates"):
            await _edit_anchor_or_send(update, context, session, "Premium feature.", InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:main")]]))
            return
        
        template = DEFAULT_TEMPLATES.get(template_id)
        if not template:
            await _edit_anchor_or_send(update, context, session, "Template not found.", InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:templates")]]))
            return
        
        # Store template info in session for time selection
        session.temp_template = template
        session.mode = "template_time"
        
        suggested_times = template.get("suggested_times", [])
        time_buttons = []
        for i in range(0, len(suggested_times), 2):
            row = []
            for j in range(2):
                if i + j < len(suggested_times):
                    time = suggested_times[i + j]
                    row.append(InlineKeyboardButton(time, callback_data=f"template_time:{time}"))
            time_buttons.append(row)
        
        time_buttons.append([InlineKeyboardButton("⌨️ Custom Time", callback_data="template_time:custom")])
        time_buttons.append([InlineKeyboardButton("⬅️ Back to Templates", callback_data="menu:templates")])
        
        text = (
            f"{template['icon']} **{template['name']}**\n"
            "━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📝 **Reminder:** {template['text']}\n"
            f"📂 **Category:** {template['category']}\n\n"
            "⏰ **Choose a time:**"
        )
        
        await _edit_anchor_or_send(update, context, session, text, InlineKeyboardMarkup(time_buttons))
        return
    
    # Creation method selection
    if data.startswith("create:"):
        _, method = data.split(":", 1)
        
        if method == "quick":
            session.mode = "create_when"
            text = (
                "⚡ **Quick Create**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "⏰ **When should I remind you?**\n\n"
                "📝 Just tell me naturally:\n"
                "• `in 2 hours`\n"
                "• `tomorrow at 9am`\n"
                "• `Friday 3pm`\n\n"
                "✨ I understand natural language!"
            )
            markup = InlineKeyboardMarkup([
                [InlineKeyboardButton("⬅️ Back", callback_data="menu:new"), InlineKeyboardButton("❌ Cancel", callback_data="flow:cancel")]
            ])
            await _edit_anchor_or_send(update, context, session, text, markup)
            return
        
        elif method == "smart":
            if not has_feature(profile, "smart_scheduling"):
                await _edit_anchor_or_send(update, context, session, "Premium feature.", InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:new")]]))
                return
            
            session.mode = "smart_create_text"
            text = (
                "🧠 **Smart Create**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "💭 **What should I remind you about?**\n\n"
                "📝 Describe your reminder and I'll suggest:\n"
                "• 🕐 Optimal timing\n"
                "• 📂 Best category\n"
                "• ⭐ Priority level\n\n"
                "🤖 **AI-powered intelligence at work!**"
            )
            await _edit_anchor_or_send(update, context, session, text, InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:new")]]))
            return
        
        elif method == "manual":
            session.mode = "manual_create_text"
            text = (
                "📝 **Manual Create**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "💭 **What should I remind you about?**\n\n"
                "📋 You'll have full control over:\n"
                "• ⏰ Timing\n"
                "• 📂 Category\n"
                "• ⭐ Priority\n"
                "• 🏷️ Tags\n\n"
                "✍️ **Start by describing your reminder:**"
            )
            await _edit_anchor_or_send(update, context, session, text, InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:new")]]))
            return
    
    # Smart time selection
    if data.startswith("smart_time:"):
        _, time_choice = data.split(":", 1)
        
        if time_choice == "custom":
            session.mode = "smart_create_custom_time"
            text = (
                "⏰ **Custom Time**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "📝 Enter when you want to be reminded:\n\n"
                "Examples:\n"
                "• `in 2 hours`\n"
                "• `tomorrow at 9am`\n"
                "• `Friday 3pm`\n\n"
                "✨ Use natural language!"
            )
            await _edit_anchor_or_send(update, context, session, text, InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:new")]]))
            return
        
        # Process smart time selection and create reminder
        try:
            tz = pytz.timezone(profile.timezone)
            now_local = now_utc().astimezone(tz)
            hour, minute = [int(x) for x in time_choice.split(":")]
            target_local = now_local.replace(hour=hour, minute=minute, second=0, microsecond=0)
            
            # If time has passed today, schedule for tomorrow
            if target_local <= now_local:
                target_local += timedelta(days=1)
            
            target_utc = target_local.astimezone(pytz.UTC)
            
            # Get suggestion data
            suggestions = getattr(session, 'temp_suggestions', {})
            category = suggestions.get('detected_category', 'other')
            
            # Create reminder
            active_count = len([r for r in reminder_store.list_by_user(user.id) if not r.done])
            ok, reason = CreditPolicy.can_create(profile, active_count)
            if not ok:
                await _edit_anchor_or_send(update, context, session, reason, InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:main")]]))
                return
            
            cost = CreditPolicy.consume_on_create(profile)
            if cost:
                profile.credits -= cost
                user_store.put_user(profile)
            
            reminder = Reminder(
                id=uuid.uuid4().hex[:10],
                chat_id=update.effective_chat.id,
                user_id=user.id,
                text=session.temp_text,
                when_iso=to_iso(target_utc),
                timezone=profile.timezone,
                created_at=to_iso(now_utc()),
                category=category,
                priority=2  # Smart created reminders get higher priority
            )
            reminder_store.put(reminder)
            scheduler.schedule_once(target_utc, reminder.id)
            
            cat_info = REMINDER_CATEGORIES.get(category, {"name": "Other", "icon": "📌"})
            
            text = (
                f"🎉 **Smart Reminder Created!**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"💭 **What:** {reminder.text}\n"
                f"📅 **When:** {human_dt(target_utc, profile.timezone)}\n"
                f"📂 **Category:** {cat_info['icon']} {cat_info['name']}\n"
                f"⭐ **Priority:** {'★' * reminder.priority}\n"
                f"🆔 **ID:** `{reminder.id}`\n\n"
                "🧠 **AI-optimized timing selected!**"
            )
            
            await _edit_anchor_or_send(update, context, session, text, InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Main", callback_data="menu:main")]]))
            session.mode = "idle"
            return
            
        except Exception as e:
            await _edit_anchor_or_send(update, context, session, f"Invalid time format: {time_choice}", InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:new")]]))
            return
    
    # Manual category selection
    if data.startswith("manual_cat:"):
        _, category = data.split(":", 1)
        session.temp_category = category
        session.mode = "manual_create_priority"
        
        # Priority selection
        priority_buttons = [
            [InlineKeyboardButton("⭐ Low (★)", callback_data="manual_priority:1")],
            [InlineKeyboardButton("⭐⭐ Normal (★★)", callback_data="manual_priority:2")],
            [InlineKeyboardButton("⭐⭐⭐ High (★★★)", callback_data="manual_priority:3")],
            [InlineKeyboardButton("⭐⭐⭐⭐ Urgent (★★★★)", callback_data="manual_priority:4")],
            [InlineKeyboardButton("⭐⭐⭐⭐⭐ Critical (★★★★★)", callback_data="manual_priority:5")],
            [InlineKeyboardButton("⬅️ Back", callback_data="menu:new")]
        ]
        
        cat_info = REMINDER_CATEGORIES.get(category, {"name": "Other", "icon": "📌"})
        
        text = (
            f"⭐ **Set Priority Level**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"💭 **Reminder:** {session.temp_text}\n"
            f"📂 **Category:** {cat_info['icon']} {cat_info['name']}\n\n"
            f"🎯 **Choose the priority level:**"
        )
        
        await _edit_anchor_or_send(update, context, session, text, InlineKeyboardMarkup(priority_buttons))
        return
    
    # Manual priority selection
    if data.startswith("manual_priority:"):
        _, priority_str = data.split(":", 1)
        session.temp_priority = int(priority_str)
        session.mode = "manual_create_time"
        
        # Show smart time suggestions for the category
        optimal_times = SmartScheduler.get_optimal_times(profile, session.temp_category)
        
        time_buttons = []
        for i in range(0, len(optimal_times), 3):
            row = []
            for j in range(3):
                if i + j < len(optimal_times):
                    time = optimal_times[i + j]
                    row.append(InlineKeyboardButton(f"🕐 {time}", callback_data=f"manual_time:{time}"))
            time_buttons.append(row)
        
        time_buttons.append([InlineKeyboardButton("⌨️ Custom Time", callback_data="manual_time:custom")])
        time_buttons.append([InlineKeyboardButton("⬅️ Back", callback_data="menu:new")])
        
        cat_info = REMINDER_CATEGORIES.get(session.temp_category, {"name": "Other", "icon": "📌"})
        
        text = (
            f"⏰ **Choose Timing**\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"💭 **Reminder:** {session.temp_text}\n"
            f"📂 **Category:** {cat_info['icon']} {cat_info['name']}\n"
            f"⭐ **Priority:** {'★' * session.temp_priority}\n\n"
            f"🕐 **Optimal times for {cat_info['name']}:**"
        )
        
        await _edit_anchor_or_send(update, context, session, text, InlineKeyboardMarkup(time_buttons))
        return
    
    # Manual time selection
    if data.startswith("manual_time:"):
        _, time_choice = data.split(":", 1)
        
        if time_choice == "custom":
            session.mode = "manual_create_custom_time"
            text = (
                "⏰ **Custom Time**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "📝 Enter when you want to be reminded:\n\n"
                "Examples:\n"
                "• `in 2 hours`\n"
                "• `tomorrow at 9am`\n"
                "• `Friday 3pm`\n\n"
                "✨ Use natural language!"
            )
            await _edit_anchor_or_send(update, context, session, text, InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:new")]]))
            return
        
        # Process manual time selection and create reminder
        try:
            tz = pytz.timezone(profile.timezone)
            now_local = now_utc().astimezone(tz)
            hour, minute = [int(x) for x in time_choice.split(":")]
            target_local = now_local.replace(hour=hour, minute=minute, second=0, microsecond=0)
            
            # If time has passed today, schedule for tomorrow
            if target_local <= now_local:
                target_local += timedelta(days=1)
            
            target_utc = target_local.astimezone(pytz.UTC)
            
            # Create reminder with all manual settings
            active_count = len([r for r in reminder_store.list_by_user(user.id) if not r.done])
            ok, reason = CreditPolicy.can_create(profile, active_count)
            if not ok:
                await _edit_anchor_or_send(update, context, session, reason, InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:main")]]))
                return
            
            cost = CreditPolicy.consume_on_create(profile)
            if cost:
                profile.credits -= cost
                user_store.put_user(profile)
            
            reminder = Reminder(
                id=uuid.uuid4().hex[:10],
                chat_id=update.effective_chat.id,
                user_id=user.id,
                text=session.temp_text,
                when_iso=to_iso(target_utc),
                timezone=profile.timezone,
                created_at=to_iso(now_utc()),
                category=session.temp_category,
                priority=session.temp_priority
            )
            reminder_store.put(reminder)
            scheduler.schedule_once(target_utc, reminder.id)
            
            cat_info = REMINDER_CATEGORIES.get(session.temp_category, {"name": "Other", "icon": "📌"})
            
            text = (
                f"🎉 **Manual Reminder Created!**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"💭 **What:** {reminder.text}\n"
                f"📅 **When:** {human_dt(target_utc, profile.timezone)}\n"
                f"📂 **Category:** {cat_info['icon']} {cat_info['name']}\n"
                f"⭐ **Priority:** {'★' * reminder.priority}\n"
                f"🆔 **ID:** `{reminder.id}`\n\n"
                "🎯 **Perfectly customized to your preferences!**"
            )
            
            await _edit_anchor_or_send(update, context, session, text, InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Main", callback_data="menu:main")]]))
            session.mode = "idle"
            return
            
        except Exception as e:
            await _edit_anchor_or_send(update, context, session, f"Invalid time format: {time_choice}", InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:new")]]))
            return
    
    # Export data
    if data.startswith("export:"):
        _, format_type = data.split(":", 1)
        
        if not has_feature(profile, "smart_scheduling"):
            await _edit_anchor_or_send(update, context, session, "Premium feature.", InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:main")]]))
            return
        
        try:
            # Generate export
            export_data = ExportEngine.export_user_data(user.id, format_type)
            format_info = EXPORT_FORMATS.get(format_type, {"name": "Unknown", "extension": ".txt"})
            
            if format_type == "json":
                export_content = json.dumps(export_data, indent=2, ensure_ascii=False)
            elif format_type == "csv":
                export_content = export_data
            else:  # txt
                export_content = export_data
            
            # For demonstration, we'll show a preview and provide download info
            preview = export_content[:500] + "..." if len(export_content) > 500 else export_content
            
            text = (
                f"✅ **Export Generated Successfully!**\n"
                f"━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"💾 **Format:** {format_info['name']}\n"
                f"📅 **Generated:** {now_utc().strftime('%Y-%m-%d %H:%M UTC')}\n"
                f"📄 **Size:** {len(export_content):,} characters\n\n"
                f"🔍 **Preview:**\n"
                f"```\n{preview}\n```\n\n"
                f"💾 **Your data has been exported successfully!**\n"
                f"*Contact support to receive the full export file*"
            )
            
            await _edit_anchor_or_send(update, context, session, text, InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Main", callback_data="menu:main")]]))
            
        except Exception as e:
            await _edit_anchor_or_send(update, context, session, f"❌ Export failed: {e}", InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:main")]]))
        return
    
    # Category browsing
    if data.startswith("category:"):
        _, category = data.split(":", 1)
        
        # Show reminders in this category
        user_reminders = [r for r in reminder_store.list_by_user(user.id) if not r.done and (r.category == category)]
        cat_info = REMINDER_CATEGORIES.get(category, {"name": "Other", "icon": "📌"})
        
        if not user_reminders:
            text = (
                f"{cat_info['icon']} **{cat_info['name']} Reminders**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "🚫 **No active reminders in this category**\n\n"
                "✨ Create your first one!"
            )
        else:
            lines = []
            for r in user_reminders[:10]:  # Limit to 10 for readability
                priority_stars = '★' * r.priority if r.priority else ''
                lines.append(f"• {priority_stars} {r.text}\n   📅 {human_dt(from_iso(r.when_iso), profile.timezone)}")
            
            more_text = f"\n\n📊 (+{len(user_reminders)-10} more)" if len(user_reminders) > 10 else ""
            
            text = (
                f"{cat_info['icon']} **{cat_info['name']} Reminders** ({len(user_reminders)})\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                + "\n\n".join(lines) + more_text
            )
        
        await _edit_anchor_or_send(update, context, session, text, InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Categories", callback_data="menu:categories")]]))
        return
    
    # Template time selection
    if data.startswith("template_time:"):
        _, time_choice = data.split(":", 1)
        
        if time_choice == "custom":
            session.mode = "template_custom_time"
            text = (
                "⏰ **Custom Time**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "📝 Enter when you want to be reminded:\n\n"
                "Examples:\n"
                "• `in 2 hours`\n"
                "• `tomorrow at 9am`\n"
                "• `Friday 3pm`\n\n"
                "✨ Use natural language!"
            )
            await _edit_anchor_or_send(update, context, session, text, InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:templates")]]))
            return
        
        # Process time selection and create reminder
        template = getattr(session, 'temp_template', None)
        if not template:
            await _edit_anchor_or_send(update, context, session, "Template session expired.", InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:templates")]]))
            return
        
        # Parse time (today at specified time)
        try:
            tz = pytz.timezone(profile.timezone)
            now_local = now_utc().astimezone(tz)
            hour, minute = [int(x) for x in time_choice.split(":")]
            target_local = now_local.replace(hour=hour, minute=minute, second=0, microsecond=0)
            
            # If time has passed today, schedule for tomorrow
            if target_local <= now_local:
                target_local += timedelta(days=1)
            
            target_utc = target_local.astimezone(pytz.UTC)
            
            # Create reminder
            active_count = len([r for r in reminder_store.list_by_user(user.id) if not r.done])
            ok, reason = CreditPolicy.can_create(profile, active_count)
            if not ok:
                await _edit_anchor_or_send(update, context, session, reason, InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:main")]]))
                return
            
            cost = CreditPolicy.consume_on_create(profile)
            if cost:
                profile.credits -= cost
                user_store.put_user(profile)
            
            reminder = Reminder(
                id=uuid.uuid4().hex[:10],
                chat_id=update.effective_chat.id,
                user_id=user.id,
                text=template['text'],
                when_iso=to_iso(target_utc),
                timezone=profile.timezone,
                created_at=to_iso(now_utc()),
                category=template['category'],
                template_id=template['id']
            )
            reminder_store.put(reminder)
            scheduler.schedule_once(target_utc, reminder.id)
            
            text = (
                f"✅ **Template Reminder Created!**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"{template['icon']} **{template['name']}**\n"
                f"📅 **When:** {human_dt(target_utc, profile.timezone)}\n"
                f"📂 **Category:** {template['category']}\n"
                f"🆔 **ID:** `{reminder.id}`\n\n"
                "🎯 All set! I'll remind you at the perfect time."
            )
            
            await _edit_anchor_or_send(update, context, session, text, InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Main", callback_data="menu:main")]]))
            session.mode = "idle"
            return
            
        except Exception as e:
            await _edit_anchor_or_send(update, context, session, f"Invalid time format: {time_choice}", InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:templates")]]))
            return

    if data.startswith("admin:"):
        _, name = data.split(":", 1)
        if not is_admin(user.id):
            await _edit_anchor_or_send(update, context, session, 
                "🔒 **Access Restricted**\n\n"
                "❌ Administrator privileges required\n"
                "🏢 Contact system administrator for access",
                InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Return to Main", callback_data="menu:main")]]))
            return
        
        # Enhanced admin menu handlers
        if name == "credits_menu":
            text = (
                "💳 **Credit Management Center**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "🎯 **Available Operations:**\n\n"
                "• Generate bulk credit codes\n"
                "• Direct credit allocation\n"
                "• Usage analytics & reporting\n"
                "• Transaction audit trails\n\n"
                "📊 **Select your action:**"
            )
            await _edit_anchor_or_send(update, context, session, text, build_admin_credits_menu())
            return
        
        if name == "plans_menu":
            text = (
                "👑 **Premium Plan Management**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "💎 **Plan Tiers Available:**\n"
                "🥈 Silver - Enhanced productivity\n"
                "🥇 Gold - Smart organization\n"
                "💎 Platinum - Ultimate experience\n\n"
                "📈 **Management Tools:**\n"
                "• Generate tier-specific codes\n"
                "• Monitor subscription analytics\n"
                "• Revenue tracking & insights\n\n"
                "🚀 **Choose operation:**"
            )
            await _edit_anchor_or_send(update, context, session, text, build_admin_plans_menu())
            return
        
        if name == "gen_credits":
            session.mode = "admin_gen_credits"
            text = (
                "💳 **Generate Credit Codes**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "📝 **Format:** `amount count [days_valid]`\n\n"
                "📋 **Examples:**\n"
                "• `100 5 30` - 5 codes worth 100 credits each, valid 30 days\n"
                "• `50 10` - 10 codes worth 50 credits each, valid 30 days (default)\n\n"
                "⚡ **Enter your parameters:**"
            )
            markup = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Credits", callback_data="admin:credits_menu")]])
            await _edit_anchor_or_send(update, context, session, text, markup)
            return
        
        if name.startswith("gen_"):
            tier = name.split("_")[1].upper()
            if tier in ["SILVER", "GOLD", "PLATINUM"]:
                session.mode = "admin_gen_plans"
                session.temp_plan_tier = tier
                tier_info = PREMIUM_TIERS.get(tier, {})
                
                text = (
                    f"👑 **Generate {tier} Plan Codes**\n"
                    "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                    f"💎 **{tier} Plan Features:**\n"
                    f"• {tier_info.get('max_active', 0)} active reminders\n"
                    f"• {tier_info.get('snooze_limit', 0)} snoozes per reminder\n"
                    f"• Premium features included\n\n"
                    "📝 **Format:** `count [days_valid]`\n\n"
                    "📋 **Examples:**\n"
                    "• `5 90` - 5 codes valid for 90 days\n"
                    "• `10` - 10 codes valid for 30 days (default)\n\n"
                    "⚡ **Enter parameters:**"
                )
                markup = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Plans", callback_data="admin:plans_menu")]])
                await _edit_anchor_or_send(update, context, session, text, markup)
                return
        
        # Additional admin features
        if name == "users_menu":
            text = (
                "👤 **User Management Center**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "🎯 **Available Operations:**\n\n"
                "• Direct user upgrades & grants\n"
                "• User activity monitoring\n"
                "• Account status management\n"
                "• Bulk user operations\n\n"
                "🛡️ **Select your action:**"
            )
            markup = InlineKeyboardMarkup([
                [InlineKeyboardButton("🎁 Grant to User", callback_data="admin:grant")],
                [InlineKeyboardButton("🔍 User Lookup", callback_data="admin:user_lookup")],
                [InlineKeyboardButton("📊 User Activity", callback_data="admin:user_activity")],
                [InlineKeyboardButton("⬅️ Back to Admin", callback_data="menu:admin")]
            ])
            await _edit_anchor_or_send(update, context, session, text, markup)
            return
        
        if name == "comms_menu":
            text = (
                "📢 **Communications Center**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "📱 **Communication Tools:**\n\n"
                "• Global announcements\n"
                "• Targeted messaging\n"
                "• User notifications\n"
                "• System alerts\n\n"
                "🎯 **Choose your method:**"
            )
            markup = InlineKeyboardMarkup([
                [InlineKeyboardButton("📢 Global Broadcast", callback_data="admin:broadcast")],
                [InlineKeyboardButton("🎯 Targeted Message", callback_data="admin:targeted_msg")],
                [InlineKeyboardButton("🔔 System Alert", callback_data="admin:system_alert")],
                [InlineKeyboardButton("⬅️ Back to Admin", callback_data="menu:admin")]
            ])
            await _edit_anchor_or_send(update, context, session, text, markup)
            return
        
        if name == "analytics_menu":
            analytics = AnalyticsEngine.get_system_analytics()
            
            text = (
                "📊 **Advanced Analytics Suite**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"📈 **Quick Overview:**\n"
                f"• {analytics['users']['total']:,} total users\n"
                f"• {analytics['reminders']['active']:,} active reminders\n"
                f"• {analytics['reminders']['completion_rate']}% completion rate\n"
                f"• {analytics['growth_metrics']['premium_conversion_rate']}% premium conversion\n\n"
                "🔍 **Detailed Reports Available:**"
            )
            markup = InlineKeyboardMarkup([
                [InlineKeyboardButton("📈 Growth Metrics", callback_data="admin:growth_report")],
                [InlineKeyboardButton("👥 User Analytics", callback_data="admin:user_analytics")],
                [InlineKeyboardButton("⏰ Reminder Insights", callback_data="admin:reminder_insights")],
                [InlineKeyboardButton("💰 Revenue Analytics", callback_data="admin:revenue_analytics")],
                [InlineKeyboardButton("⬅️ Back to Admin", callback_data="menu:admin")]
            ])
            await _edit_anchor_or_send(update, context, session, text, markup)
            return
        
        if name == "system_menu":
            settings = settings_store.get()
            sheet_enabled = settings.get("spreadsheet", {}).get("enabled", False)
            
            text = (
                "🔧 **System Configuration**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"📊 **Current Status:**\n"
                f"• Spreadsheet Sync: {'Enabled' if sheet_enabled else 'Disabled'}\n"
                f"• Bot Status: Active\n"
                f"• Data Storage: Operational\n\n"
                "⚙️ **Configuration Options:**"
            )
            markup = InlineKeyboardMarkup([
                [InlineKeyboardButton("📋 Spreadsheet Settings", callback_data="admin:sheet_settings")],
                [InlineKeyboardButton("🔄 Force Data Sync", callback_data="admin:force_sync")],
                [InlineKeyboardButton("💾 System Backup", callback_data="admin:system_backup")],
                [InlineKeyboardButton("🎛️ Bot Configuration", callback_data="admin:bot_config")],
                [InlineKeyboardButton("⬅️ Back to Admin", callback_data="menu:admin")]
            ])
            await _edit_anchor_or_send(update, context, session, text, markup)
            return
        if name == "gen_plans":
            session.mode = "admin_gen_plans"
            text = "Enter: PLAN_NAME count [days_valid]\nExample: PREMIUM 2 60"
            markup = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:admin")]])
            await _edit_anchor_or_send(update, context, session, text, markup)
            return
        if name == "grant":
            session.mode = "admin_grant"
            text = "Grant: send `user_id credits <amount>` or `user_id premium`"
            markup = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:admin")]])
            await _edit_anchor_or_send(update, context, session, text, markup)
            return
        if name == "broadcast":
            session.mode = "admin_broadcast"
            text = "Send the message to broadcast to all users"
            markup = InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:admin")]])
            await _edit_anchor_or_send(update, context, session, text, markup)
            return
        if name == "stats":
            # Get comprehensive system analytics
            analytics = AnalyticsEngine.get_system_analytics()
            
            text = (
                "📊 **System Analytics Dashboard**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"👥 **Users:** {analytics['users']['total']:,} total\n"
                f"💎 **Premium:** {analytics['users']['premium']:,} ({analytics['growth_metrics']['premium_conversion_rate']}%)\n"
                f"🆓 **Free:** {analytics['users']['free']:,}\n\n"
                f"⏰ **Reminders:** {analytics['reminders']['total']:,} total\n"
                f"🔥 **Active:** {analytics['reminders']['active']:,}\n"
                f"✅ **Completed:** {analytics['reminders']['completed']:,}\n"
                f"📈 **Completion Rate:** {analytics['reminders']['completion_rate']}%\n\n"
                f"🎯 **Avg per User:** {analytics['growth_metrics']['avg_reminders_per_user']}\n\n"
                "👑 **Tier Breakdown:**\n"
            )
            
            for tier, count in analytics['users']['tier_breakdown'].items():
                tier_emoji = {"FREE": "🆓", "SILVER": "🥈", "GOLD": "🥇", "PLATINUM": "💎"}
                text += f"{tier_emoji.get(tier, '📄')} {tier}: {count:,}\n"
            
            await _edit_anchor_or_send(update, context, session, text, InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Admin", callback_data="menu:admin")]]))
            return
        
        # Enhanced admin analytics reports
        if name == "growth_report":
            analytics = AnalyticsEngine.get_system_analytics()
            
            text = (
                "📈 **Growth Metrics Report**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"🚀 **Key Performance Indicators:**\n\n"
                f"👥 **User Growth:**\n"
                f"• Total Users: {analytics['users']['total']:,}\n"
                f"• Premium Users: {analytics['users']['premium']:,}\n"
                f"• Free Users: {analytics['users']['free']:,}\n\n"
                f"💎 **Premium Metrics:**\n"
                f"• Conversion Rate: {analytics['growth_metrics']['premium_conversion_rate']}%\n"
                f"• Silver Tier: {analytics['users']['tier_breakdown'].get('SILVER', 0):,}\n"
                f"• Gold Tier: {analytics['users']['tier_breakdown'].get('GOLD', 0):,}\n"
                f"• Platinum Tier: {analytics['users']['tier_breakdown'].get('PLATINUM', 0):,}\n\n"
                f"⏰ **Engagement:**\n"
                f"• Avg Reminders/User: {analytics['growth_metrics']['avg_reminders_per_user']}\n"
                f"• Total Reminders: {analytics['reminders']['total']:,}\n"
                f"• Completion Rate: {analytics['reminders']['completion_rate']}%"
            )
            
            await _edit_anchor_or_send(update, context, session, text, InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Analytics", callback_data="admin:analytics_menu")]]))
            return
        
        if name == "reminder_insights":
            analytics = AnalyticsEngine.get_system_analytics()
            
            text = (
                "⏰ **Reminder Insights Report**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"📊 **Reminder Statistics:**\n\n"
                f"📈 **Volume Metrics:**\n"
                f"• Total Created: {analytics['reminders']['total']:,}\n"
                f"• Currently Active: {analytics['reminders']['active']:,}\n"
                f"• Completed: {analytics['reminders']['completed']:,}\n\n"
                f"🎯 **Performance:**\n"
                f"• Completion Rate: {analytics['reminders']['completion_rate']}%\n"
                f"• Average per User: {analytics['growth_metrics']['avg_reminders_per_user']}\n\n"
                "📂 **Category Breakdown:**\n"
            )
            
            for category, count in analytics.get('categories', {}).items():
                cat_info = REMINDER_CATEGORIES.get(category, {"name": category.title(), "icon": "📌"})
                percentage = (count / analytics['reminders']['total'] * 100) if analytics['reminders']['total'] > 0 else 0
                text += f"• {cat_info['icon']} {cat_info['name']}: {count:,} ({percentage:.1f}%)\n"
            
            text += "\n📅 **Recent Activity (7 days):**\n"
            for activity in analytics.get('recent_activity', [])[:3]:
                text += f"• {activity['date']}: {activity['reminders_created']} created\n"
            
            await _edit_anchor_or_send(update, context, session, text, InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Analytics", callback_data="admin:analytics_menu")]]))
            return
        if name == "sheet_settings":
            session.mode = "admin_sheet_settings"
            s = settings_store.get().get("spreadsheet", {})
            enabled = s.get("enabled", False)
            text = (
                "📄 Spreadsheet Sync\n\n"
                f"Enabled: {'Yes' if enabled else 'No'}\n"
                "Send command: on | off | set <SHEET_ID> | creds <path-to-credentials.json>\n"
            )
            await _edit_anchor_or_send(update, context, session, text, InlineKeyboardMarkup([[InlineKeyboardButton("Toggle", callback_data="sheet:toggle")], [InlineKeyboardButton("⬅️ Back", callback_data="menu:admin")]]))
            return
        if name == "force_sync":
            try:
                await _edit_anchor_or_send(update, context, session, "🔄 Force syncing spreadsheet...", InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:admin")]]))
                await push_stats_if_enabled()
                await _edit_anchor_or_send(update, context, session, "✅ Force sync completed! Check the logs for details.", InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:admin")]]))
            except Exception as e:
                await _edit_anchor_or_send(update, context, session, f"❌ Force sync failed: {e}", InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:admin")]]))
            return
        
        # Additional admin features
        if name == "user_lookup":
            session.mode = "admin_user_lookup"
            text = (
                "🔍 **User Lookup**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                "📝 **Enter user ID to lookup:**\n\n"
                "You can search by:\n"
                "• User ID (numeric)\n"
                "• Username (without @)\n\n"
                "🔍 **Example:** `123456789`"
            )
            await _edit_anchor_or_send(update, context, session, text, InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to Users", callback_data="admin:users_menu")]]))
            return
        
        if name == "system_backup":
            try:
                # Trigger backup process
                await _edit_anchor_or_send(update, context, session, "💾 Creating system backup...", InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin:system_menu")]]))
                
                # Force stats sync as backup
                await push_stats_if_enabled()
                
                text = (
                    "✅ **System Backup Complete**\n\n"
                    "💾 **Backup includes:**\n"
                    "• User profiles and settings\n"
                    "• All reminder data\n"
                    "• System configuration\n"
                    "• Analytics and statistics\n\n"
                    "📊 **Data exported to spreadsheet**"
                )
                
                await _edit_anchor_or_send(update, context, session, text, InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to System", callback_data="admin:system_menu")]]))
            except Exception as e:
                await _edit_anchor_or_send(update, context, session, f"❌ Backup failed: {e}", InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin:system_menu")]]))
            return
        
        if name == "bot_config":
            users_data = user_store.store.get()
            all_reminders = reminder_store.all()
            settings = settings_store.get()
            
            text = (
                "🎛️ **Bot Configuration Status**\n"
                "━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"🔧 **System Status:**\n"
                f"• Bot Token: Configured ✅\n"
                f"• Admin IDs: {len(ADMIN_IDS)} configured\n"
                f"• Data Directory: {DATA_DIR} ✅\n"
                f"• Timezone: {DEFAULT_TIMEZONE}\n\n"
                f"💾 **Data Status:**\n"
                f"• Users Database: {len(users_data):,} records\n"
                f"• Reminders Database: {len(all_reminders):,} records\n"
                f"• Settings: {len(settings):,} configurations\n\n"
                f"💎 **Premium Tiers:**\n"
                f"• Free: {FREE_TIER_MAX_ACTIVE} reminders\n"
                f"• Silver: {SILVER_TIER_MAX_ACTIVE} reminders\n"
                f"• Gold: {GOLD_TIER_MAX_ACTIVE} reminders\n"
                f"• Platinum: {PLATINUM_TIER_MAX_ACTIVE} reminders"
            )
            
            await _edit_anchor_or_send(update, context, session, text, InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back to System", callback_data="admin:system_menu")]]))
            return

    # Repeating flow callbacks
    if data.startswith("repeat:"):
        parts = data.split(":")
        if len(parts) >= 3 and parts[1] == "kind":
            session.repeat_kind = parts[2]
            session.mode = "repeat_interval"
            text = "Interval? (number of days/weeks)"
            markup = InlineKeyboardMarkup([[InlineKeyboardButton("1", callback_data="repeat:set_interval:1"), InlineKeyboardButton("2", callback_data="repeat:set_interval:2"), InlineKeyboardButton("3", callback_data="repeat:set_interval:3")], [InlineKeyboardButton("⬅️ Back", callback_data="menu:repeat")]])
            await _edit_anchor_or_send(update, context, session, text, markup)
            return
        if len(parts) >= 3 and parts[1] == "set_interval":
            try:
                session.repeat_interval = int(parts[2])
            except Exception:
                session.repeat_interval = 1
            if session.repeat_kind == "weekly":
                session.mode = "repeat_dow"
                await _edit_anchor_or_send(update, context, session, "Pick a day of week:", build_dow_keyboard())
                return
            else:
                session.mode = "repeat_time"
                await _edit_anchor_or_send(update, context, session, "At what time? (HH:MM)", InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:repeat")]]))
                return
        if len(parts) >= 3 and parts[1] == "set_dow":
            try:
                session.repeat_dow = int(parts[2])
            except Exception:
                session.repeat_dow = 0
            session.mode = "repeat_time"
            await _edit_anchor_or_send(update, context, session, "At what time? (HH:MM)", InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="menu:repeat")]]))
            return

    if data.startswith("sheet:"):
        parts = data.split(":")
        s = settings_store.get()
        ss = s.get("spreadsheet", {})
        if parts[1] == "toggle":
            ss["enabled"] = not ss.get("enabled", False)
            s["spreadsheet"] = ss
            settings_store.set(s)
            await _edit_anchor_or_send(update, context, session, f"Spreadsheet sync is now {'ON' if ss['enabled'] else 'OFF'}.", InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ Back", callback_data="admin:sheet_settings")]]))
            return
    if data.startswith("flow:"):
        _, name = data.split(":", 1)
        if name == "cancel":
            session.mode = "idle"
            await show_main_menu(update, context)
            return

    # Reminder actions (snooze/done/del)
    parts = data.split(":")
    if not parts:
        return
    action = parts[0]
    if action not in {"snooze", "done", "del"}:
        return
    if len(parts) < 2:
        return
    rid = parts[1]
    r = reminder_store.get(rid)
    if not r or r.user_id != user.id:
        try:
            await query.edit_message_text("This reminder is no longer available.")
        except Exception:
            pass
        return

    if action == "snooze":
        minutes = int(parts[2]) if len(parts) >= 3 else 5
        limit = CreditPolicy.get_snooze_limit(profile)
        if r.snoozes_used >= limit:
            text = (
                f"🚫 **Snooze Limit Reached**\n\n"
                f"📊 **Used:** {r.snoozes_used}/{limit} snoozes\n"
                f"🎯 **Tier:** {profile.premium_tier}\n\n"
                f"💎 **Upgrade for more snoozes!**"
            )
            await query.edit_message_text(text)
            return
        new_when = now_utc() + timedelta(minutes=minutes)
        r.when_iso = to_iso(new_when)
        r.snoozes_used += 1
        reminder_store.put(r)
        scheduler.schedule_once(new_when, r.id)
        await query.edit_message_text(f"Snoozed to {human_dt(new_when, r.timezone)}\n{r.text}")
        return
    if action == "done":
        r.done = True
        reminder_store.put(r)
        await query.edit_message_text("Marked as done. ✅")
        return
    if action == "del":
        reminder_store.delete(r.id)
        await query.edit_message_text("Deleted. 🗑️")
        return


# -------- Admin commands --------


def require_admin(func):
    async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE):
        user = update.effective_user
        if not is_admin(user.id):
            await update.message.reply_text("Admins only.")
            return
        return await func(update, context)

    return wrapper


@require_admin
async def cmd_gen(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # /gen credits <amount> <count> [days_valid]
    # /gen premium <days> <count> [days_valid]
    args = update.message.text.split()
    if len(args) < 4:
        await update.message.reply_text(
            "Usage: /gen credits <amount> <count> [days_valid]\n/gen premium <days> <count> [days_valid]"
        )
        return
    kind = args[1].lower()
    amount = int(args[2])
    count = int(args[3])
    days_valid = int(args[4]) if len(args) >= 5 else 30
    expires_at = to_iso(now_utc() + timedelta(days=days_valid))

    codes: List[str] = []
    for _ in range(count):
        c = RedeemCode(code=generate_code("CR" if kind == "credits" else "PR"), kind=kind, amount=amount, expires_at=expires_at)
        codes_store.put(c)
        codes.append(c.code)
    await update.message.reply_text("Generated:\n" + "\n".join(codes))


@require_admin
async def cmd_grant(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # /grant <user_id> credits <amount>
    # /grant <user_id> premium
    args = update.message.text.split()
    if len(args) < 3:
        await update.message.reply_text("Usage: /grant <user_id> credits <amount> | premium")
        return
    target = int(args[1])
    action = args[2].lower()
    profile = user_store.get_user(target)
    if action == "credits" and len(args) >= 4:
        amt = int(args[3])
        profile.credits += amt
        user_store.put_user(profile)
        await update.message.reply_text(f"Granted {amt} credits to {target}.")
    elif action == "premium":
        profile.is_premium = True
        user_store.put_user(profile)
        await update.message.reply_text(f"Granted premium to {target}.")
    else:
        await update.message.reply_text("Invalid grant usage.")


@require_admin
async def cmd_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    args = update.message.text.split(maxsplit=1)
    if len(args) == 1:
        await update.message.reply_text("Usage: /broadcast <message>")
        return
    payload = args[1]
    # naive broadcast to all users in store
    users = user_store.store.get().keys()
    sent = 0
    for uid in users:
        try:
            await context.bot.send_message(chat_id=int(uid), text=payload)
            sent += 1
        except Exception:
            pass
    await update.message.reply_text(f"Broadcast sent to {sent} users.")


# ============================
# App bootstrap
# ============================


scheduler: ReminderScheduler  # set in main()


def build_application() -> Application:
    token = BOT_TOKEN
    if not token:
        raise RuntimeError("BOT_TOKEN env var is required.")
    app = ApplicationBuilder().token(token).build()
    global scheduler
    scheduler = ReminderScheduler(app, reminder_store, user_store)

    app.add_handler(CommandHandler("start", cmd_start))
    app.add_handler(CommandHandler("help", cmd_help))
    app.add_handler(CommandHandler("remind", cmd_remind))
    app.add_handler(CommandHandler("repeat", cmd_repeat))
    app.add_handler(CommandHandler("list", cmd_list))
    app.add_handler(CommandHandler("delete", cmd_delete))
    app.add_handler(CommandHandler("timezone", cmd_timezone))
    app.add_handler(CommandHandler("profile", cmd_profile))
    app.add_handler(CommandHandler("redeem", cmd_redeem))
    # Optional: commands to open admin stats/settings inline
    app.add_handler(CommandHandler("stats", lambda u, c: on_callback(type("Q", (), {"callback_query": type("CQ", (), {"data": "admin:stats", "answer": (lambda *a, **k: None), "message": u.message})(), "effective_user": u.effective_user})(), c)))

    # Admin
    app.add_handler(CommandHandler("gen", cmd_gen))
    app.add_handler(CommandHandler("grant", cmd_grant))
    app.add_handler(CommandHandler("broadcast", cmd_broadcast))

    app.add_handler(CallbackQueryHandler(on_callback))
    # Text input handler for session-driven flows
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text_message))
    return app


async def on_startup(app: Application):
    await scheduler.schedule_all_on_startup()
    logger.info("Scheduled existing reminders.")


def main():
    app = build_application()
    app.post_init = on_startup
    logger.info("Starting Reminder Bot...")
    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()


