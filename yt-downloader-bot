import sys
import asyncio
import logging
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, CallbackQuery, Message
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
)
from yt_dlp import YoutubeDL
from ytmusicapi import YTMusic
import tempfile
from typing import Optional, Dict, List, Union
import re
import telegram
import subprocess
import pkg_resources
import shutil
import json
from datetime import datetime, timedelta
import random
import string
from pathlib import Path
import os


# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


# Initialize YTMusic with error handling
try:
    ytmusic = YTMusic()
    print("✅ YTMusic initialized successfully")
except Exception as e:
    print(f"❌ Error initializing YTMusic: {str(e)}")
    ytmusic = None


# Constants for subscription tiers
SUBSCRIPTION_TIERS = {
    "FREE": {"downloads_per_day": 2, "quality_options": ["128"]},
    "SILVER": {"downloads_per_day": 5, "quality_options": ["128", "192"]},
    "GOLD": {"downloads_per_day": 10, "quality_options": ["128", "192", "256"]},
    "PLATINUM": {"downloads_per_day": float('inf'), "quality_options": ["128", "192", "256", "320"]}
}


# Add to constants
REFERRAL_REWARDS = {
    10: "SILVER",    # 10 referrals for Silver
    30: "GOLD",      # 30 referrals for Gold
    50: "PLATINUM"   # 50 referrals for Platinum
}


# File paths
DATA_DIR = Path("bot_data")
CODES_FILE = DATA_DIR / "redeem_codes.json"
USERS_FILE = DATA_DIR / "users.json"
REFERRALS_FILE = DATA_DIR / "referrals.json"
FEEDBACK_FILE = DATA_DIR / "feedback.json"
MAINTENANCE_FILE = DATA_DIR / "maintenance.json"
BANNED_USERS_FILE = DATA_DIR / "banned_users.json"


class DataManager:
    def __init__(self):
        """Initialize data storage"""
        self._setup_data_directory()
        self.codes = self._load_json(CODES_FILE, {})
        self.users = self._load_json(USERS_FILE, {})
        self.referrals = self._load_json(REFERRALS_FILE, {})
        self.feedback = self._load_json(FEEDBACK_FILE, {})
        self.maintenance = self._load_json(MAINTENANCE_FILE, {"enabled": False, "message": None})
        self.banned_users = self._load_json(BANNED_USERS_FILE, {})


    def _setup_data_directory(self):
        """Create necessary directories and files"""
        DATA_DIR.mkdir(exist_ok=True)
        for file in [CODES_FILE, USERS_FILE, REFERRALS_FILE, FEEDBACK_FILE, MAINTENANCE_FILE, BANNED_USERS_FILE]:
            if not file.exists():
                file.write_text("{}")


    def _load_json(self, file_path: Path, default: dict) -> dict:
        """Load JSON data from file"""
        try:
            return json.loads(file_path.read_text())
        except:
            return default


    def _save_json(self, file_path: Path, data: dict):
        """Save JSON data to file"""
        file_path.write_text(json.dumps(data, indent=2))


    def save_all(self):
        """Save all data to files"""
        self._save_json(CODES_FILE, self.codes)
        self._save_json(USERS_FILE, self.users)
        self._save_json(REFERRALS_FILE, self.referrals)
        self._save_json(FEEDBACK_FILE, self.feedback)
        self._save_json(MAINTENANCE_FILE, self.maintenance)
        self._save_json(BANNED_USERS_FILE, self.banned_users)


class DownloadProgress:
    def __init__(self, message, filename: str):
        try:
            self.message = message
            self.filename = filename
            self.last_update_time = datetime.now()
            self.last_percentage = 0
            self.loop = asyncio.get_running_loop()
        except Exception as e:
            print(f"❌ Error initializing: {str(e)}")
            raise


    async def update_status(self, text: str):
        """Safely update the status message"""
        try:
            await self.message.edit_text(text)
        except Exception as e:
            logger.error(f"Failed to update progress: {str(e)}")


    def progress_hook(self, d):
        """Progress hook that creates a task for async updates"""
        if d["status"] == "downloading":
            # Calculate percentage
            total_bytes = d.get("total_bytes") or d.get("total_bytes_estimate", 0)
            downloaded_bytes = d.get("downloaded_bytes", 0)


            if total_bytes > 0:
                percentage = (downloaded_bytes / total_bytes) * 100
                current_time = datetime.now()


                # Update progress bar every 2.5% or at least 1 second passed
                if (
                    percentage - self.last_percentage >= 2.5
                    or (current_time - self.last_update_time).seconds >= 1
                ):
                    self.last_update_time = current_time
                    self.last_percentage = percentage


                    # Calculate progress bar
                    progress_length = 20
                    filled_length = int(progress_length * percentage / 100)
                    progress_bar = "█" * filled_length + "░" * (
                        progress_length - filled_length
                    )


                    # Format status text
                    status_text = (
                        f"⏬ Downloading: {self.filename}\n"
                        f"[{progress_bar}] {percentage:.1f}%\n"
                        f"💾 {downloaded_bytes/1024/1024:.1f}MB / {total_bytes/1024/1024:.1f}MB"
                    )


                    # Create task for async update
                    future = asyncio.run_coroutine_threadsafe(
                        self.update_status(status_text), self.loop
                    )


                    # Handle any exceptions from the task
                    def callback(future):
                        try:
                            future.result()
                        except Exception as e:
                            logger.error(f"Error in progress update: {str(e)}")


                    future.add_done_callback(callback)


    def _format_size(self, bytes_value: int) -> str:
        for unit in ["B", "KB", "MB", "GB"]:
            if bytes_value < 1024:
                return f"{bytes_value:.1f} {unit}"
            bytes_value /= 1024
        return f"{bytes_value:.1f} TB"


    def _format_time(self, seconds: float) -> str:
        minutes, seconds = divmod(int(seconds), 60)
        hours, minutes = divmod(minutes, 60)
        if hours > 0:
            return f"{hours}:{minutes:02d}:{seconds:02d}"
        return f"{minutes}:{seconds:02d}"


class MusicBot:
    def __init__(self, token: str):
        """Initialize the bot with the given token"""
        try:
            self.application = Application.builder().token(token).build()
            self.data_manager = DataManager()  # Initialize DataManager
            self.setup_handlers()
            self.active_downloads: Dict[int, asyncio.Task] = {}
            print("🤖 Bot initialized successfully!")
        except Exception as e:
            print(f"❌ Error initializing bot: {str(e)}")
            raise


    def setup_handlers(self):
        """Set up all command and callback handlers"""
        # Command handlers
        self.application.add_handler(CommandHandler("start", self.start))
        self.application.add_handler(CommandHandler("help", self.help_command))
        self.application.add_handler(CommandHandler("settings", self.settings_command))
        self.application.add_handler(CommandHandler("admin", self.admin_command))
        self.application.add_handler(CommandHandler("ban", self.handle_user_ban))
        self.application.add_handler(CommandHandler("unban", self.handle_user_unban))
        self.application.add_handler(CommandHandler("userinfo", self.handle_user_info))
        self.application.add_handler(CommandHandler("broadcast", self.broadcast_command))
        self.application.add_handler(CommandHandler("feedback", self.feedback_command))
        self.application.add_handler(CommandHandler("profile", self.profile_command))
        self.application.add_handler(CommandHandler("referral", self.referral_command))
        # Missing command handlers
        self.application.add_handler(CommandHandler("gencode", self.generate_code_command))
        self.application.add_handler(CommandHandler("respond", self.respond_to_feedback))
        self.application.add_handler(CommandHandler("listfeedback", self.list_feedback))
        self.application.add_handler(CommandHandler("cancel", self.cancel_command))


        # Callback query handlers (specific first, generic last)
        self.application.add_handler(CallbackQueryHandler(self.handle_admin_callback, pattern="^admin_.*"))
        self.application.add_handler(CallbackQueryHandler(self.handle_feedback_callback, pattern="^feedback_.*"))
        self.application.add_handler(CallbackQueryHandler(self.handle_referral_callback, pattern="^referral_.*"))
        self.application.add_handler(CallbackQueryHandler(self.handle_broadcast_callback, pattern="^broadcast_.*"))

        

        # Download history handlers with more specific patterns
        self.application.add_handler(CallbackQueryHandler(
            self.handle_download_history_callback, 
            pattern="^download_history$"
        ))
        self.application.add_handler(CallbackQueryHandler(
            self.handle_history_navigation, 
            pattern="^history_(next|prev)$"
        ))
        self.application.add_handler(CallbackQueryHandler(
            self.handle_history_clear, 
            pattern="^history_clear$"
        ))
        self.application.add_handler(CallbackQueryHandler(
            self.handle_history_clear_confirm, 
            pattern="^history_clear_confirm$"
        ))


        # Generic callback handler should be added AFTER specific ones
        self.application.add_handler(CallbackQueryHandler(self.handle_callback))

        # Make sure this is the last handler
        self.application.add_handler(MessageHandler(
            filters.TEXT & ~filters.COMMAND, 
            self.handle_message
        ))


    async def start(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /start command"""
        try:
            user = update.effective_user
            user_id = str(user.id)
            is_admin = await self._is_admin(user.id)


            # Check if user is banned
            if user_id in self.data_manager.banned_users:
                ban_info = self.data_manager.banned_users[user_id]
                await update.message.reply_text(
                    f"❌ You are banned from using this bot.\n"
                    f"Reason: {ban_info['reason']}"
                )
                return


            # Process referral if present in start parameter
            if context.args and context.args[0].startswith('REF_'):
                referral_code = context.args[0][4:]  # Remove 'REF_' prefix
                await self._process_referral(user_id, referral_code, update.message)


            # Initialize user data if new user
            if user_id not in self.data_manager.users:
                self.data_manager.users[user_id] = {
                    "username": user.username,
                    "join_date": datetime.now().isoformat(),
                    "tier": "FREE",
                    "downloads_today": 0,
                    "total_downloads": 0,
                    "last_active": datetime.now().isoformat(),
                    "last_download_reset": datetime.now().isoformat(),
                    "referral_count": 0,
                    "referrals": []
                }
                self.data_manager.save_all()


            await self.show_main_menu(update.message, is_admin)


        except Exception as e:
            logger.error(f"Start command error: {str(e)}")
            await update.message.reply_text(
                "❌ An error occurred. Please try again or contact support."
            )


    async def _process_referral(self, user_id: str, referral_code: str, message: Message):
        """Process referral code when a new user joins"""
        try:
            # Debug log
            logger.info(f"Processing referral: user {user_id} with code {referral_code}")
            
            # If user already exists in database, they're not a new user
            # so don't process the referral
            if user_id in self.data_manager.users:
                logger.info(f"User {user_id} already exists, skipping referral")
                return

            # Find referrer by code
            referrer_id = None
            for uid, data in self.data_manager.users.items():
                if data.get('referral_code') == referral_code:
                    referrer_id = uid
                    break

            if not referrer_id:
                logger.warning(f"No referrer found for code {referral_code}")
                return

            # Debug log
            logger.info(f"Found referrer: {referrer_id}")

            # Don't allow self-referrals
            if referrer_id == user_id:
                await message.reply_text("❌ You cannot refer yourself!")
                return

            # Update referrer's data
            referrer_data = self.data_manager.users[referrer_id]
            if user_id in referrer_data.get('referrals', []):
                logger.info(f"User {user_id} already referred by {referrer_id}")
                return


            # Update referrer's data
            if 'referrals' not in referrer_data:
                referrer_data['referrals'] = []
            referrer_data['referrals'].append(user_id)
            referrer_data['referral_count'] = len(referrer_data['referrals'])

            # Get new user's username or full name
            new_user = message.from_user
            user_identifier = new_user.username or f"{new_user.first_name} {new_user.last_name or ''}"

            # Debug log
            logger.info("Preparing notification message")

            # Escape special characters for MarkdownV2
            for ch in ["_", "*", "[", "]", "(", ")", "!", "-", "."]:
                user_identifier = user_identifier.replace(ch, f"\\{ch}")
            
            notification_text = (
                "🎉 *New Referral\\!*\n\n"
                f"User {user_identifier} joined using your referral link\\!\n"
                f"You now have *{referrer_data['referral_count']}* referrals\n\n"
            )

            # Add tier upgrade information if eligible
            for count, tier in sorted(REFERRAL_REWARDS.items()):
                if (referrer_data['referral_count'] >= count and 
                    SUBSCRIPTION_TIERS[tier]["downloads_per_day"] > 
                    SUBSCRIPTION_TIERS[referrer_data.get("tier", "FREE")]["downloads_per_day"]):
                    notification_text += (
                        "🌟 *Congratulations\\!*\n"
                        f"You can now upgrade to *{tier}* tier\\!\n"
                        f"• Daily Downloads: {SUBSCRIPTION_TIERS[tier]['downloads_per_day']}\n"
                        f"• Quality Options: up to {max(SUBSCRIPTION_TIERS[tier]['quality_options'])}kbps\n\n"
                        "Use the Profile menu to claim your reward\\!"
                    )
                    break
            else:
                # Show progress to next tier
                next_tier = None
                for count, tier in sorted(REFERRAL_REWARDS.items()):
                    if referrer_data['referral_count'] < count:
                        next_tier = (count, tier)
                        break
                
                if next_tier:
                    count, tier = next_tier
                    remaining = count - referrer_data['referral_count']
                    progress = referrer_data['referral_count'] / count
                    progress_bar = "█" * int(progress * 10) + "░" * (10 - int(progress * 10))
                    
                    notification_text += (
                        "📈 *Progress to Next Tier*\n"
                        f"{progress_bar} {int(progress * 100)}%\n"
                        f"Need {remaining} more referrals for *{tier}* tier\\!\n"
                    )

            # Add referral link reminder
            me = await self.application.bot.get_me()
            bot_username = me.username
            # Telegram links must include @ in the displayed username but not in URL; ensure correct casing
            referral_link = f"https://t.me/{bot_username}?start=REF_{referrer_data['referral_code']}"
            notification_text += (
                "\n🔗 *Your Referral Link:*\n"
                f"{referral_link}\n"
                "Share it to earn more rewards\\!"
            )



            # Create inline keyboard for quick actions
            keyboard = [
                [InlineKeyboardButton("👤 View Profile", callback_data="profile")],
                [InlineKeyboardButton("📊 Referral Stats", callback_data="referral_stats")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)

            # Send notification to referrer
            try:
                logger.info(f"Attempting to send notification to referrer {referrer_id}")
                await message._bot.send_message(
                    chat_id=int(referrer_id),
                    text=notification_text,
                    reply_markup=reply_markup,
                    parse_mode="MarkdownV2"  # Changed to MarkdownV2 for better formatting
                )
                logger.info("Notification sent successfully")
            except Exception as e:
                logger.error(f"Failed to notify referrer {referrer_id}: {str(e)}")
                logger.exception(e)

            self.data_manager.save_all()

            # Notify new user
            await message.reply_text(
                "👋 Welcome\\! You've joined using a referral link."
            )

        except Exception as e:
            logger.error(f"Error processing referral: {str(e)}")
            logger.exception(e)


    async def show_main_menu(self, message: Message, is_admin: bool = False, as_edit: bool = False):
        """Show main menu with proper buttons based on user role"""
        keyboard = [
            [InlineKeyboardButton("🔍 Search Music", callback_data="search")],
            [InlineKeyboardButton("📋 Playlist Download", callback_data="playlist")],
            [InlineKeyboardButton("⚙️ Settings", callback_data="settings")],
            [InlineKeyboardButton("👤 Profile", callback_data="profile")],
            [InlineKeyboardButton("❓ Help", callback_data="help")]
        ]


        # Add admin button if user is admin
        if is_admin:
            keyboard.append([InlineKeyboardButton("🔧 Admin Panel", callback_data="admin_panel")])


        reply_markup = InlineKeyboardMarkup(keyboard)


        welcome_text = (
            "👋 *Welcome to YouTube Music Downloader!*\n\n"
            "I can help you:\n"
            "• Search and download music\n"
            "• Download from YouTube links\n"
            "• Download playlists\n"
            "• Customize download settings\n\n"
            "What would you like to do?"
        )


        # Prefer editing in callback-driven navigation to avoid spam
        if as_edit:
            await message.edit_text(
                welcome_text,
                reply_markup=reply_markup,
                parse_mode="Markdown"
            )
        else:
            await message.reply_text(
                welcome_text,
                reply_markup=reply_markup,
                parse_mode="Markdown"
            )


    async def help_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show help message"""
        keyboard = [
            [InlineKeyboardButton("🔍 Search Help", callback_data="help_search")],
            [InlineKeyboardButton("📋 Playlist Help", callback_data="help_playlist")],
            [InlineKeyboardButton("⚙️ Settings Help", callback_data="help_settings")],
            [InlineKeyboardButton("🔙 Back", callback_data="main_menu")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)


        help_text = (
            "*❓ Help & Information*\n\n"
            "Here's how to use the bot:\n\n"
            "🔍 *Search & Download:*\n"
            "• Click Search Music or send song name\n"
            "• Select from search results\n\n"
            "📋 *Playlist Download:*\n"
            "• Send YouTube playlist link\n"
            "• Select songs to download\n\n"
            "⚙️ *Settings:*\n"
            "• Set audio quality\n"
            "• Choose download mode\n\n"
            "Select a topic for more details:"
        )


        if update.callback_query:
            await update.callback_query.message.edit_text(
                help_text, reply_markup=reply_markup, parse_mode="Markdown"
            )
        else:
            await update.message.reply_text(
                help_text, reply_markup=reply_markup, parse_mode="Markdown"
            )


    async def settings_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show settings menu"""
        current_mode = context.user_data.get("mode", "audio")
        current_quality = context.user_data.get("quality", "192")


        # Format the quality display based on mode
        if current_mode == "audio":
            quality_display = f"🎵 Audio Quality ({current_quality}kbps)"
            mode_display = "📥 Mode: Audio (MP3)"
        else:
            quality_display = f"🎥 Video Quality ({current_quality}p)"
            mode_display = "📥 Mode: Video (MP4)"


        keyboard = [
            [InlineKeyboardButton(quality_display, callback_data="setting_quality")],
            [InlineKeyboardButton(mode_display, callback_data="setting_mode")],
            [InlineKeyboardButton("🔙 Back to Menu", callback_data="main_menu")],
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)


        settings_text = (
            "⚙️ *Settings*\n\n"
            f"Current Settings:\n"
            f"• {mode_display}\n"
            f"• Quality: {current_quality}{'p' if current_mode == 'video' else 'kbps'}\n\n"
            "Select an option to change:"
        )


        if update.callback_query:
            await update.callback_query.message.edit_text(
                settings_text,
                reply_markup=reply_markup,
                parse_mode="Markdown",
            )
        else:
            await update.message.reply_text(
                settings_text,
                reply_markup=reply_markup,
                parse_mode="Markdown",
            )


    async def cancel_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle the /cancel command"""
        user_id = update.effective_user.id
        if user_id in self.active_downloads:
            self.active_downloads[user_id].cancel()
            del self.active_downloads[user_id]
            await update.message.reply_text("✅ Download cancelled successfully!")
        else:
            await update.message.reply_text("❌ No active downloads to cancel.")


    async def handle_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle text messages including feedback submissions"""
        try:
            if "feedback_category" in context.user_data:
                # This is a feedback submission
                await self.process_feedback_submission(update.message, context)
                return


            # Broadcast composition
            if context.user_data.get("awaiting_broadcast"):
                await self.handle_broadcast_message(update, context)
                return

            # Maintenance message set flow
            if context.user_data.get("awaiting_maintenance_message"):
                self.data_manager.maintenance["message"] = update.message.text
                self.data_manager.save_all()
                context.user_data.pop("awaiting_maintenance_message", None)
                # Attempt to edit the last admin message if present; else send confirmation
                await update.message.reply_text("✅ Maintenance message updated.")
                return


            if context.user_data.get('awaiting_redeem_code'):
                await self.process_redeem_code(update.message, context)
                return


            message = update.message.text


            if message.startswith("https://"):
                if "playlist" in message:
                    await self.handle_playlist(update.message, message, context)
                else:
                    await self.handle_single_download(update.message, message, context)
            else:
                # Send status message before starting the search
                status_message = await update.message.reply_text("🔍 Searching...")
                # Treat as search query
                try:
                    search_results = ytmusic.search(message, filter="songs", limit=20)
                    if not search_results:
                        await status_message.edit_text("❌ No results found!")
                        return


                    context.user_data["search_results"] = search_results
                    context.user_data["search_page"] = 0


                    await self.show_search_results(status_message, context)


                except Exception as e:
                    logger.error(f"Search error: {str(e)}")
                    await status_message.edit_text("❌ Search failed. Please try again.")


        except Exception as e:
            logger.error(f"Message handling error: {str(e)}")
            await update.message.reply_text("❌ An error occurred. Please try again.")


    async def handle_single_download(
        self,
        message: Message,
        url: str,
        context: ContextTypes.DEFAULT_TYPE,
    ):
        """Handle single song download from a URL"""
        try:
            chat_id = message.chat.id
            user_id = str(chat_id)
            
            # Check download limits before starting
            if not await self.check_download_limits(user_id):
                return


            # Get user's quality preference
            user_data = self.data_manager.users.get(user_id, {})
            quality = user_data.get("preferred_quality", "128")
            
            status_message = await message.reply_text("⏳ Processing request...")

            try:
                # Download and send the media
                await self.download_and_send(message.chat.id, url, status_message, context)

                # Update counts
                user_data = self.data_manager.users.get(user_id, {})
                user_data["downloads_today"] = user_data.get("downloads_today", 0) + 1
                user_data["total_downloads"] = user_data.get("total_downloads", 0) + 1
                user_data["last_active"] = datetime.now().isoformat()
                self.data_manager.users[user_id] = user_data
                self.data_manager.save_all()
            except Exception as e:
                logger.error(f"Download error: {str(e)}")
                await status_message.edit_text(f"❌ Download failed: {str(e)}")


        except Exception as e:
            logger.error(f"Single download handler error: {str(e)}")
            await message.reply_text("❌ An unexpected error occurred. Please try again.")

    def _is_youtube_link(self, text: str) -> bool:
        """Check if the text is a YouTube link"""
        youtube_regex = r"(youtube\.com|youtu\.be)(\/watch\?v=|\/)([^&]+)"
        return bool(re.search(youtube_regex, text))


    async def handle_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle callback queries"""
        query = update.callback_query
        await query.answer()


        try:
            is_admin = await self._is_admin(query.from_user.id)


            if query.data == "download_history":
                # Handle download history separately
                await self.show_download_history(query.message, str(query.from_user.id))
            elif query.data == "search":
                await query.message.edit_text(
                    "🔍 Please send me the name of the song or artist you want to search for."
                )
            elif query.data == "redeem":
                await self.show_redeem_interface(query.message, context)
            elif query.data == "get_referral":
                await self.show_referral_link(query.message, context)
            elif query.data == "playlist":
                await query.message.edit_text(
                    "📋 Please send me a YouTube playlist link to download songs from it."
                )
            elif query.data == "settings":
                await self.settings_command(update, context)
            elif query.data == "help":
                await self.help_command(update, context)
            elif query.data == "profile":
                await self.profile_command(update, context)  # Updated
            elif query.data == "main_menu":
                await self.show_main_menu(query.message, is_admin, as_edit=True)
            elif query.data == "admin_panel":
                if is_admin:
                    await self.admin_command(update, context)  # Updated
                else:
                    await query.message.edit_text("❌ This feature is for admins only.")
            elif query.data.startswith("download_"):
                video_id = "_".join(query.data.split("_")[1:])
                await self.handle_download(query.message, video_id, context)
            elif query.data.startswith("search_"):
                action = query.data.split("_")[1]
                if action in ["next", "prev"]:
                    await self.handle_search_pagination(query, context)
            elif query.data.startswith("setting_"):
                await self.handle_settings_callback(query, context)
            elif query.data.startswith("quality_"):
                await self.handle_quality_setting(query, context)
            elif query.data.startswith("mode_"):
                await self.handle_mode_setting(query, context)
            elif query.data.startswith("pl_"):
                await self.handle_playlist_callback(query, context)
            elif query.data == "back_settings":
                await self.settings_command(update, context)
            elif query.data.startswith("help_"):
                section = query.data
                await self.show_help_section(query.message, section)
        except Exception as e:
            logger.error(f"Callback error: {str(e)}")
            keyboard = [[InlineKeyboardButton("🔙 Main Menu", callback_data="main_menu")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.message.edit_text(
                f"❌ An error occurred: {str(e)}", reply_markup=reply_markup
            )

    async def download_and_send(
        self,
        chat_id: int,
        url: str,
        status_message: Message,
        context: ContextTypes.DEFAULT_TYPE,
    ):
        """Download and send media"""
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                download_mode = context.user_data.get("mode", "audio")
                quality = context.user_data.get("quality", "192")


                def sanitize_filename(title):
                    # Replace problematic characters
                    chars = {'�����': '|', '：': ':', '＆': '&', ' ': '_'}
                    for old, new in chars.items():
                        title = title.replace(old, new)
                    return title


                # Configure yt-dlp options
                if download_mode == "audio":
                    ydl_opts = {
                        "format": "bestaudio/best",
                        "outtmpl": os.path.join(temp_dir, "%(title)s.%(ext)s"),
                        "progress_hooks": [],
                        "noplaylist": True,
                        "postprocessors": [
                            {
                                "key": "FFmpegExtractAudio",
                                "preferredcodec": "mp3",
                                "preferredquality": quality,
                            }
                        ],
                    }
                else:
                    format_string = f"bestvideo[height<={quality}][ext=mp4]+bestaudio[ext=m4a]/best[height<={quality}]"
                    ydl_opts = {
                        "format": format_string,
                        "outtmpl": os.path.join(temp_dir, "%(title)s.%(ext)s"),
                        "progress_hooks": [],
                        "noplaylist": True,
                        "merge_output_format": "mp4",
                        "postprocessors": [{
                            "key": "FFmpegVideoConvertor",
                            "preferedformat": "mp4",
                        }],
                    }


                # Initialize progress tracker
                progress = DownloadProgress(status_message, "Downloading...")
                ydl_opts["progress_hooks"] = [progress.progress_hook]


                with YoutubeDL(ydl_opts) as ydl:
                    info = await asyncio.get_event_loop().run_in_executor(
                        None, lambda: ydl.extract_info(url, download=True)
                    )


                    if not info:
                        raise Exception("Could not download the file")


                    # Update status to show completion
                    await status_message.edit_text(
                        "✅ Download complete!\n📤 Uploading to Telegram..."
                    )


                    # Get sanitized filename
                    sanitized_title = sanitize_filename(info['title'])
                    if download_mode == "audio":
                        filename = os.path.join(temp_dir, f"{sanitized_title}.mp3")
                    else:
                        filename = os.path.join(temp_dir, f"{sanitized_title}.mp4")


                    # Check if file exists with original name if sanitized not found
                    if not os.path.exists(filename):
                        original_filename = os.path.join(temp_dir, f"{info['title']}.{'mp3' if download_mode == 'audio' else 'mp4'}")
                        if os.path.exists(original_filename):
                            filename = original_filename
                        else:
                            # Try to find the file in the temp directory
                            files = os.listdir(temp_dir)
                            matching_files = [f for f in files if f.endswith('.mp3' if download_mode == 'audio' else '.mp4')]
                            if matching_files:
                                filename = os.path.join(temp_dir, matching_files[0])
                            else:
                                raise FileNotFoundError(f"Could not find downloaded file in {temp_dir}")


                    # Send the file
                    with open(filename, "rb") as file:
                        if download_mode == "audio":
                            await context.bot.send_audio(
                                chat_id=chat_id,
                                audio=file,
                                title=info.get("title", "Unknown"),
                                performer=info.get("artist", "Unknown"),
                                duration=int(info.get("duration", 0)),
                            )
                        else:
                            await context.bot.send_video(
                                chat_id=chat_id,
                                video=file,
                                caption=info.get("title", "Unknown"),
                                duration=int(info.get("duration", 0)),
                            )


                    # Show completion message
                    keyboard = [
                        [InlineKeyboardButton("🔍 Search Again", callback_data="search")],
                        [InlineKeyboardButton("🔙 Main Menu", callback_data="main_menu")],
                    ]
                    reply_markup = InlineKeyboardMarkup(keyboard)
                    await status_message.edit_text(
                        "✅ Download and upload complete!\nWhat would you like to do next?",
                        reply_markup=reply_markup,
                    )


                    # After successful download, add to history
                    user_data = self.data_manager.users.get(str(chat_id), {})
                    if 'download_history' not in user_data:
                        user_data['download_history'] = []
                    
                    download_info = {
                        'title': info.get('title', 'Unknown'),
                        'date': datetime.now().isoformat(),
                        'mode': download_mode,
                        'quality': quality,
                        'url': url
                    }
                    
                    # Keep only last 50 downloads
                    user_data['download_history'] = ([download_info] + user_data['download_history'])[:50]
                    self.data_manager.save_all()


        except Exception as e:
            logger.error(f"Download error: {str(e)}")
            keyboard = [[InlineKeyboardButton("🔙 Main Menu", callback_data="main_menu")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await status_message.edit_text(
                f"❌ Download failed: {str(e)}", reply_markup=reply_markup
            )


    async def handle_playlist(
        self,
        message: Message,
        url: str,
        context: ContextTypes.DEFAULT_TYPE,
    ):
        """Handle playlist download"""
        try:
            status_message = await message.reply_text("📋 Processing playlist...")


            # Configure yt-dlp options
            ydl_opts = {
                "quiet": True,
                "extract_flat": True,
                "force_generic_extractor": False,
            }


            # Get playlist info
            with YoutubeDL(ydl_opts) as ydl:
                try:
                    # Use lambda to avoid download parameter issue
                    playlist_info = await asyncio.get_event_loop().run_in_executor(
                        None, lambda: ydl.extract_info(url, download=False)
                    )


                    if not playlist_info or "entries" not in playlist_info:
                        await status_message.edit_text("❌ Invalid playlist link")
                        return


                    # Store playlist info
                    context.user_data["playlist"] = {
                        "info": playlist_info,
                        "page": 0,
                        "selected_tracks": set(),
                        "tracks": playlist_info.get("entries", []),
                        "title": playlist_info.get("title", "Unknown"),
                    }


                    await self.show_playlist_page(status_message, context)


                except Exception as e:
                    logger.error(f"Playlist error: {str(e)}")
                    await status_message.edit_text(
                        f"❌ Error processing playlist: {str(e)}"
                    )


        except Exception as e:
            logger.error(f"Playlist handler error: {str(e)}")
            await message.reply_text("❌ Error processing playlist. Please try again.")


    async def show_playlist_page(
        self, message: Message, context: ContextTypes.DEFAULT_TYPE
    ):
        """Show playlist tracks with selection status"""
        try:
            playlist_data = context.user_data.get("playlist", {})
            if not playlist_data:
                await message.edit_text(
                    "❌ Playlist data not found. Please try again.",
                    reply_markup=InlineKeyboardMarkup(
                        [[InlineKeyboardButton("🔙 Main Menu", callback_data="main_menu")]]
                    ),
                )
                return


            current_page = playlist_data.get("page", 0)
            tracks = playlist_data.get("tracks", [])
            selected_tracks = playlist_data.get("selected_tracks", set())


            # Calculate pagination
            items_per_page = 5
            start_idx = current_page * items_per_page
            end_idx = min(start_idx + items_per_page, len(tracks))


            # Create track selection buttons
            keyboard = []
            for track in tracks[start_idx:end_idx]:
                track_id = track["id"]
                title = track["title"]
                is_selected = track_id in selected_tracks


                # Create button text with selection status
                button_text = f"{'✅' if is_selected else '⭕'} {title}"
                if len(button_text) > 60:
                    button_text = button_text[:57] + "..."


                keyboard.append(
                    [
                        InlineKeyboardButton(
                            button_text, callback_data=f"pl_sel_{track_id}"
                        )
                    ]
                )


            # Add navigation buttons
            nav_buttons = []
            if current_page > 0:
                nav_buttons.append(
                    InlineKeyboardButton("⬅️ Previous", callback_data="pl_prev")
                )
            if end_idx < len(tracks):
                nav_buttons.append(
                    InlineKeyboardButton("Next ➡️", callback_data="pl_next")
                )
            if nav_buttons:
                keyboard.append(nav_buttons)


            # Add action buttons
            keyboard.extend(
                [
                    [
                        InlineKeyboardButton(
                            f"📥 Download Selected ({len(selected_tracks)})",
                            callback_data="pl_download",
                        )
                    ],
                    [InlineKeyboardButton("🔙 Main Menu", callback_data="main_menu")],
                ]
            )


            # Create message text
            total_pages = (len(tracks) + items_per_page - 1) // items_per_page
            text = (
                f"📋 *Playlist: {playlist_data.get('title', 'Unknown')}*\n"
                f"Page {current_page + 1}/{total_pages}\n"
                f"Selected: {len(selected_tracks)} tracks\n\n"
                "Click on tracks to select/deselect:"
            )


            # Update message
            await message.edit_text(
                text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown"
            )


        except Exception as e:
            logger.error(f"Show playlist error: {str(e)}")
            await message.edit_text(
                "❌ Error displaying playlist. Please try again.",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Main Menu", callback_data="main_menu")]]
                ),
            )


    async def handle_playlist_callback(
        self, query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE
    ):
        """Handle playlist-related callbacks"""
        try:
            data = query.data.split("_")
            if len(data) < 2:
                return


            action = data[1]
            playlist_data = context.user_data.get("playlist", {})


            if action == "sel":
                # Handle track selection
                if len(data) < 3:
                    return
                track_id = data[2]
                if "selected_tracks" not in playlist_data:
                    playlist_data["selected_tracks"] = set()


                if track_id in playlist_data["selected_tracks"]:
                    playlist_data["selected_tracks"].remove(track_id)
                else:
                    playlist_data["selected_tracks"].add(track_id)


                context.user_data["playlist"] = playlist_data
                await self.show_playlist_page(query.message, context)


            elif action == "next":
                # Handle next page
                current_page = playlist_data.get("page", 0)
                total_tracks = len(playlist_data.get("tracks", []))
                if (current_page + 1) * 5 < total_tracks:
                    playlist_data["page"] = current_page + 1
                    context.user_data["playlist"] = playlist_data
                    await self.show_playlist_page(query.message, context)


            elif action == "prev":
                # Handle previous page
                current_page = playlist_data.get("page", 0)
                if current_page > 0:
                    playlist_data["page"] = current_page - 1
                    context.user_data["playlist"] = playlist_data
                    await self.show_playlist_page(query.message, context)


            elif action == "download":
                # Handle download selected tracks
                selected_tracks = list(playlist_data.get("selected_tracks", set()))
                if selected_tracks:
                    await self.download_playlist_tracks(
                        query.message, context, selected_tracks
                    )
                else:
                    await query.message.edit_text(
                        "❌ Please select at least one track to download.",
                        reply_markup=InlineKeyboardMarkup(
                            [
                                [
                                    InlineKeyboardButton(
                                        "🔙 Back to Playlist", callback_data="playlist"
                                    )
                                ]
                            ]
                        ),
                    )


        except Exception as e:
            logger.error(f"Playlist callback error: {str(e)}")
            await query.message.edit_text(
                "❌ An error occurred. Please try again.",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Main Menu", callback_data="main_menu")]]
                ),
            )


    async def download_playlist_tracks(
        self,
        message: Message,
        context: ContextTypes.DEFAULT_TYPE,
        track_ids: List[str],
    ):
        """Download selected playlist tracks"""
        try:
            total_tracks = len(track_ids)
            if total_tracks == 0:
                await message.edit_text("❌ No tracks selected for download.")
                return


            status_text = f"⏳ Downloading {total_tracks} tracks..."
            status_message = await message.edit_text(status_text)


            for i, track_id in enumerate(track_ids, 1):
                try:
                    url = f"https://www.youtube.com/watch?v={track_id}"
                    await status_message.edit_text(
                        f"⏳ Downloading track {i}/{total_tracks}..."
                    )


                    await self.download_and_send(
                        message.chat_id, url, status_message, context
                    )


                except Exception as e:
                    logger.error(f"Error downloading track {track_id}: {str(e)}")
                    await status_message.edit_text(
                        f"⚠️ Error downloading track {i}/{total_tracks}: {str(e)}"
                    )
                    await asyncio.sleep(2)


            # Final completion message
            keyboard = [[InlineKeyboardButton("🔙 Main Menu", callback_data="main_menu")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await message.edit_text(
                "✅ Playlist download complete!", reply_markup=reply_markup
            )


        except Exception as e:
            logger.error(f"Playlist download error: {str(e)}")
            await message.edit_text(
                f"❌ Error downloading playlist: {str(e)}",
                reply_markup=InlineKeyboardMarkup(
                    [[InlineKeyboardButton("🔙 Main Menu", callback_data="main_menu")]]
                ),
            )


    async def show_help_section(self, message, section):
        """Show specific help section"""
        help_texts = {
            "help_search": (
                "*🔍 Music Search Help*\n\n"
                "To search for a song:\n"
                "1. Type the song name and artist\n"
                "2. Select from search results\n"
                "3. Wait for download to complete\n\n"
                "You can also directly paste a YouTube Music link."
            ),
            "help_playlist": (
                "*📋 Playlist Download Help*\n\n"
                "To download a playlist:\n"
                "1. Send the YouTube Music playlist link\n"
                "2. Choose to download all or select songs\n"
                "3. Downloads will start automatically\n\n"
                "Note: Maximum 10 songs per request."
            ),
            "help_settings": (
                "*⚙️ Settings Help*\n\n"
                "In Settings, you can:\n"
                "• Change audio quality (128kbps, 192kbps, 320kbps)\n"
                "• Switch between audio and video download modes\n\n"
                "Adjust these to suit your preferences."
            ),
        }


        keyboard = [[InlineKeyboardButton("🔙 Back to Help", callback_data="help")]]
        reply_markup = InlineKeyboardMarkup(keyboard)


        await message.edit_text(
            help_texts.get(section, "Help section not found."),
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )


    async def handle_download(
        self, message: Message, video_id: str, context: ContextTypes.DEFAULT_TYPE
    ):
        """Handle single song download"""
        user_id = str(message.chat.id)


        # Check download limits
        if not await self.check_download_limits(user_id):
            tier = self.data_manager.users[user_id].get("tier", "FREE")
            limit = SUBSCRIPTION_TIERS[tier]["downloads_per_day"]
            await message.reply_text(
                f"❌ Daily download limit reached ({limit} downloads)\n"
                "Try again tomorrow or upgrade your tier!"
            )
            return


        try:
            status_message = await message.reply_text("⏳ Starting download...")
            url = f"https://www.youtube.com/watch?v={video_id}"


            await self.download_and_send(
                message.chat_id, url, status_message, context
            )


            # Update download count after successful download
            user_data = self.data_manager.users[user_id]
            user_data["downloads_today"] = user_data.get("downloads_today", 0) + 1
            user_data["total_downloads"] = user_data.get("total_downloads", 0) + 1
            self.data_manager.save_all()


        except Exception as e:
            logger.error(f"Download error: {str(e)}")
            keyboard = [[InlineKeyboardButton("🔙 Main Menu", callback_data="main_menu")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await status_message.edit_text(
                f"❌ Download failed: {str(e)}", reply_markup=reply_markup
            )


    async def show_search_results(
        self, message: Message, context: ContextTypes.DEFAULT_TYPE
    ):
        """Show paginated search results"""
        results = context.user_data.get("search_results", [])
        page = context.user_data.get("search_page", 0)
        items_per_page = 5

        start_idx = page * items_per_page
        end_idx = start_idx + items_per_page
        current_items = results[start_idx:end_idx]

        keyboard = []
        for item in current_items:
            title = item["title"]
            artist = item["artists"][0]["name"]
            video_id = item["videoId"]
            button_text = f"🎵 {title} - {artist}"
            keyboard.append(
                [
                    InlineKeyboardButton(
                        button_text[:60] + "..." if len(button_text) > 60 else button_text,
                        callback_data=f"download_{video_id}",
                    )
                ]
            )

        # Add navigation buttons
        nav_buttons = []
        if page > 0:
            nav_buttons.append(
                InlineKeyboardButton("⬅️ Previous", callback_data="search_prev")
            )
        if end_idx < len(results):
            nav_buttons.append(InlineKeyboardButton("Next ➡️", callback_data="search_next"))

        if nav_buttons:
            keyboard.append(nav_buttons)

        keyboard.append([InlineKeyboardButton("🔙 Main Menu", callback_data="main_menu")])
        reply_markup = InlineKeyboardMarkup(keyboard)

        total_pages = (len(results) + items_per_page - 1) // items_per_page
        text = (
            f"🔍 *Search Results*\n"
            f"Page {page + 1}/{total_pages}\n"
            "Select a song to download:"
        )

        await message.edit_text(text, reply_markup=reply_markup, parse_mode="Markdown")


    async def handle_search_pagination(
        self, query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE
    ):
        """Handle search pagination"""
        current_page = context.user_data.get("search_page", 0)


        if query.data == "search_next":
            context.user_data["search_page"] = current_page + 1
        elif query.data == "search_prev":
            context.user_data["search_page"] = max(0, current_page - 1)


        await self.show_search_results(query.message, context)


    async def handle_settings_callback(
        self, query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE
    ):
        """Handle settings-related callbacks"""
        action = query.data.split("_")[1]
        current_mode = context.user_data.get("mode", "audio")


        if action == "quality":
            if current_mode == "audio":
                sel = context.user_data.get("quality", "192")
                def label(q, text):
                    return f"{'✅ ' if sel == q else ''}{text}"
                keyboard = [
                    [InlineKeyboardButton(label("128", "128 kbps (Standard)"), callback_data="quality_128")],
                    [InlineKeyboardButton(label("192", "192 kbps (High)"), callback_data="quality_192")],
                    [InlineKeyboardButton(label("320", "320 kbps (Best)"), callback_data="quality_320")],
                    [InlineKeyboardButton("🔙 Back", callback_data="settings")],
                ]
                message_text = "🎵 Select Audio Quality:"
            else:
                sel = context.user_data.get("quality", "720")
                def vlabel(q):
                    return f"{'✅ ' if sel == q else ''}{q}p"
                keyboard = [
                    [InlineKeyboardButton(vlabel("360"), callback_data="quality_360")],
                    [InlineKeyboardButton(vlabel("480"), callback_data="quality_480")],
                    [InlineKeyboardButton(vlabel("720"), callback_data="quality_720")],
                    [InlineKeyboardButton(vlabel("1080"), callback_data="quality_1080")],
                    [InlineKeyboardButton("🔙 Back", callback_data="settings")],
                ]
                message_text = "🎥 Select Video Quality:"
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.message.edit_text(message_text, reply_markup=reply_markup)


        elif action == "mode":
            sel_mode = context.user_data.get("mode", "audio")
            keyboard = [
                [InlineKeyboardButton(f"{'✅ ' if sel_mode == 'audio' else ''}Audio Only (MP3)", callback_data="mode_audio")],
                [InlineKeyboardButton(f"{'✅ ' if sel_mode == 'video' else ''}Video (MP4)", callback_data="mode_video")],
                [InlineKeyboardButton("🔙 Back", callback_data="settings")],
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.message.edit_text(
                "📥 Select Download Mode:", reply_markup=reply_markup
            )


    async def handle_quality_setting(
        self, query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE
    ):
        """Handle quality setting changes"""
        quality = query.data.split("_")[1]
        mode = context.user_data.get("mode", "audio")
        context.user_data["quality"] = quality


        keyboard = [[InlineKeyboardButton("🔙 Back to Settings", callback_data="settings")]]
        reply_markup = InlineKeyboardMarkup(keyboard)


        if mode == "audio":
            message = f"✅ Audio quality set to {quality}kbps"
        else:
            message = f"✅ Video quality set to {quality}p"


        await query.message.edit_text(message, reply_markup=reply_markup)


    async def handle_mode_setting(
        self, query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE
    ):
        """Handle download mode selection"""
        mode = query.data.split("_")[1]
        context.user_data["mode"] = mode
        
        # Reset quality to default when switching modes
        if mode == "audio":
            context.user_data["quality"] = "192"  # Default audio quality
            mode_text = "Audio Only (MP3)"
        else:
            context.user_data["quality"] = "720"  # Default video quality
            mode_text = "Video (MP4)"


        keyboard = [[InlineKeyboardButton("🔙 Back to Settings", callback_data="settings")]]
        reply_markup = InlineKeyboardMarkup(keyboard)


        await query.message.edit_text(
            f"✅ Download mode set to: {mode_text}\nDefault quality has been set.",
            reply_markup=reply_markup
        )


    async def profile_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Show user profile and subscription information"""
        try:
            user = update.effective_user
            user_id = str(user.id)
            
            # Initialize user data if it doesn't exist
            if user_id not in self.data_manager.users:
                self.data_manager.users[user_id] = {
                    "username": user.username,
                    "join_date": datetime.now().isoformat(),
                    "tier": "FREE",
                    "downloads_today": 0,
                    "total_downloads": 0,
                    "last_active": datetime.now().isoformat(),
                    "last_download_reset": datetime.now().isoformat(),
                    "referral_count": 0,
                    "referrals": []
                }
                self.data_manager.save_all()


            user_data = self.data_manager.users[user_id]
            tier = user_data.get("tier", "FREE")
            downloads_today = user_data.get("downloads_today", 0)
            total_downloads = user_data.get("total_downloads", 0)
            referral_count = user_data.get("referral_count", 0)
            join_date = datetime.fromisoformat(user_data.get("join_date", datetime.now().isoformat()))
            last_active = datetime.fromisoformat(user_data.get("last_active", datetime.now().isoformat()))


            # Calculate account age
            account_age = (datetime.now() - join_date).days


            # Generate referral code if not exists
            if 'referral_code' not in user_data:
                user_data['referral_code'] = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
                self.data_manager.save_all()


            # Create profile message
            profile_text = (
                f"👤 *User Profile*\n\n"
                f"🆔 User ID: {user_id}\n"
                f"👤 Username: @{user.username if user.username else 'None'}\n"
                f"📛 Name: {user.first_name} {user.last_name if user.last_name else ''}\n"
                f"📅 Joined: {join_date.strftime('%Y-%m-%d')}\n"
                f"⏳ Account Age: {account_age} days\n"
                f"⌚️ Last Active: {last_active.strftime('%Y-%m-%d %H:%M')}\n\n"
                
                f"🎫 *Subscription Details*\n"
                f"📊 Current Tier: *{tier}*\n"
                f"💾 Quality Options: up to {max(SUBSCRIPTION_TIERS[tier]['quality_options'])}kbps\n"
                f"📥 Daily Limit: {SUBSCRIPTION_TIERS[tier]['downloads_per_day']} downloads\n\n"
                
                f"📊 *Statistics*\n"
                f"• Downloads Today: {downloads_today}/{SUBSCRIPTION_TIERS[tier]['downloads_per_day']}\n"
                f"• Total Downloads: {total_downloads}\n"
                f"• Referral Count: {referral_count}\n"
            )


            # Add tier benefits
            profile_text += (
                f"\n💫 *Tier Benefits*\n"
                f"• Daily Downloads: {SUBSCRIPTION_TIERS[tier]['downloads_per_day']}\n"
                f"• Quality Options: {', '.join(SUBSCRIPTION_TIERS[tier]['quality_options'])}kbps\n"
            )


            # Add referral progress if not PLATINUM
            if tier != "PLATINUM":
                next_tier = None
                for count, reward_tier in sorted(REFERRAL_REWARDS.items()):
                    if referral_count < count and SUBSCRIPTION_TIERS[reward_tier]["downloads_per_day"] > SUBSCRIPTION_TIERS[tier]["downloads_per_day"]:
                        next_tier = (count, reward_tier)
                        break
                        
                if next_tier:
                    count, reward_tier = next_tier
                    remaining = count - referral_count
                    progress = (referral_count / count) * 10
                    progress_bar = "█" * int(progress) + "░" * (10 - int(progress))
                    
                    profile_text += (
                        f"\n📈 *Referral Progress*\n"
                        f"{progress_bar} {int((referral_count / count) * 100)}%\n"
                        f"Need {remaining} more referrals for {reward_tier} tier!\n"
                        f"Next Tier Benefits:\n"
                        f"• Daily Downloads: {SUBSCRIPTION_TIERS[reward_tier]['downloads_per_day']}\n"
                        f"• Quality Options: up to {max(SUBSCRIPTION_TIERS[reward_tier]['quality_options'])}kbps\n"
                    )


            # Create keyboard
            keyboard = [
                [InlineKeyboardButton("🎟️ Redeem Code", callback_data="redeem")],
                [InlineKeyboardButton("🔗 Get Referral Link", callback_data="get_referral")],
                [InlineKeyboardButton("📊 Download History", callback_data="download_history")],
                [InlineKeyboardButton("🔙 Back to Menu", callback_data="main_menu")]
            ]


            # Add upgrade button if eligible for a higher tier
            for count, reward_tier in sorted(REFERRAL_REWARDS.items()):
                if (referral_count >= count and 
                    SUBSCRIPTION_TIERS[reward_tier]["downloads_per_day"] > 
                    SUBSCRIPTION_TIERS[tier]["downloads_per_day"]):
                    keyboard.insert(0, [InlineKeyboardButton("⭐ Claim Tier Upgrade", callback_data=f"upgrade_{reward_tier}")])
                    break


            reply_markup = InlineKeyboardMarkup(keyboard)


            # Send or edit message
            if update.callback_query:
                await update.callback_query.message.edit_text(
                    profile_text,
                    reply_markup=reply_markup,
                    parse_mode="Markdown"
                )
            else:
                await update.message.reply_text(
                    profile_text,
                    reply_markup=reply_markup,
                    parse_mode="Markdown"
                )


        except Exception as e:
            logger.error(f"Profile command error: {str(e)}")
            error_message = "❌ An error occurred while loading your profile. Please try again."
            if update.callback_query:
                await update.callback_query.message.edit_text(error_message)
            else:
                await update.message.reply_text(error_message)


    async def admin_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle /admin command"""
        try:
            if not await self._is_admin(update.effective_user.id):
                await update.message.reply_text("❌ This command is for admins only.")
                return


            keyboard = [
                [
                    InlineKeyboardButton("👥 User Management", callback_data="admin_users"),
                    InlineKeyboardButton("🛠️ Maintenance", callback_data="admin_maintenance")
                ],
                [
                    InlineKeyboardButton("📊 Statistics", callback_data="admin_stats"),
                    InlineKeyboardButton("📢 Broadcast", callback_data="admin_broadcast")
                ],
                [
                    InlineKeyboardButton("🎫 Generate Codes", callback_data="admin_gencode"),
                    InlineKeyboardButton("⚙️ Settings", callback_data="admin_settings")
                ],
                [InlineKeyboardButton("🔙 Main Menu", callback_data="main_menu")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)


            text = (
                "🔧 *Admin Control Panel*\n\n"
                "Select an option to manage:"
            )

            if update.callback_query:
                await update.callback_query.message.edit_text(
                    text,
                    reply_markup=reply_markup,
                    parse_mode="Markdown"
                )
            else:
                await update.message.reply_text(
                    text,
                    reply_markup=reply_markup,
                    parse_mode="Markdown"
                )

        except Exception as e:
            logger.error(f"Admin command error: {str(e)}")
            error_message = "❌ An error occurred while accessing admin panel."
            if isinstance(update, Update):
                if update.callback_query:
                    await update.callback_query.message.reply_text(error_message)
                else:
                    await update.message.reply_text(error_message)
            else:
                await update.reply_text(error_message)




    async def _is_admin(self, user_id: int) -> bool:
        """Check if user is an admin"""
        admin_ids = os.getenv("ADMIN_IDS", "").split(",")
        return str(user_id) in admin_ids


    async def generate_code_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Generate redeem codes (admin only)"""
        if not await self._is_admin(update.effective_user.id):
            await update.message.reply_text("❌ This command is for admins only.")
            return


        try:
            # Parse command arguments
            args = context.args
            if len(args) != 2:
                await update.message.reply_text(
                    "Usage: /gencode <tier> <count>\n"
                    "Tiers: SILVER, GOLD, PLATINUM"
                )
                return


            tier, count = args[0].upper(), int(args[1])
            if tier not in ["SILVER", "GOLD", "PLATINUM"]:
                await update.message.reply_text("Invalid tier specified.")
                return


            # Generate codes
            codes = []
            for _ in range(count):
                code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=12))
                self.data_manager.codes[code] = {
                    "tier": tier,
                    "created_at": datetime.now().isoformat(),
                    "used": False
                }
                codes.append(code)


            self.data_manager.save_all()


            # Send codes to admin
            codes_text = "\n".join(codes)
            await update.message.reply_text(
                f"Generated {count} {tier} codes:\n\n{codes_text}",
                parse_mode="Markdown"
            )


        except Exception as e:
            await update.message.reply_text(f"Error generating codes: {str(e)}")
    
    async def referral_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle referral system"""
        user_id = str(update.effective_user.id)
        user_data = self.data_manager.users.get(user_id, {})
        
        # Generate or get existing referral code
        if "referral_code" not in user_data:
            user_data["referral_code"] = self._generate_referral_code()
            user_data["referrals"] = []
            user_data["referral_rewards_claimed"] = []
            self.data_manager.users[user_id] = user_data
            self.data_manager.save_all()


        await self.show_referral_menu(update.message, user_data)


    def _generate_referral_code(self) -> str:
        """Generate unique referral code"""
        while True:
            code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
            # Check if code is unique
            if not any(u.get("referral_code") == code for u in self.data_manager.users.values()):
                return code


    async def show_referral_menu(self, message: Message, user_data: dict):
        """Show referral menu with statistics"""
        referral_count = len(user_data.get("referrals", []))
        current_tier = user_data.get("tier", "FREE")
        
        # Calculate next reward tier
        next_tier = None
        next_tier_count = None
        for count, tier in sorted(REFERRAL_REWARDS.items()):
            if referral_count < count and (current_tier == "FREE" or 
               SUBSCRIPTION_TIERS[tier]["downloads_per_day"] > SUBSCRIPTION_TIERS[current_tier]["downloads_per_day"]):
                next_tier = tier
                next_tier_count = count
                break


        bot_username = (await message.bot.get_me()).username
        referral_link = f"https://t.me/{bot_username}?start=REF_{user_data['referral_code']}"


        # Create progress bar for next tier
        if next_tier:
            progress = referral_count / next_tier_count
            progress_bar = "█" * int(progress * 10) + "░" * (10 - int(progress * 10))
        else:
            progress_bar = "█" * 10


        stats_text = (
            "🎯 *Your Referral Status*\n\n"
            f"👥 Total Referrals: {referral_count}\n"
            f"🎖️ Current Tier: {current_tier}\n"
            f"📊 Progress: [{progress_bar}]\n"
        )


        if next_tier:
            stats_text += f"\n🎯 Next Reward: {next_tier} Tier ({next_tier_count - referral_count} more referrals needed)"


        stats_text += (
            f"\n\n🔗 *Your Referral Link:*\n"
            f"{referral_link}\n\n"
            "Share this link with friends to earn rewards!"
        )


        keyboard = [
            [InlineKeyboardButton("📊 Detailed Stats", callback_data="referral_stats")],
            [InlineKeyboardButton("🎁 Claim Rewards", callback_data="referral_claim")],
            [InlineKeyboardButton("❓ How it Works", callback_data="referral_help")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)


        await message.reply_text(
            stats_text,
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )


    async def handle_referral_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle referral-related callbacks"""
        query = update.callback_query
        action = query.data.split("_")[1]
        user_id = str(query.from_user.id)
        user_data = self.data_manager.users.get(user_id, {})


        try:
            if action == "stats":
                await self.show_referral_statistics(query.message, user_data)
            elif action == "claim":
                await self.handle_reward_claim(query.message, user_data)
            elif action == "help":
                await self.show_referral_help(query.message)
            
            await query.answer()
        except Exception as e:
            logger.error(f"Referral callback error: {str(e)}")
            await query.answer("An error occurred")


    async def show_referral_statistics(self, message: Message, user_data: dict):
        """Show detailed referral statistics"""
        referrals = user_data.get("referrals", [])
        
        # Get referral user details
        referral_details = []
        for ref_id in referrals:
            ref_data = self.data_manager.users.get(ref_id, {})
            join_date = datetime.fromisoformat(ref_data.get("join_date", "2000-01-01"))
            referral_details.append({
                "username": ref_data.get("username", "Unknown"),
                "date": join_date,
                "tier": ref_data.get("tier", "FREE")
            })


        # Sort by date
        referral_details.sort(key=lambda x: x["date"], reverse=True)


        stats_text = "📊 *Detailed Referral Statistics*\n\n"
        
        if referral_details:
            stats_text += "Recent Referrals:\n"
            for i, ref in enumerate(referral_details[:5], 1):
                stats_text += (
                    f"{i}. @{ref['username']}\n"
                    f"   Joined: {ref['date'].strftime('%Y-%m-%d')}\n"
                    f"   Tier: {ref['tier']}\n"
                )
        else:
            stats_text += "No referrals yet. Share your link to get started!"


        keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="referral_back")]]
        reply_markup = InlineKeyboardMarkup(keyboard)


        await message.edit_text(
            stats_text,
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )


    async def handle_reward_claim(self, message: Message, user_data: dict):
        """Handle reward claiming"""
        referral_count = len(user_data.get("referrals", []))
        claimed_rewards = set(user_data.get("referral_rewards_claimed", []))
        available_rewards = []


        for count, tier in REFERRAL_REWARDS.items():
            if (referral_count >= count and 
                str(count) not in claimed_rewards and 
                SUBSCRIPTION_TIERS[tier]["downloads_per_day"] > 
                SUBSCRIPTION_TIERS[user_data.get("tier", "FREE")]["downloads_per_day"]):
                available_rewards.append((count, tier))


        if not available_rewards:
            keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="referral_back")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await message.edit_text(
                "🎁 *Rewards Status*\n\n"
                "No new rewards available to claim.\n"
                "Keep inviting friends to earn rewards!",
                reply_markup=reply_markup,
                parse_mode="Markdown"
            )
            return


        # Claim highest available tier
        count, new_tier = max(available_rewards, key=lambda x: SUBSCRIPTION_TIERS[x[1]]["downloads_per_day"])
        
        user_data["tier"] = new_tier
        user_data["referral_rewards_claimed"] = list(claimed_rewards | {str(count)})
        self.data_manager.users[str(message.chat.id)] = user_data
        self.data_manager.save_all()


        keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="referral_back")]]
        reply_markup = InlineKeyboardMarkup(keyboard)


        await message.edit_text(
            f"🎉 *Congratulations!*\n\n"
            f"You've claimed the {new_tier} tier reward!\n"
            f"New daily download limit: {SUBSCRIPTION_TIERS[new_tier]['downloads_per_day']}\n"
            f"New quality options: {', '.join(SUBSCRIPTION_TIERS[new_tier]['quality_options'])}kbps",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )


    async def show_referral_help(self, message: Message):
        """Show referral system help"""
        help_text = (
            "❓ *How the Referral System Works*\n\n"
            "1️⃣ Share your referral link with friends\n"
            "2️⃣ When they join using your link, you get credit\n"
            "3️⃣ Earn rewards based on referral count:\n\n"
            "🎁 *Rewards Tiers*\n"
            "• 10 referrals: Silver Tier\n"
            "• 30 referrals: Gold Tier\n"
            "• 50 referrals: Platinum Tier\n\n"
            "📝 *Tier Benefits*\n"
            "• Silver: 5 downloads/day, up to 192kbps\n"
            "• Gold: 10 downloads/day, up to 256kbps\n"
            "• Platinum: Unlimited downloads, up to 320kbps"
        )


        keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="referral_back")]]
        reply_markup = InlineKeyboardMarkup(keyboard)


        await message.edit_text(
            help_text,
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )


    def run(self):
        """Run the bot"""
        try:
            print("🤖 Bot is starting...")
            self.application.run_polling()
        except Exception as e:
            logger.error(f"Error running bot: {str(e)}")
        finally:
            print("🛑 Bot is shutting down...")
            # Cleanup any remaining downloads
            for chat_id in list(self.active_downloads.keys()):
                asyncio.create_task(self.cleanup_downloads(chat_id))



    async def cleanup_downloads(self, chat_id: int):
        """Clean up completed or failed downloads"""
        if chat_id in self.active_downloads:
            task = self.active_downloads[chat_id]
            if task.done():
                del self.active_downloads[chat_id]



    async def feedback_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle user feedback submission"""
        if len(context.args) == 0:
            await update.message.reply_text(
                "📝 *How to submit feedback:*\n\n"
                "Use this command followed by your message:\n"
                "/feedback Your message here\n\n"
                "Example:\n"
                "/feedback The download speed is great!",
                parse_mode="Markdown"
            )
            return





        user = update.effective_user
        feedback_text = ' '.join(context.args)
        feedback_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
        
        feedback_data = {
            "id": feedback_id,
            "user_id": str(user.id),
            "username": user.username or "Unknown",
            "text": feedback_text,
            "timestamp": datetime.now().isoformat(),
            "status": "pending",
            "admin_response": None
        }


        self.data_manager.feedback[feedback_id] = feedback_data
        self.data_manager.save_all()


        await update.message.reply_text(
            "✅ Thank you for your feedback!\n"
            f"Feedback ID: {feedback_id}\n\n"
            "An admin will review it soon.",
            parse_mode="Markdown"
        )


        await self._notify_admins_of_feedback(feedback_data)



    async def _notify_admins_of_feedback(self, feedback_data: dict):
        """Notify admins about new feedback"""
        category_emojis = {
            "bug": "🐞",
            "suggestion": "💡",
            "praise": "👍",
            "question": "❓"
        }
        category = feedback_data.get("category", "general")
        emoji = category_emojis.get(category, "📝")


        admin_message = (
            f"{emoji} *New {category.title()} Feedback*\n\n"
            f"ID: {feedback_data['id']}\n"
            f"From: @{feedback_data['username']} ({feedback_data['user_id']})\n"
            f"Message: {feedback_data['text']}\n\n"
            "Use /respond {feedback_id} your_response to reply"
        )


        admin_ids = os.getenv("ADMIN_IDS", "").split(",")
        for admin_id in admin_ids:
            try:
                keyboard = [
                    [
                        InlineKeyboardButton("✅ Resolve", callback_data=f"admin_resolve_{feedback_data['id']}"),
                        InlineKeyboardButton("❌ Dismiss", callback_data=f"admin_dismiss_{feedback_data['id']}")
                    ]
                ]
                reply_markup = InlineKeyboardMarkup(keyboard)


                await self.application.bot.send_message(
                    chat_id=int(admin_id),
                    text=admin_message,
                    reply_markup=reply_markup,
                    parse_mode="Markdown"
                )
            except Exception as e:
                logger.error(f"Failed to notify admin {admin_id}: {str(e)}")


    async def respond_to_feedback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle admin responses to feedback"""
        if not await self._is_admin(update.effective_user.id):
            await update.message.reply_text("❌ This command is for admins only.")
            return


        if len(context.args) < 2:
            await update.message.reply_text(
                "Usage: /respond feedback_id your_response",
                parse_mode="Markdown"
            )
            return


        feedback_id = context.args[0]
        response = ' '.join(context.args[1:])


        if feedback_id not in self.data_manager.feedback:
            await update.message.reply_text("❌ Feedback ID not found.")
            return


        feedback_data = self.data_manager.feedback[feedback_id]
        feedback_data["status"] = "resolved"
        feedback_data["admin_response"] = response
        self.data_manager.save_all()


        try:
            # Notify user of response
            await self.application.bot.send_message(
                chat_id=int(feedback_data["user_id"]),
                text=(
                    "📬 *Admin Response to Your Feedback*\n\n"
                    f"Your feedback: {feedback_data['text']}\n"
                    f"Admin response: {response}"
                ),
                parse_mode="Markdown"
            )
            await update.message.reply_text("✅ Response sent to user.")
        except Exception as e:
            await update.message.reply_text(f"❌ Failed to send response to user: {str(e)}")


    async def list_feedback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """List all feedback for admins"""
        if not await self._is_admin(update.effective_user.id):
            await update.message.reply_text("❌ This command is for admins only.")
            return


        status_filter = context.args[0] if context.args else None
        feedback_list = []


        for f_id, data in self.data_manager.feedback.items():
            if not status_filter or data["status"] == status_filter:
                feedback_list.append(
                    f"ID: {f_id}\n"
                    f"Status: {data['status']}\n"
                    f"From: @{data['username']}\n"
                    f"Message: {data['text']}\n"
                    f"Time: {data['timestamp']}\n"
                    f"Response: {data['admin_response'] or 'None'}\n"
                    "-------------------"
                )


        if not feedback_list:
            await update.message.reply_text("No feedback found.")
            return

        # Split long messages if needed
        message = "📬 *Feedback List*\n\n" + "\n".join(feedback_list)
        if len(message) > 4096:
            chunks = [message[i:i+4096] for i in range(0, len(message), 4096)]
            for chunk in chunks:
                await update.message.reply_text(chunk, parse_mode="Markdown")
        else:
            await update.message.reply_text(message, parse_mode="Markdown")


    async def handle_feedback_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle feedback-related callbacks"""
        query = update.callback_query
        action = query.data.split("_")[1]


        try:
            if action == "start":
                await self.show_feedback_categories(query.message)
            elif action == "category":
                category = query.data.split("_")[2]
                context.user_data["feedback_category"] = category
                await self.ask_for_feedback(query.message)
            elif action == "submit":
                await self.submit_feedback(query.message, context)
            elif action == "cancel":
                await self.cancel_feedback(query.message, context)
            
            await query.answer()
        except Exception as e:
            logger.error(f"Feedback callback error: {str(e)}")
            await query.answer("An error occurred")


    async def show_feedback_categories(self, message: Message):
        """Show feedback category selection"""
        keyboard = [
            [
                InlineKeyboardButton("🐞 Bug Report", callback_data="feedback_category_bug"),
                InlineKeyboardButton("💡 Suggestion", callback_data="feedback_category_suggestion")
            ],
            [
                InlineKeyboardButton("👍 Praise", callback_data="feedback_category_praise"),
                InlineKeyboardButton("❓ Question", callback_data="feedback_category_question")
            ],
            [InlineKeyboardButton("❌ Cancel", callback_data="feedback_cancel")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)


        await message.edit_text(
            "📝 *What kind of feedback would you like to submit?*\n\n"
            "Choose a category:",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )


    async def ask_for_feedback(self, message: Message):
        """Ask user for feedback text"""
        keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data="feedback_cancel")]]
        reply_markup = InlineKeyboardMarkup(keyboard)


        await message.edit_text(
            "📝 *Please write your feedback*\n\n"
            "Reply to this message with your feedback text.\n"
            "Click Cancel to abort.",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )


    async def process_feedback_submission(self, message: Message, context: ContextTypes.DEFAULT_TYPE):
        """Process submitted feedback text"""
        feedback_text = message.text
        category = context.user_data["feedback_category"]
        user = message.from_user
        feedback_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))


        feedback_data = {
            "id": feedback_id,
            "user_id": str(user.id),
            "username": user.username or "Unknown",
            "category": category,
            "text": feedback_text,
            "timestamp": datetime.now().isoformat(),
            "status": "pending",
            "admin_response": None
        }


        self.data_manager.feedback[feedback_id] = feedback_data
        self.data_manager.save_all()


        # Clear feedback state
        del context.user_data["feedback_category"]


        # Show confirmation
        keyboard = [
            [InlineKeyboardButton("📝 Submit Another", callback_data="feedback_start")],
            [InlineKeyboardButton("🔙 Main Menu", callback_data="main_menu")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)


        await message.reply_text(
            "✅ *Thank you for your feedback!*\n\n"
            f"Feedback ID: {feedback_id}\n"
            "An admin will review it soon.",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )


        # Notify admins
        await self._notify_admins_of_feedback(feedback_data)


    async def cancel_feedback(self, message: Message, context: ContextTypes.DEFAULT_TYPE):
        """Cancel feedback submission"""
        if "feedback_category" in context.user_data:
            del context.user_data["feedback_category"]


        keyboard = [[InlineKeyboardButton("🔙 Main Menu", callback_data="main_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)


        await message.edit_text(
            "❌ Feedback submission cancelled.",
            reply_markup=reply_markup
        )

    async def broadcast_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle broadcast command - show broadcast options"""
        if not await self._is_admin(update.effective_user.id):
            await update.message.reply_text("❌ This command is for admins only.")
            return


        keyboard = [
            [
                InlineKeyboardButton("📢 All Users", callback_data="broadcast_all"),
                InlineKeyboardButton("💫 Premium Users", callback_data="broadcast_premium")
            ],
            [
                InlineKeyboardButton("🆓 Free Users", callback_data="broadcast_free"),
                InlineKeyboardButton("⭐ Active Users", callback_data="broadcast_active")
            ],
            [InlineKeyboardButton("❌ Cancel", callback_data="broadcast_cancel")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)


        await update.message.reply_text(
            "📢 *Broadcast Message*\n\n"
            "Select target audience for your broadcast:",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )


    async def handle_broadcast_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle broadcast-related callbacks"""
        query = update.callback_query
        action = query.data.split("_")[1]


        if not await self._is_admin(query.from_user.id):
            await query.answer("❌ Admin only feature")
            return


        try:
            if action == "cancel":
                await query.message.edit_text("📢 Broadcast cancelled.")
                return


            # Store broadcast type in context
            context.user_data["broadcast_type"] = action
            
            # Show preview options
            keyboard = [
                [InlineKeyboardButton("✅ Preview", callback_data="broadcast_preview")],
                [InlineKeyboardButton("❌ Cancel", callback_data="broadcast_cancel")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)


            await query.message.edit_text(
                "📝 *Send your broadcast message*\n\n"
                "Reply to this message with the text you want to broadcast.\n"
                "You can use Markdown formatting.\n\n"
                "• Use *text* for *bold*\n"
                "• Use _text_ for _italic_\n"
                "• Use [text](URL) for links\n\n"
                "Optional: Add an image to your message.",
                reply_markup=reply_markup,
                parse_mode="Markdown"
            )


            # Set state for handling the next message
            context.user_data["awaiting_broadcast"] = True


        except Exception as e:
            logger.error(f"Broadcast callback error: {str(e)}")
            await query.answer("An error occurred")
            # Clean up context data in case of error
            context.user_data.pop("broadcast_type", None)
            context.user_data.pop("awaiting_broadcast", None)


    async def handle_broadcast_message(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle the broadcast message content"""
        if not context.user_data.get("awaiting_broadcast"):
            return


        # Store the message content
        context.user_data["broadcast_message"] = update.message.text
        context.user_data["broadcast_image"] = None


        # If message has an image, store it
        if update.message.photo:
            context.user_data["broadcast_image"] = update.message.photo[-1].file_id


        # Show confirmation keyboard
        keyboard = [
            [
                InlineKeyboardButton("✅ Send", callback_data="broadcast_confirm"),
                InlineKeyboardButton("❌ Cancel", callback_data="broadcast_cancel")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)


        preview_text = (
            "📢 *Broadcast Preview*\n\n"
            f"Target: {context.user_data['broadcast_type'].upper()}\n"
            f"Message:\n\n{context.user_data['broadcast_message']}"
        )


        if context.user_data["broadcast_image"]:
            await update.message.reply_photo(
                context.user_data["broadcast_image"],
                caption=preview_text,
                reply_markup=reply_markup,
                parse_mode="Markdown"
            )
        else:
            await update.message.reply_text(
                preview_text,
                reply_markup=reply_markup,
                parse_mode="Markdown"
            )


    async def send_broadcast(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Send the broadcast message to selected users"""
        query = update.callback_query
        broadcast_type = context.user_data["broadcast_type"]
        message = context.user_data["broadcast_message"]
        image = context.user_data.get("broadcast_image")


        # Get target users based on broadcast type
        target_users = await self._get_target_users(broadcast_type)


        # Initialize counters
        success_count = 0
        fail_count = 0
        
        # Show progress message
        progress_message = await query.message.edit_text(
            "📤 Sending broadcast...\n"
            "This may take a while."
        )


        # Send messages
        for user_id in target_users:
            try:
                if image:
                    await self.application.bot.send_photo(
                        chat_id=user_id,
                        photo=image,
                        caption=message,
                        parse_mode="Markdown"
                    )
                else:
                    await self.application.bot.send_message(
                        chat_id=user_id,
                        text=message,
                        parse_mode="Markdown"
                    )
                success_count += 1
                
                # Update progress every 10 messages
                if success_count % 10 == 0:
                    await progress_message.edit_text(
                        f"📤 Sending broadcast...\n"
                        f"Sent: {success_count}\n"
                        f"Failed: {fail_count}"
                    )
                
                # Add delay to avoid hitting rate limits
                await asyncio.sleep(0.05)
                
            except Exception as e:
                logger.error(f"Failed to send broadcast to {user_id}: {str(e)}")
                fail_count += 1


        # Show final results
        await progress_message.edit_text(
            "✅ *Broadcast Complete*\n\n"
            f"Successfully sent: {success_count}\n"
            f"Failed: {fail_count}",
            parse_mode="Markdown"
        )


        # Clear broadcast data
        context.user_data.clear()


    async def _get_target_users(self, broadcast_type: str) -> List[int]:
        """Get list of target users based on broadcast type"""
        users = []
        current_time = datetime.now()
        
        for user_id, data in self.data_manager.users.items():
            if broadcast_type == "all":
                users.append(int(user_id))
            
            elif broadcast_type == "premium":
                if data.get("tier") in ["SILVER", "GOLD", "PLATINUM"]:
                    users.append(int(user_id))
            
            elif broadcast_type == "free":
                if data.get("tier") == "FREE":
                    users.append(int(user_id))
            
            elif broadcast_type == "active":
                # Consider users active if they used the bot in the last 7 days
                last_active = datetime.fromisoformat(data.get("last_active", "2000-01-01"))
                if (current_time - last_active).days <= 7:
                    users.append(int(user_id))


        return users

    async def show_admin_stats(self, message: Message):
        """Show admin statistics"""
        total_users = len(self.data_manager.users)
        active_users = sum(1 for user in self.data_manager.users.values() 
                          if (datetime.now() - datetime.fromisoformat(user.get('last_active', '2000-01-01'))).days < 7)
        total_downloads = sum(user.get('total_downloads', 0) for user in self.data_manager.users.values())
        
        keyboard = [
            [
                InlineKeyboardButton("📢 Detailed Stats", callback_data="admin_detailed_stats"),
                InlineKeyboardButton("📈 Usage Graph", callback_data="admin_usage_graph")
            ],
            [InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await message.edit_text(
            "📊 *Bot Statistics*\n\n"
            f"Total Users: {total_users}\n"
            f"Active Users (7d): {active_users}\n"
            f"Total Downloads: {total_downloads}",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )

    async def show_user_management(self, message: Message):
        """Show user management interface"""
        try:
            total_users = len(self.data_manager.users)
            banned_users = len(self.data_manager.banned_users)
            
            # Calculate active users (7 days)
            now = datetime.now()
            active_users = sum(
                1 for u in self.data_manager.users.values()
                if (now - datetime.fromisoformat(u.get('last_active', '2000-01-01'))).days < 7
            )


            keyboard = [
                [
                    InlineKeyboardButton("🔍 Search Users", callback_data="admin_search_users"),
                    InlineKeyboardButton("📋 List Users", callback_data="admin_list_users")
                ],
                [
                    InlineKeyboardButton("⛔️ Banned Users", callback_data="admin_banned_users"),
                    InlineKeyboardButton("📊 User Stats", callback_data="admin_user_stats")
                ],
                [InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel")]
            ]


            text = (
                "👥 *User Management*\n\n"
                f"Total Users: {total_users:,}\n"
                f"Active Users (7d): {active_users:,}\n"
                f"Banned Users: {banned_users:,}\n\n"
                "Select an option:"
            )


            if isinstance(message, Message):
                await message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")
            else:
                await message.edit_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode="Markdown")


        except Exception as e:
            logger.error(f"Show user management error: {str(e)}")
            await message.edit_text("❌ Error loading user management.")



    async def list_users(self, message: Message, page: int = 0):
        """List users with pagination"""
        try:
            users = list(self.data_manager.users.items())
            users.sort(key=lambda x: datetime.fromisoformat(x[1].get('last_active', '2000-01-01')), reverse=True)
            
            items_per_page = 5
            start_idx = page * items_per_page
            end_idx = min(start_idx + items_per_page, len(users))
            current_users = users[start_idx:end_idx]
            total_pages = (len(users) + items_per_page - 1) // items_per_page

            text = f"👥 *User List* (Page {page + 1}/{total_pages})\n\n"
            
            for user_id, user_data in current_users:
                username = user_data.get('username', 'No username')
                tier = user_data.get('tier', 'FREE')
                last_active = datetime.fromisoformat(user_data.get('last_active', '2000-01-01')).strftime('%Y-%m-%d')
                text += (
                    f"👤 *{username}*\n"
                    f"ID: {user_id}\n"
                    f"Tier: {tier}\n"
                    f"Last Active: {last_active}\n"
                    f"{'🚫 Banned' if user_id in self.data_manager.banned_users else ''}\n\n"
                )

            keyboard = []
            nav_row = []
            
            if page > 0:
                nav_row.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"admin_list_users_{page-1}"))
            if end_idx < len(users):
                nav_row.append(InlineKeyboardButton("Next ➡️", callback_data=f"admin_list_users_{page+1}"))
            
            if nav_row:
                keyboard.append(nav_row)
            keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="admin_users")])

            await message.edit_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="Markdown"
            )

        except Exception as e:
            logger.error(f"List users error: {str(e)}")
            await message.edit_text(
                "❌ Error listing users.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔙 Back", callback_data="admin_users")
                ]])
            )


    async def list_banned_users(self, message: Message):
        """List all banned users"""
        try:
            if not self.data_manager.banned_users:
                text = "No banned users."
            else:
                text = "⛔️ *Banned Users*\n\n"
                for user_id, ban_data in self.data_manager.banned_users.items():
                    user_data = self.data_manager.users.get(user_id, {})
                    username = user_data.get('username', 'No username')
                    ban_date = datetime.fromisoformat(ban_data.get('banned_at', '2000-01-01')).strftime('%Y-%m-%d')
                    text += f"• {username} (ID: {user_id})\n  Banned on: {ban_date}\n  Reason: {ban_data.get('reason', 'N/A')}\n\n"


            keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="admin_users")]]
            
            await message.edit_text(
                text,
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="Markdown"
            )


        except Exception as e:
            logger.error(f"List banned users error: {str(e)}")
            await message.edit_text("❌ Error listing banned users.")



    async def show_user_search(self, message: Message):
        """Show user search interface"""
        keyboard = [
            [InlineKeyboardButton("🔙 Back to User Management", callback_data="admin_users")]
        ]
        
        await message.edit_text(
            "🔍 *Search Users*\n\n"
            "Send a message with either:\n"
            "• User ID\n"
            "• Username (with or without @)\n"
            "• Name\n\n"
            "Example: 123456789 or @username",
            reply_markup=InlineKeyboardMarkup(keyboard),
            parse_mode="Markdown"
        )


    async def ban_user(self, message: Message, user_id: str):
        """Ban a user"""
        try:
            if user_id not in self.data_manager.users:
                await message.edit_text(
                    "❌ User not found.",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔙 Back", callback_data="admin_users")
                    ]])
                )
                return


            # Add user to banned users
            self.data_manager.banned_users[user_id] = {
                "banned_at": datetime.now().isoformat(),
                "banned_by": str(message.chat.id),
                "reason": "Admin action"
            }
            self.data_manager.save_all()


            await message.edit_text(
                f"✅ User {user_id} has been banned.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔙🔙 Back", callback_data="admin_users")
                ]])
            )


        except Exception as e:
            logger.error(f"Ban user error: {str(e)}")
            await message.edit_text("❌ Error banning user. Please try again.")


    async def unban_user(self, message: Message, user_id: str):
        """Unban a user"""
        try:
            if user_id not in self.data_manager.banned_users:
                await message.edit_text(
                    "❌ User is not banned.",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔙 Back", callback_data="admin_users")
                    ]])
                )
                return


            # Remove user from banned users
            del self.data_manager.banned_users[user_id]
            self.data_manager.save_all()


            await message.edit_text(
                f"✅ User {user_id} has been unbanned.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔙 Back", callback_data="admin_users")
                ]])
            )


        except Exception as e:
            logger.error(f"Unban user error: {str(e)}")
            await message.edit_text("❌ Error unbanning user. Please try again.")


    async def show_user_info(self, message: Message, user_id: str):
        """Show detailed information about a user"""
        try:
            user_data = self.data_manager.users.get(user_id)
            if not user_data:
                await message.edit_text(
                    "❌ User not found.",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔙 Back", callback_data="admin_users")
                    ]])
                )
                return


            # Format user information
            join_date = datetime.fromisoformat(user_data.get('join_date', '2000-01-01'))
            last_active = datetime.fromisoformat(user_data.get('last_active', '2000-01-01'))
            
            text = (
                f"👤 *User Information*\n\n"
                f"ID: {user_id}\n"
                f"Username: @{user_data.get('username', 'None')}\n"
                f"Tier: {user_data.get('tier', 'FREE')}\n"
                f"Joined: {join_date.strftime('%Y-%m-%d')}\n"
                f"Last Active: {last_active.strftime('%Y-%m-%d %H:%M')}\n"
                f"Total Downloads: {user_data.get('total_downloads', 0)}\n"
                f"Downloads Today: {user_data.get('downloads_today', 0)}\n"
                f"Referral Count: {user_data.get('referral_count', 0)}\n\n"
                f"Status: {'🚫 Banned' if user_id in self.data_manager.banned_users else '✅ Active'}"
            )


            # Create action buttons
            keyboard = []
            if user_id in self.data_manager.banned_users:
                keyboard.append([InlineKeyboardButton("♻️ Unban User", callback_data=f"admin_unban_{user_id}")])
            else:
                keyboard.append([InlineKeyboardButton("🚫 Ban User", callback_data=f"admin_ban_{user_id}")])


            keyboard.extend([
                [InlineKeyboardButton("📊 Download History", callback_data=f"admin_history_{user_id}")],
                [InlineKeyboardButton("🔙 Back to User Management", callback_data="admin_users")]
            ])


            reply_markup = InlineKeyboardMarkup(keyboard)


            await message.edit_text(
                text,
                reply_markup=reply_markup,
                parse_mode="Markdown"
            )


        except Exception as e:
            logger.error(f"Show user info error: {str(e)}")
            await message.edit_text(
                "❌ Error loading user information. Please try again.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔙 Back", callback_data="admin_users")
                ]])
            )
            
    async def show_download_settings(self, message: Message):
        """Show download settings"""
        try:
            keyboard = [
                [InlineKeyboardButton("📊 Quality Presets", callback_data="admin_quality_settings")],
                [InlineKeyboardButton("⏳ Download Limits", callback_data="admin_limit_settings")],
                [InlineKeyboardButton("📁 Storage Settings", callback_data="admin_storage_settings")],
                [InlineKeyboardButton("🔙 Back", callback_data="admin_settings")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)


            await message.edit_text(
                "📥 *Download Settings*\n\n"
                "Select a setting to modify:",
                reply_markup=reply_markup,
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.error(f"Download settings error: {str(e)}")
            await message.edit_text("❌ Error showing download settings. Please try again.")


    async def show_ban_interface(self, message: Message):
        """Show ban user interface"""
        keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="admin_users")]]
        reply_markup = InlineKeyboardMarkup(keyboard)


        await message.edit_text(
            "🚫 *Ban User*\n\n"
            "To ban a user, use the command:\n"
            "/ban user_id reason\n\n"
            "Example:\n"
            "/ban 123456789 Spam",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )


    async def show_unban_interface(self, message: Message):
        """Show unban user interface"""
        keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="admin_users")]]
        reply_markup = InlineKeyboardMarkup(keyboard)


        await message.edit_text(
            "✅ *Unban User*\n\n"
            "To unban a user, use the command:\n"
            "/unban user_id\n\n"
            "Example:\n"
            "/unban 123456789",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )


    async def show_user_info_interface(self, message: Message):
        """Show user info interface"""
        keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="admin_users")]]
        reply_markup = InlineKeyboardMarkup(keyboard)


        await message.edit_text(
            "👤 *User Info*\n\n"
            "To view user information, use the command:\n"
            "/userinfo user_id",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )


    async def show_user_list(self, message: Message, page: int = 0):
        """Show paginated user list"""
        users_list = list(self.data_manager.users.items())
        users_per_page = 5
        start_idx = page * users_per_page
        end_idx = start_idx + users_per_page
        current_users = users_list[start_idx:end_idx]
        
        text = "📋 *User List*\n\n"
        for user_id, data in current_users:
            text += (
                f"👤 *User ID:* {user_id}\n"
                f"📝 Username: @{data.get('username', 'Unknown')}\n"
                f"🎫 Tier: {data.get('tier', 'FREE')}\n"
                f"📅 Joined: {data.get('join_date', 'Unknown')[:10]}\n"
                "───────────────\n"
            )
        
        # Pagination buttons
        keyboard = []
        if page > 0:
            keyboard.append(InlineKeyboardButton("⬅️ Previous", callback_data=f"admin_userlist_{page-1}"))
        if end_idx < len(users_list):
            keyboard.append(InlineKeyboardButton("➡️ Next", callback_data=f"admin_userlist_{page+1}"))
        
        keyboard = [keyboard] if keyboard else []
        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="admin_users")])
        
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await message.edit_text(
            text,
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )


    async def handle_user_info_action(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle user info action"""
        query = update.callback_query
        context.user_data['awaiting_userinfo_id'] = True
        
        keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="admin_users")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.message.edit_text(
            "👤 *User Info*\n\n"
            "Please enter the user ID to view information.\n"
            "Format: userid",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )


    async def handle_ban_action(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle ban user action"""
        query = update.callback_query
        context.user_data['awaiting_ban_userid'] = True
        
        keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="admin_users")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.message.edit_text(
            "🚫 *Ban User*\n\n"
            "Please enter the user ID to ban.\n"
            "Format: userid reason\n"
            "Example: 123456789 Spam",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )


    async def handle_unban_action(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle unban user action"""
        query = update.callback_query
        context.user_data['awaiting_unban_userid'] = True
        
        keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="admin_users")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.message.edit_text(
            "✅ *Unban User*\n\n"
            "Please enter the user ID to unban.\n"
            "Format: userid",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )


    async def show_maintenance_options(self, message: Message):
        """Show maintenance mode options"""
        try:
            maintenance_status = self.data_manager.maintenance.get("enabled", False)
            maintenance_msg = self.data_manager.maintenance.get("message", "Bot is under maintenance.")
            
            status = "🔴 Enabled" if maintenance_status else "🟢 Disabled"
            
            keyboard = [
                [InlineKeyboardButton(
                    "🔴 Disable Maintenance" if maintenance_status else "🟢 Enable Maintenance",
                    callback_data="admin_toggle_maintenance"
                )],
                [InlineKeyboardButton("✏️ Edit Message", callback_data="admin_edit_maintenance_msg")],
                [InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)


            await message.edit_text(
                f"🛠️ *Maintenance Mode*\n\n"
                f"Current Status: {status}\n\n"
                f"Message:\n{maintenance_msg}",
                reply_markup=reply_markup,
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.error(f"Maintenance options error: {str(e)}")
            await message.edit_text("❌ Error showing maintenance options. Please try again.")


    async def toggle_maintenance_mode(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Toggle maintenance mode on/off"""
        if not await self._is_admin(update.effective_user.id):
            return
        
        self.data_manager.maintenance["enabled"] = not self.data_manager.maintenance["enabled"]
        status = "enabled" if self.data_manager.maintenance["enabled"] else "disabled"
        self.data_manager.save_all()
        
        await update.message.reply_text(f"✅ Maintenance mode {status}")


    async def show_bot_statistics(self, message: Message):
        """Show bot statistics"""
        try:
            total_users = len(self.data_manager.users)
            
            # Calculate premium users
            premium_users = len([u for u in self.data_manager.users.values() 
                               if u.get("tier", "FREE") != "FREE"])
            
            # Calculate active users (last 7 days)
            week_ago = datetime.now() - timedelta(days=7)
            active_users = len([u for u in self.data_manager.users.values() 
                              if datetime.fromisoformat(u.get("last_active", "2000-01-01")) > week_ago])
            
            # Calculate total downloads
            total_downloads = sum(u.get("total_downloads", 0) for u in self.data_manager.users.values())
            
            # Calculate today's downloads
            today_downloads = sum(u.get("downloads_today", 0) for u in self.data_manager.users.values())


            stats_text = (
                "📊 *Bot Statistics*\n\n"
                f"👥 Total Users: {total_users:,}\n"
                f"💫 Premium Users: {premium_users:,}\n"
                f"⭐ Active Users (7d): {active_users:,}\n"
                f"📥 Total Downloads: {total_downloads:,}\n"
                f"📥 Today's Downloads: {today_downloads:,}\n"
                f"🚫 Banned Users: {len(self.data_manager.banned_users):,}\n\n"
                f"🕒 Last Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
            )


            keyboard = [[InlineKeyboardButton("🔄 Refresh", callback_data="admin_stats_refresh")],
                       [InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]]
            reply_markup = InlineKeyboardMarkup(keyboard)


            await message.edit_text(
                stats_text,
                reply_markup=reply_markup,
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.error(f"Statistics error: {str(e)}")
            await message.edit_text("❌ Error fetching statistics. Please try again.")


    async def check_maintenance_mode(self, user_id: int) -> bool:
        """Check if bot is in maintenance mode"""
        if self.data_manager.maintenance["enabled"] and not await self._is_admin(user_id):
            return True
        return False


    async def check_download_limits(self, user_id: str) -> bool:
        """Check if user has reached their download limits"""
        user_data = self.data_manager.users.get(user_id, {})
        tier = user_data.get("tier", "FREE")
        downloads_today = user_data.get("downloads_today", 0)
        last_reset = datetime.fromisoformat(user_data.get("last_download_reset", "2000-01-01"))


        # Reset daily downloads if it's a new day
        if datetime.now().date() > last_reset.date():
            user_data["downloads_today"] = 0
            user_data["last_download_reset"] = datetime.now().isoformat()
            downloads_today = 0
            self.data_manager.save_all()


        return downloads_today < SUBSCRIPTION_TIERS[tier]["downloads_per_day"]


    async def show_broadcast_menu(self, message: Message):
        """Show broadcast menu"""
        try:
            keyboard = [
                [
                    InlineKeyboardButton("📢 All Users", callback_data="broadcast_all"),
                    InlineKeyboardButton("💫 Premium Users", callback_data="broadcast_premium")
                ],
                [
                    InlineKeyboardButton("🆓 Free Users", callback_data="broadcast_free"),
                    InlineKeyboardButton("⭐ Active Users", callback_data="broadcast_active")
                ],
                [InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)


            await message.edit_text(
                "📢 *Broadcast Message*\n\n"
                "Select target audience for your broadcast:",
                reply_markup=reply_markup,
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.error(f"Broadcast menu error: {str(e)}")
            await message.edit_text("❌ Error showing broadcast menu. Please try again.")


    async def handle_broadcast_selection(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle broadcast audience selection"""
        try:
            query = update.callback_query
            broadcast_type = query.data.split("_")[1]
            
            context.user_data["broadcast_type"] = broadcast_type
            
            keyboard = [[InlineKeyboardButton("❌ Cancel", callback_data="broadcast_cancel")]]
            reply_markup = InlineKeyboardMarkup(keyboard)


            await query.message.edit_text(
                "📝 *Send your broadcast message*\n\n"
                "Reply to this message with the text you want to broadcast.\n"
                "You can use Markdown formatting.\n\n"
                "• Use *text* for *bold*\n"
                "• Use _text_ for _italic_\n"
                "• Use [text](URL) for links\n\n"
                "Optional: Add an image to your message.",
                reply_markup=reply_markup,
                parse_mode="Markdown"
            )
            
            context.user_data["awaiting_broadcast"] = True
            
        except Exception as e:
            logger.error(f"Broadcast selection error: {str(e)}")
            await query.message.edit_text("❌ Error processing broadcast selection. Please try again.")

    async def handle_code_generation(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle code generation"""
        try:
            query = update.callback_query
            tier = query.data.split("_")[1].upper()
            
            # Generate 5 codes
            codes = []
            for _ in range(5):
                code = ''.join(random.choices(string.ascii_uppercase + string.digits, k=12))
                self.data_manager.codes[code] = {
                    "tier": tier,
                    "created_at": datetime.now().isoformat(),
                    "used": False
                }
                codes.append(code)
            
            self.data_manager.save_all()
            
            # Show generated codes
            codes_text = "\n".join([f"{code}" for code in codes])
            
            keyboard = [
                [InlineKeyboardButton("🔄 Generate More", callback_data=f"gencode_{tier.lower()}")],
                [InlineKeyboardButton("🔙 Back", callback_data="admin_gencode")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)


            await query.message.edit_text(
                f"✅ Generated {tier} Codes:\n\n{codes_text}",
                reply_markup=reply_markup,
                parse_mode="Markdown"
            )
            
        except Exception as e:
            logger.error(f"Code generation error: {str(e)}")
            await query.message.edit_text("❌ Error generating codes. Please try again.")


    async def handle_user_ban(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle the /ban command"""
        try:
            # Check if user is admin
            if str(update.effective_user.id) not in os.getenv("ADMIN_IDS", "").split(","):
                await update.message.reply_text("❌ This command is for admins only.")
                return


            # Check command format
            if not context.args or len(context.args) < 2:
                await update.message.reply_text(
                    "❌ Incorrect format. Use:\n"
                    "/ban user_id reason"
                )
                return


            user_id = context.args[0]
            reason = " ".join(context.args[1:])


            # Check if user exists
            if user_id not in self.data_manager.users:
                await update.message.reply_text("❌ User not found.")
                return


            # Add user to banned users
            self.data_manager.banned_users[user_id] = {
                "banned_by": str(update.effective_user.id),
                "banned_at": datetime.now().isoformat(),
                "reason": reason
            }
            self.data_manager.save_all()


            await update.message.reply_text(
                f"✅ User {user_id} has been banned.\n"
                f"Reason: {reason}",
                parse_mode="Markdown"
            )


        except Exception as e:
            logger.error(f"Ban error: {str(e)}")
            await update.message.reply_text("❌ An error occurred while banning the user.")



    async def handle_user_unban(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle the /unban command"""
        try:
            # Check if user is admin
            if str(update.effective_user.id) not in os.getenv("ADMIN_IDS", "").split(","):
                await update.message.reply_text("❌ This command is for admins only.")
                return


            # Check command format
            if not context.args:
                await update.message.reply_text(
                    "❌ Incorrect format. Use:\n"
                    "/unban user_id"
                )
                return


            user_id = context.args[0]


            # Check if user is banned
            if user_id not in self.data_manager.banned_users:
                await update.message.reply_text("❌ User is not banned.")
                return


            # Remove user from banned users
            del self.data_manager.banned_users[user_id]
            self.data_manager.save_all()


            await update.message.reply_text(
                f"✅ User {user_id} has been unbanned.",
                parse_mode="Markdown"
            )


        except Exception as e:
            logger.error(f"Unban error: {str(e)}")
            await update.message.reply_text("❌ An error occurred while unbanning the user.")


    async def handle_user_info(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle the /userinfo command"""
        try:
            # Check if user is admin
            if str(update.effective_user.id) not in os.getenv("ADMIN_IDS", "").split(","):
                await update.message.reply_text("❌ This command is for admins only.")
                return


            # Check command format
            if not context.args:
                await update.message.reply_text(
                    "❌ Incorrect format. Use:\n"
                    "/userinfo user_id"
                )
                return


            user_id = context.args[0]


            # Check if user exists
            if user_id not in self.data_manager.users:
                await update.message.reply_text("❌ User not found.")
                return


            user_data = self.data_manager.users[user_id]
            banned_info = self.data_manager.banned_users.get(user_id, None)


            # Format user information
            info_text = (
                f"👤 *User Information*\n\n"
                f"🆔 User ID: {user_id}\n"
                f"👤 Username: @{user_data.get('username', 'Unknown')}\n"
                f"🎫 Tier: {user_data.get('tier', 'FREE')}\n"
                f"📅 Join Date: {user_data.get('join_date', 'Unknown')[:10]}\n"
                f"📥 Total Downloads: {user_data.get('total_downloads', 0)}\n"
                f"📥 Downloads Today: {user_data.get('downloads_today', 0)}\n"
                f"⏰ Last Active: {user_data.get('last_active', 'Unknown')[:19]}\n"
            )


            if banned_info:
                info_text += (
                    f"\n🚫 *Ban Information*\n"
                    f"📅 Banned At: {banned_info['banned_at'][:19]}\n"
                    f"👤 Banned By: {banned_info['banned_by']}\n"
                    f"📝 Reason: {banned_info['reason']}\n"
                )


            keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="admin_users")]]
            reply_markup = InlineKeyboardMarkup(keyboard)


            await update.message.reply_text(
                info_text,
                reply_markup=reply_markup,
                parse_mode="Markdown"
            )


        except Exception as e:
            logger.error(f"User info error: {str(e)}")
            await update.message.reply_text("❌ An error occurred while fetching user information.")


    async def show_redeem_interface(self, message: Message, context: ContextTypes.DEFAULT_TYPE):
        """Show interface for redeeming codes"""
        keyboard = [
            [InlineKeyboardButton("🔙 Back to Profile", callback_data="profile")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)


        await message.edit_text(
            "🎟️ *Redeem Code*\n\n"
            "To redeem a code, simply send it as a message.\n"
            "Format: XXXX-XXXX-XXXX\n\n"
            "Note: Codes are case-sensitive.",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )
        
        # Set state to await code
        context.user_data['awaiting_redeem_code'] = True


    async def show_referral_link(self, message: Message, context: ContextTypes.DEFAULT_TYPE):
        """Show user's referral link"""
        user_id = str(message.chat.id)
        user_data = self.data_manager.users.get(user_id, {})
        
        # Generate referral code if not exists
        if 'referral_code' not in user_data:
            user_data['referral_code'] = ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))
            self.data_manager.users[user_id] = user_data
            self.data_manager.save_all()
        
        me = await self.application.bot.get_me()
        bot_username = me.username
        referral_link = f"https://t.me/{bot_username}?start=REF_{user_data['referral_code']}"
        
        keyboard = [
            [InlineKeyboardButton("🔙 Back to Profile", callback_data="profile")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await message.edit_text(
            "🔗 *Your Referral Link*\n\n"
            f"{referral_link}\n\n"
            "Share this link with friends to earn rewards!\n\n"
            "Rewards:\n"
            "• 10 referrals: Silver Tier\n"
            "• 30 referrals: Gold Tier\n"
            "• 50 referrals: Platinum Tier",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )


    async def process_redeem_code(self, message: Message, context: ContextTypes.DEFAULT_TYPE):
        """Process redemption code"""
        code = message.text.strip()
        user_id = str(message.from_user.id)
        
        # Clear awaiting state
        context.user_data.pop('awaiting_redeem_code', None)
        
        try:
            # Check if code exists and is unused
            if code not in self.data_manager.codes:
                raise ValueError("Invalid code")
                
            code_data = self.data_manager.codes[code]
            if code_data.get("used"):
                raise ValueError("Code already used")
                
            # Apply the reward
            new_tier = code_data["tier"]
            current_tier = self.data_manager.users[user_id].get("tier", "FREE")
            
            # Only upgrade if new tier is better
            if (SUBSCRIPTION_TIERS[new_tier]["downloads_per_day"] > 
                SUBSCRIPTION_TIERS[current_tier]["downloads_per_day"]):
                self.data_manager.users[user_id]["tier"] = new_tier
                
                # Mark code as used
                code_data["used"] = True
                code_data["used_by"] = user_id
                code_data["used_at"] = datetime.now().isoformat()
                
                self.data_manager.save_all()
                
                keyboard = [[InlineKeyboardButton("🔙 Back to Profile", callback_data="profile")]]
                reply_markup = InlineKeyboardMarkup(keyboard)
                
                await message.reply_text(
                    f"✅ Code redeemed successfully!\n"
                    f"Your new tier: {new_tier}\n"
                    f"Daily downloads: {SUBSCRIPTION_TIERS[new_tier]['downloads_per_day']}\n"
                    f"Quality options: {', '.join(SUBSCRIPTION_TIERS[new_tier]['quality_options'])}kbps",
                    reply_markup=reply_markup
                )
            else:
                await message.reply_text(
                    "❌ This code provides a tier that is not higher than your current tier.",
                    reply_markup=InlineKeyboardMarkup([[
                        InlineKeyboardButton("🔙 Back to Profile", callback_data="profile")
                    ]])
                )
                
        except ValueError as e:
            await message.reply_text(
                f"❌ {str(e)}",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔙 Back to Profile", callback_data="profile")
                ]])
            )
        except Exception as e:
            logger.error(f"Error processing code: {str(e)}")
            await message.reply_text(
                "❌ An error occurred while processing the code. Please try again.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔙 Back to Profile", callback_data="profile")
                ]])
            )


    async def handle_download_history_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle download history display"""
        try:
            query = update.callback_query
            user_id = str(query.from_user.id)
            user_data = self.data_manager.users.get(user_id, {})
            
            # Initialize download history if it doesn't exist
            if 'download_history' not in user_data:
                user_data['download_history'] = []
                self.data_manager.save_all()
            
            history = user_data.get('download_history', [])
            page = context.user_data.get('history_page', 0)
            items_per_page = 5
            
            # Calculate total pages
            total_items = len(history)
            total_pages = max(1, (total_items + items_per_page - 1) // items_per_page)
            
            if total_items == 0:
                # No download history
                text = (
                    "📂 *Download History*\n\n"
                    "No downloads yet. Start downloading some music!"
                )
                keyboard = [[InlineKeyboardButton("🔙 Back to Profile", callback_data="profile")]]
            else:
                # Get items for current page
                start_idx = page * items_per_page
                end_idx = min(start_idx + items_per_page, total_items)
                current_items = history[start_idx:end_idx]
                
                # Create message text
                text = f"📂 *Download History*\n\nPage {page + 1}/{total_pages}\n\n"
                
                for i, item in enumerate(current_items, start=1):
                    download_date = datetime.fromisoformat(item.get('date', '')).strftime('%Y-%m-%d %H:%M')
                    title = item.get('title', 'Unknown')
                    mode = item.get('mode', 'audio')
                    quality = item.get('quality', 'unknown')
                    
                    text += (
                        f"{i}. *{title}*\n"
                        f"   📅 {download_date}\n"
                        f"   🎵 {mode.title()} • {quality}\n\n"
                    )
                
                # Create navigation keyboard
                keyboard = []
                nav_buttons = []
                
                if page > 0:
                    nav_buttons.append(
                        InlineKeyboardButton("⬅️ Previous", callback_data="history_prev")
                    )
                if (page + 1) < total_pages:
                    nav_buttons.append(
                        InlineKeyboardButton("Next ➡️", callback_data="history_next")
                    )
                
                if nav_buttons:
                    keyboard.append(nav_buttons)
                
                keyboard.extend([
                    [InlineKeyboardButton("🗑️ Clear History", callback_data="history_clear")],
                    [InlineKeyboardButton("🔙 Back to Profile", callback_data="profile")]
                ])
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await query.message.edit_text(
                text,
                reply_markup=reply_markup,
                parse_mode="Markdown"
            )
            
        except Exception as e:
            logger.error(f"Download history error: {str(e)}")
            await query.message.edit_text(
                "❌ Error loading download history. Please try again.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔙 Back to Profile", callback_data="profile")
                ]])
            )


    async def handle_history_navigation(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle download history navigation"""
        query = update.callback_query
        action = query.data.split("_")[1]
        
        current_page = context.user_data.get('history_page', 0)
        
        if action == "next":
            context.user_data['history_page'] = current_page + 1
        elif action == "prev":
            context.user_data['history_page'] = max(0, current_page - 1)
        
        await self.handle_download_history_callback(update, context)


    async def handle_admin_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle admin panel callbacks"""
        query = update.callback_query
        data = query.data.split("_")
        action = data[1] if len(data) > 1 else None

        if not await self._is_admin(query.from_user.id):
            await query.answer("❌ Admin only feature")
            return

        try:
            if action == "users":
                await self.show_user_management(query.message)
            elif action == "maintenance":
                await self.show_maintenance_panel(query.message)
            elif action == "stats":
                await self.show_admin_stats(query.message)
            elif action == "broadcast":
                await self.show_broadcast_options(query.message)
            elif action == "gencode":
                await self.show_code_generation(query.message)
            elif action == "settings":
                await self.show_admin_settings(query.message)
            elif action == "panel":
                # Return to main admin panel
                await self.admin_command(update, context)
            # Maintenance sub-actions
            elif action == "toggle" and len(data) > 2 and data[2] == "maintenance":
                self.data_manager.maintenance["enabled"] = not self.data_manager.maintenance.get("enabled", False)
                self.data_manager.save_all()
                await self.show_maintenance_panel(query.message)
            elif action == "reset" and len(data) > 2 and data[2] == "maintenance":
                self.data_manager.maintenance["message"] = "Bot is under maintenance."
                self.data_manager.save_all()
                await self.show_maintenance_panel(query.message)
            elif action == "set" and len(data) > 2 and data[2] == "maintenance":
                context.user_data["awaiting_maintenance_message"] = True
                await query.message.edit_text(
                    "📝 Send the new maintenance message in your next message.\n\n"
                    "Click Back to cancel.",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="admin_maintenance")]])
                )
            
            await query.answer()
        except Exception as e:
            logger.error(f"Admin callback error: {str(e)}")
            await query.answer("An error occurred")

    

    async def show_maintenance_panel(self, message: Message):
        """Show maintenance panel"""
        maintenance_status = self.data_manager.maintenance["enabled"]
        status_text = "🟢 Active" if not maintenance_status else "🔴 Maintenance"
        
        keyboard = [
            [InlineKeyboardButton(
                f"Toggle Maintenance ({status_text})", 
                callback_data="admin_toggle_maintenance"
            )],
            [
                InlineKeyboardButton("📝 Set Message", callback_data="admin_set_maintenance_msg"),
                InlineKeyboardButton("🔄 Reset", callback_data="admin_reset_maintenance")
            ],
            [InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        current_msg = self.data_manager.maintenance.get("message", "Bot is under maintenance")
        
        await message.edit_text(
            "🛠️ *Maintenance Control*\n\n"
            f"Current Status: {status_text}\n"
            f"Message: {current_msg}",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )

    async def show_code_generation(self, message: Message):
        """Show code generation interface"""
        try:
            keyboard = [
                [
                    InlineKeyboardButton("🥈 Silver Codes", callback_data="gencode_silver"),
                    InlineKeyboardButton("🥇 Gold Codes", callback_data="gencode_gold")
                ],
                [
                    InlineKeyboardButton("💎 Platinum Codes", callback_data="gencode_platinum"),
                    InlineKeyboardButton("📋 View Codes", callback_data="gencode_view")
                ],
                [InlineKeyboardButton("🔙 Back", callback_data="admin_panel")]
            ]
            reply_markup = InlineKeyboardMarkup(keyboard)


            await message.edit_text(
                "🎫 *Generate Redeem Codes*\n\n"
                "Select code tier to generate:",
                reply_markup=reply_markup,
                parse_mode="Markdown"
            )
        except Exception as e:
            logger.error(f"Code generation menu error: {str(e)}")
            await message.edit_text("❌ Error showing code generation menu. Please try again.")


    async def show_admin_settings(self, message: Message):
        """Show admin settings panel"""
        keyboard = [
            [
                InlineKeyboardButton("⚙️ Bot Settings", callback_data="admin_bot_settings"),
                InlineKeyboardButton("👥 User Settings", callback_data="admin_user_settings")
            ],
            [
                InlineKeyboardButton("🔒 Security", callback_data="admin_security"),
                InlineKeyboardButton("📝 Logs", callback_data="admin_logs")
            ],
            [InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await message.edit_text(
            "⚙️ *Admin Settings*\n\n"
            "Select a category to configure:",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )

    

    

    

    

    async def show_download_history(self, message: Message, user_id: str):
        """Show download history for a user"""
        try:
            user_data = self.data_manager.users.get(user_id, {})
            
            # Initialize download history if it doesn't exist
            if 'download_history' not in user_data:
                user_data['download_history'] = []
                self.data_manager.save_all()
            
            history = user_data.get('download_history', [])
            items_per_page = 5
            
            # Calculate total pages
            total_items = len(history)
            total_pages = max(1, (total_items + items_per_page - 1) // items_per_page)
            
            if total_items == 0:
                # No download history
                text = (
                    "📂 *Download History*\n\n"
                    "No downloads yet. Start downloading some music!"
                )
                keyboard = [[InlineKeyboardButton("🔙 Back to Profile", callback_data="profile")]]
            else:
                # Get first page of items
                current_items = history[:items_per_page]
                
                # Create message text
                text = f"📂 *Download History*\n\nPage 1/{total_pages}\n\n"
                
                for i, item in enumerate(current_items, start=1):
                    download_date = datetime.fromisoformat(item.get('date', '')).strftime('%Y-%m-%d %H:%M')
                    title = item.get('title', 'Unknown')
                    mode = item.get('mode', 'audio')
                    quality = item.get('quality', 'unknown')
                    
                    text += (
                        f"{i}. *{title}*\n"
                        f"   📅 {download_date}\n"
                        f"   🎵 {mode.title()} • {quality}\n\n"
                    )
                
                # Create navigation keyboard
                keyboard = []
                
                # Add next page button if there are more items
                if total_pages > 1:
                    keyboard.append([InlineKeyboardButton("Next ➡️", callback_data="history_next")])
                
                keyboard.extend([
                    [InlineKeyboardButton("🗑️ Clear History", callback_data="history_clear")],
                    [InlineKeyboardButton("🔙 Back to Profile", callback_data="profile")]
                ])
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await message.edit_text(
                text,
                reply_markup=reply_markup,
                parse_mode="Markdown"
            )
            
        except Exception as e:
            logger.error(f"Show download history error: {str(e)}")
            await message.edit_text(
                "❌ Error loading download history. Please try again.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔙 Back to Profile", callback_data="profile")
                ]])
            )

    async def show_download_history_next(self, message: Message, user_id: str):
        """Show next page of download history"""
        try:
            user_data = self.data_manager.users.get(user_id, {})
            history = user_data.get('download_history', [])
            items_per_page = 5
            
            # Get current page from message text
            current_page = int(message.text.split('/')[0].split()[-1])
            start_index = current_page * items_per_page
            end_index = start_index + items_per_page
            
            # Calculate total pages
            total_items = len(history)
            total_pages = max(1, (total_items + items_per_page - 1) // items_per_page)
            
            # Get next page of items
            current_items = history[start_index:end_index]
            
            # Create message text
            text = f"📂 *Download History*\n\nPage {current_page + 1}/{total_pages}\n\n"
            
            for i, item in enumerate(current_items, start=start_index + 1):
                download_date = datetime.fromisoformat(item.get('date', '')).strftime('%Y-%m-%d %H:%M')
                title = item.get('title', 'Unknown')
                mode = item.get('mode', 'audio')
                quality = item.get('quality', 'unknown')
                
                text += (
                    f"{i}. *{title}*\n"
                    f"   📅 {download_date}\n"
                    f"   🎵 {mode.title()} • {quality}\n\n"
                )
            
            # Create navigation keyboard
            keyboard = []
            
            # Add previous page button
            if current_page > 1:
                keyboard.append([InlineKeyboardButton("⬅️ Previous", callback_data="history_prev")])
            
            # Add next page button if there are more items
            if current_page < total_pages:
                keyboard.append([InlineKeyboardButton("Next ➡️", callback_data="history_next")])
            
            keyboard.extend([
                [InlineKeyboardButton("🗑️ Clear History", callback_data="history_clear")],
                [InlineKeyboardButton("🔙 Back to Profile", callback_data="profile")]
            ])
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await message.edit_text(
                text,
                reply_markup=reply_markup,
                parse_mode="Markdown"
            )
            
        except Exception as e:
            logger.error(f"Show download history next error: {str(e)}")
            await message.edit_text(
                "❌ Error loading download history. Please try again.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔙 Back to Profile", callback_data="profile")
                ]])
            )

    async def show_download_history_prev(self, message: Message, user_id: str):
        """Show previous page of download history"""
        try:
            user_data = self.data_manager.users.get(user_id, {})
            history = user_data.get('download_history', [])
            items_per_page = 5
            
            # Get current page from message text
            current_page = int(message.text.split('/')[0].split()[-1])
            start_index = max(0, (current_page - 2) * items_per_page)
            end_index = start_index + items_per_page
            
            # Calculate total pages
            total_items = len(history)
            total_pages = max(1, (total_items + items_per_page - 1) // items_per_page)
            
            # Get previous page of items
            current_items = history[start_index:end_index]
            
            # Create message text
            text = f"📂 *Download History*\n\nPage {current_page - 1}/{total_pages}\n\n"
            
            for i, item in enumerate(current_items, start=start_index + 1):
                download_date = datetime.fromisoformat(item.get('date', '')).strftime('%Y-%m-%d %H:%M')
                title = item.get('title', 'Unknown')
                mode = item.get('mode', 'audio')
                quality = item.get('quality', 'unknown')
                
                text += (
                    f"{i}. *{title}*\n"
                    f"   📅 {download_date}\n"
                    f"   🎵 {mode.title()} • {quality}\n\n"
                )
            
            # Create navigation keyboard
            keyboard = []
            
            # Add previous page button if not on first page
            if current_page > 2:
                keyboard.append([InlineKeyboardButton("⬅️ Previous", callback_data="history_prev")])
            
            # Add next page button if there are more items
            if current_page < total_pages:
                keyboard.append([InlineKeyboardButton("Next ➡️", callback_data="history_next")])
            
            keyboard.extend([
                [InlineKeyboardButton("🗑️ Clear History", callback_data="history_clear")],
                [InlineKeyboardButton("🔙 Back to Profile", callback_data="profile")]
            ])
            
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await message.edit_text(
                text,
                reply_markup=reply_markup,
                parse_mode="Markdown"
            )
            
        except Exception as e:
            logger.error(f"Show download history prev error: {str(e)}")
            await message.edit_text(
                "❌ Error loading download history. Please try again.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton("🔙 Back to Profile", callback_data="profile")
                ]])
            )

    async def handle_history_clear(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle clear history request"""
        query = update.callback_query
        user_id = str(query.from_user.id)
        
        keyboard = [
            [
                InlineKeyboardButton("✅ Yes", callback_data="history_clear_confirm"),
                InlineKeyboardButton("❌ No", callback_data="download_history")
            ]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await query.message.edit_text(
            "🗑️ *Clear Download History*\n\n"
            "Are you sure you want to clear your download history?\n"
            "This action cannot be undone.",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )

    async def handle_history_clear_confirm(self, update: Update, context: ContextTypes.DEFAULT_TYPE):
        """Handle history clear confirmation"""
        query = update.callback_query
        user_id = str(query.from_user.id)
        
        try:
            if user_id in self.data_manager.users:
                self.data_manager.users[user_id]['download_history'] = []
                self.data_manager.save_all()
                
            await self.show_download_history(query.message, user_id)
            
        except Exception as e:
            logger.error(f"Error clearing history: {str(e)}")
            keyboard = [[InlineKeyboardButton("🔙 Back", callback_data="profile")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await query.message.edit_text(
                "❌ Error clearing download history.",
                reply_markup=reply_markup
            )

    async def show_broadcast_options(self, message: Message):
        """Show broadcast options"""
        keyboard = [
            [
                InlineKeyboardButton("📢 All Users", callback_data="broadcast_all"),
                InlineKeyboardButton("💫 Premium Users", callback_data="broadcast_premium")
            ],
            [
                InlineKeyboardButton("🆓 Free Users", callback_data="broadcast_free"),
                InlineKeyboardButton("⭐ Active Users", callback_data="broadcast_active")
            ],
            [InlineKeyboardButton("🔙 Back to Admin", callback_data="admin_panel")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)

        await message.edit_text(
            "📢 *Broadcast Message*\n\n"
            "Select target audience:",
            reply_markup=reply_markup,
            parse_mode="Markdown"
        )

def check_and_install_dependencies():
    """Check and install required dependencies"""
    print("🔍 Starting dependency check...")

    def install_package(package_name, version=None):
        try:
            if version:
                subprocess.check_call(
                    [sys.executable, "-m", "pip", "install", f"{package_name}>={version}"]
                )
            else:
                subprocess.check_call(
                    [sys.executable, "-m", "pip", "install", package_name]
                )
            print(f"✅ Installed {package_name}")
        except Exception as e:
            print(f"❌ Failed to install {package_name}: {str(e)}")
            return False
        return True

    def install_ffmpeg():
        """Install FFmpeg based on the operating system"""
        try:
            if sys.platform.startswith('linux'):
                # For Linux systems
                subprocess.check_call(['sudo', 'apt-get', 'update'])
                subprocess.check_call(['sudo', 'apt-get', 'install', '-y', 'ffmpeg'])
            elif sys.platform.startswith('darwin'):
                # For macOS using Homebrew
                try:
                    subprocess.check_call(['brew', '--version'])
                except:
                    # Install Homebrew if not installed
                    subprocess.check_call(['/bin/bash', '-c', 
                        '"$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"'])
                subprocess.check_call(['brew', 'install', 'ffmpeg'])
            elif sys.platform.startswith('win'):
                # For Windows using chocolatey
                try:
                    subprocess.check_call(['choco', '--version'])
                except:
                    # Install chocolatey if not installed
                    subprocess.check_call(['powershell', '-Command', 
                        'Set-ExecutionPolicy Bypass -Scope Process -Force; [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072; iex ((New-Object System.Net.WebClient).DownloadString(\'https://chocolatey.org/install.ps1\'))'])
                subprocess.check_call(['choco', 'install', 'ffmpeg', '-y'])
            print("✅ FFmpeg installed successfully")
            return True
        except Exception as e:
            print(f"❌ Failed to install FFmpeg: {str(e)}")
            print("Please install FFmpeg manually from: https://ffmpeg.org/download.html")
            return False

    # Required Python packages with minimum versions
    required_packages = {
        "python-telegram-bot": "20.7",
        "yt-dlp": "2023.11.16",
        "ytmusicapi": "1.3.2",
        "requests": "2.31.0",
        "aiohttp": "3.9.1",
        "pillow": "10.0.0",
        "pycryptodome": "3.19.0",
        "beautifulsoup4": "4.12.2",
    }

    # Check and install Python packages
    all_packages_installed = True
    for package, version in required_packages.items():
        try:
            pkg_resources.require(f"{package}>={version}")
            print(f"✅ {package} is already installed")
        except (pkg_resources.DistributionNotFound, pkg_resources.VersionConflict):
            print(f"📦 Installing {package}...")
            if not install_package(package, version):
                all_packages_installed = False

    # Check and install FFmpeg
    ffmpeg_installed = False
    if shutil.which('ffmpeg') is None:
        print("📦 FFmpeg not found. Attempting to install...")
        ffmpeg_installed = install_ffmpeg()
    else:
        print("✅ FFmpeg is already installed")
        ffmpeg_installed = True

    # Create necessary directories
    try:
        data_dirs = ["bot_data", "downloads", "temp"]
        for directory in data_dirs:
            os.makedirs(directory, exist_ok=True)
        print("✅ Created necessary directories")
    except Exception as e:
        print(f"❌ Failed to create directories: {str(e)}")
        all_packages_installed = False

    # Final status
    if all_packages_installed and ffmpeg_installed:
        print("✅ All dependencies are installed and ready!")
        return True
    else:
        print("⚠️ Some dependencies could not be installed. Please install them manually.")
        return False

def main():
    """Main function to run the bot"""
    try:
        # Check dependencies first
        if not check_and_install_dependencies():
            print("❌ Critical dependencies are missing. Please install them and try again.")
            sys.exit(1)


        # Get bot token
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        if not token:
            print("❌ Error: TELEGRAM_BOT_TOKEN environment variable not set")
            print("Please set your bot token using:")
            print("export TELEGRAM_BOT_TOKEN='your-token-here'")
            sys.exit(1)


        # Initialize and run the bot
        print("🤖 Starting bot...")
        bot = MusicBot(token)
        bot.run()

    except Exception as e:
        print(f"❌ Error: {str(e)}")
        sys.exit(1)




if __name__ == "__main__":
    main()
