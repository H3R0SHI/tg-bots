Modern Reminder Telegram Bot
============================

Features
- Per-user JSON storage in `data/` (profiles, reminders, codes)
- Natural language reminders: `/remind <when> | <what>` (e.g., `in 2h | Walk`)
- Repeating reminders: `/repeat daily | Meditate at 07:30`, `/repeat weekly 1 0 | Review at 09:00`
- Inline actions on reminders: Snooze, Done, Delete
- Timezone per user: `/timezone Europe/Berlin`
- Credit system and premium tier
- Redeem codes: `/redeem <CODE>`
- Admin tools: `/gen`, `/grant`, `/broadcast`

Setup
1) Create bot via BotFather and get the token.
2) Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3) Set environment variables:
   - `BOT_TOKEN` (required)
   - `ADMIN_IDS` comma-separated Telegram user IDs with admin rights
   - If you want Google Sheets sync: 
     * Create a Google Service Account and download the JSON credentials
     * Run `python setup_google_sheets.py` for guided setup
     * Or set environment variable `GOOGLE_APPLICATION_CREDENTIALS=/path/to/creds.json`
     * Share your spreadsheet with the service account email (found in the JSON file)
4) Run:
   ```bash
   python reminderbot.py
   ```

Commands
- `/start`, `/help` – basics
- `/remind <when> | <what>` – one-shot reminder
- `/repeat daily|weekly [interval] [dow] | <text> at <HH:MM>` – repeating reminder
- `/list` – show active reminders
- `/delete <id>` – delete a reminder
- `/timezone <Region/City>` – set your timezone
- `/profile` – plan & credits
- `/redeem <CODE>` – add credits or premium

Admin
- `/gen credits <amount> <count> [days_valid]`
- `/gen premium <days> <count> [days_valid]` (premium is simplified as toggle)
- `/grant <user_id> credits <amount>`
- `/grant <user_id> premium`
- `/broadcast <message>`

Data layout
- `data/users.json` – user profiles: timezone, is_premium, credits
- `data/reminders.json` – reminders keyed by ID
- `data/codes.json` – redeemable codes

Notes
- Time parsing uses `dateparser` and respects user timezone.
- Free users consume 1 credit per new reminder; premium users do not.
- Free users: up to 20 active reminders; Premium: up to 500.
- Snooze limits: Free=1 per reminder, Premium=5 per reminder.

