import asyncio, os
from dotenv import load_dotenv
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
import httpx

from backend.app.db import supabase

load_dotenv()
TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
API_URL = os.getenv("BACKEND_API_URL", "http://localhost:8000")


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("PredArb Bot is alive. Use /help for commands.")
    # Upsert profile into Supabase
    user = update.effective_user
    if supabase and user:
        try:
            supabase.table("profiles").upsert(
                {
                    "telegram_user_id": user.id,
                    "username": user.username,
                    "prefs": {},
                }
            ).execute()
        except Exception as e:
            # Log error but keep bot responsive
            print(f"Supabase upsert failed: {e}")


async def help_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("/start /help /search /alerts /groups /analyze")


async def search_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = " ".join(context.args)
    if not query:
        await update.message.reply_text("Usage: /search <query>")
        return
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(f"{API_URL}/search", params={"q": query, "limit": 5})
            data = resp.json()
            if data.get("ok"):
                items = data.get("items", [])
                text = "\n".join(f"{i['id']}: {i.get('title','')}" for i in items) or "No results."
            else:
                text = "Search failed"
        except Exception as e:
            text = f"Error: {e}"
    await update.message.reply_text(text)


async def alerts_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("Alerts feature coming soon.")


async def groups_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.get(f"{API_URL}/groups", params={"limit": 5})
            data = resp.json()
            if data.get("ok"):
                items = data.get("items", [])
                text = "\n".join(f"{g['id']}: {g.get('title','')}" for g in items) or "No groups."
            else:
                text = "Failed to fetch groups"
        except Exception as e:
            text = f"Error: {e}"
    await update.message.reply_text(text)


async def analyze_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    async with httpx.AsyncClient() as client:
        try:
            resp = await client.post(f"{API_URL}/analyze/run")
            data = resp.json()
            if data.get("ok"):
                text = f"Analysis triggered ({data.get('mode')})"
            else:
                text = "Analyze failed"
        except Exception as e:
            text = f"Error: {e}"
    await update.message.reply_text(text)


def main():
    if not TOKEN:
        raise SystemExit("TELEGRAM_BOT_TOKEN not set in environment")
    app = Application.builder().token(TOKEN).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("help", help_cmd))
    app.add_handler(CommandHandler("search", search_cmd))
    app.add_handler(CommandHandler("alerts", alerts_cmd))
    app.add_handler(CommandHandler("groups", groups_cmd))
    app.add_handler(CommandHandler("analyze", analyze_cmd))
    app.run_polling()


if __name__ == "__main__":
    main()

