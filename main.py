import asyncio
import aiohttp
import random
import os
from pathlib import Path
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from aiogram.fsm.storage.memory import MemoryStorage
from motor.motor_asyncio import AsyncIOMotorClient


def _load_env(path: str = ".env"):
    try:
        from dotenv import load_dotenv
        load_dotenv(path)
        return
    except Exception:
        pass

    p = Path(path)
    if not p.exists():
        return

    for line in p.read_text(encoding="utf-8").splitlines():
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


_load_env()

BOT_TOKEN = os.environ.get("BOT_TOKEN")
MONGO_URI = os.environ.get("MONGO_URI")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is required")
if not MONGO_URI:
    raise RuntimeError("MONGO_URI is required")


bot = Bot(BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

mongo = AsyncIOMotorClient(MONGO_URI)
db = mongo["meeff_db"]
config = db["config"]

user_tokens = {}
matching_tasks = {}
user_stats = {}

HEADERS_TEMPLATE = {
    "User-Agent": "okhttp/5.1.0 (Linux; Android 13)",
    "Accept": "application/json",
    "Accept-Encoding": "gzip",
    "Connection": "keep-alive",
    "Host": "api.meeff.com",
}

ANSWER_URL = "https://api.meeff.com/user/undoableAnswer/v5/?userId={user_id}&isOkay=1"


async def fetch_users(session, explore_url):
    async with session.get(explore_url) as res:
        text = await res.text()
        if res.status != 200:
            return res.status, text, None
        try:
            return res.status, text, await res.json(content_type=None)
        except:
            return res.status, text, None


async def start_matching(chat_id, token, explore_url):
    key = f"{chat_id}:{token}"
    headers = HEADERS_TEMPLATE | {"meeff-access-token": token}

    stats = {"requests": 0, "cycles": 0, "errors": 0}
    user_stats[key] = stats

    stat_msg = await bot.send_message(chat_id, "Loading stats...")

    timeout = aiohttp.ClientTimeout(total=30)
    connector = aiohttp.TCPConnector(ssl=False)

    stop_reason = None
    empty_count = 0

    try:
        async with aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers=headers
        ) as session:

            async def answer_user(user_id):
                nonlocal stop_reason
                try:
                    async with session.get(ANSWER_URL.format(user_id=user_id)) as r:
                        txt = await r.text()
                        if r.status == 429 or "LikeExceeded" in txt:
                            stop_reason = "LIMIT EXCEEDED"
                            return False
                        if r.status == 401 or "AuthRequired" in txt:
                            stop_reason = "TOKEN EXPIRED"
                            return False
                        return True
                except:
                    stats["errors"] += 1
                    return True

            while key in matching_tasks:
                status, raw, data = await fetch_users(session, explore_url)

                if status == 401 or "AuthRequired" in str(raw):
                    stop_reason = "TOKEN EXPIRED"
                    break

                users = (data or {}).get("users", [])
                if not users:
                    empty_count += 1
                    if empty_count >= 6:
                        stop_reason = "NO USERS FOUND"
                        break
                    await asyncio.sleep(1)
                    continue

                empty_count = 0
                tasks = []

                for u in users:
                    if not u.get("_id"):
                        continue
                    tasks.append(asyncio.create_task(answer_user(u["_id"])))
                    stats["requests"] += 1
                    await asyncio.sleep(random.uniform(0.05, 0.2))

                results = await asyncio.gather(*tasks)
                if False in results:
                    break

                stats["cycles"] += 1
                await stat_msg.edit_text(
                    f"Live Stats:\n"
                    f"Requests: {stats['requests']}\n"
                    f"Cycles: {stats['cycles']}\n"
                    f"Errors: {stats['errors']}"
                )

                await asyncio.sleep(random.uniform(1, 2))

    except Exception as e:
        await stat_msg.edit_text(f"Error: {e}")

    matching_tasks.pop(key, None)
    user_stats.pop(key, None)

    if stop_reason:
        await stat_msg.edit_text(
            f"Stopped:\n"
            f"Requests: {stats['requests']}\n"
            f"Cycles: {stats['cycles']}\n"
            f"Errors: {stats['errors']}\n\n"
            f"⚠️ {stop_reason}"
        )


# ---------------- COMMANDS ----------------

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await message.answer("Send Meeff token.")


@dp.message(Command("stop"))
async def cmd_stop(message: types.Message):
    chat_id = message.chat.id
    keys = [k for k in matching_tasks if k.startswith(f"{chat_id}:")]

    if not keys:
        return await message.answer("Not running.")

    for k in keys:
        matching_tasks[k].cancel()
        matching_tasks.pop(k, None)

    await message.answer(
        "Stopped.",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="Start Matching")]],
            resize_keyboard=True
        )
    )


@dp.message(Command("seturl"))
async def cmd_seturl(message: types.Message):
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].startswith("https://"):
        return await message.answer("Usage: /seturl <https://url>")

    await config.update_one(
        {"_id": "explore_url"},
        {"$set": {"url": parts[1]}},
        upsert=True
    )
    await message.answer("✔️ URL saved.")


# ---------------- BUTTONS ----------------

@dp.message(F.text == "Start Matching")
async def start_btn(message: types.Message):
    chat_id = message.chat.id
    tokens = user_tokens.get(chat_id)

    if not tokens:
        return await message.answer("Send token first.")

    data = await config.find_one({"_id": "explore_url"})
    if not data:
        return await message.answer("Use /seturl first.")

    for token in tokens:
        key = f"{chat_id}:{token}"
        if key not in matching_tasks:
            matching_tasks[key] = asyncio.create_task(
                start_matching(chat_id, token, data["url"])
            )

    await message.answer(
        "Matching started.",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="Stop Matching")]],
            resize_keyboard=True
        )
    )


# ---------------- TOKEN HANDLER (FIXED) ----------------

@dp.message(F.text & ~F.text.startswith("/"))
async def receive_token(message: types.Message):
    chat_id = message.chat.id
    token = message.text.strip()

    user_tokens.setdefault(chat_id, [])

    if token in user_tokens[chat_id]:
        return await message.answer("Token already saved.")

    user_tokens[chat_id].append(token)

    await message.answer(
        "✔️ Token saved.",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="Start Matching")]],
            resize_keyboard=True
        )
    )


async def main():
    await bot.set_my_commands([
        types.BotCommand(command="start", description="Start bot"),
        types.BotCommand(command="seturl", description="Set explore URL"),
        types.BotCommand(command="stop", description="Stop matching"),
    ])
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
