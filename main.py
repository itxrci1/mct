import asyncio
import aiohttp
import random
import os
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from motor.motor_asyncio import AsyncIOMotorClient

# ================= LOAD ENV =================
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
MONGO_URI = os.getenv("MONGO_URI")

# ================= GLOBALS =================
user_tokens = {}
matching_tasks = {}

mongo = AsyncIOMotorClient(MONGO_URI)
config = mongo["meeff_db"]["config"]

bot = Bot(BOT_TOKEN)
dp = Dispatcher()

HEADERS = {
    "User-Agent": "okhttp/5.1.0 (Linux; Android 13)",
    "Accept": "application/json",
    "Connection": "keep-alive",
    "Host": "api.meeff.com",
}

ANSWER_URL = "https://api.meeff.com/user/undoableAnswer/v5/?userId={user_id}&isOkay=1"


# ================= HELPERS =================
async def fetch_users(session, explore_url):
    async with session.get(explore_url) as res:
        text = await res.text()
        try:
            data = await res.json(content_type=None)
        except:
            data = None
        return res.status, text, data


# ================= MATCHING =================
async def start_matching(chat_id, token, explore_url):
    headers = HEADERS.copy()
    headers["meeff-access-token"] = token

    stats = {
        "requests": 0,
        "cycles": 0,
        "errors": 0,
        "matched": 0,
        "loops": 0,
    }

    stop_keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Stop Matching")]],
        resize_keyboard=True
    )

    status_msg = await bot.send_message(
        chat_id,
        "🔄 Matching started...\n\n⏳ Waiting for users...",
        reply_markup=stop_keyboard
    )

    timeout = aiohttp.ClientTimeout(total=30)
    connector = aiohttp.TCPConnector(ssl=False, limit_per_host=10)

    async with aiohttp.ClientSession(
        timeout=timeout,
        connector=connector,
        headers=headers
    ) as session:

        async def answer_user(user_id):
            try:
                async with session.get(ANSWER_URL.format(user_id=user_id)) as res:
                    text = await res.text()

                    if res.status == 401 or "AuthRequired" in text:
                        return False

                    if res.status == 429 or "LikeExceeded" in text:
                        return False

                    stats["matched"] += 1
                    return True
            except:
                stats["errors"] += 1
                return True

        while chat_id in matching_tasks:
            stats["loops"] += 1

            status, raw, data = await fetch_users(session, explore_url)

            batch_size = 0

            if data and data.get("users"):
                users = data["users"]
                batch_size = len(users)
                tasks = []

                for user in users:
                    uid = user.get("_id")
                    if not uid:
                        continue

                    tasks.append(asyncio.create_task(answer_user(uid)))
                    stats["requests"] += 1

                    if len(tasks) >= 10:
                        await asyncio.gather(*tasks)
                        tasks.clear()

                    await asyncio.sleep(random.uniform(0.05, 0.2))

                if tasks:
                    await asyncio.gather(*tasks)

                stats["cycles"] += 1

            # 🔥 ALWAYS update stats (THIS IS THE FIX)
            try:
                await status_msg.edit_text(
                    f"🔄 Matching running...\n\n"
                    f"🔁 Loop: {stats['loops']}\n"
                    f"📦 Batches: {stats['cycles']}\n"
                    f"👥 Users last batch: {batch_size}\n"
                    f"✅ Matched: {stats['matched']}\n"
                    f"📡 Requests: {stats['requests']}\n"
                    f"⚠️ Errors: {stats['errors']}\n\n"
                    f"⏱ Still working..."
                )
            except:
                pass

            await asyncio.sleep(2)


# ================= BOT COMMANDS =================
@dp.message(Command("start"))
async def start(message: types.Message):
    await message.answer("Send Meeff token")


@dp.message(Command("seturl"))
async def set_url(message: types.Message):
    url = message.text.replace("/seturl", "").strip()
    if not url.startswith("https://"):
        return await message.answer("Invalid URL")

    await config.update_one(
        {"_id": "explore_url"},
        {"$set": {"url": url}},
        upsert=True
    )
    await message.answer("Explore URL saved")


@dp.message(F.text == "Start Matching")
async def start_btn(message: types.Message):
    chat_id = message.chat.id

    if chat_id not in user_tokens:
        return await message.answer("Send token first")

    data = await config.find_one({"_id": "explore_url"})
    if not data:
        return await message.answer("Use /seturl first")

    task = asyncio.create_task(
        start_matching(chat_id, user_tokens[chat_id], data["url"])
    )
    matching_tasks[chat_id] = task


@dp.message(F.text == "Stop Matching")
@dp.message(Command("stop"))
async def stop(message: types.Message):
    matching_tasks.pop(message.chat.id, None)

    await message.answer(
        "Matching stopped",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="Start Matching")]],
            resize_keyboard=True
        )
    )


@dp.message(F.text)
async def receive_token(message: types.Message):
    user_tokens[message.chat.id] = message.text.strip()
    await message.answer(
        "Token saved",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="Start Matching")]],
            resize_keyboard=True
        )
    )


# ================= RUN =================
async def main():
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
