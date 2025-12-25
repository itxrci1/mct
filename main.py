import asyncio
import aiohttp
import random
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton
from motor.motor_asyncio import AsyncIOMotorClient

BOT_TOKEN = "8300519461:AAGub3h_FqGkggWkGGE95Pgh8k4u6deI_F4"
MONGO_URI = "mongodb+srv://itxcriminal:qureshihashmI1@cluster0.jyqy9.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# chat_id -> list of tokens
user_tokens = {}

# (chat_id, token) -> asyncio.Task
matching_tasks = {}

mongo = AsyncIOMotorClient(MONGO_URI)
config = mongo["meeff_db"]["config"]

bot = Bot(BOT_TOKEN)
dp = Dispatcher()

HEADERS = {
    "User-Agent": "okhttp/5.1.0 (Linux; Android 13)",
    "Accept-Encoding": "gzip",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Host": "api.meeff.com",
}

ANSWER_URL = "https://api.meeff.com/user/undoableAnswer/v5/?userId={user_id}&isOkay=1"


async def fetch_users(session, explore_url):
    async with session.get(explore_url) as res:
        status = res.status
        text = await res.text()
        if status != 200:
            return status, text, None
        try:
            data = await res.json(content_type=None)
        except:
            return status, text, None
        return status, text, data


async def start_matching(chat_id, token, explore_url):
    headers = HEADERS.copy()
    headers["meeff-access-token"] = token

    stats = {"requests": 0, "cycles": 0, "errors": 0}
    stat_msg = await bot.send_message(chat_id, f"Matching started\nToken: {token[:10]}...")

    stop_keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="Stop Matching")]],
        resize_keyboard=True
    )
    await stat_msg.edit_reply_markup(stop_keyboard)

    timeout = aiohttp.ClientTimeout(total=30)
    connector = aiohttp.TCPConnector(ssl=False, limit_per_host=10)
    empty_count = 0
    stop_reason = None

    try:
        async with aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers=headers
        ) as session:

            async def answer_user(user_id):
                nonlocal stop_reason
                try:
                    async with session.get(ANSWER_URL.format(user_id=user_id)) as res:
                        text = await res.text()
                        if res.status == 429 or "LikeExceeded" in text:
                            stop_reason = "limit"
                            return False
                        if res.status == 401 or "AuthRequired" in text:
                            stop_reason = "token"
                            return False
                        return True
                except:
                    stats["errors"] += 1
                    return True

            while (chat_id, token) in matching_tasks:
                status, raw_text, data = await fetch_users(session, explore_url)

                if status == 401 or "AuthRequired" in str(raw_text):
                    stop_reason = "token"
                    break

                if data is None or not data.get("users"):
                    empty_count += 1
                    if empty_count >= 6:
                        stop_reason = "empty"
                        break
                    await asyncio.sleep(1)
                    continue

                empty_count = 0
                tasks = []
                results = []

                for user in data["users"]:
                    user_id = user.get("_id")
                    if not user_id:
                        continue

                    tasks.append(asyncio.create_task(answer_user(user_id)))
                    stats["requests"] += 1
                    await asyncio.sleep(random.uniform(0.05, 0.2))

                    if len(tasks) >= 10:
                        results.extend(await asyncio.gather(*tasks))
                        tasks.clear()

                if tasks:
                    results.extend(await asyncio.gather(*tasks))

                if False in results:
                    break

                stats["cycles"] += 1
                await stat_msg.edit_text(
                    f"Token: {token[:10]}...\n"
                    f"Requests: {stats['requests']}\n"
                    f"Cycles: {stats['cycles']}\n"
                    f"Errors: {stats['errors']}\n"
                    f"Send /stop to stop all"
                )

                await asyncio.sleep(random.uniform(1, 2))

    except Exception as e:
        await stat_msg.edit_text(f"Error: {e}")

    if stop_reason == "limit":
        text = "Daily limit reached"
    elif stop_reason == "token":
        text = "Token expired"
    elif stop_reason == "empty":
        text = "No users found repeatedly"
    else:
        text = "Matching stopped"

    await bot.send_message(chat_id, f"{text}\nToken: {token[:10]}...")
    matching_tasks.pop((chat_id, token), None)


@dp.message(Command("start"))
async def start(message: types.Message):
    await message.answer("Send Meeff token(s). Each token starts automatically.")


@dp.message(Command("seturl"))
async def set_url(message: types.Message):
    url = message.text.replace("/seturl", "").strip()
    if not url.startswith("https://"):
        return await message.answer("Invalid URL")
    await config.update_one({"_id": "explore_url"}, {"$set": {"url": url}}, upsert=True)
    await message.answer("Explore URL saved")


@dp.message(Command("stop"))
@dp.message(F.text == "Stop Matching")
async def stop(message: types.Message):
    chat_id = message.chat.id
    stopped = False

    for key in list(matching_tasks.keys()):
        if key[0] == chat_id:
            matching_tasks[key].cancel()
            matching_tasks.pop(key, None)
            stopped = True

    if stopped:
        await message.answer("All matching tasks stopped")
    else:
        await message.answer("No matching running")


@dp.message(F.text)
async def receive_token(message: types.Message):
    chat_id = message.chat.id
    token = message.text.strip()

    if chat_id not in user_tokens:
        user_tokens[chat_id] = []

    user_tokens[chat_id].append(token)

    data = await config.find_one({"_id": "explore_url"})
    if not data:
        return await message.answer("Use /seturl first")

    explore_url = data["url"]

    task = asyncio.create_task(start_matching(chat_id, token, explore_url))
    matching_tasks[(chat_id, token)] = task

    await message.answer("Token received. Matching started ✅")


async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
