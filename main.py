import asyncio
import logging
from datetime import datetime, timezone
from config import PRIVATE_CHANNELS 

from aiogram import Bot, Dispatcher, Router, F
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import CommandStart, Command
from aiogram.types import (
    Message,
    CallbackQuery,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)

from config import (
    BOT_TOKEN,
    REQUIRED_CHANNELS,
    REF_BONUS,
    MIN_WITHDRAW,
    USD_RATE,
    ADMINS,
    BOT_START_DATE,
    TASKS,
    PAYOUTS_CHANNEL_URL,
)
from db import (
    add_fake_refs,
    get_fake_refs,
    set_custom_stat,
    get_custom_stat,
    init_db,
    create_user,
    get_user,
    activate_user,
    get_balance,
    add_balance,
    get_last_bonus_at,
    set_last_bonus_at,
    is_banned,
    ban_user,
    unban_user,
    create_withdrawal,
    get_withdraw,
    set_withdraw_status,
    get_stats,
    list_all_users,
    get_top_referrers,
    create_task_submission,
    get_task_submission,
    set_task_status,
    get_last_task_submission,
    has_any_approved_task,
    list_new_withdrawals,
    get_language,
    set_language,
    list_users,          # 🔹 ДОБАВИЛ ЭТО
    count_users,
    list_users_page,
)

logging.basicConfig(level=logging.INFO)

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()
router = Router()
dp.include_router(router)

# Простые FSM-состояния (на словарях)
user_state: dict[int, str] = {}
pending_withdraw: dict[int, dict] = {}

task_state: dict[int, str] = {}
pending_task: dict[int, dict] = {}

DAILY_BONUS = 0.3
DAILY_HOURS = 24


# ============ ЯЗЫКИ (RU/UA) ============

BUTTONS = {
    "ru": {
        "subscribe": "📢 Подписка",
        "profile": "💼 Мой профиль",
        "invite": "👥 Пригласить друга",
        "daily": "🎁 Ежедневный бонус",
        "stats": "📊 Статистика",
        "withdraw": "💸 Вывод средств",
        "tasks": "📝 Задания",
        "top": "🏆 Топ рефералов",
        "rules": "📜 Правила",
        "payouts": "💸 Канал с выплатами",
    },
    "ua": {
        "subscribe": "📢 Підписка",
        "profile": "💼 Мій профіль",
        "invite": "👥 Запросити друга",
        "daily": "🎁 Щоденний бонус",
        "stats": "📊 Статистика",
        "withdraw": "💸 Виведення коштів",
        "tasks": "📝 Завдання",
        "top": "🏆 Топ рефералів",
        "rules": "📜 Правила",
        "payouts": "💸 Канал с выплатами",
    },
}

TEXTS = {
    "ru": {
        "choose_lang": "🌍 Выбери язык / Оберіть мову:",
        "not_sub": "❌ Ты не подписан на обязательные каналы.\nПодпишись и нажми «Проверить подписку».",
        "send_phone": "📱 Отправь корректный номер телефона.\nПоддерживаемые коды: +380, +7, +375.",
        "access_open": "🎉 <b>Доступ к боту открыт!</b>\nПользуйся меню ниже 👇",
        "banned": "🚫 Ты заблокирован в боте.",
        "phone_saved": "📱 Номер успешно сохранён!",
        "only_own_phone": "❌ Можно отправлять только <b>свой</b> номер!",
        "bad_phone": "❌ Некорректный номер.\nДозволені коди: +380, +7, +375.",
        "phone_used": "❌ Этот номер уже привязан к другому аккаунту.",
        "sub_menu": "📢 Подпишись на каналы и нажми «Проверить подписку» 👇",
    },
    "ua": {
        "choose_lang": "🌍 Обери мову / Choose language:",
        "not_sub": "❌ Ти не підписаний на обовʼязкові канали.\nПідпишись і натисни «Перевірити підписку».",
        "send_phone": "📱 Надішли коректний номер телефону.\nПідтримувані коди: +380, +7, +375.",
        "access_open": "🎉 <b>Доступ до бота відкрито!</b>\nКористуйся меню нижче 👇",
        "banned": "🚫 Тебе заблоковано в боті.",
        "phone_saved": "📱 Номер успішно збережено!",
        "only_own_phone": "❌ Можна надсилати тільки <b>свій</b> номер!",
        "bad_phone": "❌ Невідповідний номер.\nДозволені коди: +380, +7, +375.",
        "phone_used": "❌ Цей номер уже привʼязаний до іншого акаунта.",
        "sub_menu": "📢 Підпишись на канали та натисни «Перевірити підписку» 👇",
    },
}


def get_lang(user_id: int) -> str:
    lang = get_language(user_id)
    if lang not in ("ru", "ua", "unset"):
        return "unset"
    return lang


def tr(user_id: int, key: str) -> str:
    lang = get_lang(user_id)
    if lang == "unset":
        lang = "ru"
    return TEXTS.get(lang, TEXTS["ru"]).get(key, key)


def lang_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[[
            InlineKeyboardButton(text="🇷🇺 Русский", callback_data="lang:ru"),
            InlineKeyboardButton(text="🇺🇦 Українська", callback_data="lang:ua"),
        ]]
    )

# каналы без админки: не ломаем бота, но сообщаем админу один раз
notified_channels: set[str] = set()



# ============ ХЕЛПЕРЫ ============

def fmt_money(amount: float) -> str:
    return f"{amount:.2f} грн (~{amount / USD_RATE:.2f} $)"




def get_bot_days_running() -> int:
    try:
        start = datetime.strptime(BOT_START_DATE, "%d.%m.%Y")
        now = datetime.now(timezone.utc)
        return max((now - start).days, 0)
    except Exception:
        return 0


def _channel_to_url(ch: str) -> str:
    ch = ch.strip()
    if ch.startswith("http://") or ch.startswith("https://"):
        return ch
    ch = ch.lstrip("@")
    return f"https://t.me/{ch}"


def _normalize_channel_id(ch: str) -> str | None:
    ch = ch.strip()
    if ch.startswith("http://") or ch.startswith("https://"):
        parts = ch.split("/")
        last = parts[-1]
        if not last:
            return None
        if last.startswith("+"):
            # через прямой приватный инвайт проверить нельзя
            return None
        return "@" + last
    if ch.startswith("@"):
        return ch
    if ch:
        return "@" + ch
    return None


def get_task_by_id(task_id: str) -> dict | None:
    for t in TASKS:
        if t.get("id") == task_id:
            return t
    return None


def user_is_admin(tg_id: int) -> bool:
    """🔹 Своя проверка админа по списку ADMINS из config.py"""
    return tg_id in ADMINS


# ============ КЛАВИАТУРЫ ============

def main_keyboard(lang: str = 'ru') -> ReplyKeyboardMarkup:
    if lang not in ('ru','ua'):
        lang='ru'
    b = BUTTONS[lang]
    kb = [
        [KeyboardButton(text=b['profile'])],
        [KeyboardButton(text=b['invite'])],
        [KeyboardButton(text=b['daily']), KeyboardButton(text=b['stats'])],
        [KeyboardButton(text=b['withdraw'])],
        [KeyboardButton(text=b['tasks'])],
        [KeyboardButton(text=b['top']), KeyboardButton(text=b['rules'])],
        [KeyboardButton(text=b['payouts'])],
    ]
    return ReplyKeyboardMarkup(resize_keyboard=True, keyboard=kb)

def subscribe_keyboard() -> InlineKeyboardMarkup:
    buttons = []

    for idx, ch in enumerate(REQUIRED_CHANNELS, start=1):
        ch = ch.strip()
        if ch in PRIVATE_CHANNELS:
            url = PRIVATE_CHANNELS[ch]
        else:
            url = _channel_to_url(ch)

        buttons.append([InlineKeyboardButton(text=f"📢 Канал {idx}", url=url)])

    buttons.append([InlineKeyboardButton(text="🔄 Проверить подписку", callback_data="check_sub")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)





# ============ КАНАЛ С ВЫПЛАТАМИ ============

def payouts_inline_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💸 Перейти в канал выплат", url=PAYOUTS_CHANNEL_URL)]
        ]
    )


@router.message(F.text.in_([BUTTONS["ru"]["payouts"], BUTTONS["ua"]["payouts"]]))
async def payouts_channel_button(message: Message):
    if not await ensure_full_access(message):
        return

    await message.answer(
        "💸 Все выплаты публикуются в нашем канале 👇",
        reply_markup=payouts_inline_keyboard(),
        disable_web_page_preview=True,
    )
def withdraw_method_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💳 На карту", callback_data="wd_method:card")],
            [InlineKeyboardButton(text="💰 На криптобот", callback_data="wd_method:crypto")],
        ]
    )


def tasks_menu_keyboard() -> InlineKeyboardMarkup:
    buttons = []
    for t in TASKS:
        buttons.append(
            [InlineKeyboardButton(text=t["title"], callback_data=f"task:{t['id']}")]
        )
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def task_actions_keyboard(task_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="📸 Отправить скрин", callback_data=f"task_proof:{task_id}")],
            [InlineKeyboardButton(text="⬅️ Назад к заданиям", callback_data="tasks_back")],
        ]
    )


# ============ ПРОВЕРКИ ============

async def is_subscribed(user_id: int) -> bool:
    """Проверка подписки на все обязательные каналы (support username + ID)."""
    for raw in REQUIRED_CHANNELS:
        ch = raw.strip()

        # 1) Если это ID канала вида -100...
        if ch.startswith("-100"):
            try:
                chat_id = int(ch)
            except ValueError:
                logging.warning(f"Некорректный ID канала в REQUIRED_CHANNELS: {ch}")
                return False

        # 2) Если это ссылка https://t.me/....
        elif ch.startswith("http://") or ch.startswith("https://"):
            parts = ch.split("/")
            last = parts[-1]
            if not last or last.startswith("+"):
                logging.warning(f"Нельзя проверить подписку по инвайт-ссылке: {ch}")
                return False
            chat_id = "@" + last

        # 3) @username
        elif ch.startswith("@"):
            chat_id = ch

        # 4) просто username
        else:
            chat_id = "@" + ch

        try:
            member = await bot.get_chat_member(chat_id, user_id)
            if member.status not in ("member", "administrator", "creator"):
                return False
        except Exception as e:
            msg = str(e)
            logging.debug(f"Ошибка проверки подписки {user_id} на {chat_id}: {msg}")

            low = msg.lower()
            if ("forbidden" in low) or ("not a member" in low) or ("chat not found" in low) or ("member list is inaccessible" in low):
                key = str(chat_id)
                if key not in notified_channels:
                    notified_channels.add(key)
                    for adm in ADMINS:
                        try:
                            await bot.send_message(adm, f"⚠️ Канал {chat_id} не проверяется: боту не дали доступ (нужно добавить бота админом/право видеть участников).\nПока что канал временно пропускается в проверке.")
                        except Exception:
                            pass
                continue

            return False

    return True


async def ensure_full_access(message: Message) -> bool:
    """
    Общая проверка доступа:
    - не забанен
    - подписан на обязательные каналы

    Телефон здесь больше НЕ проверяем, 
    он нужен только при первом входе/активации.
    """
    user_id = message.from_user.id

    # Бан
    if is_banned(user_id):
        await message.answer(tr(user_id, "banned"))
        return False

    # Подписка
    if not await is_subscribed(user_id):
        await message.answer(
            tr(user_id, "not_sub"),
            reply_markup=subscribe_keyboard(),
        )
        return False

    return True




async def try_qualify_referral(user_id: int):
    """Засчитываем реферала ТОЛЬКО если он:
    1) забрал бонус (есть last_bonus_at)
    2) выполнил хотя бы 1 задание (есть approved task_submissions)

    Порядок не важен: функцию вызываем и после бонуса, и после approve задания.
    """
    try:
        u = get_user(user_id)
    except Exception:
        return

    if not u:
        return

    # get_user: (tg_id, balance, referrer_id, activated, phone, created_at, last_bonus_at, banned)
    referrer_id = u[2]
    activated = int(u[3] or 0)

    # Уже засчитан
    if activated == 1:
        return

    # Нет реферера
    if not referrer_id:
        return

    # 1) бонус должен быть забран
    if not get_last_bonus_at(user_id):
        return

    # 2) хотя бы 1 одобренное задание
    if not has_any_approved_task(user_id):
        return

    # Засчитываем реферала: отмечаем activated=1 и начисляем бонус рефереру (один раз)
    # activate_user вернет referrer_id только при первом засчёте.
    try:
        ref = activate_user(user_id)
    except Exception:
        return

    if not ref:
        return

    try:
        add_balance(ref, REF_BONUS)
    except Exception:
        return

    # Уведомление рефереру (не критично)
    try:
        await bot.send_message(
            ref,
            f"✅ У тебя новый активный реферал: <code>{user_id}</code>\n"
            f"Начислено: <b>{fmt_money(REF_BONUS)}</b>."
        )
    except Exception:
        pass




async def try_activate_and_open_menu(user_id: int, chat_id: int):
    if is_banned(user_id):
        await bot.send_message(chat_id, tr(user_id, "banned"))
        return

    if not await is_subscribed(user_id):
        await bot.send_message(
            chat_id,
            tr(user_id, "not_sub"),
            reply_markup=subscribe_keyboard(),
        )
        return

    # ⚠️ Реферал засчитывается НЕ при входе, а только после: бонус + 1 задание.

    lang = get_lang(user_id)


    if lang == "unset":


        await bot.send_message(


            chat_id,


            tr(user_id, "choose_lang"),


            reply_markup=lang_keyboard(),


        )


        return



    await bot.send_message(


        chat_id,


        tr(user_id, "access_open"),


        reply_markup=main_keyboard(lang),


    )


# ============ /start, подписка, телефон ============

@router.message(CommandStart())
async def cmd_start(message: Message):
    user_id = message.from_user.id
    text_parts = (message.text or "").split()

    if is_banned(user_id):
        await message.answer(tr(user_id, "banned"))
        return

    ref_id = None
    if len(text_parts) > 1:
        try:
            r = int(text_parts[1])
            if r != user_id:
                ref_id = r
        except Exception:
            pass

    create_user(user_id, ref_id)

    # ВСЕГДА показываем спонсоров при входе
    await message.answer(
        tr(user_id, "sub_menu"),
        reply_markup=subscribe_keyboard(),
    )



@router.callback_query(F.data == "check_sub")
async def check_sub_handler(call: CallbackQuery):
    await try_activate_and_open_menu(call.from_user.id, call.message.chat.id)
    await call.answer()

# ============ ВЫБОР ЯЗЫКА ============

@router.callback_query(F.data.startswith('lang:'))
async def set_lang_handler(call: CallbackQuery):
    user_id = call.from_user.id
    lang = call.data.split(':', 1)[1]
    if lang not in ('ru','ua'):
        lang = 'ru'
    set_language(user_id, lang)
    await call.message.answer(tr(user_id, 'access_open'), reply_markup=main_keyboard(lang))
    await call.answer()

# ============ ПРОФИЛЬ, РЕФЫ, БОНУС, СТАТИСТИКА, ПРАВИЛА, ТОП ============

@router.message(F.text.in_([BUTTONS["ru"]["profile"], BUTTONS["ua"]["profile"]]))
async def my_profile(message: Message):
    if not await ensure_full_access(message):
        return

    user_id = message.from_user.id
    bal = get_balance(user_id)
    me = await bot.get_me()
    ref_link = f"https://t.me/{me.username}?start={user_id}"

    text = (
        "👤 <b>Твой профиль</b>\n\n"
        f"💰 Баланс: <b>{fmt_money(bal)}</b>\n"
                f"👥 Реф. ссылка:\n<code>{ref_link}</code>\n\n"
        f"За каждого друга, который заберёт бонус и выполнит хотя бы 1 задание — "
        f"ты получаешь <b>{fmt_money(REF_BONUS)}</b>."
    )
    await message.answer(text)


@router.message(F.text.in_([BUTTONS["ru"]["invite"], BUTTONS["ua"]["invite"]]))
async def invite_friend(message: Message):
    if not await ensure_full_access(message):
        return

    user_id = message.from_user.id
    me = await bot.get_me()
    ref_link = f"https://t.me/{me.username}?start={user_id}"

    await message.answer(
        "Отправь эту ссылку друзьям:\n"
        f"<code>{ref_link}</code>\n\n"
        f"За каждого друга, который заберёт бонус и выполнит хотя бы 1 задание, ты получишь <b>{fmt_money(REF_BONUS)}</b>.",
    )


@router.message(F.text.in_([BUTTONS["ru"]["daily"], BUTTONS["ua"]["daily"]]))
async def daily_bonus(message: Message):
    if not await ensure_full_access(message):
        return

    user_id = message.from_user.id
    now = datetime.now(timezone.utc)
    last = get_last_bonus_at(user_id)

    if last:
        try:
            last_dt = datetime.fromisoformat(last)
            delta = now - last_dt
            if delta.total_seconds() < DAILY_HOURS * 3600:
                remain = DAILY_HOURS * 3600 - delta.total_seconds()
                h = int(remain // 3600)
                m = int((remain % 3600) // 60)
                await message.answer(
                    f"⏳ Бонус уже забран.\n"
                    f"Следующий будет доступен через <b>{h} ч {m} мин</b>."
                )
                return
        except Exception:
            pass

    add_balance(user_id, DAILY_BONUS)
    set_last_bonus_at(user_id, now.isoformat())
    await try_qualify_referral(user_id)
    bal = get_balance(user_id)

    await message.answer(
        f"🎁 Ты получил бонус <b>{fmt_money(DAILY_BONUS)}</b>!\n"
        f"Текущий баланс: <b>{fmt_money(bal)}</b>."
    )


@router.message(F.text.in_([BUTTONS["ru"]["stats"], BUTTONS["ua"]["stats"]]))
async def stats_public(message: Message):
    s = get_stats()
    days = get_bot_days_running()

    text = (
        "📊 <b>Статистика бота</b>\n\n"
        f"👥 Всего пользователей: <b>{s['total_users']}</b>\n"
        f"🔥 Активированных: <b>{s['activated_users']}</b>\n"
        f"🆕 Новых за 24 часа: <b>{s['new_24h']}</b>\n"
        f"📅 Бот работает: <b>{days} дн.</b> (с {BOT_START_DATE})"
    )
    await message.answer(text)


@router.message(F.text.in_([BUTTONS["ru"]["rules"], BUTTONS["ua"]["rules"]]))
async def rules(message: Message):
    if not await ensure_full_access(message):
        return

    text = (
        "📜 <b>Правила бота</b>\n\n"
        "❗ Запрещено:\n"
        "— Создавать много аккаунтов (мультиаккаунты)\n"
        "— Использовать фейки и виртуалки\n"
        "— Отправлять поддельные скрины\n"
        "— Абузить задания и реферальную систему\n"
        "— Отписываться от спонсорских каналов после выплат\n\n"
        "Админ может отклонить выплату или заблокировать аккаунт без объяснения причин.\n\n"
        "Используя бот, ты автоматически соглашаешься с этими правилами ✅"
    )
    await message.answer(text)


@router.message(F.text.in_([BUTTONS["ru"]["top"], BUTTONS["ua"]["top"]]))
async def top_referrals(message: Message):
    if not await ensure_full_access(message):
        return

    real = get_top_referrers(limit=10)
    fake = get_fake_refs()

    top_dict = {}

    for ref,cnt in real:
        top_dict[ref] = top_dict.get(ref,0) + cnt

    for ref,cnt in fake:
        top_dict[ref] = top_dict.get(ref,0) + cnt

    top = sorted(top_dict.items(), key=lambda x: x[1], reverse=True)[:10]
    if not top:
        await message.answer("Пока нет активных рефералов.")
        return

    lines = ["🏆 <b>Топ рефералов</b>\n"]
    for i, (ref_id, cnt) in enumerate(top, start=1):
        earned = cnt * REF_BONUS
        name = f"<code>{ref_id}</code>"
        try:
            chat = await bot.get_chat(ref_id)
            if chat.username:
                name = f"@{chat.username}"
        except Exception:
            pass
        lines.append(f"{i}. {name} — {cnt} реф. — заработал <b>{fmt_money(earned)}</b>")

    await message.answer("\n".join(lines))


# ============ ЗАДАНИЯ ============

@router.message(F.text.in_([BUTTONS["ru"]["tasks"], BUTTONS["ua"]["tasks"]]))
async def tasks_menu_handler(message: Message):
    if not await ensure_full_access(message):
        return

    if not TASKS:
        await message.answer("Пока нет доступных заданий.")
        return

    text = "📝 <b>Доступные задания</b>:\n\n"
    for t in TASKS:
        text += f"• {t['title']} — <b>{fmt_money(t['price'])}</b>\n"

    text += (
        "\nЕсли хотите добавить СВОЁ задание в бот — пишите сюда: @Bassss6\n\n"
        "Выбери задание из списка ниже 👇"
    )

    await message.answer(text, reply_markup=tasks_menu_keyboard())


@router.callback_query(F.data == "tasks_back")
async def tasks_back(call: CallbackQuery):
    await tasks_menu_handler(call.message)
    await call.answer()


@router.callback_query(F.data.startswith("task:"))
async def open_task(call: CallbackQuery):
    task_id = call.data.split(":", 1)[1]
    t = get_task_by_id(task_id)
    if not t:
        await call.answer("Задание не найдено", show_alert=True)
        return

    last = get_last_task_submission(call.from_user.id, task_id)
    if last and last[1] in ("pending", "approved"):
        await call.message.answer("❌ Ты уже выполнял это задание или оно на проверке.")
        await call.answer()
        return

    text = (
        f"🔸 <b>{t['title']}</b>\n\n"
        f"Награда: <b>{fmt_money(t['price'])}</b>\n\n"
        f"{t['instructions']}"
    )
    await call.message.answer(text, reply_markup=task_actions_keyboard(task_id))
    await call.answer()


@router.callback_query(F.data.startswith("task_proof:"))
async def task_proof_start(call: CallbackQuery):
    task_id = call.data.split(":", 1)[1]
    t = get_task_by_id(task_id)
    if not t:
        await call.answer("Задание не найдено", show_alert=True)
        return

    user_id = call.from_user.id
    last = get_last_task_submission(user_id, task_id)
    if last and last[1] in ("pending", "approved"):
        await call.message.answer("❌ Ты уже выполнял это задание или оно на проверке.")
        await call.answer()
        return

    task_state[user_id] = "waiting_proof"
    pending_task[user_id] = {"task_id": task_id}

    await call.message.answer("📸 Отправь скрин выполнения задания одним фото.")
    await call.answer()


@router.message(F.photo)
async def handle_task_photo(message: Message):
    user_id = message.from_user.id
    if task_state.get(user_id) != "waiting_proof":
        return

    if not await ensure_full_access(message):
        task_state.pop(user_id, None)
        pending_task.pop(user_id, None)
        return

    data = pending_task.get(user_id)
    if not data or "task_id" not in data:
        await message.answer("Ошибка состояния. Попробуй открыть задание заново.")
        task_state.pop(user_id, None)
        pending_task.pop(user_id, None)
        return

    task_id = data["task_id"]
    t = get_task_by_id(task_id)
    if not t:
        await message.answer("Задание не найдено. Попробуй позже.")
        task_state.pop(user_id, None)
        pending_task.pop(user_id, None)
        return

    file_id = message.photo[-1].file_id
    caption = message.caption or ""

    sub_id = create_task_submission(user_id, task_id, file_id, caption)

    await message.answer(
        "✅ Скрин отправлен на проверку.\n"
        "После проверки админом ты получишь уведомление."
    )

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✔️ Принять", callback_data=f"task_ok:{sub_id}"),
                InlineKeyboardButton(text="❌ Отклонить", callback_data=f"task_no:{sub_id}"),
            ]
        ]
    )

    for adm in ADMINS:
        try:
            await bot.send_photo(
                adm,
                photo=file_id,
                caption=(
                    f"📝 <b>Новая заявка по заданию</b>\n"
                    f"ID заявки: <code>{sub_id}</code>\n"
                    f"Задание: <b>{t['title']}</b>\n"
                    f"Пользователь: <code>{user_id}</code>\n\n"
                    f"Комментарий юзера:\n{caption or '—'}"
                ),
                reply_markup=kb,
            )
        except Exception:
            pass

    task_state.pop(user_id, None)
    pending_task.pop(user_id, None)


@router.callback_query(F.data.startswith("task_ok:"))
async def task_ok(call: CallbackQuery):
    if call.from_user.id not in ADMINS:
        await call.answer("Не админ", show_alert=True)
        return

    sub_id = int(call.data.split(":", 1)[1])
    sub = get_task_submission(sub_id)
    if not sub:
        await call.answer("Заявка не найдена", show_alert=True)
        return

    tg_id = sub[1]
    task_id = sub[2]
    status = sub[3]

    if status == "approved":
        await call.answer("Уже одобрено", show_alert=True)
        return
    if status == "rejected":
        await call.answer("Уже отклонено", show_alert=True)
        return

    t = get_task_by_id(task_id)
    if not t:
        await call.answer("Задание не найдено", show_alert=True)
        return

    set_task_status(sub_id, "approved")
    add_balance(tg_id, t["price"])

    # Проверяем, не стал ли реферал "активным" (бонус + 1 задание)
    await try_qualify_referral(tg_id)

    try:
        await call.message.edit_caption(
            (call.message.caption or "") + "\n\n✔️ <b>Одобрено админом</b>"
        )
    except Exception:
        try:
            await call.message.edit_text(
                (call.message.text or "") + "\n\n✔️ <b>Одобрено админом</b>"
            )
        except Exception:
            pass

    await call.answer("Принято")

    try:
        await bot.send_message(
            tg_id,
            f"🎉 Задание <b>{t['title']}</b> одобрено!\n"
            f"Тебе начислено: <b>{fmt_money(t['price'])}</b>."
        )
    except Exception:
        pass


@router.callback_query(F.data.startswith("task_no:"))
async def task_no(call: CallbackQuery):
    if call.from_user.id not in ADMINS:
        await call.answer("Не админ", show_alert=True)
        return

    sub_id = int(call.data.split(":", 1)[1])
    sub = get_task_submission(sub_id)
    if not sub:
        await call.answer("Заявка не найдена", show_alert=True)
        return

    tg_id = sub[1]
    status = sub[3]

    if status == "approved":
        await call.answer("Уже одобрено", show_alert=True)
        return
    if status == "rejected":
        await call.answer("Уже отклонено", show_alert=True)
        return

    set_task_status(sub_id, "rejected")

    try:
        await call.message.edit_caption(
            (call.message.caption or "") + "\n\n❌ <b>Отклонено админом</b>"
        )
    except Exception:
        try:
            await call.message.edit_text(
                (call.message.text or "") + "\n\n❌ <b>Отклонено админом</b>"
            )
        except Exception:
            pass

    await call.answer("Отклонено")

    try:
        await bot.send_message(
            tg_id,
            "❌ Твоя заявка по заданию была отклонена админом."
        )
    except Exception:
        pass


# ============ ВЫВОД СРЕДСТВ ============

@router.message(F.text.in_([BUTTONS["ru"]["withdraw"], BUTTONS["ua"]["withdraw"]]))
async def start_withdraw(message: Message):
    if not await ensure_full_access(message):
        return

    user_id = message.from_user.id
    bal = get_balance(user_id)

    if bal < MIN_WITHDRAW:
        await message.answer(
            f"Минимальная сумма для вывода — <b>{fmt_money(MIN_WITHDRAW)}</b>.\n"
            f"Твой баланс: <b>{fmt_money(bal)}</b>."
        )
        return

    await message.answer(
        f"На балансе: <b>{fmt_money(bal)}</b>\n"
        "Выбери способ вывода 👇",
        reply_markup=withdraw_method_keyboard(),
    )


@router.callback_query(F.data.startswith("wd_method:"))
async def choose_withdraw_method(call: CallbackQuery):
    user_id = call.from_user.id

    if is_banned(user_id):
        await call.message.answer("🚫 Ты заблокирован в боте.")
        await call.answer()
        return

    if not await is_subscribed(user_id):
        await call.message.answer(
            "❌ Ты не подписан на обязательные каналы.",
            reply_markup=subscribe_keyboard(),
        )
        await call.answer()
        return

    method = call.data.split(":", 1)[1]
    bal = get_balance(user_id)

    if bal < MIN_WITHDRAW:
        await call.message.answer(
            f"Минимальная сумма для вывода — <b>{fmt_money(MIN_WITHDRAW)}</b>.\n"
            f"Твой баланс: <b>{fmt_money(bal)}</b>."
        )
        await call.answer()
        return

    pending_withdraw[user_id] = {"method": method}
    user_state[user_id] = "waiting_amount"

    await call.message.answer(
        f"Баланс: <b>{fmt_money(bal)}</b>\n"
        f"Введи сумму для вывода (от {fmt_money(MIN_WITHDRAW)}):"
    )
    await call.answer()


@router.message(lambda m: user_state.get(m.from_user.id) is not None)
async def withdraw_states(message: Message):
    user_id = message.from_user.id
    state = user_state.get(user_id)
    text = (message.text or "").strip()

    if not await ensure_full_access(message):
        user_state.pop(user_id, None)
        pending_withdraw.pop(user_id, None)
        return

    if state == "waiting_amount":
        try:
            amount = float(text.replace(",", "."))
        except ValueError:
            await message.answer("❌ Введи сумму числом, например 50 или 75.5")
            return

        bal = get_balance(user_id)
        if amount < MIN_WITHDRAW:
            await message.answer(
                f"Минимальная сумма для вывода — <b>{fmt_money(MIN_WITHDRAW)}</b>."
            )
            return
        if amount > bal:
            await message.answer(
                f"❌ Недостаточно средств.\nТвой баланс: <b>{fmt_money(bal)}</b>."
            )
            return

        pending_withdraw.setdefault(user_id, {})
        pending_withdraw[user_id]["amount"] = amount

        method = pending_withdraw[user_id].get("method")
        if method == "card":
            user_state[user_id] = "waiting_card"
            await message.answer("Введи номер карты (16 цифр, можно с пробелами):")
        elif method == "crypto":
            user_state[user_id] = "waiting_crypto"
            await message.answer("Введи данные для вывода на криптобот:")
        else:
            await message.answer("Ошибка состояния. Попробуй начать вывод заново.")
            user_state.pop(user_id, None)
            pending_withdraw.pop(user_id, None)

        return

    if state == "waiting_card":
        card_raw = text.replace(" ", "")
        if not card_raw.isdigit() or len(card_raw) != 16:
            await message.answer("❌ Номер карты должен содержать 16 цифр.")
            return

        data = pending_withdraw.get(user_id)
        if not data or "amount" not in data:
            await message.answer("Ошибка состояния. Попробуй снова начать вывод.")
            user_state.pop(user_id, None)
            pending_withdraw.pop(user_id, None)
            return

        amount = data["amount"]
        add_balance(user_id, -amount)

        wd_id = create_withdrawal(user_id, "card", card_raw, amount)

        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="✔️ Одобрить", callback_data=f"wd_ok:{wd_id}"),
                    InlineKeyboardButton(text="❌ Отклонить", callback_data=f"wd_no:{wd_id}"),
                ]
            ]
        )

        await message.answer(
            f"🔄 Заявка на вывод <b>{fmt_money(amount)}</b> отправлена админу!\n"
            f"ID: <code>{wd_id}</code>"
        )

        for adm in ADMINS:
            try:
                await bot.send_message(
                    adm,
                    f"💸 <b>Новая заявка на вывод</b>\n"
                    f"ID: {wd_id}\n"
                    f"Пользователь: <code>{user_id}</code>\n"
                    f"Метод: карта\n"
                    f"Карта: <code>{card_raw}</code>\n"
                    f"Сумма: <b>{fmt_money(amount)}</b>",
                    reply_markup=kb,
                )
            except Exception:
                pass

        user_state.pop(user_id, None)
        pending_withdraw.pop(user_id, None)
        return

    if state == "waiting_crypto":
        details = text.strip()
        if len(details) < 5:
            await message.answer("❌ Введи корректные данные для криптобота.")
            return

        data = pending_withdraw.get(user_id)
        if not data or "amount" not in data:
            await message.answer("Ошибка состояния. Попробуй снова начать вывод.")
            user_state.pop(user_id, None)
            pending_withdraw.pop(user_id, None)
            return

        amount = data["amount"]
        add_balance(user_id, -amount)

        wd_id = create_withdrawal(user_id, "crypto", details, amount)

        kb = InlineKeyboardMarkup(
            inline_keyboard=[
                [
                    InlineKeyboardButton(text="✔️ Одобрить", callback_data=f"wd_ok:{wd_id}"),
                    InlineKeyboardButton(text="❌ Отклонить", callback_data=f"wd_no:{wd_id}"),
                ]
            ]
        )

        await message.answer(
            f"🔄 Заявка на вывод <b>{fmt_money(amount)}</b> отправлена админу!\n"
            f"ID: <code>{wd_id}</code>"
        )

        for adm in ADMINS:
            try:
                await bot.send_message(
                    adm,
                    f"💸 <b>Новая заявка на вывод</b>\n"
                    f"ID: {wd_id}\n"
                    f"Пользователь: <code>{user_id}</code>\n"
                    f"Метод: криптобот\n"
                    f"Реквизиты: <code>{details}</code>\n"
                    f"Сумма: <b>{fmt_money(amount)}</b>",
                    reply_markup=kb,
                )
            except Exception:
                pass

        user_state.pop(user_id, None)
        pending_withdraw.pop(user_id, None)
        return


@router.callback_query(F.data.startswith("wd_ok:"))
async def wd_ok(call: CallbackQuery):
    if call.from_user.id not in ADMINS:
        await call.answer("Не админ", show_alert=True)
        return

    wd_id = int(call.data.split(":", 1)[1])
    wd = get_withdraw(wd_id)
    if not wd:
        await call.answer("❌ Заявка не найдена", show_alert=True)
        return

    set_withdraw_status(wd_id, "approved")

    tg_id = wd[1]
    amount = wd[4]

    await call.answer("✔️ Выплата одобрена")
    try:
        await call.message.edit_text(f"✔️ Выплата подтверждена (ID {wd_id})")
    except Exception:
        pass

    try:
        await bot.send_message(
            tg_id,
            f"🎉 Твоя выплата <b>{fmt_money(amount)}</b> одобрена и скоро будет отправлена!"
        )
    except Exception:
        pass


@router.callback_query(F.data.startswith("wd_no:"))
async def wd_no(call: CallbackQuery):
    if call.from_user.id not in ADMINS:
        await call.answer("Не админ", show_alert=True)
        return

    wd_id = int(call.data.split(":", 1)[1])
    wd = get_withdraw(wd_id)
    if not wd:
        await call.answer("❌ Заявка не найдена", show_alert=True)
        return

    tg_id = wd[1]
    amount = wd[4]

    set_withdraw_status(wd_id, "rejected")

    await call.answer("❌ Выплата отклонена")
    try:
        await call.message.edit_text(f"❌ Выплата отклонена (ID {wd_id})")
    except Exception:
        pass

    try:
        await bot.send_message(
            tg_id,
            "❌ Твоя заявка на вывод была отклонена администрацией.\n"
            "<i>Средства не возвращаются.</i>"
        )
    except Exception:
        pass


# ============ АДМИН-КОМАНДЫ ============

@router.message(Command("admin"))
async def admin_panel(message: Message):
    """Главное админ-меню /admin"""
    if not user_is_admin(message.from_user.id):
        return

    s = get_stats()
    days = (datetime.now(timezone.utc).date() - datetime.strptime(BOT_START_DATE, "%d.%m.%Y").date()).days

    text = (
        "<b>Админ-панель</b>\n\n"
        f"👥 Всего пользователей: <b>{s['total_users']}</b>\n"
        f"✅ Активировано: <b>{s['activated_users']}</b>\n"
        f"📱 С привязанным телефоном: <b>{s['with_phone']}</b>\n"
        f"⛔ Забанено: <b>{s['banned_users']}</b>\n"
        f"🆕 Новых за 24 часа: <b>{s['new_24h']}</b>\n"
        f"📅 Бот работает: <b>{days} дн.</b> (с {BOT_START_DATE})\n\n"
        "Команды:\n"
        "/users — список пользователей\n"
        "/ban id — бан\n"
        "/unban id — разбан\n"
        "/addbal id сумма — добавить баланс\n"
        "/subbal id сумма — снять баланс\n"
        "/msg id текст — написать пользователю\n"
        "/all текст — рассылка всем\n"
        "/pending — новые заявки на вывод\n"
    )
    await message.answer(text)


@router.message(Command("users"))
async def admin_users(message: Message):
    """Постраничный список пользователей (по 50)"""
    if not user_is_admin(message.from_user.id):
        return

    page = 0
    text, kb = _format_users_page(page)
    await message.answer(text, reply_markup=kb)


USERS_PER_PAGE = 50


def _users_keyboard(page: int, total: int) -> InlineKeyboardMarkup:
    max_page = max(0, (total - 1) // USERS_PER_PAGE)

    row = []
    if page > 0:
        row.append(InlineKeyboardButton(text="⬅️ Назад", callback_data=f"users_page:{page-1}"))
    row.append(InlineKeyboardButton(text=f"{page+1}/{max_page+1}", callback_data="users_page:noop"))
    if page < max_page:
        row.append(InlineKeyboardButton(text="Вперёд ➡️", callback_data=f"users_page:{page+1}"))

    return InlineKeyboardMarkup(inline_keyboard=[row])


def _format_users_page(page: int):
    total = count_users()
    max_page = max(0, (total - 1) // USERS_PER_PAGE)
    page = max(0, min(page, max_page))

    offset = page * USERS_PER_PAGE
    rows = list_users_page(offset=offset, limit=USERS_PER_PAGE)

    text = f"👥 <b>Пользователи:</b> {total}\n📄 <b>Страница:</b> {page+1}/{max_page+1}\n\n"
    if not rows:
        text += "Пользователей пока нет."
        return text, _users_keyboard(page, total)

    for tg_id, balance, activated, banned, created_at in rows:
        a = "✅" if int(activated) == 1 else "❌"
        b = "🚫" if int(banned) == 1 else "—"
        text += f"ID: <code>{tg_id}</code> | 💰 {float(balance):.2f} | A:{a} | Ban:{b}\n"

    return text, _users_keyboard(page, total)


@router.callback_query(F.data.startswith("users_page:"))
async def cb_users_page(call: CallbackQuery):
    if not user_is_admin(call.from_user.id):
        await call.answer("Нет доступа", show_alert=True)
        return

    _, value = call.data.split(":", 1)
    if value == "noop":
        await call.answer()
        return

    try:
        page = int(value)
    except ValueError:
        await call.answer()
        return

    text, kb = _format_users_page(page)
    try:
        await call.message.edit_text(text, reply_markup=kb)
    except Exception:
        await call.message.answer(text, reply_markup=kb)

    await call.answer()



@router.message(Command("ban"))
async def admin_ban(message: Message):
    """Бан пользователя: /ban 123456789"""
    if not user_is_admin(message.from_user.id):
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: <code>/ban 123456789</code>")
        return

    try:
        tg_id = int(parts[1])
    except ValueError:
        await message.answer("ID должен быть числом.")
        return

    ban_user(tg_id)
    await message.answer(f"🚫 Пользователь <code>{tg_id}</code> забанен.")


@router.message(Command("unban"))
async def admin_unban(message: Message):
    """Разбан пользователя: /unban 123456789"""
    if not user_is_admin(message.from_user.id):
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: <code>/unban 123456789</code>")
        return

    try:
        tg_id = int(parts[1])
    except ValueError:
        await message.answer("ID должен быть числом.")
        return

    unban_user(tg_id)
    await message.answer(f"✅ Пользователь <code>{tg_id}</code> разбанен.")


@router.message(Command("addbal"))
async def admin_addbal(message: Message):
    """
    /addbal <tg_id> <сумма>
    Пример: /addbal 1428837532 10
    """
    if not user_is_admin(message.from_user.id):
        return

    parts = message.text.split(maxsplit=3)
    if len(parts) < 3:
        await message.answer("Использование: <code>/addbal 123456789 5</code>")
        return

    try:
        tg_id = int(parts[1])
        amount = float(parts[2].replace(",", "."))
    except ValueError:
        await message.answer("ID и сумма должны быть числами.")
        return

    add_balance(tg_id, amount)
    await message.answer(
        f"✅ Баланс пользователя <code>{tg_id}</code> увеличен на <b>{amount:.2f} грн</b>."
    )
    try:
        await bot.send_message(
            tg_id,
            f"💰 Тебе начислено администратором: <b>{amount:.2f} грн</b>."
        )
    except Exception:
        pass


@router.message(Command("subbal"))
async def admin_subbal(message: Message):
    """
    /subbal <tg_id> <сумма>
    Пример: /subbal 1428837532 5
    """
    if not user_is_admin(message.from_user.id):
        return

    parts = message.text.split(maxsplit=3)
    if len(parts) < 3:
        await message.answer("Использование: <code>/subbal 123456789 5</code>")
        return

    try:
        tg_id = int(parts[1])
        amount = float(parts[2].replace(",", "."))
    except ValueError:
        await message.answer("ID и сумма должны быть числами.")
        return

    add_balance(tg_id, -amount)
    await message.answer(
        f"✅ С баланса пользователя <code>{tg_id}</code> снято <b>{amount:.2f} грн</b>."
    )
    try:
        await bot.send_message(
            tg_id,
            f"💸 С твоего баланса администратором снято: <b>{amount:.2f} грн</b>."
        )
    except Exception:
        pass


@router.message(Command("msg"))
async def admin_msg(message: Message):
    """
    /msg <tg_id> <текст>
    """
    if not user_is_admin(message.from_user.id):
        return

    parts = message.text.split(maxsplit=2)
    if len(parts) < 3:
        await message.answer("Использование: <code>/msg 123456789 Текст</code>")
        return

    try:
        tg_id = int(parts[1])
    except ValueError:
        await message.answer("ID должен быть числом.")
        return

    text_to_send = parts[2]

    try:
        await bot.send_message(tg_id, text_to_send)
        await message.answer("✅ Сообщение отправлено.")
    except Exception:
        await message.answer("❌ Не удалось отправить сообщение этому пользователю.")


@router.message(Command("all"))
async def admin_all(message: Message):
    """
    /all <текст> — отправить всем пользователям
    """
    if not user_is_admin(message.from_user.id):
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Использование: <code>/all Текст рассылки</code>")
        return

    text_to_send = parts[1]
    users = list_users(limit=1000000)

    sent = 0
    for u in users:
        tg_id = u[0]
        try:
            await bot.send_message(tg_id, text_to_send)
            sent += 1
        except Exception:
            pass

    await message.answer(f"📢 Рассылка отправлена <b>{sent}</b> пользователям.")


@router.message(Command("pending"))
async def admin_pending(message: Message):
    """
    /pending — показать новые (не обработанные) заявки на вывод
    """
    if not user_is_admin(message.from_user.id):
        return

    wds = list_new_withdrawals(limit=30)
    if not wds:
        await message.answer("🧾 Новых заявок на вывод нет.")
        return

    lines = ["🧾 <b>Новые заявки на вывод:</b>"]
    for wd in wds:
        wd_id, tg_id, method, details, amount, status, created_at = wd
        lines.append(
            f"\nID: <code>{wd_id}</code>\n"
            f"👤 Пользователь: <code>{tg_id}</code>\n"
            f"💰 Сумма: <b>{amount:.2f} грн</b>\n"
            f"📦 Метод: <b>{method}</b>\n"
            f"📄 Детали: {details}\n"
            f"⏰ Создано: {created_at}\n"
        )

    lines.append("\nℹ️ Обрабатывай заявки через кнопки под сообщениями бота с заявками.")
    await message.answer("\n".join(lines))



# ============ СТАРТ БОТА ============

async def main():
    init_db()
    print("BOT STARTED")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())


# ===== НАКРУТКА РЕФЕРАЛОВ =====

@router.message(Command("addref"))
async def admin_addref(message: Message):

    if not user_is_admin(message.from_user.id):
        return

    parts = message.text.split()

    if len(parts) != 3:
        await message.answer("Использование: /addref id количество")
        return

    tg_id = int(parts[1])
    amount = int(parts[2])

    add_fake_refs(tg_id, amount)

    await message.answer(f"Добавлено {amount} рефералов пользователю {tg_id}")


# ===== ИЗМЕНЕНИЕ СТАТИСТИКИ =====

@router.message(Command("setusers"))
async def admin_set_users(message: Message):

    if not user_is_admin(message.from_user.id):
        return

    parts = message.text.split()

    if len(parts) != 2:
        await message.answer("Использование: /setusers число")
        return

    value = int(parts[1])

    set_custom_stat("users", value)

    await message.answer(f"Статистика пользователей установлена: {value}")
