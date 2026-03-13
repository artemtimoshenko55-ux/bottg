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
    PAYOUTS_CHANNEL_URL,
)
from db import (
    init_db,
    create_user,
    get_user,
    activate_user,
    get_balance,
    add_balance,    is_banned,
    ban_user,
    unban_user,
    create_withdrawal,
    get_withdraw,
    set_withdraw_status,
    get_stats,
    list_all_users,
    get_top_referrers,    list_new_withdrawals,
    get_language,
    set_language,
    list_users,          # 🔹 ДОБАВИЛ ЭТО
    count_users,
    get_active_ref_count,
    get_ref_withdraw_count,
    increment_ref_withdraw_count,
    set_manual_refs,
    add_manual_refs,
    get_fake_total,
    set_fake_total,
    list_users_page,
)

logging.basicConfig(level=logging.INFO)

# ===== GLOBAL ANTI-SPAM =====
_last_action = {}
_COOLDOWN = 2

def anti_spam(user_id: int):
    from datetime import datetime
    now = datetime.now().timestamp()
    if user_id in _last_action:
        if now - _last_action[user_id] < _COOLDOWN:
            return False
    _last_action[user_id] = now
    return True


bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode="HTML"))
dp = Dispatcher()
router = Router()
dp.include_router(router)

# Простые FSM-состояния (на словарях)
user_state: dict[int, str] = {}
pending_withdraw: dict[int, dict] = {}




# ============ ЯЗЫКИ (RU/UA) ============

BUTTONS = {
    "ru": {
        # RU forcibly mapped to UA

        "subscribe": "📢 Подписка",
        "profile": "💼 Мой профиль",
        "invite": "👥 Пригласить друга",        "stats": "📊 Статистика",        "top": "🏆 Топ рефералов",
        "ref50": "💸 50 грн",
    },
    "ua": {
        "subscribe": "📢 Підписка",
        "profile": "💼 Мій профіль",
        "invite": "👥 Запросити друга",        "stats": "📊 Статистика",        "top": "🏆 Топ рефералів",
        "ref50": "💸 50 грн",
    },
}

TEXTS = {
    "ru": {
        # RU forcibly mapped to UA

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
    return 'ua'

# ORIGINAL DISABLED
#
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
    return f"{amount:.2f} грн"




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

def main_keyboard(lang: str = "ru") -> ReplyKeyboardMarkup:
    if lang not in ("ru","ua"):
        lang="ru"
    b = BUTTONS[lang]
    kb = [
        [KeyboardButton(text=b["profile"])],
        [KeyboardButton(text="🏦 Кабінет")],
        [KeyboardButton(text=b["invite"])],
        [KeyboardButton(text=b["stats"])],
        [KeyboardButton(text=b["ref50"])],
        [KeyboardButton(text=b["top"])],
    ]
    return ReplyKeyboardMarkup(keyboard=kb, resize_keyboard=True)

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

def withdraw_method_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="💳 На карту", callback_data="wd_method:card")],
            [InlineKeyboardButton(text="💰 На криптобот", callback_data="wd_method:crypto")],
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


# ============ АДМИН-КОМАНДЫ ============

@router.message(Command("admin"))
async def admin_panel(message: Message):
    if not user_is_admin(message.from_user.id):
        return

    s = get_stats()
    days = get_bot_days_running()

    fake = get_fake_total()
    total = fake if fake > 0 else s["total_users"]

    text = (
        "<b>Админ-панель</b>\n\n"
        f"👥 Користувачів: <b>{total}</b>\n"
        f"📱 С телефоном: <b>{s['with_phone']}</b>\n"
        f"⛔ Забанено: <b>{s['banned_users']}</b>\n"
        f""
        f"📅 Работает: <b>{days} дней</b>\n\n"
        "Команды:\n"
        "/users\n"
        "/ban id\n"
        "/unban id\n"
        "/addbal id сумма\n"
        "/subbal id сумма\n"
        "/msg id текст\n"
        "/all текст\n"
        "/pending\n"
    )

    await message.answer(text)




@router.message(Command("setstats"))
async def set_stats(message: Message):
    if not user_is_admin(message.from_user.id):
        await message.answer("❌ Нет доступа.")
        return

    parts = message.text.split()
    if len(parts) != 2:
        await message.answer("Использование: /setstats 5000")
        return

    try:
        value = int(parts[1])
        if value < 0:
            value = 0
        set_fake_total(value)
        await message.answer(f"✅ Статистика установлена: {value}")
    except ValueError:
        await message.answer("❌ Нужно указать число.")


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






# ===============================
# REFERRAL BONUS 50 UAH (10 REFS)
# ===============================

@router.message(F.text.in_([BUTTONS["ru"]["ref50"], BUTTONS["ua"]["ref50"]]))
async def ref_bonus_handler(message: Message):
    if not await ensure_full_access(message):
        return

    user_id = message.from_user.id

    refs = get_active_ref_count(user_id)
    withdraws = get_ref_withdraw_count(user_id)

    if refs < 10:
        await message.answer(
            f"❌ Для отримання 50 грн потрібно 10 активних рефералів\n\n"
            f"👥 У вас: {refs}/10"
        )
        return

    balance = get_balance(user_id)

    if balance >= 50:
        await message.answer(
            "❗️ Ви вже отримали кошти на баланс!\n"
            "Виведіть їх щоб отримати знову"
        )
        return

    add_balance(user_id, 50)

    await message.answer(
        "✅ Вам зараховано <b>50.0 грн</b>!"
    )


# ===============================
# WITHDRAW CABINET
# ===============================

@router.message(F.text == "🏦 Кабінет")
async def cabinet_handler(message: Message):
    user_id = message.from_user.id
    balance = get_balance(user_id)

    await message.answer(
        f"🏦 <b>Кабінет</b>\n\n"
        f"💰 Баланс: <b>{balance:.2f} грн</b>\n\n"
        "Введіть номер банківської картки:"
    )

    user_state[user_id] = "enter_card"


@router.message()
async def withdraw_states(message: Message):
    user_id = message.from_user.id

    # allow commands and normal messages if not in withdraw state
    if user_id not in user_state:
        return

    # Ввод карты
    if user_state.get(user_id) == "enter_card":

        card = message.text.strip()

        if len(card) < 12:
            await message.answer("❌ Невірний номер картки")
            return

        pending_withdraw[user_id] = {"card": card}

        await message.answer(
            "Введіть суму для виводу:"
        )

        user_state[user_id] = "enter_amount"
        return

    # Ввод суммы
    if user_state.get(user_id) == "enter_amount":

        try:
            amount = float(message.text)
        except:
            await message.answer("❌ Невірна сума")
            return

        data = pending_withdraw.get(user_id)

        wid = create_withdrawal(
            user_id,
            "card",
            data["card"],
            amount
        )

        await message.answer(
            "👨‍💻| Ваша заявка на вивід успішно прийнята\n"
            "➖➖➖➖➖➖➖➖➖➖➖➖➖➖\n"
            "🏦| Заявка буде оброблена адміністрацією на протязі 48 годин"
        )

        user_state[user_id] = None
        pending_withdraw[user_id] = {}
        return



# =========================
# ADMIN REF COMMANDS
# =========================

@router.message(Command("addref"))
async def admin_addref(message: Message):
    if not user_is_admin(message.from_user.id):
        return

    parts = message.text.split()
    if len(parts) != 3:
        await message.answer("Использование: /addref user_id количество")
        return

    try:
        tg_id = int(parts[1])
        count = int(parts[2])
    except:
        await message.answer("❌ Неверные данные")
        return

    add_manual_refs(tg_id, count)
    refs = get_active_ref_count(tg_id)

    await message.answer(
        f"✅ Добавлено {count} рефералов пользователю {tg_id}\n"
        f"Теперь всего: {refs}"
    )


@router.message(Command("setref"))
async def admin_setref(message: Message):
    if not user_is_admin(message.from_user.id):
        return

    parts = message.text.split()
    if len(parts) != 3:
        await message.answer("Использование: /setref user_id количество")
        return

    try:
        tg_id = int(parts[1])
        count = int(parts[2])
    except:
        await message.answer("❌ Неверные данные")
        return

    set_manual_refs(tg_id, count)

    await message.answer(
        f"✅ Пользователю {tg_id} установлено {count} рефералов"
    )


# =========================
# ADMIN FAKE STATS
# =========================

@router.message(Command("setstats"))
async def admin_setstats(message: Message):
    if not user_is_admin(message.from_user.id):
        return

    parts = message.text.split()
    if len(parts) != 2:
        await message.answer("Использование: /setstats число")
        return

    try:
        value = int(parts[1])
    except:
        await message.answer("❌ Нужно число")
        return

    set_fake_total(value)

    await message.answer(f"📊 Статистика пользователей установлена: {value}")



# ================= ADMIN WITHDRAW PENDING =================

@router.message(Command("pending"))
async def pending(message: Message):
    if not user_is_admin(message.from_user.id):
        return

    rows = list_new_withdrawals()

    if not rows:
        await message.answer("❌ Немає заявок")
        return

    for wid, uid, method, details, amount, status, created in rows:

        kb = InlineKeyboardMarkup(
            inline_keyboard=[[
                InlineKeyboardButton(text="✅ Одобрити", callback_data=f"pay:{wid}"),
                InlineKeyboardButton(text="❌ Відхилити", callback_data=f"reject:{wid}")
            ]]
        )

        text = (
            f"💸 Заявка #{wid}\n"
            f"👤 ID: {uid}\n"
            f"💳 Метод: {method}\n"
            f"📄 Реквізит: {details}\n"
            f"💰 Сума: {amount}"
        )

        await message.answer(text, reply_markup=kb)


@router.callback_query(F.data.startswith("pay:"))
async def pay_withdraw(call: CallbackQuery):

    if not user_is_admin(call.from_user.id):
        return

    wid = int(call.data.split(":")[1])
    set_withdraw_status(wid,"paid")

    await call.message.edit_text("✅ Виплата підтверджена")


@router.callback_query(F.data.startswith("reject:"))
async def reject_withdraw(call: CallbackQuery):

    if not user_is_admin(call.from_user.id):
        return

    wid = int(call.data.split(":")[1])
    set_withdraw_status(wid,"rejected")

    await call.message.edit_text("❌ Заявка відхилена")


# =========================
# START BOT
# =========================

async def main():
    init_db()
    await bot.delete_webhook(drop_pending_updates=True)
    print("Bot started...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
