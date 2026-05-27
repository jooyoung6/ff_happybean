import asyncio
import base64
import html as html_lib
import json
import os
import queue
import re
import threading
import time
import traceback
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Callable, Optional
from urllib.parse import parse_qs, unquote, urljoin, urlparse

from bs4 import BeautifulSoup
from sqlalchemy import Boolean, Column, DateTime, ForeignKey, Integer, String, Text, create_engine, text
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

# Keep this in sync across cookie creation and actual collection. Naver can invalidate
# sessions when the browser identity changes.
IPHONE13_UA = (
    "Mozilla/5.0 (iPhone; CPU iPhone OS 15_0 like Mac OS X) "
    "AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.0 Mobile/15E148 Safari/604.1"
)
DESKTOP_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
)
REWARD_URL_PREFIXES = (
    "https://campaign2.naver.com",
    "https://ofw.adison.co",
    "https://external-token.pay.naver.com",
    "https://point.pay.naver.com/bridge/eventbenefit",
)
NAVERPAY_MAIN_URL = "https://point.pay.naver.com/pc/main"
NAVERPAY_MISSION_DETAIL_URL = "https://point.pay.naver.com/pc/mission-detail?dataType=category&rank=20&pageKey=all"

LOGIN_SESSION_TTL = 300
_login_sessions = {}
_login_sessions_lock = threading.Lock()
_login_loop = None
_login_loop_lock = threading.Lock()


Base = declarative_base()


class CampaignUrl(Base):
    __tablename__ = "campaign_urls"

    url = Column(String, primary_key=True)
    date_added = Column(DateTime, default=datetime.now)
    is_available = Column(Boolean, default=True)


class UrlVisit(Base):
    __tablename__ = "url_visits"

    url = Column(String, ForeignKey("campaign_urls.url"), primary_key=True)
    user_id = Column(String, primary_key=True)
    visited_at = Column(DateTime)
    campaign_url = relationship("CampaignUrl")


class User(Base):
    __tablename__ = "user"

    user_id = Column(String, primary_key=True)
    storage_state = Column(Text, nullable=True)
    updated_at = Column(DateTime, default=datetime.now)


class RunHistory(Base):
    __tablename__ = "run_history"

    id = Column(Integer, primary_key=True, autoincrement=True)
    started_at = Column(DateTime, default=datetime.now)
    finished_at = Column(DateTime)
    status = Column(String, default="running")
    account_count = Column(Integer, default=0)
    collected_url_count = Column(Integer, default=0)
    skipped_url_count = Column(Integer, default=0)
    visited_url_count = Column(Integer, default=0)
    estimated_points = Column(Integer, default=0)
    message = Column(Text, default="")


class RunDetail(Base):
    __tablename__ = "run_detail"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(Integer, ForeignKey("run_history.id"))
    user_id = Column(String, default="")
    url = Column(Text, default="")
    status = Column(String, default="")
    point = Column(Integer, default=0)
    message = Column(Text, default="")
    created_at = Column(DateTime, default=datetime.now)
    run = relationship("RunHistory")


class Database:
    def __init__(self, db_path: str):
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self.db_path = db_path
        self.engine = create_engine(f"sqlite:///{db_path}", connect_args={"check_same_thread": False})
        self.Session = sessionmaker(bind=self.engine)

    def create_all(self):
        Base.metadata.create_all(bind=self.engine)
        self._ensure_columns()

    def _ensure_columns(self):
        with self.engine.begin() as conn:
            existing = {row[1] for row in conn.execute(text("PRAGMA table_info(run_history)"))}
            if "skipped_url_count" not in existing:
                conn.execute(text("ALTER TABLE run_history ADD COLUMN skipped_url_count INTEGER DEFAULT 0"))

    @contextmanager
    def get_session(self):
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()


@dataclass
class AccountConfig:
    user_id: str
    password: str = ""
    keep_login: bool = True


@dataclass
class RunConfig:
    db_path: str
    cookie_dir: str
    accounts: list[AccountConfig]
    reward_proxy_url: str = ""
    login_proxy_url: str = ""
    no_paper_record: bool = False
    keep_campaign_days: int = 60
    keep_user_days: int = 7


@dataclass
class DetailResult:
    user_id: str = ""
    url: str = ""
    status: str = ""
    point: int = 0
    message: str = ""


@dataclass
class RunResult:
    started_at: datetime = field(default_factory=datetime.now)
    finished_at: Optional[datetime] = None
    status: str = "running"
    account_count: int = 0
    collected_url_count: int = 0
    skipped_url_count: int = 0
    visited_url_count: int = 0
    estimated_points: int = 0
    message: str = ""
    details: list[DetailResult] = field(default_factory=list)


def parse_accounts(ids_text: str, passwords_text: str) -> list[AccountConfig]:
    ids = [x.strip() for x in str(ids_text or "").split("|") if x.strip()]
    passwords = [x.strip() for x in str(passwords_text or "").split("|")]
    return [AccountConfig(user_id=nid, password=passwords[idx].strip() if idx < len(passwords) else "", keep_login=True) for idx, nid in enumerate(ids)]


def run_sync(config: RunConfig, log: Optional[Callable[[str], None]] = None) -> RunResult:
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return _run_coro_blocking(run(config, log=log))
    return _run_async_in_thread(lambda: run(config, log=log))


def _run_coro_blocking(coro):
    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        try:
            asyncio.events._set_running_loop(None)
        except Exception:
            pass
        return loop.run_until_complete(coro)
    finally:
        try:
            asyncio.set_event_loop(None)
        except Exception:
            pass
        loop.close()


def _run_async_in_thread(coro_factory):
    result_queue = queue.Queue(maxsize=1)

    def runner():
        try:
            result_queue.put((True, _run_coro_blocking(coro_factory())))
        except Exception as e:
            result_queue.put((False, e))

    thread = threading.Thread(target=runner, daemon=True)
    thread.start()
    ok, value = result_queue.get()
    thread.join()
    if ok:
        return value
    raise value


async def run(config: RunConfig, log: Optional[Callable[[str], None]] = None) -> RunResult:
    db = Database(config.db_path)
    db.create_all()
    result = RunResult(account_count=len(config.accounts))

    def emit(message: str):
        if log:
            log(message)

    with db.get_session() as session_db:
        history = RunHistory(started_at=result.started_at, status="running", account_count=result.account_count)
        session_db.add(history)
        session_db.flush()

        try:
            emit("Campaign URL collection started")
            emit(f"Proxy mode login={mask_proxy_url(config.login_proxy_url)} reward={mask_proxy_url(config.reward_proxy_url)}")
            collected_urls = await save_naver_campaign_urls(
                session_db,
                emit,
                config.reward_proxy_url,
                config.accounts,
                config.cookie_dir,
                config.login_proxy_url,
            )
            result.collected_url_count = len(collected_urls)
            emit(f"Campaign URL collection finished: {result.collected_url_count}")

            for account in config.accounts:
                account_result = await process_account(
                    account,
                    session_db,
                    config.cookie_dir,
                    config.reward_proxy_url,
                    emit,
                    config.login_proxy_url,
                )
                result.estimated_points += account_result.estimated_points
                result.skipped_url_count += account_result.skipped_url_count
                result.visited_url_count += account_result.visited_url_count
                result.details.extend(account_result.details)
                for detail in account_result.details:
                    session_db.add(
                        RunDetail(
                            run_id=history.id,
                            user_id=detail.user_id,
                            url=detail.url,
                            status=detail.status,
                            point=detail.point,
                            message=detail.message,
                        )
                    )
                if config.no_paper_record and account_result.visited_url_count == 0 and not account_result.details:
                    session_db.add(
                        RunDetail(
                            run_id=history.id,
                            user_id=account.user_id,
                            status="no_url",
                            message="No unvisited campaign URL",
                        )
                    )

            delete_old_stuff(session_db, config.keep_campaign_days, config.keep_user_days, emit)
            result.status = "completed"
            result.message = ""
        except Exception as e:
            result.status = "error"
            result.message = str(e)
            emit(traceback.format_exc())
        finally:
            result.finished_at = datetime.now()
            history.finished_at = result.finished_at
            history.status = result.status
            history.account_count = result.account_count
            history.collected_url_count = result.collected_url_count
            history.skipped_url_count = result.skipped_url_count
            history.visited_url_count = result.visited_url_count
            history.estimated_points = result.estimated_points
            history.message = result.message

    return result


async def run_manual_link(
    db_path: str,
    cookie_dir: str,
    accounts: list[AccountConfig],
    link: str,
    reward_proxy_url: str = "",
    log: Optional[Callable[[str], None]] = None,
    login_proxy_url: str = "",
) -> RunResult:
    db = Database(db_path)
    db.create_all()
    result = RunResult(account_count=len(accounts), collected_url_count=1)
    link = str(link or "").strip()

    def emit(message: str):
        if log:
            log(message)

    if not link:
        result.status = "warning"
        result.message = "manual link is empty"
        return result

    with db.get_session() as session_db:
        history = RunHistory(started_at=result.started_at, status="running", account_count=result.account_count, collected_url_count=1)
        session_db.add(history)
        session_db.flush()

        try:
            emit(f"Manual reward started: {link}")
            emit(f"Proxy mode login={mask_proxy_url(login_proxy_url)} reward={mask_proxy_url(reward_proxy_url)}")
            for account in accounts:
                account_result = await process_account_manual_link(
                    account,
                    session_db,
                    cookie_dir,
                    reward_proxy_url,
                    link,
                    emit,
                    login_proxy_url,
                )
                result.estimated_points += account_result.estimated_points
                result.skipped_url_count += account_result.skipped_url_count
                result.visited_url_count += account_result.visited_url_count
                result.details.extend(account_result.details)
                for detail in account_result.details:
                    session_db.add(
                        RunDetail(
                            run_id=history.id,
                            user_id=detail.user_id,
                            url=detail.url,
                            status=detail.status,
                            point=detail.point,
                            message=detail.message,
                        )
                    )

            result.status = "completed"
            result.message = ""
        except Exception as e:
            result.status = "error"
            result.message = str(e)
            emit(traceback.format_exc())
        finally:
            result.finished_at = datetime.now()
            history.finished_at = result.finished_at
            history.status = result.status
            history.account_count = result.account_count
            history.collected_url_count = result.collected_url_count
            history.skipped_url_count = result.skipped_url_count
            history.visited_url_count = result.visited_url_count
            history.estimated_points = result.estimated_points
            history.message = result.message

    return result


def run_manual_link_sync(
    db_path: str,
    cookie_dir: str,
    accounts: list[AccountConfig],
    link: str,
    reward_proxy_url: str = "",
    log: Optional[Callable[[str], None]] = None,
    login_proxy_url: str = "",
):
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return _run_coro_blocking(run_manual_link(db_path, cookie_dir, accounts, link, reward_proxy_url, log, login_proxy_url))
    return _run_async_in_thread(lambda: run_manual_link(db_path, cookie_dir, accounts, link, reward_proxy_url, log, login_proxy_url))


@dataclass
class AccountResult:
    user_id: str
    visited_url_count: int = 0
    skipped_url_count: int = 0
    estimated_points: int = 0
    details: list[DetailResult] = field(default_factory=list)


def cookie_path(cookie_dir: str, nid: str) -> str:
    return os.path.join(cookie_dir, f"{nid}.json")


def browser_profile_dir(cookie_dir: str, nid: str) -> str:
    safe_id = re.sub(r"[^A-Za-z0-9_.-]+", "_", str(nid or "").strip()) or "default"
    return os.path.join(os.path.dirname(cookie_dir), "browser_profiles", safe_id)


def get_storage_state(nid: str, session_db, cookie_dir: str):
    path = cookie_path(cookie_dir, nid)
    if os.path.exists(path):
        return path
    user = session_db.query(User).filter_by(user_id=nid).first()
    if user and user.storage_state:
        try:
            return json.loads(user.storage_state)
        except Exception:
            return None
    return None


def check_storage_data_status(data):
    import time

    now = time.time()
    key_cookies = ("NID_AUT", "NID_SES", "NID_JST")
    required_cookies = ("NID_AUT", "NID_SES")
    expired = []
    session_only = []
    valid = []
    for cookie in (data or {}).get("cookies", []):
        if cookie.get("name") not in key_cookies:
            continue
        exp = cookie.get("expires", -1)
        if exp == -1:
            session_only.append(cookie.get("name"))
        elif exp < now:
            expired.append(cookie.get("name"))
        else:
            valid.append(cookie.get("name"))
    if expired:
        return False, f"expired cookies: {', '.join(expired)}"
    if session_only:
        return False, f"session-only cookies: {', '.join(session_only)}"
    missing = [name for name in required_cookies if not any(item.startswith(name) or item == name for item in valid)]
    if missing:
        return False, f"required cookies missing: {', '.join(missing)}"
    return True, f"valid cookies: {', '.join(valid)}"


def check_cookie_status(nid: str, cookie_dir: str):
    path = cookie_path(cookie_dir, nid)
    if not os.path.exists(path):
        return False, "cookie file missing"

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return check_storage_data_status(data)
    except Exception as e:
        return False, f"cookie read error: {e}"


def save_storage_state(cookie_dir: str, nid: str, storage_state):
    os.makedirs(cookie_dir, exist_ok=True)
    with open(cookie_path(cookie_dir, nid), "w", encoding="utf-8") as f:
        json.dump(storage_state, f, ensure_ascii=False, indent=2)


def storage_cookie_values(storage_state):
    values = {}
    try:
        for cookie in (storage_state or {}).get("cookies", []):
            name = cookie.get("name")
            if name in ("NID_AUT", "NID_SES", "NID_JST"):
                values[name] = cookie.get("value") or ""
    except Exception:
        return {}
    return values


def storage_has_required_cookies(storage_state):
    values = storage_cookie_values(storage_state)
    return bool(values.get("NID_AUT") and values.get("NID_SES"))


async def password_login_on_page(page, account: AccountConfig, emit: Callable[[str], None]):
    if not account.password:
        emit(f"{account.user_id}: login diagnostic - password empty")
        return False, "password empty"
    emit(f"{account.user_id}: login diagnostic - opening Naver login page")
    ok, message = await goto_naver_login(page, emit, account.user_id)
    if not ok:
        emit(f"{account.user_id}: login diagnostic - login page open failed: {message}")
        return False, message
    emit(f"{account.user_id}: login diagnostic - login page loaded url={page.url}")
    try:
        id_count = await page.locator("#id").count()
        pw_count = await page.locator("#pw").count()
        emit(f"{account.user_id}: login diagnostic - input fields id={id_count} pw={pw_count}")
    except Exception as e:
        emit(f"{account.user_id}: login diagnostic - input field check failed: {e}")
    await page.locator("#id").click()
    await asyncio.sleep(0.5)
    await page.locator("#id").type(account.user_id, delay=80)
    await asyncio.sleep(0.3)
    await page.locator("#pw").click()
    await asyncio.sleep(0.5)
    await page.locator("#pw").type(account.password, delay=80)
    await set_keep_login(page, account.keep_login, emit, account.user_id)
    await asyncio.sleep(0.5)
    await page.locator("#pw").press("Enter")
    emit(f"{account.user_id}: login diagnostic - submitted credentials url={page.url}")
    last_cookie_text = ""
    last_url = ""
    for attempt in range(1, 6):
        await asyncio.sleep(1)
        storage = await page.context.storage_state()
        values = storage_cookie_values(storage)
        cookie_text = ",".join([name for name in ("NID_AUT", "NID_SES", "NID_JST") if values.get(name)]) or "none"
        current_url = page.url
        if cookie_text != last_cookie_text or current_url != last_url or attempt in (1, 5):
            emit(f"{account.user_id}: login diagnostic - wait {attempt}/5 url={current_url} cookies={cookie_text}")
            last_cookie_text = cookie_text
            last_url = current_url
        if storage_has_required_cookies(storage):
            emit(f"{account.user_id}: login success")
            return True, "login success"
    try:
        title = await page.title()
    except Exception:
        title = ""
    try:
        body_text = (await page.locator("body").inner_text(timeout=3000))[:500]
    except Exception as e:
        body_text = f"body read failed: {e}"
    emit(f"{account.user_id}: login diagnostic - failed final_url={page.url} title={title} body={body_text}")
    return False, "login did not produce required cookies"


async def set_keep_login(page, enabled: bool, emit: Callable[[str], None], user_id: str = ""):
    selectors = [
        "#keep",
        "input[name='nvlong']",
        "input[id*='keep' i]",
        "label[for='keep']",
    ]
    for selector in selectors:
        try:
            locator = page.locator(selector).first
            if await locator.count() == 0:
                continue
            tag = (await locator.evaluate("(el) => el.tagName.toLowerCase()")).lower()
            if tag == "input":
                checked = await locator.is_checked()
                if bool(checked) != bool(enabled):
                    await locator.click()
            else:
                input_checked = await page.locator("#keep").is_checked()
                if bool(input_checked) != bool(enabled):
                    await locator.click()
            emit(f"{user_id}: login diagnostic - keep-login set to {enabled}")
            return True
        except Exception:
            continue
    emit(f"{user_id}: login diagnostic - keep-login control not found")
    return False


async def goto_naver_login(page, emit: Optional[Callable[[str], None]] = None, user_id: str = ""):
    url = "https://nid.naver.com/nidlogin.login"
    errors = []
    for wait_until, timeout in (("domcontentloaded", 30000), ("commit", 20000)):
        try:
            if emit:
                emit(f"{user_id}: login diagnostic - goto start wait_until={wait_until} timeout={timeout}ms")
            await page.goto(url, wait_until=wait_until, timeout=timeout)
            try:
                await page.wait_for_selector("#id", timeout=30000)
            except Exception:
                pass
            if emit:
                emit(f"{user_id}: login diagnostic - goto success wait_until={wait_until} url={page.url}")
            return True, "login page loaded"
        except Exception as e:
            message = f"{wait_until}: {e}"
            errors.append(message)
            if emit:
                emit(f"{user_id}: login diagnostic - goto failed {message}")
    return False, "login page timeout - " + " / ".join(errors[-2:])


async def naver_login(page, account: AccountConfig, cookie_dir: str, emit: Callable[[str], None]):
    is_valid, cookie_msg = check_cookie_status(account.user_id, cookie_dir)
    if is_valid:
        emit(f"{account.user_id}: using saved cookie ({cookie_msg})")
        return True, cookie_msg

    emit(f"{account.user_id}: cookie unavailable ({cookie_msg}); trying id/password login")
    try:
        login_ok, login_message = await password_login_on_page(page, account, emit)
        if login_ok:
            return True, login_message
        return False, f"{cookie_msg}; {login_message}"
    except Exception as e:
        emit(f"{account.user_id}: login failed - {e}")
        return False, str(e)


def refresh_cookie_sync(account: AccountConfig, cookie_dir: str, proxy_url: str = "", log: Optional[Callable[[str], None]] = None):
    async def _run():
        screen = await open_login_screen(account, cookie_dir, proxy_url, log)
        if screen.get("ret") != "success":
            return screen
        return await submit_login_captcha(account.user_id, "", cookie_dir, log)

    return _run_login_async(_run())


async def refresh_cookie(account: AccountConfig, cookie_dir: str, proxy_url: str = "", log: Optional[Callable[[str], None]] = None):
    from playwright.async_api import async_playwright
    from playwright_stealth import Stealth

    def emit(message: str):
        if log:
            log(message)

    os.makedirs(cookie_dir, exist_ok=True)
    async with async_playwright() as playwright:
        launch_kwargs = {"headless": True}
        proxy = playwright_proxy(proxy_url)
        if proxy:
            launch_kwargs["proxy"] = proxy
        browser = await playwright.chromium.launch(**launch_kwargs)
        context = await browser.new_context(
            user_agent=IPHONE13_UA,
            viewport={"width": 390, "height": 844},
            device_scale_factor=3,
            is_mobile=True,
            has_touch=True,
        )
        page = await context.new_page()
        await Stealth().apply_stealth_async(page)
        try:
            login_ok, login_message = await password_login_on_page(page, account, emit)
            if not login_ok:
                emit(f"{account.user_id}: cookie refresh failed - {login_message}")
                return {"ret": "warning", "msg": "cookie refresh failed. Check plugin log for login diagnostics.", "cookies": {}}
            storage = await context.storage_state()
            with open(cookie_path(cookie_dir, account.user_id), "w", encoding="utf-8") as f:
                json.dump(storage, f, ensure_ascii=False, indent=2)
            return {"ret": "success", "msg": "cookie refreshed", "cookies": storage_cookie_values(storage)}
        finally:
            await context.close()
            await browser.close()


async def refresh_cookie_from_profile(
    account: AccountConfig,
    cookie_dir: str,
    proxy_url: str = "",
    emit: Optional[Callable[[str], None]] = None,
    allow_password_login: bool = True,
):
    from playwright.async_api import async_playwright
    from playwright_stealth import Stealth

    def log(message: str):
        if emit:
            emit(message)

    if not account.user_id:
        return False, "account id required"

    profile_dir = browser_profile_dir(cookie_dir, account.user_id)
    os.makedirs(profile_dir, exist_ok=True)
    os.makedirs(cookie_dir, exist_ok=True)
    log(f"{account.user_id}: cookie auto refresh profile check started proxy={mask_proxy_url(proxy_url)}")

    async with async_playwright() as playwright:
        launch_kwargs = {
            "headless": True,
            "user_agent": IPHONE13_UA,
            "viewport": {"width": 390, "height": 844},
            "device_scale_factor": 3,
            "is_mobile": True,
            "has_touch": True,
        }
        proxy = playwright_proxy(proxy_url)
        if proxy:
            launch_kwargs["proxy"] = proxy
        log(f"{account.user_id}: cookie auto refresh opening persistent browser")
        context = await playwright.chromium.launch_persistent_context(profile_dir, **launch_kwargs)
        try:
            page = context.pages[0] if context.pages else await context.new_page()
            await Stealth().apply_stealth_async(page)

            storage = await context.storage_state()
            ok, message = check_storage_data_status(storage)
            if ok:
                save_storage_state(cookie_dir, account.user_id, storage)
                log(f"{account.user_id}: cookie auto refresh from browser profile ({message})")
                return True, message

            try:
                log(f"{account.user_id}: cookie auto refresh checking pay login page")
                await page.goto("https://pay.naver.com/pointshistory/list?category=all", wait_until="domcontentloaded", timeout=30000)
                await page.wait_for_timeout(3000)
            except Exception as e:
                log(f"{account.user_id}: browser profile login check failed - {e}")

            storage = await context.storage_state()
            ok, message = check_storage_data_status(storage)
            if ok:
                save_storage_state(cookie_dir, account.user_id, storage)
                log(f"{account.user_id}: cookie auto refresh after login check ({message})")
                return True, message

            if not allow_password_login:
                return False, message
            if not account.password:
                return False, f"{message}; password empty"

            log(f"{account.user_id}: browser profile cookie unavailable ({message}); trying id/password login")
            login_ok, login_message = await password_login_on_page(page, account, log)
            if not login_ok:
                return False, login_message

            storage = await context.storage_state()
            ok, message = check_storage_data_status(storage)
            if not ok:
                return False, message
            save_storage_state(cookie_dir, account.user_id, storage)
            log(f"{account.user_id}: cookie auto refresh by login ({message})")
            return True, message
        finally:
            await context.close()


async def ensure_cookie_storage_state(
    account: AccountConfig,
    session_db,
    cookie_dir: str,
    proxy_url: str,
    emit: Callable[[str], None],
    allow_password_login: bool = True,
):
    ok, message = check_cookie_status(account.user_id, cookie_dir)
    if ok:
        return get_storage_state(account.user_id, session_db, cookie_dir)

    emit(f"{account.user_id}: cookie unavailable ({message}); trying auto refresh")
    refresh_ok, refresh_message = await refresh_cookie_from_profile(
        account,
        cookie_dir,
        proxy_url,
        emit,
        allow_password_login=allow_password_login,
    )
    if refresh_ok:
        return get_storage_state(account.user_id, session_db, cookie_dir)

    emit(f"{account.user_id}: cookie auto refresh failed - {refresh_message}")
    return None


def _run_async(coro):
    try:
        return asyncio.run(coro)
    except RuntimeError:
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(coro)
        finally:
            loop.close()


def _get_login_loop():
    global _login_loop
    with _login_loop_lock:
        if _login_loop and _login_loop.is_running():
            return _login_loop
        loop = asyncio.new_event_loop()

        def run_loop():
            asyncio.set_event_loop(loop)
            loop.run_forever()

        thread = threading.Thread(target=run_loop, daemon=True)
        thread.start()
        _login_loop = loop
        return loop


def _run_login_async(coro):
    future = asyncio.run_coroutine_threadsafe(coro, _get_login_loop())
    return future.result()


async def _close_login_session(user_id: str):
    session = None
    with _login_sessions_lock:
        session = _login_sessions.pop(user_id, None)
    if not session:
        return
    for key in ("context", "browser", "playwright"):
        obj = session.get(key)
        if obj is None:
            continue
        try:
            if key == "playwright":
                await obj.stop()
            else:
                await obj.close()
        except Exception:
            pass


def close_login_session_sync(user_id: str):
    return _run_login_async(_close_login_session(str(user_id or "").strip()))


async def _cleanup_login_sessions():
    now = time.time()
    expired = []
    with _login_sessions_lock:
        for user_id, session in list(_login_sessions.items()):
            if now - float(session.get("created_at") or now) > LOGIN_SESSION_TTL:
                expired.append(user_id)
    for user_id in expired:
        await _close_login_session(user_id)


async def _login_screenshot_payload(page) -> str:
    png = await page.screenshot(full_page=True, type="png")
    return "data:image/png;base64," + base64.b64encode(png).decode("ascii")


async def _dispatch_input_events(locator):
    try:
        await locator.evaluate(
            """(el) => {
                el.dispatchEvent(new Event('input', {bubbles: true}));
                el.dispatchEvent(new Event('change', {bubbles: true}));
                el.dispatchEvent(new KeyboardEvent('keyup', {bubbles: true}));
            }"""
        )
    except Exception:
        pass


async def _wait_for_login_cookies(context, timeout_seconds: int = 5):
    for _ in range(timeout_seconds):
        await asyncio.sleep(1)
        storage = await context.storage_state()
        if storage_has_required_cookies(storage):
            return storage
    return await context.storage_state()


async def open_login_screen(account: AccountConfig, cookie_dir: str, proxy_url: str = "", log: Optional[Callable[[str], None]] = None):
    from playwright.async_api import async_playwright
    from playwright_stealth import Stealth

    def emit(message: str):
        if log:
            log(message)

    if not account.user_id or not account.password:
        return {"ret": "warning", "msg": "account id/password required"}
    await _cleanup_login_sessions()
    normalized_proxy_url = str(proxy_url or "").strip()
    with _login_sessions_lock:
        existing_session = _login_sessions.get(account.user_id)
    if existing_session and existing_session.get("proxy_url", "") != normalized_proxy_url:
        emit(f"{account.user_id}: manual login session proxy changed; reopening browser")
        await _close_login_session(account.user_id)
        existing_session = None
    if existing_session and existing_session.get("page"):
        page = existing_session.get("page")
        existing_session["account"] = account
        existing_session["created_at"] = time.time()
        emit(f"{account.user_id}: manual login session reused url={page.url}")
        return {"ret": "success", "msg": "current login screen captured", "screenshot": await _login_screenshot_payload(page), "url": page.url}
    os.makedirs(cookie_dir, exist_ok=True)

    playwright = await async_playwright().start()
    launch_kwargs = {"headless": True}
    proxy = playwright_proxy(normalized_proxy_url)
    if proxy:
        launch_kwargs["proxy"] = proxy
        emit(f"{account.user_id}: manual login proxy enabled {mask_proxy_url(normalized_proxy_url)}")
    else:
        emit(f"{account.user_id}: manual login proxy disabled")
    profile_dir = browser_profile_dir(cookie_dir, account.user_id)
    os.makedirs(profile_dir, exist_ok=True)
    persistent_kwargs = {
        "user_agent": IPHONE13_UA,
        "viewport": {"width": 390, "height": 844},
        "device_scale_factor": 3,
        "is_mobile": True,
        "has_touch": True,
    }
    persistent_kwargs.update(launch_kwargs)
    context = await playwright.chromium.launch_persistent_context(profile_dir, **persistent_kwargs)
    page = context.pages[0] if context.pages else await context.new_page()
    await Stealth().apply_stealth_async(page)
    try:
        storage = await context.storage_state()
        ok, cookie_message = check_storage_data_status(storage)
        if ok:
            save_storage_state(cookie_dir, account.user_id, storage)
            await context.close()
            await playwright.stop()
            emit(f"{account.user_id}: manual login skipped; browser profile already logged in ({cookie_message})")
            return {"ret": "success", "msg": "cookie refreshed", "cookies": storage_cookie_values(storage)}
        ok, message = await goto_naver_login(page, emit, account.user_id)
        if not ok:
            await context.close()
            await playwright.stop()
            return {"ret": "warning", "msg": message}
        screenshot = await _login_screenshot_payload(page)
        with _login_sessions_lock:
            _login_sessions[account.user_id] = {
                "playwright": playwright,
                "context": context,
                "page": page,
                "account": account,
                "created_at": time.time(),
                "proxy_url": normalized_proxy_url,
            }
        emit(f"{account.user_id}: manual login session opened url={page.url}")
        return {"ret": "success", "msg": "login screen captured", "screenshot": screenshot, "url": page.url}
    except Exception as e:
        await context.close()
        await playwright.stop()
        return {"ret": "danger", "msg": f"login screen capture failed: {str(e)}"}


def open_login_screen_sync(account: AccountConfig, cookie_dir: str, proxy_url: str = "", log: Optional[Callable[[str], None]] = None):
    return _run_login_async(open_login_screen(account, cookie_dir, proxy_url, log))


async def submit_login_captcha(user_id: str, captcha_text: str, cookie_dir: str, log: Optional[Callable[[str], None]] = None):
    def emit(message: str):
        if log:
            log(message)

    user_id = str(user_id or "").strip()
    captcha_text = str(captcha_text or "").strip()
    if not user_id:
        return {"ret": "warning", "msg": "account id required"}
    with _login_sessions_lock:
        session = _login_sessions.get(user_id)
    if not session:
        return {"ret": "warning", "msg": "login screen session not found or expired"}
    page = session.get("page")
    context = session.get("context")
    account = session.get("account")
    try:
        emit(f"{user_id}: manual login diagnostic - submit requested url={page.url} captcha={'yes' if captcha_text else 'no'}")
        if account and account.user_id:
            id_field = page.locator("#id")
            if await id_field.count():
                await id_field.click()
                await id_field.fill("")
                await id_field.type(account.user_id, delay=80)
                await _dispatch_input_events(id_field)
        if account and account.password:
            pw_field = page.locator("#pw")
            if await pw_field.count():
                await pw_field.click()
                await pw_field.fill("")
                await pw_field.type(account.password, delay=80)
                await _dispatch_input_events(pw_field)
        if account:
            await set_keep_login(page, account.keep_login, emit, user_id)

        selectors = [
            "#captcha",
            "input[name='captcha']",
            "input[id*='captcha' i]",
            "input[name*='captcha' i]",
            "input[type='text']",
        ]
        if captcha_text:
            filled = False
            for selector in selectors:
                locator = page.locator(selector)
                count = await locator.count()
                for idx in range(count):
                    field = locator.nth(idx)
                    try:
                        if await field.is_visible():
                            field_id = await field.get_attribute("id") or ""
                            field_name = await field.get_attribute("name") or ""
                            if field_id == "id" or field_id == "pw" or field_name == "id" or field_name == "pw":
                                continue
                            await field.click()
                            await field.fill(captcha_text)
                            await _dispatch_input_events(field)
                            filled = True
                            break
                    except Exception:
                        continue
                if filled:
                    break
            if not filled:
                return {"ret": "warning", "msg": "captcha input field not found", "screenshot": await _login_screenshot_payload(page)}

        await asyncio.sleep(0.5)
        try:
            submit = page.locator("#log\\.login").first
            if await submit.count() == 0:
                submit = page.locator("button[type='submit'], input[type='submit']").first
            await submit.click(timeout=5000, force=True)
        except Exception as e:
            emit(f"{user_id}: manual login diagnostic - submit click failed: {e}; pressing Enter")
            await page.keyboard.press("Enter")
        try:
            await page.wait_for_load_state("domcontentloaded", timeout=3000)
        except Exception:
            pass
        await asyncio.sleep(1)
        emit(f"{user_id}: manual login diagnostic - submitted url={page.url}")
        storage = await _wait_for_login_cookies(context, 5)
        if not storage_has_required_cookies(storage):
            emit(f"{user_id}: manual login diagnostic - required cookies missing url={page.url}")
            return {"ret": "warning", "msg": "login did not produce required cookies", "screenshot": await _login_screenshot_payload(page)}
        os.makedirs(cookie_dir, exist_ok=True)
        with open(cookie_path(cookie_dir, user_id), "w", encoding="utf-8") as f:
            json.dump(storage, f, ensure_ascii=False, indent=2)
        await _close_login_session(user_id)
        emit(f"{user_id}: captcha login success")
        return {"ret": "success", "msg": "cookie refreshed", "cookies": storage_cookie_values(storage)}
    except Exception as e:
        screenshot = ""
        try:
            screenshot = await _login_screenshot_payload(page)
        except Exception:
            pass
        return {"ret": "danger", "msg": f"captcha submit failed: {str(e)}", "screenshot": screenshot}


def submit_login_captcha_sync(user_id: str, captcha_text: str, cookie_dir: str, log: Optional[Callable[[str], None]] = None):
    return _run_login_async(submit_login_captcha(user_id, captcha_text, cookie_dir, log))


async def campaign_page_diagnostic(page, user_id: str, label: str, emit: Callable[[str], None]):
    async def limited(coro, timeout=2):
        return await asyncio.wait_for(coro, timeout=timeout)

    try:
        current_url = page.url
    except Exception:
        current_url = ""
    try:
        title = await limited(page.title())
    except Exception as e:
        title = f"title read failed: {e}"
    try:
        ready_state = await limited(page.evaluate("() => document.readyState"))
    except Exception as e:
        ready_state = f"readyState read failed: {e}"
    try:
        point_buttons = await limited(page.locator("a.popup_link, button, a").filter(has_text="포인트 받기").count())
    except Exception as e:
        point_buttons = f"button count failed: {e}"
    try:
        popup_links = await limited(page.locator("a.popup_link").count())
    except Exception as e:
        popup_links = f"popup link count failed: {e}"
    try:
        raw_body = await limited(page.locator("body").inner_text(timeout=2000), timeout=3)
        body_text = campaign_relevant_body(raw_body)
    except Exception as e:
        body_text = f"body read failed: {e}"
    if body_text:
        emit(
            f"{user_id}: campaign diagnostic - {label} url={current_url} title={title} "
            f"ready={ready_state} point_buttons={point_buttons} popup_links={popup_links} body={body_text}"
        )


def campaign_relevant_body(body_text: str) -> str:
    text = re.sub(r"\s+", " ", str(body_text or "")).strip()
    if not text:
        return ""

    patterns = [
        r"클릭 적립은 캠페인 당\s*1회만 적립됩니다\.?\s*확인",
        r"다음 페이지에서\s*\d+초 이상\s*머물러야\s*네이버페이 포인트\s*\d+원이 적립돼요\.?\s*포인트 받기",
        r"\d+초 후 적립\s*\d+원 적립 완료",
        r"알림신청 완료!?\s*알림을 취소했습니다\.?\s*쇼핑라이브 보고\s*포인트 받기\s*\d+원",
        r"쇼핑라이브 보고\s*포인트 받기\s*\d+원",
        r"\d+원 적립 완료",
    ]
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            return match.group(0)
    return ""


async def click_and_dwell(page, locator, dwell_seconds: float, user_id: str, emit: Callable[[str], None], label: str, js_click: bool = False):
    context = page.context
    before_pages = list(context.pages)
    before_url = page.url
    if js_click:
        await locator.evaluate(
            """el => {
                const target = el.closest('a, button, [role="button"], [onclick]') || el;
                target.click();
            }"""
        )
    else:
        await locator.click(timeout=5000, force=True)

    await asyncio.sleep(1)
    pages = list(context.pages)
    new_pages = [candidate for candidate in pages if candidate not in before_pages]
    active_page = new_pages[-1] if new_pages else page
    try:
        await active_page.wait_for_load_state("domcontentloaded", timeout=5000)
    except Exception as e:
        emit(f"{user_id}: {label} domcontentloaded wait skipped - {e}")
    try:
        await active_page.bring_to_front()
    except Exception:
        pass
    await asyncio.sleep(dwell_seconds)
    emit(f"{user_id}: visited {active_page.url}")
    try:
        await campaign_page_diagnostic(active_page, user_id, f"{label} after dwell", emit)
    except Exception as e:
        emit(f"{user_id}: {label} diagnostic skipped after dwell - {e}")
    return active_page


async def process_campaign2_link(page, link: str, session_db, user_id: str, emit: Callable[[str], None]):
    try:
        await page.goto(link, wait_until="domcontentloaded", timeout=30000)
    except Exception:
        await campaign_page_diagnostic(page, user_id, "campaign2 goto timeout/error", emit)
        raise
    try:
        await page.wait_for_load_state("networkidle", timeout=10000)
    except Exception:
        pass
    await wait_for_campaign2_popup(page)
    soup = BeautifulSoup(await page.content(), "html.parser")
    block_divs = soup.find_all("div", style=re.compile(r"display\s*:\s*block", re.IGNORECASE), class_=lambda x: x != "dimmed")
    block_div = block_divs[0] if block_divs else None
    if block_div is None:
        await campaign_page_diagnostic(page, user_id, "campaign2 result popup missing", emit)
        return DetailResult(user_id=user_id, url=link, status="skipped", message="result popup not found")

    text = block_div.get_text(" ", strip=True)
    status = "visited"
    if "1회만" in text and "적립" in text:
        message = "already received once"
        return DetailResult(user_id=user_id, url=link, status="skipped", message=message)
    if "적립돼요" in text:
        dwell_match = re.search(r"(\d+)\s*초\s*이상", text or "")
        required_dwell = int(dwell_match.group(1)) if dwell_match else 3
        dwell_seconds = required_dwell + 3
        try:
            await click_campaign2_point_button_and_dwell(page, dwell_seconds, user_id, emit)
        except Exception:
            await campaign_page_diagnostic(page, user_id, "campaign2 point button click failed", emit)
            raise
        status = "visited"
        message = f"visited by dwell rule: {required_dwell}s+"
    else:
        message = text
        if "적립 기간이 아닙니다" in text:
            campaign_url = session_db.query(CampaignUrl).filter_by(url=link).first()
            if campaign_url:
                campaign_url.is_available = False
            status = "unavailable"
    await asyncio.sleep(3)
    return DetailResult(user_id=user_id, url=link, status=status, point=0, message=message)


async def wait_for_campaign2_popup(page, timeout_ms: int = 12000):
    deadline = time.time() + (timeout_ms / 1000)
    while time.time() < deadline:
        try:
            body_text = await page.locator("body").inner_text(timeout=1000)
        except Exception:
            body_text = ""
        if "포인트 받기" in body_text or "1회만" in body_text or "적립돼요" in body_text:
            return
        await asyncio.sleep(0.5)


async def click_campaign2_point_button_and_dwell(page, dwell_seconds: float, user_id: str, emit: Callable[[str], None]):
    context = page.context
    before_pages = list(context.pages)
    before_url = page.url
    clicked = await page.evaluate(
        """() => {
            const norm = s => (s || '').replace(/\\s+/g, ' ').trim();
            const candidates = Array.from(document.querySelectorAll('a.popup_link, button, a, [role="button"], [onclick]'));
            for (const el of candidates) {
                const text = norm(el.innerText || el.textContent);
                if (!text.includes('포인트 받기')) continue;
                const rect = el.getBoundingClientRect();
                const style = window.getComputedStyle(el);
                if (rect.width <= 0 || rect.height <= 0 || style.display === 'none' || style.visibility === 'hidden') continue;
                el.click();
                return true;
            }
            return false;
        }"""
    )
    if not clicked:
        raise RuntimeError("campaign2 point button not found")
    await asyncio.sleep(1)
    pages = list(context.pages)
    new_pages = [candidate for candidate in pages if candidate not in before_pages]
    active_page = new_pages[-1] if new_pages else page
    try:
        await active_page.wait_for_load_state("domcontentloaded", timeout=5000)
    except Exception:
        pass
    try:
        await active_page.bring_to_front()
    except Exception:
        pass
    await asyncio.sleep(dwell_seconds)
    emit(f"{user_id}: visited {active_page.url}")
    return active_page


async def handle_pincrux_shopping_live(page, link: str, user_id: str, emit: Callable[[str], None]):
    await campaign_page_diagnostic(page, user_id, "pincrux shopping live loaded", emit)
    short_reward_count = await handle_pincrux_short_live_cards(page, user_id, emit)
    clicked = False
    watched_page = page
    live_watch_seconds = await extract_pincrux_watch_seconds(page, default_seconds=5)
    live_dwell_seconds = live_watch_seconds + 3
    candidates = [
        ("text=라이브 중이에요!", True),
        ("text=5초 보면", True),
        ("xpath=//*[contains(normalize-space(.), '라이브 중이에요!')]/ancestor::*[self::a or self::button or @role='button' or @onclick][1]", False),
        ("xpath=//*[contains(normalize-space(.), '5초 보면')]/ancestor::*[self::a or self::button or @role='button' or @onclick][1]", False),
    ]
    for selector, js_click in candidates:
        try:
            locator = page.locator(selector).first
            if await locator.count() == 0:
                continue
            watched_page = await click_and_dwell(page, locator, live_dwell_seconds, user_id, emit, f"pincrux live selector={selector}", js_click=js_click)
            clicked = True
            break
        except Exception as e:
            emit(f"{user_id}: pincrux live click failed selector={selector} error={e}")
    if not clicked:
        await campaign_page_diagnostic(page, user_id, "pincrux live click target missing", emit)
        return DetailResult(user_id=user_id, url=link, status="skipped", message="pincrux live click target not found")

    completed = False
    finish_selectors = [
        "text=끝",
        "text=포인트 받기",
        "text=받기",
        "xpath=//*[contains(normalize-space(.), '끝') or contains(normalize-space(.), '포인트 받기') or contains(normalize-space(.), '받기')]/ancestor::*[self::a or self::button or @role='button' or @onclick][1]",
    ]
    for selector in finish_selectors:
        try:
            locator = watched_page.locator(selector).first
            if await locator.count() == 0:
                continue
            await click_and_dwell(watched_page, locator, 2, user_id, emit, f"pincrux live finish selector={selector}", js_click=selector.startswith("text="))
            completed = True
            break
        except Exception as e:
            emit(f"{user_id}: pincrux live finish click failed selector={selector} error={e}")
    await campaign_page_diagnostic(watched_page, user_id, "pincrux live watched", emit)
    try:
        body_text = await watched_page.locator("body").inner_text(timeout=3000)
    except Exception:
        body_text = ""
    message = f"pincrux live clicked, watched {live_dwell_seconds}s, and finish clicked" if completed else f"pincrux live clicked and watched {live_dwell_seconds}s; finish button not found"
    if short_reward_count > 0:
        message += f"; short live cards visited: {short_reward_count}"
    return DetailResult(user_id=user_id, url=link, status="visited", message=message)


async def extract_pincrux_watch_seconds(page, default_seconds: int = 5) -> int:
    try:
        body_text = await page.locator("body").inner_text(timeout=2000)
    except Exception:
        body_text = ""
    patterns = [
        r"(\d+)\s*초\s*이상\s*시청",
        r"(\d+)\s*초\s*보면",
    ]
    for pattern in patterns:
        match = re.search(pattern, body_text or "")
        if match:
            return max(1, int(match.group(1)))
    return max(1, int(default_seconds or 5))


async def handle_pincrux_short_live_cards(page, user_id: str, emit: Callable[[str], None]) -> int:
    await expand_pincrux_live_more(page)
    try:
        rows = await collect_pincrux_short_live_rows(page)
    except Exception:
        return 0

    visited = 0
    source_url = page.url
    for row in rows[:40]:
        href = str(row.get("href") or "")
        target_id = str(row.get("id") or "")
        if not href and not target_id:
            continue
        seconds = max(1, int(row.get("seconds") or 1))
        work_page = None
        try:
            if href:
                work_page = await page.context.new_page()
                await work_page.goto(href, wait_until="domcontentloaded", timeout=30000)
                active_page = work_page
            else:
                before_pages = list(page.context.pages)
                locator = page.locator(f'[data-naverpaper-short-live-id="{target_id}"]').first
                if await locator.count() == 0:
                    continue
                await locator.click(timeout=5000, force=True)
                await asyncio.sleep(1)
                pages = list(page.context.pages)
                active_page = ([candidate for candidate in pages if candidate not in before_pages] or [page])[-1]
            try:
                await active_page.wait_for_load_state("domcontentloaded", timeout=3000)
            except Exception:
                pass
            active_page = await follow_pincrux_note_reward_button(active_page, seconds)
            await asyncio.sleep(seconds + 3)
            emit(f"{user_id}: visited {active_page.url}")
            visited += 1
            if work_page is None and active_page is not page:
                try:
                    await active_page.close()
                except Exception:
                    pass
                try:
                    await page.bring_to_front()
                except Exception:
                    pass
            if work_page is None and page.url != source_url:
                try:
                    await page.goto(source_url, wait_until="domcontentloaded", timeout=20000)
                    await expand_pincrux_live_more(page)
                except Exception:
                    pass
        except Exception as e:
            emit(f"{user_id}: pincrux short live card failed - {e}")
        finally:
            if work_page is not None:
                try:
                    await work_page.close()
                except Exception:
                    pass
                try:
                    await page.bring_to_front()
                except Exception:
                    pass
    return visited


async def expand_pincrux_live_more(page, max_clicks: int = 5):
    for _ in range(max_clicks):
        try:
            button = page.locator("button, a, [role='button']").filter(has_text="라이브 더 보기").first
            if await button.count() == 0:
                break
            await button.scroll_into_view_if_needed(timeout=2000)
            await button.click(timeout=3000, force=True)
            await asyncio.sleep(0.7)
        except Exception:
            break


async def collect_pincrux_short_live_rows(page):
    return await page.evaluate(
        """() => {
            const norm = s => (s || '').replace(/\\s+/g, ' ').trim();
            const toHref = el => {
                const href = el.href || '';
                if (href) return href;
                const onclick = el.getAttribute('onclick') || '';
                const match = onclick.match(/Go\\(['"]([^'"]+)['"]/);
                if (!match) return '';
                try {
                    return new URL(match[1].replace(/&amp;/g, '&'), location.href).href;
                } catch (e) {
                    return match[1].replace(/&amp;/g, '&');
                }
            };
            const candidates = [];
            const seen = new Set();
            const roots = Array.from(document.querySelectorAll('a[href], button, [role="button"], [onclick], .comming-list-item'));
            for (const el of roots) {
                const text = norm(el.innerText || el.textContent);
                const onclick = el.getAttribute('onclick') || '';
                const href = toHref(el);
                const secondsMatch = text.match(/(\\d+)\\s*초\\s*보면/);
                const pointMatch = text.match(/(\\d+)\\s*원/);
                const isShortLive =
                    secondsMatch ||
                    ((href.includes('note.html') || onclick.includes('note.html')) && pointMatch) ||
                    ((href.includes('live-view.html') || onclick.includes('live-view.html')) && pointMatch);
                if (!isShortLive || /적립\\s*완료|적립완료/.test(text)) continue;
                const rect = el.getBoundingClientRect();
                if (rect.width <= 0 || rect.height <= 0) continue;
                if (!el.dataset.naverpaperShortLiveId) {
                    el.dataset.naverpaperShortLiveId = String(candidates.length + 1);
                }
                let seconds = secondsMatch ? Number(secondsMatch[1] || 1) : 1;
                if (!secondsMatch && (href.includes('live-view.html') || onclick.includes('live-view.html'))) {
                    seconds = 5;
                }
                const key = href || el.dataset.naverpaperShortLiveId || text;
                if (seen.has(key)) continue;
                seen.add(key);
                candidates.push({
                    id: el.dataset.naverpaperShortLiveId,
                    href,
                    text,
                    seconds,
                    point: pointMatch ? Number(pointMatch[1] || 0) : 0
                });
            }
            return candidates;
        }"""
    )


async def follow_pincrux_note_reward_button(page, seconds: int):
    try:
        body_text = await page.locator("body").inner_text(timeout=2000)
    except Exception:
        body_text = ""
    if "초 보고" not in body_text or "적립" not in body_text:
        return page
    before_pages = list(page.context.pages)
    clicked = await page.evaluate(
        """(seconds) => {
            const norm = s => (s || '').replace(/\\s+/g, ' ').trim();
            const exact = new RegExp(String(seconds) + '\\\\s*초\\\\s*보고\\\\s*\\\\d+\\\\s*원\\\\s*적립');
            const loose = /\\d+\\s*초\\s*보고\\s*\\d+\\s*원\\s*적립/;
            for (const el of Array.from(document.querySelectorAll('button, a, [role="button"], [onclick]'))) {
                const text = norm(el.innerText || el.textContent);
                if (!exact.test(text) && !loose.test(text)) continue;
                const rect = el.getBoundingClientRect();
                const style = window.getComputedStyle(el);
                if (rect.width <= 0 || rect.height <= 0 || style.display === 'none' || style.visibility === 'hidden') continue;
                el.click();
                return true;
            }
            return false;
        }""",
        seconds,
    )
    if not clicked:
        return page
    await asyncio.sleep(1)
    pages = list(page.context.pages)
    new_pages = [candidate for candidate in pages if candidate not in before_pages]
    active_page = new_pages[-1] if new_pages else page
    try:
        await active_page.wait_for_load_state("domcontentloaded", timeout=5000)
    except Exception:
        pass
    return active_page


def ppomppu_target_url(link: str) -> str:
    try:
        target = (parse_qs(urlparse(link).query).get("target") or [""])[0]
        if not target:
            return ""
        padding = "=" * (-len(target) % 4)
        return base64.urlsafe_b64decode((target + padding).encode("ascii")).decode("utf-8", errors="ignore")
    except Exception:
        return ""


async def visit_campaign_url(page, link: str, session_db, user_id: str, emit: Callable[[str], None]):
    if link.startswith("https://campaign2"):
        return await process_campaign2_link(page, link, session_db, user_id, emit)

    try:
        await page.goto(link, wait_until="domcontentloaded", timeout=20000)
    except Exception:
        await campaign_page_diagnostic(page, user_id, "generic campaign goto timeout/error", emit)
        raise
    try:
        await page.wait_for_load_state("networkidle", timeout=5000)
    except Exception:
        pass
    emit(f"{user_id}: visited {page.url}")
    if page.url.startswith("https://campaign2"):
        return await process_campaign2_link(page, page.url, session_db, user_id, emit)
    if "point.pay.naver.com/mission-detail" in page.url or "point.pay.naver.com/pc/mission-detail" in page.url:
        return await handle_naverpay_mission_detail(page, link, session_db, user_id, emit)
    if "nsl.pincrux.com/shopping-live" in page.url or "nsl.pincrux.com/shopping-live" in link:
        return await handle_pincrux_shopping_live(page, link, user_id, emit)
    return DetailResult(user_id=user_id, url=link, status="visited", message=f"visited: {page.url}")


async def handle_naverpay_mission_detail(page, link: str, session_db, user_id: str, emit: Callable[[str], None]):
    try:
        await collect_lazy_page_content(page)
        rows = await collect_naverpay_mission_reward_rows(page)
    except Exception:
        rows = []

    visited = 0
    source_url = page.url
    for row in rows[:40]:
        href = str(row.get("href") or "")
        target_id = str(row.get("id") or "")
        if not href and not target_id:
            continue
        work_page = None
        try:
            if href:
                if href == source_url:
                    continue
                work_page = await page.context.new_page()
                await work_page.goto(href, wait_until="domcontentloaded", timeout=30000)
                active_page = work_page
            else:
                before_pages = list(page.context.pages)
                locator = page.locator(f'[data-naverpaper-mission-id="{target_id}"]').first
                if await locator.count() == 0:
                    continue
                await locator.scroll_into_view_if_needed(timeout=2000)
                await locator.click(timeout=5000, force=True)
                await asyncio.sleep(1)
                pages = list(page.context.pages)
                active_page = ([candidate for candidate in pages if candidate not in before_pages] or [page])[-1]
            try:
                await active_page.wait_for_load_state("domcontentloaded", timeout=5000)
            except Exception:
                pass
            await wait_for_bridge_redirect(active_page)
            if active_page.url.startswith("https://campaign2"):
                detail = await process_campaign2_link(active_page, active_page.url, session_db, user_id, emit)
                if detail.status not in ("skipped", "unavailable", "no_url", "error"):
                    visited += 1
            else:
                await asyncio.sleep(4)
                emit(f"{user_id}: visited {active_page.url}")
                visited += 1
            if work_page is None and active_page is not page:
                try:
                    await active_page.close()
                except Exception:
                    pass
            if work_page is None and page.url != source_url:
                try:
                    await page.goto(source_url, wait_until="domcontentloaded", timeout=20000)
                    await collect_lazy_page_content(page)
                except Exception:
                    pass
        except Exception as e:
            emit(f"{user_id}: naverpay mission click failed - {e}")
        finally:
            if work_page is not None:
                try:
                    await work_page.close()
                except Exception:
                    pass
                try:
                    await page.bring_to_front()
                except Exception:
                    pass
    return DetailResult(user_id=user_id, url=link, status="visited", message=f"mission rewards visited: {visited}")


async def wait_for_bridge_redirect(page, timeout_ms: int = 8000):
    start_url = page.url
    deadline = time.time() + (timeout_ms / 1000)
    while time.time() < deadline:
        current_url = page.url
        if current_url != start_url and "point.pay.naver.com/bridge/eventbenefit" not in current_url:
            break
        try:
            await page.wait_for_load_state("domcontentloaded", timeout=1000)
        except Exception:
            pass
        await asyncio.sleep(0.5)
    try:
        await page.wait_for_load_state("networkidle", timeout=2000)
    except Exception:
        pass


async def collect_lazy_page_content(page):
    try:
        last_height = 0
        stable_count = 0
        for _ in range(12):
            height = await page.evaluate("() => document.body.scrollHeight")
            if height == last_height:
                stable_count += 1
            else:
                stable_count = 0
            if stable_count >= 2:
                break
            last_height = height
            await page.evaluate("() => window.scrollTo(0, document.body.scrollHeight)")
            await asyncio.sleep(0.7)
        await page.evaluate("() => window.scrollTo(0, 0)")
        await asyncio.sleep(0.2)
    except Exception:
        pass


async def collect_naverpay_mission_reward_rows(page):
    return await page.evaluate(
        """() => {
            const norm = s => (s || '').replace(/\\s+/g, ' ').trim();
            const candidates = [];
            const seen = new Set();
            const elements = Array.from(document.querySelectorAll('a[href], button, [role="button"], [onclick]'));
            for (const el of elements) {
                const row = el.closest('li, article, section, div') || el;
                const text = norm(row.innerText || row.textContent);
                if (!/클릭\\s*\\d+원/.test(text)) continue;
                if (/적립\\s*완료|적립완료/.test(text)) continue;
                const actionText = norm(el.innerText || el.textContent);
                if (actionText && !/(혜택보기|쿠폰받기|확인하기|적립|받기|클릭)/.test(actionText) && !/클릭\\s*\\d+원/.test(actionText)) continue;
                const rect = el.getBoundingClientRect();
                if (rect.width <= 0 || rect.height <= 0) continue;
                if (!el.dataset.naverpaperMissionId) {
                    el.dataset.naverpaperMissionId = String(candidates.length + 1);
                }
                const href = el.href || '';
                const key = href || el.dataset.naverpaperMissionId || text;
                if (seen.has(key)) continue;
                seen.add(key);
                candidates.push({ id: el.dataset.naverpaperMissionId, href, text });
            }
            return candidates;
        }"""
    )


async def process_campaign_links(page, campaign_links, session_db, user_id: str, emit: Callable[[str], None]):
    details = []
    for link in campaign_links:
        try:
            if link.startswith("https://campaign2"):
                detail = await process_campaign2_link(page, link, session_db, user_id, emit)
            elif link.startswith("https://s.ppomppu.co.kr"):
                fallback_url = ppomppu_target_url(link)
                try:
                    await page.goto(link, wait_until="domcontentloaded", timeout=10000)
                    try:
                        await page.wait_for_load_state("networkidle", timeout=5000)
                    except Exception:
                        pass
                except Exception as e:
                    await campaign_page_diagnostic(page, user_id, "ppomppu redirect timeout/error", emit)
                    emit(f"{user_id}: ppomppu redirect timeout; fallback={fallback_url or 'none'} error={e}")
                redirected_url = page.url
                target_url = redirected_url
                if redirected_url.startswith("https://s.ppomppu.co.kr") and fallback_url:
                    target_url = fallback_url
                if target_url.startswith("https://campaign2"):
                    detail = await process_campaign2_link(page, target_url, session_db, user_id, emit)
                else:
                    detail = await visit_campaign_url(page, target_url, session_db, user_id, emit)
                detail.url = link
            else:
                detail = await visit_campaign_url(page, link, session_db, user_id, emit)
            existing_visit = session_db.query(UrlVisit).filter_by(url=link, user_id=user_id).first()
            if not existing_visit:
                session_db.add(UrlVisit(url=link, user_id=user_id, visited_at=datetime.now()))
            details.append(detail)
        except Exception as e:
            details.append(DetailResult(user_id=user_id, url=link, status="error", message=str(e)))
            emit(f"{user_id}: campaign URL error - {link} - {e}")
    return details


def playwright_proxy(proxy_url: str):
    proxy_url = str(proxy_url or "").strip()
    if not proxy_url:
        return None
    parsed = urlparse(proxy_url)
    if parsed.scheme and parsed.hostname:
        host = parsed.hostname
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        server = f"{parsed.scheme}://{host}"
        if parsed.port:
            server = f"{server}:{parsed.port}"
        proxy = {"server": server}
        if parsed.username:
            proxy["username"] = unquote(parsed.username)
        if parsed.password:
            proxy["password"] = unquote(parsed.password)
        return proxy
    return {"server": proxy_url}


def mask_proxy_url(proxy_url: str) -> str:
    proxy_url = str(proxy_url or "").strip()
    if not proxy_url:
        return "disabled"
    parsed = urlparse(proxy_url)
    if parsed.scheme and parsed.hostname:
        host = parsed.hostname
        if ":" in host and not host.startswith("["):
            host = f"[{host}]"
        server = f"{parsed.scheme}://{host}"
        if parsed.port:
            server = f"{server}:{parsed.port}"
        if parsed.username:
            return f"{server} (auth)"
        return server
    return proxy_url


async def process_account(
    account: AccountConfig,
    session_db,
    cookie_dir: str,
    reward_proxy_url: str,
    emit: Callable[[str], None],
    login_proxy_url: str = "",
) -> AccountResult:
    from playwright.async_api import async_playwright
    from playwright_stealth import Stealth

    result = AccountResult(user_id=account.user_id)
    campaign_links, already_visited_count = await fetch_naver_campaign_urls(session_db, account.user_id)
    result.skipped_url_count = already_visited_count
    if not campaign_links:
        return result

    storage_state = await ensure_cookie_storage_state(account, session_db, cookie_dir, login_proxy_url, emit)
    if not storage_state:
        result.details.append(DetailResult(user_id=account.user_id, status="login_error", message="cookie auto refresh failed"))
        return result
    os.makedirs(cookie_dir, exist_ok=True)
    async with async_playwright() as playwright:
        launch_kwargs = {"headless": True}
        proxy = playwright_proxy(reward_proxy_url)
        if proxy:
            launch_kwargs["proxy"] = proxy
        browser = await playwright.chromium.launch(**launch_kwargs)
        context = await browser.new_context(
            user_agent=IPHONE13_UA,
            viewport={"width": 390, "height": 844},
            device_scale_factor=3,
            is_mobile=True,
            has_touch=True,
            storage_state=storage_state,
        )
        page = await context.new_page()
        await Stealth().apply_stealth_async(page)

        login_ok, login_message = await naver_login(page, account, cookie_dir, emit)
        if login_ok:
            before_points = await get_naverpay_point_balance(page)
            details = await process_campaign_links(page, campaign_links, session_db, account.user_id, emit)
            after_points = await get_naverpay_point_balance(page)
            result.details.extend(details)
            result.skipped_url_count += len([x for x in details if x.status in ("skipped", "unavailable", "no_url")])
            result.visited_url_count = len([x for x in details if x.status not in ("error", "skipped", "unavailable", "no_url")])
            if before_points is not None and after_points is not None:
                result.estimated_points = max(0, after_points - before_points)
                emit(f"{account.user_id}: point balance {before_points} -> {after_points} (+{result.estimated_points})")
            else:
                result.estimated_points = 0
            new_storage = await context.storage_state()
            session_db.merge(User(user_id=account.user_id, storage_state=json.dumps(new_storage), updated_at=datetime.now()))
            with open(cookie_path(cookie_dir, account.user_id), "w", encoding="utf-8") as f:
                json.dump(new_storage, f, ensure_ascii=False, indent=2)
        else:
            result.details.append(DetailResult(user_id=account.user_id, status="login_error", message=login_message))
        await context.close()
        await browser.close()

    return result


async def get_naverpay_point_balance(page) -> Optional[int]:
    try:
        await page.goto("https://pay.naver.com/pointshistory/list?category=all", wait_until="domcontentloaded", timeout=30000)
        await page.wait_for_timeout(5000)
        body = await page.locator("body").inner_text(timeout=5000)
        lines = [x.strip() for x in body.splitlines() if x.strip()]
        for idx, line in enumerate(lines):
            if line == "내 포인트":
                for item in lines[idx + 1 : idx + 8]:
                    if item.endswith("원") and any(ch.isdigit() for ch in item):
                        return int(re.sub(r"[^0-9]", "", item) or "0")
    except Exception:
        pass
    return None


async def process_account_manual_link(
    account: AccountConfig,
    session_db,
    cookie_dir: str,
    reward_proxy_url: str,
    link: str,
    emit: Callable[[str], None],
    login_proxy_url: str = "",
) -> AccountResult:
    from playwright.async_api import async_playwright
    from playwright_stealth import Stealth

    result = AccountResult(user_id=account.user_id)
    storage_state = await ensure_cookie_storage_state(account, session_db, cookie_dir, login_proxy_url, emit)
    if not storage_state:
        result.details.append(DetailResult(user_id=account.user_id, status="login_error", message="cookie auto refresh failed"))
        return result
    os.makedirs(cookie_dir, exist_ok=True)
    async with async_playwright() as playwright:
        launch_kwargs = {"headless": True}
        proxy = playwright_proxy(reward_proxy_url)
        if proxy:
            launch_kwargs["proxy"] = proxy
        browser = await playwright.chromium.launch(**launch_kwargs)
        context = await browser.new_context(
            user_agent=IPHONE13_UA,
            viewport={"width": 390, "height": 844},
            device_scale_factor=3,
            is_mobile=True,
            has_touch=True,
            storage_state=storage_state,
        )
        page = await context.new_page()
        await Stealth().apply_stealth_async(page)

        login_ok, login_message = await naver_login(page, account, cookie_dir, emit)
        if login_ok:
            before_points = await get_naverpay_point_balance(page)
            details = await process_campaign_links(page, [link], session_db, account.user_id, emit)
            after_points = await get_naverpay_point_balance(page)
            result.details.extend(details)
            result.skipped_url_count += len([x for x in details if x.status in ("skipped", "unavailable", "no_url")])
            result.visited_url_count = len([x for x in details if x.status not in ("error", "skipped", "unavailable", "no_url")])
            if before_points is not None and after_points is not None:
                result.estimated_points = max(0, after_points - before_points)
                emit(f"{account.user_id}: point balance {before_points} -> {after_points} (+{result.estimated_points})")
            else:
                result.estimated_points = 0
            new_storage = await context.storage_state()
            session_db.merge(User(user_id=account.user_id, storage_state=json.dumps(new_storage), updated_at=datetime.now()))
            with open(cookie_path(cookie_dir, account.user_id), "w", encoding="utf-8") as f:
                json.dump(new_storage, f, ensure_ascii=False, indent=2)
        else:
            result.details.append(DetailResult(user_id=account.user_id, url=link, status="login_error", message=login_message))
        await context.close()
        await browser.close()

    return result


async def fetch(url: str, session, proxy_url: str = "", user_agent: str = IPHONE13_UA):
    headers = {"User-Agent": user_agent or IPHONE13_UA}
    proxy = str(proxy_url or "").strip() or None
    async with session.get(url, headers=headers, proxy=proxy, timeout=20) as response:
        return await response.text(errors="ignore")


def _looks_like_cloudflare(html: str) -> bool:
    if not html:
        return True
    lowered = html.lower()
    return "just a moment" in lowered or "cf_chl_" in lowered or "challenge-platform" in lowered or "verify you are human" in lowered


async def fetch_with_playwright(url: str, proxy_url: str = "") -> str:
    from playwright.async_api import async_playwright
    from playwright_stealth import Stealth

    async with async_playwright() as pw:
        launch_kwargs = {"headless": True}
        proxy = playwright_proxy(proxy_url)
        if proxy:
            launch_kwargs["proxy"] = proxy
        browser = await pw.chromium.launch(**launch_kwargs)
        context = await browser.new_context(
            user_agent=IPHONE13_UA,
            locale="ko-KR",
            viewport={"width": 390, "height": 844},
            is_mobile=True,
            has_touch=True,
        )
        page = await context.new_page()
        await Stealth().apply_stealth_async(page)
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            try:
                await page.wait_for_load_state("networkidle", timeout=10000)
            except Exception:
                pass
            return await page.content()
        finally:
            await context.close()
            await browser.close()


async def get_html(url: str, session, emit: Callable[[str], None], proxy_url: str = "", user_agent: str = IPHONE13_UA) -> str:
    html = await fetch(url, session, proxy_url, user_agent=user_agent)
    if _looks_like_cloudflare(html):
        try:
            html = await fetch_with_playwright(url, proxy_url)
        except Exception as e:
            emit(f"{url}: Playwright fallback failed - {e}")
    return html or ""


async def get_soup(url: str, session, emit: Callable[[str], None], proxy_url: str = "", user_agent: str = IPHONE13_UA) -> BeautifulSoup:
    html = await get_html(url, session, emit, proxy_url, user_agent=user_agent)
    return BeautifulSoup(html or "", "html.parser")


async def process_url(url, session, process_func, collected_urls, emit, proxy_url: str = ""):
    soup = await get_soup(url, session, emit, proxy_url)
    await process_func(url, soup, session, collected_urls, emit, proxy_url)


async def process_clien_url(url, soup, session, collected_urls, emit, proxy_url: str = ""):
    initial_count = len(collected_urls)
    naver_links = []
    for span in soup.select('[class="list_item symph-row"]'):
        a_tag = span.select_one(':-soup-contains("네이버")')
        if a_tag and span.get("href"):
            naver_links.append(span["href"])

    for link in naver_links:
        full_link = urljoin(url, link)
        inner_soup = BeautifulSoup(await fetch(full_link, session, proxy_url), "html.parser")
        for a_tag in inner_soup.find_all("a", href=True):
            href = a_tag["href"]
            if (href.startswith("https://campaign2.naver.com") or href.startswith("https://ofw.adison.co")) and len(href) > 40:
                add_reward_url(collected_urls, href)


async def process_ppomppu_url(url, soup, session, collected_urls, emit, proxy_url: str = ""):
    initial_count = len(collected_urls)
    base_url = "https://m.ppomppu.co.kr"
    naver_links = []
    for a_tag in soup.find_all("a", href=True):
        if "네이버페이" in a_tag.get_text():
            naver_links.append(a_tag["href"])

    for link in naver_links:
        full_link = urljoin(base_url, link)
        inner_soup = BeautifulSoup(await fetch(full_link, session, proxy_url), "html.parser")
        for a_tag in inner_soup.find_all("a", class_="noeffect", href=True):
            href = a_tag["href"]
            if href.startswith("https://s.ppomppu.co.kr?idno=coupon") and len(href) > 40:
                collected_urls.add(href)


def is_reward_url(href: str) -> bool:
    if not href or not href.startswith(REWARD_URL_PREFIXES) or len(href) <= 40:
        return False
    if href.startswith("https://campaign2.naver.com"):
        event_id = (parse_qs(urlparse(href).query).get("eventId") or [""])[0]
        return len(event_id) >= 10
    return True


def canonical_reward_url(href: str) -> str:
    href = str(href or "").strip()
    if href.startswith("https://campaign2.naver.com"):
        event_id = (parse_qs(urlparse(href).query).get("eventId") or [""])[0]
        if event_id:
            return f"https://campaign2.naver.com/npay/v2/click-point/?eventId={event_id}"
    return href


def add_reward_url(collected_urls, href: str):
    href = canonical_reward_url(href)
    if is_reward_url(href):
        collected_urls.add(href)


def _decode_js_string(value: str) -> str:
    try:
        return json.loads(f'"{value}"')
    except Exception:
        return value


def extract_campaign_urls_from_html(html: str) -> list[str]:
    normalized = html_lib.unescape(
        str(html or "")
        .replace("\\/", "/")
        .replace("\\u0026", "&")
        .replace("\\u002F", "/")
        .replace("\\u003c", "<")
        .replace("\\u003C", "<")
        .replace("\\u003e", ">")
        .replace("\\u003E", ">")
        .replace("\\u003D", "=")
        .replace("\\u003d", "=")
        .replace("\\u003F", "?")
        .replace("\\u003f", "?")
        .replace("&amp;", "&")
    )
    urls = []
    seen = set()
    pattern = r"https://(?:campaign2\.naver\.com|ofw\.adison\.co|external-token\.pay\.naver\.com|point\.pay\.naver\.com/bridge/eventbenefit)[^\"'<>\\\s]+"
    for match in re.finditer(pattern, normalized):
        href = canonical_reward_url(match.group(0).rstrip(").,"))
        if is_reward_url(href) and href not in seen:
            seen.add(href)
            urls.append(href)
    return urls


def extract_damoang_post_links(html: str, base_url: str) -> list[str]:
    match = re.search(r"posts:\[(.*?)\],notices:", str(html or ""), re.DOTALL)
    if not match:
        return []
    links = []
    seen = set()
    pattern = r"\{id:(\d+),title:\"((?:\\.|[^\"\\])*)\".*?category:\"((?:\\.|[^\"\\])*)\""
    for item in re.finditer(pattern, match.group(1), re.DOTALL):
        post_id = item.group(1)
        title = _decode_js_string(item.group(2))
        category = _decode_js_string(item.group(3))
        if "네이버페이" not in title and "네이버페이" not in category:
            continue
        link = f"{base_url.rstrip('/')}/{post_id}"
        if link not in seen:
            seen.add(link)
            links.append(link)
    return links


async def process_damoang_url(url, soup, session, collected_urls, emit, proxy_url: str = ""):
    from playwright.async_api import async_playwright
    from playwright_stealth import Stealth

    initial_count = len(collected_urls)
    launch_kwargs = {"headless": True}
    proxy = playwright_proxy(proxy_url)
    if proxy:
        launch_kwargs["proxy"] = proxy

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(**launch_kwargs)
        context = await browser.new_context(
            user_agent=DESKTOP_UA,
            locale="ko-KR",
            viewport={"width": 1365, "height": 900},
        )
        page = await context.new_page()
        await Stealth().apply_stealth_async(page)
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            try:
                await page.wait_for_load_state("networkidle", timeout=10000)
            except Exception:
                pass
            page_html = await page.content()
            list_soup = BeautifulSoup(page_html, "html.parser")

            naver_links = []
            for a_tag in list_soup.find_all("a", href=True):
                text = (a_tag.get_text() or "").strip()
                href = a_tag.get("href")
                if href and text and "네이버페이" in text:
                    full_link = urljoin(url, href)
                    if full_link not in naver_links:
                        naver_links.append(full_link)

            for link in extract_damoang_post_links(page_html, url):
                if link not in naver_links:
                    naver_links.append(link)

            for idx, link in enumerate(naver_links, 1):
                try:
                    await page.goto(link, wait_until="domcontentloaded", timeout=30000)
                    try:
                        await page.wait_for_load_state("networkidle", timeout=7000)
                    except Exception:
                        pass
                    detail_html = await page.content()
                    inner_soup = BeautifulSoup(detail_html, "html.parser")
                    before_count = len(collected_urls)
                    for a_tag in inner_soup.find_all("a", href=True):
                        href = a_tag.get("href")
                        if is_reward_url(href):
                            add_reward_url(collected_urls, href)
                    for href in extract_campaign_urls_from_html(detail_html):
                        add_reward_url(collected_urls, href)
                except Exception as e:
                    emit(f"damoang post playwright error {idx}/{len(naver_links)}: {e}")
        finally:
            await context.close()
            await browser.close()


async def process_naverpay_point_main(
    account: AccountConfig,
    session_db,
    cookie_dir: str,
    collected_urls,
    emit,
    reward_proxy_url: str = "",
    login_proxy_url: str = "",
):
    from playwright.async_api import async_playwright
    from playwright_stealth import Stealth

    url = NAVERPAY_MAIN_URL
    initial_count = len(collected_urls)
    storage_state = await ensure_cookie_storage_state(account, session_db, cookie_dir, login_proxy_url, emit)
    if not storage_state:
        emit(f"naverpay point main skipped for {account.user_id}: cookie missing")
        return

    launch_kwargs = {"headless": True}
    proxy = playwright_proxy(reward_proxy_url)
    if proxy:
        launch_kwargs["proxy"] = proxy

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(**launch_kwargs)
        context = await browser.new_context(
            user_agent=DESKTOP_UA,
            locale="ko-KR",
            viewport={"width": 1365, "height": 1000},
            storage_state=storage_state,
        )
        page = await context.new_page()
        await Stealth().apply_stealth_async(page)
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=30000)
            await page.wait_for_timeout(8000)
            body_text = await page.locator("body").inner_text(timeout=5000)
            if "로그인" in body_text[:500] and "포인트" not in body_text[:500]:
                return

            point_rows = await _naverpay_text_hrefs(page, "클릭하고")
            live_rows = await _naverpay_text_hrefs(page, "쇼핑라이브 보고")

            for href, text in live_rows:
                if is_reward_url(href):
                    add_reward_url(collected_urls, href)

            for href, text in point_rows:
                if is_reward_url(href):
                    add_reward_url(collected_urls, href)
                    emit(f"naverpay point main direct click link: {href}")
        finally:
            await context.close()
            await browser.close()


async def process_naverpay_mission_detail_source(
    account: AccountConfig,
    session_db,
    cookie_dir: str,
    collected_urls,
    emit,
    reward_proxy_url: str = "",
    login_proxy_url: str = "",
):
    from playwright.async_api import async_playwright
    from playwright_stealth import Stealth

    emit(f"naverpay mission detail started for {account.user_id}")
    storage_state = await ensure_cookie_storage_state(account, session_db, cookie_dir, login_proxy_url, emit)
    if not storage_state:
        emit(f"naverpay mission detail skipped for {account.user_id}: cookie missing")
        return

    launch_kwargs = {"headless": True}
    proxy = playwright_proxy(reward_proxy_url)
    if proxy:
        launch_kwargs["proxy"] = proxy
    emit(f"naverpay mission detail browser opening for {account.user_id} proxy={mask_proxy_url(reward_proxy_url)}")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(**launch_kwargs)
        context = await browser.new_context(
            user_agent=DESKTOP_UA,
            locale="ko-KR",
            viewport={"width": 1365, "height": 1000},
            storage_state=storage_state,
        )
        page = await context.new_page()
        await Stealth().apply_stealth_async(page)
        try:
            emit(f"naverpay mission detail page loading for {account.user_id}")
            await page.goto(NAVERPAY_MISSION_DETAIL_URL, wait_until="domcontentloaded", timeout=30000)
            emit(f"naverpay mission detail page loaded for {account.user_id}: {page.url}")
            await page.wait_for_timeout(3000)
            await collect_lazy_page_content(page)
            emit(f"naverpay mission detail lazy content collected for {account.user_id}")
            bridge_urls = await collect_naverpay_mission_source_urls(page)
            emit(f"naverpay mission detail bridge urls for {account.user_id}: {len(bridge_urls)}")
            campaign_urls = await resolve_naverpay_bridge_urls_to_campaign2(page, bridge_urls, emit, account.user_id)
            emit(f"naverpay mission detail campaign urls for {account.user_id}: {len(campaign_urls)}")
            for reward_url in campaign_urls:
                add_reward_url(collected_urls, reward_url)
        finally:
            await context.close()
            await browser.close()


async def collect_naverpay_mission_source_urls(page) -> list[str]:
    try:
        raw_rows = await page.evaluate(
            """() => {
                const norm = s => (s || '').replace(/\\s+/g, ' ').trim();
                const urls = [];
                const seen = new Set();
                for (const a of Array.from(document.querySelectorAll('a[href]'))) {
                    const href = a.href || '';
                    if (!href.startsWith('https://point.pay.naver.com/bridge/eventbenefit')) continue;
                    const text = norm(a.innerText || a.textContent);
                    if (!/클릭\\s*\\d+원/.test(text)) continue;
                    if (/적립\\s*완료|적립완료/.test(text)) continue;
                    const rect = a.getBoundingClientRect();
                    if (rect.width <= 0 || rect.height <= 0) continue;
                    if (seen.has(href)) continue;
                    seen.add(href);
                    urls.push(href);
                }
                return urls;
            }"""
        )
    except Exception:
        return []
    return [str(url or "").strip() for url in raw_rows or [] if is_reward_url(str(url or "").strip())]


async def resolve_naverpay_bridge_urls_to_campaign2(
    page,
    bridge_urls: list[str],
    emit: Optional[Callable[[str], None]] = None,
    user_id: str = "",
) -> list[str]:
    resolved = []
    seen = set()
    total = len(bridge_urls)
    for idx, bridge_url in enumerate(bridge_urls, 1):
        if emit and (idx == 1 or idx == total or idx % 5 == 0):
            emit(f"naverpay mission detail resolving bridge urls for {user_id}: {idx}/{total}")
        campaign_url = await resolve_naverpay_bridge_url_to_campaign2(page, bridge_url)
        if not campaign_url:
            campaign_url = bridge_url
        campaign_url = canonical_reward_url(campaign_url)
        if is_reward_url(campaign_url) and campaign_url not in seen:
            seen.add(campaign_url)
            resolved.append(campaign_url)
    return resolved


async def resolve_naverpay_bridge_url_to_campaign2(page, bridge_url: str) -> str:
    try:
        await page.goto(bridge_url, wait_until="domcontentloaded", timeout=30000)
    except Exception:
        return ""
    deadline = time.time() + 10
    while time.time() < deadline:
        current_url = page.url
        if current_url.startswith("https://campaign2.naver.com"):
            return current_url
        try:
            body_text = await page.locator("body").inner_text(timeout=500)
        except Exception:
            body_text = ""
        if ("1회만" in body_text or "포인트 받기" in body_text or "적립돼요" in body_text) and page.url.startswith("https://campaign2.naver.com"):
            return page.url
        await asyncio.sleep(0.5)
    return page.url if page.url.startswith("https://campaign2.naver.com") else ""



async def _naverpay_text_hrefs(page, needle: str) -> list[tuple[str, str]]:
    try:
        raw_rows = await page.evaluate(
            """needle => {
                const norm = s => (s || '').replace(/\\s+/g, ' ').trim();
                return Array.from(document.querySelectorAll('a[href]')).map(a => {
                    const rect = a.getBoundingClientRect();
                    return {
                        href: a.href,
                        text: norm(a.innerText || a.textContent),
                        visible: rect.width > 0 && rect.height > 0
                    };
                }).filter(row => row.visible && row.href && row.text.includes(needle));
            }""",
            needle,
        )
    except Exception:
        return []

    rows = []
    seen = set()
    for item in raw_rows or []:
        href = str(item.get("href") or "").strip()
        text = str(item.get("text") or "").strip()
        key = (href, text)
        if href and key not in seen:
            seen.add(key)
            rows.append((href, text))
    return rows


async def save_naver_campaign_urls(
    session_db,
    emit: Callable[[str], None],
    reward_proxy_url: str = "",
    accounts: Optional[list[AccountConfig]] = None,
    cookie_dir: str = "",
    login_proxy_url: str = "",
):
    from aiohttp import ClientSession, ClientTimeout

    collected_urls = set()
    urls = [
        ("https://www.clien.net/service/board/jirum", process_clien_url),
        ("https://m.ppomppu.co.kr/new/bbs_list.php?id=coupon&extref=1", process_ppomppu_url),
        ("https://damoang.net/economy", process_damoang_url),
    ]
    for account in accounts or []:
        try:
            before = len(collected_urls)
            emit(f"collection source {NAVERPAY_MISSION_DETAIL_URL} ({account.user_id}) started")
            await process_naverpay_mission_detail_source(
                account,
                session_db,
                cookie_dir,
                collected_urls,
                emit,
                reward_proxy_url,
                login_proxy_url,
            )
            emit(f"collection source {NAVERPAY_MISSION_DETAIL_URL} ({account.user_id}): +{len(collected_urls) - before}")
        except Exception as e:
            emit(f"{NAVERPAY_MISSION_DETAIL_URL}: collection error for {account.user_id} - {e}")

    timeout = ClientTimeout(total=30, connect=10, sock_connect=10, sock_read=20)
    async with ClientSession(timeout=timeout) as session:
        for url, process_func in urls:
            try:
                before = len(collected_urls)
                emit(f"collection source {url} started")
                if process_func is process_damoang_url:
                    await process_func(url, BeautifulSoup("", "html.parser"), session, collected_urls, emit, reward_proxy_url)
                else:
                    await process_url(url, session, process_func, collected_urls, emit, reward_proxy_url)
                emit(f"collection source {url}: +{len(collected_urls) - before}")
            except Exception as e:
                emit(f"{url}: collection error - {e}")

    for account in accounts or []:
        try:
            before = len(collected_urls)
            emit(f"collection source {NAVERPAY_MAIN_URL} ({account.user_id}) started")
            await process_naverpay_point_main(
                account,
                session_db,
                cookie_dir,
                collected_urls,
                emit,
                reward_proxy_url,
                login_proxy_url,
            )
            emit(f"collection source {NAVERPAY_MAIN_URL} ({account.user_id}): +{len(collected_urls) - before}")
        except Exception as e:
            emit(f"{NAVERPAY_MAIN_URL}: collection error for {account.user_id} - {e}")

    for link in collected_urls:
        existing_url = session_db.query(CampaignUrl).filter_by(url=link).first()
        if not existing_url:
            session_db.add(CampaignUrl(url=link))
    return collected_urls


async def fetch_naver_campaign_urls(session_db, nid: str):
    campaign_links = set()
    already_visited_count = 0
    available_urls = session_db.query(CampaignUrl).filter_by(is_available=True).all()
    for url_obj in available_urls:
        if should_revisit_url(url_obj.url):
            campaign_links.add(url_obj.url)
            continue
        existing_visit = session_db.query(UrlVisit).filter_by(url=url_obj.url, user_id=nid).first()
        if not existing_visit:
            campaign_links.add(url_obj.url)
        else:
            already_visited_count += 1
    return campaign_links, already_visited_count


def should_revisit_url(url: str) -> bool:
    return "external-token.pay.naver.com/entry/pincrux" in str(url or "") or "nsl.pincrux.com/shopping-live" in str(url or "")


def delete_old_stuff(session_db, keep_campaign_days: int, keep_user_days: int, emit: Callable[[str], None]):
    current_date = datetime.now()
    campaign_cutoff = current_date - timedelta(days=max(1, int(keep_campaign_days or 60)))
    user_cutoff = current_date - timedelta(days=max(1, int(keep_user_days or 7)))
    try:
        old_urls = session_db.query(CampaignUrl).filter(CampaignUrl.date_added < campaign_cutoff)
        for old_url in old_urls:
            session_db.query(UrlVisit).filter_by(url=old_url.url).delete()
        old_urls.delete()
        session_db.query(User).filter(User.updated_at < user_cutoff).delete()
    except Exception as e:
        emit(f"cleanup error - {e}")


def recent_runs(db_path: str, limit: int = 30):
    db = Database(db_path)
    db.create_all()
    with db.get_session() as session:
        rows = session.query(RunHistory).order_by(RunHistory.id.desc()).limit(limit).all()
        data = []
        for row in rows:
            skipped_count = (
                session.query(RunDetail)
                .filter(RunDetail.run_id == row.id, RunDetail.status.in_(["skipped", "unavailable", "no_url"]))
                .count()
            )
            data.append(_history_to_dict(row, skipped_count))
        return data


def run_details(db_path: str, run_id: int):
    db = Database(db_path)
    db.create_all()
    with db.get_session() as session:
        rows = session.query(RunDetail).filter_by(run_id=run_id).order_by(RunDetail.id.asc()).all()
        return [_detail_to_dict(row) for row in rows]


def cookie_statuses(db_path: str, cookie_dir: str, accounts: list[AccountConfig]):
    db = Database(db_path)
    db.create_all()
    rows = []
    with db.get_session() as session:
        for account in accounts:
            valid, message = check_cookie_status(account.user_id, cookie_dir)
            user = session.query(User).filter_by(user_id=account.user_id).first()
            rows.append(
                {
                    "user_id": account.user_id,
                    "valid": valid,
                    "message": message,
                    "db_storage": bool(user and user.storage_state),
                    "updated_at": user.updated_at.strftime("%Y-%m-%d %H:%M:%S") if user and user.updated_at else "",
                }
            )
    return rows


def _history_to_dict(row: RunHistory, skipped_count: int = 0):
    total_skipped = getattr(row, "skipped_url_count", 0) or skipped_count
    return {
        "id": row.id,
        "started_at": _fmt(row.started_at),
        "finished_at": _fmt(row.finished_at),
        "status": row.status,
        "account_count": row.account_count,
        "collected_url_count": row.collected_url_count,
        "skipped_url_count": total_skipped,
        "visited_url_count": row.visited_url_count,
        "estimated_points": row.estimated_points,
        "message": row.message or "",
    }


def _detail_to_dict(row: RunDetail):
    return {
        "id": row.id,
        "run_id": row.run_id,
        "user_id": row.user_id,
        "url": row.url,
        "status": row.status,
        "point": row.point,
        "message": row.message or "",
        "created_at": _fmt(row.created_at),
    }


def _fmt(value):
    if not value:
        return ""
    return value.strftime("%Y-%m-%d %H:%M:%S")
