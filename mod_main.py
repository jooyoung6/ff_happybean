import os
import json
import subprocess
import sys
import threading
import time
import traceback
import uuid
from importlib import metadata

from flask import jsonify, render_template

from .setup import *


NP = None
NP_IMPORT_ERROR = None
SOURCE_NOT_AVAILABLE = object()


def _load_source_module():
    global NP, NP_IMPORT_ERROR
    if NP is not None:
        return NP
    try:
        from . import source_naverpaper as module

        NP = module
        NP_IMPORT_ERROR = None
        return NP
    except Exception as e:
        NP_IMPORT_ERROR = e
        raise


class ModuleMain(PluginModuleBase):
    PYTHON_PACKAGES = [
        {"key": "aiohttp", "dist": "aiohttp", "import": "aiohttp", "spec": "aiohttp>=3.9.1"},
        {"key": "beautifulsoup4", "dist": "beautifulsoup4", "import": "bs4", "spec": "beautifulsoup4>=4.12.2"},
        {"key": "playwright", "dist": "playwright", "import": "playwright", "spec": "playwright>=1.52.0"},
        {"key": "playwright-stealth", "dist": "playwright-stealth", "import": "playwright_stealth", "spec": "playwright-stealth>=2.0.3"},
        {"key": "SQLAlchemy", "dist": "SQLAlchemy", "import": "sqlalchemy", "spec": "SQLAlchemy>=2.0.49"},
    ]
    install_jobs = []
    install_jobs_lock = threading.Lock()
    collection_lock = threading.Lock()
    collection_running = False
    happybean_lock = threading.Lock()
    happybean_running = False

    SOURCE_DEPENDENCY_KEYS = ("aiohttp", "beautifulsoup4", "playwright", "playwright-stealth", "SQLAlchemy")

    def __init__(self, P):
        super(ModuleMain, self).__init__(P, name="main", first_menu="setting", scheduler_desc="NaverPaper")
        default_route_socketio_module(self, attach="/install")
        self.db_default = {
            f"{self.name}_db_version": "1",
            "naver_ids": "",
            "naver_passwords": "",
            "naver_profiles": "[]",
            "paper_schedule_enabled": "False",
            "paper_schedule": "0 3,9,15,21 * * *",
            "keep_campaign_days": "60",
            "keep_user_days": "7",
            "no_paper_record": "True",
            "proxy_enabled": "False",
            "proxy_reward_enabled": "",
            "proxy_login_enabled": "False",
            "proxy_url": "",
            "happybean_schedule_enabled": "False",
            "happybean_schedule": "0 9 * * *",
        }
        self._source_warning_logged = False

    def plugin_load(self):
        try:
            self._ensure_dirs()
            self._sync_scheduler()
            self._sync_happybean_scheduler()
        except Exception as e:
            P.logger.error(f"Exception:{str(e)}")
            P.logger.error(traceback.format_exc())

    def _np(self):
        return _load_source_module()

    def _try_np(self):
        try:
            module = self._np()
            self._source_warning_logged = False
            return module
        except Exception:
            if not self._source_warning_logged:
                P.logger.warning("[NaverPaper] source module is not available. Install required packages first.")
                P.logger.warning(traceback.format_exc())
                self._source_warning_logged = True
            return None

    def _source_dependency_message(self):
        missing = [item["key"] for item in self.PYTHON_PACKAGES if item["key"] in self.SOURCE_DEPENDENCY_KEYS and not self._is_dependency_installed(item["key"])]
        if missing:
            return "Required Python packages are missing: %s. Install them from the package install menu." % ", ".join(missing)
        if NP_IMPORT_ERROR:
            return str(NP_IMPORT_ERROR)
        return "Source module is not available. Check the log and package install menu."

    def _source_dependency_warning(self):
        try:
            self._np()
            return None
        except Exception:
            return {"ret": "warning", "msg": self._source_dependency_message()}

    def process_menu(self, sub, req):
        arg = P.ModelSetting.to_dict()
        if not arg.get("proxy_reward_enabled"):
            arg["proxy_reward_enabled"] = arg.get("proxy_enabled", "False")
        arg["scheduler_active"] = F.scheduler.is_include(self._job_id())
        arg["is_running"] = F.scheduler.is_running(self._job_id())
        arg["db_path"] = self._db_path()
        arg["cookie_dir"] = self._cookie_dir()
        arg["package_name"] = P.package_name
        if sub == "install":
            arg["accounts"] = []
            arg["profiles"] = []
            arg["profiles_json"] = "[]"
            arg["cookie_statuses"] = []
            arg["install_status"] = self._dependency_status()
            return render_template(f"{P.package_name}_{self.name}_{sub}.html", arg=arg)
        np_module = self._try_np()
        if np_module is None:
            arg["dependency_error"] = self._source_dependency_message()
        accounts = self._accounts(np_module)
        arg["accounts"] = accounts
        arg["profiles"] = self._profiles(np_module)
        arg["profiles_json"] = json.dumps(arg["profiles"], ensure_ascii=False)
        arg["cookie_statuses"] = np_module.cookie_statuses(self._db_path(), self._cookie_dir(), accounts) if np_module else []
        if sub == "result":
            arg["runs"] = np_module.recent_runs(self._db_path(), 50) if np_module else []
            profile_order = [item.get("user_id") for item in arg["profiles"] if item.get("user_id")]
            arg["run_account_tabs"] = np_module.recent_run_account_tabs(self._db_path(), 50, profile_order) if np_module else []
            arg["happybean_runs"] = np_module.recent_happybean_runs(self._db_path(), 30) if np_module else []
        return render_template(f"{P.package_name}_{self.name}_{sub}.html", arg=arg)

    def process_command(self, command, arg1, arg2, arg3, req):
        try:
            if command == "run_now":
                return jsonify(self._start_run_now_background())
            if command == "manual_reward":
                return jsonify(self._start_manual_reward_background(arg1))
            if command == "sync_scheduler":
                return jsonify(self._sync_scheduler(arg1, arg2))
            if command == "result_details":
                warning = self._source_dependency_warning()
                if warning:
                    return jsonify(warning)
                run_id = int(str(arg1 or "0").strip() or "0")
                return jsonify({"ret": "success", "data": self._np().run_details(self._db_path(), run_id)})
            if command == "cookie_status":
                warning = self._source_dependency_warning()
                if warning:
                    return jsonify(warning)
                return jsonify({"ret": "success", "data": self._np().cookie_statuses(self._db_path(), self._cookie_dir(), self._accounts())})
            if command == "profile_save":
                return jsonify(self._profile_save(arg1))
            if command == "profile_save_all":
                return jsonify(self._profile_save_all(arg1))
            if command == "profile_delete":
                return jsonify(self._profile_delete(arg1))
            if command == "profile_refresh_cookie":
                return jsonify(self._profile_refresh_cookie(arg1))
            if command == "profile_login_screen":
                return jsonify(self._profile_login_screen(arg1))
            if command == "profile_submit_captcha":
                return jsonify(self._profile_submit_captcha(arg1, arg2))
            if command == "profile_login_session_close":
                warning = self._source_dependency_warning()
                if warning:
                    return jsonify(warning)
                self._np().close_login_session_sync(arg1)
                return jsonify({"ret": "success", "msg": "login session closed"})
            if command == "profile_list":
                return jsonify({"ret": "success", "data": self._profile_rows()})
            if command == "install_dependency":
                return jsonify(self._install_start(arg1))
            if command == "install_all_dependencies":
                return jsonify(self._install_start("all"))
            if command == "dependency_status":
                return jsonify({"ret": "success", "data": self._dependency_status()})
            if command == "install_jobs":
                return jsonify(self._install_job_status())
            if command == "run_happybean_now":
                return jsonify(self._start_happybean_run_background())
            if command == "sync_happybean_scheduler":
                return jsonify(self._sync_happybean_scheduler(arg1, arg2))
            if command == "happybean_run_details":
                warning = self._source_dependency_warning()
                if warning:
                    return jsonify(warning)
                run_id = int(str(arg1 or "0").strip() or "0")
                return jsonify({"ret": "success", "data": self._np().happybean_run_details(self._db_path(), run_id)})
            if command == "happybean_screenshot":
                import base64, os as _os
                ss_path = str(arg1 or "")
                if not ss_path or not _os.path.isfile(ss_path):
                    return jsonify({"ret": "error", "msg": "파일 없음"})
                with open(ss_path, "rb") as f:
                    data = base64.b64encode(f.read()).decode("utf-8")
                return jsonify({"ret": "success", "data": data})
            return jsonify({"ret": "warning", "msg": f"unsupported command: {command}"})
        except Exception as e:
            P.logger.error(f"Exception:{str(e)}")
            P.logger.error(traceback.format_exc())
            return jsonify({"ret": "error", "msg": str(e)})

    def scheduler_function(self):
        if not self._mark_collection_running():
            P.logger.info("[NaverPaper] collection already running; scheduled run skipped")
            return
        try:
            self._run_now()
            self._sync_profiles_from_cookie_files()
        except Exception:
            P.logger.error(traceback.format_exc())
        finally:
            self._mark_collection_finished()

    def _start_run_now_background(self):
        warning = self._source_dependency_warning()
        if warning:
            return warning
        if not self._accounts():
            return {"ret": "warning", "msg": "Naver account profile is empty"}
        if not self._mark_collection_running():
            return {"ret": "warning", "msg": "NaverPaper is already running"}
        thread = threading.Thread(target=self._run_now_background, daemon=True)
        thread.start()
        return {"ret": "success", "msg": "NaverPaper started in background", "data": {"status": "RUNNING"}}

    def _run_now_background(self):
        try:
            result = self._run_now()
            self._sync_profiles_from_cookie_files()
            P.logger.info(
                "[NaverPaper] background run finished collected=%s skipped=%s visited=%s points=%s",
                result.collected_url_count,
                result.skipped_url_count,
                result.visited_url_count,
                result.estimated_points,
            )
            for account_result in getattr(result, "account_results", []) or []:
                P.logger.info(
                    "[NaverPaper] account result user=%s skipped=%s visited=%s points=%s details=%s",
                    getattr(account_result, "user_id", ""),
                    getattr(account_result, "skipped_url_count", 0),
                    getattr(account_result, "visited_url_count", 0),
                    getattr(account_result, "estimated_points", 0),
                    len(getattr(account_result, "details", []) or []),
                )
        except Exception:
            P.logger.error(traceback.format_exc())
        finally:
            self._mark_collection_finished()

    def _start_manual_reward_background(self, link):
        warning = self._source_dependency_warning()
        if warning:
            return warning
        link = str(link or "").strip()
        if not link:
            return {"ret": "warning", "msg": "링크를 입력하세요."}
        if not (link.startswith("http://") or link.startswith("https://")):
            return {"ret": "warning", "msg": "http 또는 https 링크를 입력하세요."}
        if not self._accounts():
            return {"ret": "warning", "msg": "Naver account profile is empty"}
        if not self._mark_collection_running():
            return {"ret": "warning", "msg": "NaverPaper is already running"}
        thread = threading.Thread(target=self._manual_reward_background, args=(link,), daemon=True)
        thread.start()
        return {"ret": "success", "msg": "수동 적립을 백그라운드로 시작했습니다.", "data": {"status": "RUNNING"}}

    def _manual_reward_background(self, link):
        try:
            np_module = self._np()
            result = np_module.run_manual_link_sync(
                self._db_path(),
                self._cookie_dir(),
                self._accounts(),
                link,
                self._reward_proxy_url(),
                log=lambda msg: P.logger.info(f"[NaverPaper] {msg}"),
                login_proxy_url=self._login_proxy_url(),
            )
            self._sync_profiles_from_cookie_files()
            P.logger.info(
                "[NaverPaper] manual reward finished link=%s skipped=%s visited=%s points=%s",
                link,
                result.skipped_url_count,
                result.visited_url_count,
                result.estimated_points,
            )
            for account_result in getattr(result, "account_results", []) or []:
                P.logger.info(
                    "[NaverPaper] manual account result user=%s skipped=%s visited=%s points=%s details=%s",
                    getattr(account_result, "user_id", ""),
                    getattr(account_result, "skipped_url_count", 0),
                    getattr(account_result, "visited_url_count", 0),
                    getattr(account_result, "estimated_points", 0),
                    len(getattr(account_result, "details", []) or []),
                )
        except Exception:
            P.logger.error(traceback.format_exc())
        finally:
            self._mark_collection_finished()

    def _mark_collection_running(self):
        with self.collection_lock:
            if self.collection_running:
                return False
            self.collection_running = True
            return True

    def _mark_collection_finished(self):
        with self.collection_lock:
            self.collection_running = False

    def _run_now(self):
        np_module = self._np()
        self._ensure_dirs()
        accounts = self._accounts()
        if not accounts:
            raise ValueError("Naver account profile is empty")
        config = np_module.RunConfig(
            db_path=self._db_path(),
            cookie_dir=self._cookie_dir(),
            accounts=accounts,
            reward_proxy_url=self._reward_proxy_url(),
            login_proxy_url=self._login_proxy_url(),
            no_paper_record=self._bool_setting("no_paper_record", True),
            keep_campaign_days=self._int_setting("keep_campaign_days", 60),
            keep_user_days=self._int_setting("keep_user_days", 7),
        )
        return np_module.run_sync(config, log=lambda msg: P.logger.info(f"[NaverPaper] {msg}"))

    def happybean_scheduler_function(self):
        if not self._mark_happybean_running():
            P.logger.info("[NaverPaper] happybean already running; scheduled run skipped")
            return
        try:
            self._run_happybean_now()
        except Exception:
            P.logger.error(traceback.format_exc())
        finally:
            self._mark_happybean_finished()

    def _start_happybean_run_background(self):
        warning = self._source_dependency_warning()
        if warning:
            return warning
        if not self._accounts():
            return {"ret": "warning", "msg": "Naver account profile is empty"}
        if not self._mark_happybean_running():
            return {"ret": "warning", "msg": "해피빈 콩받기가 이미 실행 중입니다"}
        thread = threading.Thread(target=self._happybean_run_background, daemon=True)
        thread.start()
        return {"ret": "success", "msg": "해피빈 콩받기를 백그라운드로 시작했습니다.", "data": {"status": "RUNNING"}}

    def _happybean_run_background(self):
        try:
            self._run_happybean_now()
        except Exception:
            P.logger.error(traceback.format_exc())
        finally:
            self._mark_happybean_finished()

    def _run_happybean_now(self):
        np_module = self._np()
        self._ensure_dirs()
        accounts = self._accounts()
        if not accounts:
            raise ValueError("Naver account profile is empty")
        config = np_module.RunConfig(
            db_path=self._db_path(),
            cookie_dir=self._cookie_dir(),
            accounts=accounts,
            login_proxy_url=self._login_proxy_url(),
        )
        result = np_module.run_happybean_sync(config, log=lambda msg: P.logger.info(f"[NaverPaper] {msg}"))
        P.logger.info(
            "[NaverPaper] happybean finished accounts=%s details=%s",
            result.account_count,
            len(result.details),
        )
        return result

    def _mark_happybean_running(self):
        with self.happybean_lock:
            if self.happybean_running:
                return False
            self.happybean_running = True
            return True

    def _mark_happybean_finished(self):
        with self.happybean_lock:
            self.happybean_running = False

    def _sync_happybean_scheduler(self, enabled_override=None, schedule_override=None):
        if enabled_override is not None:
            enabled_text = str(enabled_override).strip().lower()
            P.ModelSetting.set("happybean_schedule_enabled", "True" if enabled_text in ("true", "1", "on", "yes") else "False")
        if schedule_override is not None:
            P.ModelSetting.set("happybean_schedule", str(schedule_override or "").strip())

        job_id = self._happybean_job_id()
        if F.scheduler.is_include(job_id):
            F.scheduler.remove_job(job_id)

        enabled = self._bool_setting("happybean_schedule_enabled", False)
        schedule = str(P.ModelSetting.get("happybean_schedule") or "").strip()
        if enabled and schedule:
            F.scheduler.add_job_instance(Job(P.package_name, job_id, schedule, self.happybean_scheduler_function, "해피빈 콩받기"))
        return {"ret": "success", "scheduler_active": F.scheduler.is_include(job_id)}

    def _happybean_job_id(self):
        return f"{P.package_name}_happybean"

    def _sync_scheduler(self, enabled_override=None, schedule_override=None):
        if enabled_override is not None:
            enabled_text = str(enabled_override).strip().lower()
            P.ModelSetting.set("paper_schedule_enabled", "True" if enabled_text in ("true", "1", "on", "yes") else "False")
        if schedule_override is not None:
            P.ModelSetting.set("paper_schedule", str(schedule_override or "").strip())

        job_id = self._job_id()
        if F.scheduler.is_include(job_id):
            F.scheduler.remove_job(job_id)

        enabled = self._bool_setting("paper_schedule_enabled", False)
        schedule = str(P.ModelSetting.get("paper_schedule") or "").strip()
        if enabled and schedule:
            F.scheduler.add_job_instance(Job(P.package_name, job_id, schedule, self.scheduler_function, "NaverPaper collection"))
        return {"ret": "success", "scheduler_active": F.scheduler.is_include(job_id)}

    def _job_id(self):
        return f"{P.package_name}_collection"

    def _accounts(self, np_module=SOURCE_NOT_AVAILABLE):
        if np_module is SOURCE_NOT_AVAILABLE:
            np_module = self._try_np()
        profiles = self._profiles(np_module)
        if profiles:
            if not np_module:
                return [item for item in profiles if item.get("user_id")]
            return [
                np_module.AccountConfig(
                    user_id=item.get("user_id", ""),
                    password=item.get("password", ""),
                    keep_login=self._profile_bool(item.get("keep_login"), True),
                )
                for item in profiles
                if item.get("user_id")
            ]
        if np_module:
            return np_module.parse_accounts(P.ModelSetting.get("naver_ids"), P.ModelSetting.get("naver_passwords"))
        return self._parse_accounts_fallback(P.ModelSetting.get("naver_ids"), P.ModelSetting.get("naver_passwords"))

    def _profiles(self, np_module=SOURCE_NOT_AVAILABLE):
        raw = P.ModelSetting.get("naver_profiles") or "[]"
        try:
            data = json.loads(raw)
            if isinstance(data, list):
                profiles = []
                for idx, item in enumerate(data):
                    profile = self._normalize_profile(item)
                    if not profile.get("user_id"):
                        continue
                    if not profile.get("profile_order"):
                        profile["profile_order"] = idx + 1
                    profiles.append(profile)
                return sorted(profiles, key=lambda item: item.get("profile_order") or 0)
        except Exception:
            pass
        legacy = []
        if np_module is SOURCE_NOT_AVAILABLE:
            np_module = self._try_np()
        accounts = np_module.parse_accounts(P.ModelSetting.get("naver_ids"), P.ModelSetting.get("naver_passwords")) if np_module else self._parse_accounts_fallback(P.ModelSetting.get("naver_ids"), P.ModelSetting.get("naver_passwords"))
        for account in accounts:
            legacy.append({"user_id": account.user_id if hasattr(account, "user_id") else account.get("user_id", ""), "password": account.password if hasattr(account, "password") else account.get("password", ""), "keep_login": True, "nid_aut": "", "nid_ses": "", "nid_jst": ""})
        return legacy

    def _parse_accounts_fallback(self, ids_text, passwords_text):
        ids = [line.strip() for line in str(ids_text or "").replace(",", "\n").splitlines() if line.strip()]
        passwords = [line.strip() for line in str(passwords_text or "").replace(",", "\n").splitlines()]
        rows = []
        for idx, user_id in enumerate(ids):
            rows.append({"user_id": user_id, "password": passwords[idx] if idx < len(passwords) else "", "keep_login": True})
        return rows

    def _normalize_profile(self, item):
        item = item if isinstance(item, dict) else {}
        try:
            profile_order = int(item.get("profile_order") or item.get("order") or 0)
        except Exception:
            profile_order = 0
        return {
            "user_id": str(item.get("user_id") or "").strip(),
            "password": str(item.get("password") or "").strip(),
            "keep_login": self._profile_bool(item.get("keep_login"), True),
            "nid_aut": str(item.get("nid_aut") or "").strip(),
            "nid_ses": str(item.get("nid_ses") or "").strip(),
            "nid_jst": str(item.get("nid_jst") or "").strip(),
            "profile_order": profile_order,
        }

    def _profile_rows(self):
        np_module = self._try_np()
        return {
            "profiles": self._profiles(np_module),
            "cookie_statuses": np_module.cookie_statuses(self._db_path(), self._cookie_dir(), self._accounts(np_module)) if np_module else [],
        }

    def _profile_save(self, payload):
        try:
            profile = self._normalize_profile(json.loads(payload or "{}"))
        except Exception:
            return {"ret": "warning", "msg": "invalid profile payload"}
        if not profile.get("user_id"):
            return {"ret": "warning", "msg": "account id is required"}
        profiles = [item for item in self._profiles() if item.get("user_id") != profile["user_id"]]
        if not profile.get("profile_order"):
            profile["profile_order"] = self._next_profile_order(profiles)
        profiles.append(profile)
        P.ModelSetting.set("naver_profiles", json.dumps(profiles, ensure_ascii=False))
        self._write_profile_cookie(profile)
        return {"ret": "success", "msg": "profile saved", "data": self._profile_rows()}

    def _profile_save_all(self, payload):
        try:
            parsed = json.loads(payload or "{}")
        except Exception:
            return {"ret": "warning", "msg": "invalid profile payload"}
        rows = parsed.get("profiles") if isinstance(parsed, dict) else parsed
        deleted_ids = parsed.get("deleted_ids", []) if isinstance(parsed, dict) else []
        if not isinstance(rows, list):
            return {"ret": "warning", "msg": "invalid profile list"}

        previous_profiles = self._profiles()
        previous_order = {item.get("user_id"): item.get("profile_order") or idx + 1 for idx, item in enumerate(previous_profiles) if item.get("user_id")}
        next_order = self._next_profile_order(previous_profiles)
        normalized = []
        seen = set()
        for idx, item in enumerate(rows):
            profile = self._normalize_profile(item)
            user_id = profile.get("user_id")
            if not user_id:
                continue
            if user_id in previous_order:
                profile["profile_order"] = previous_order[user_id]
            else:
                profile["profile_order"] = next_order
                next_order += 1
            if user_id in seen:
                return {"ret": "warning", "msg": f"duplicated account id: {user_id}"}
            seen.add(user_id)
            normalized.append(profile)

        previous_ids = {item.get("user_id") for item in previous_profiles if item.get("user_id")}
        current_ids = {item.get("user_id") for item in normalized if item.get("user_id")}
        remove_ids = set(str(x or "").strip() for x in deleted_ids if str(x or "").strip())
        remove_ids.update(previous_ids - current_ids)

        normalized = sorted(normalized, key=lambda item: item.get("profile_order") or 0)
        P.ModelSetting.set("naver_profiles", json.dumps(normalized, ensure_ascii=False))
        for profile in normalized:
            self._write_profile_cookie(profile)
        for user_id in remove_ids:
            cookie_path = os.path.join(self._cookie_dir(), f"{user_id}.json")
            try:
                if os.path.exists(cookie_path):
                    os.remove(cookie_path)
            except Exception:
                P.logger.warning("[NaverPaper] failed to delete cookie file: %s", cookie_path)
        return {"ret": "success", "msg": "profiles saved", "data": self._profile_rows()}

    def _profile_refresh_cookie(self, user_id):
        block = self._collection_login_block_message()
        if block:
            return block
        warning = self._source_dependency_warning()
        if warning:
            return warning
        np_module = self._np()
        user_id = str(user_id or "").strip()
        profile = None
        for item in self._profiles():
            if item.get("user_id") == user_id:
                profile = item
                break
        if not profile:
            return {"ret": "warning", "msg": "profile not found"}
        if not profile.get("password"):
            return {"ret": "warning", "msg": "password is empty; enter cookies manually"}
        result = np_module.refresh_cookie_sync(
            np_module.AccountConfig(user_id=profile["user_id"], password=profile.get("password", ""), keep_login=self._profile_bool(profile.get("keep_login"), True)),
            self._cookie_dir(),
            self._login_proxy_url(),
            log=lambda msg: P.logger.info(f"[NaverPaper] {msg}"),
        )
        if result.get("ret") != "success":
            return {"ret": result.get("ret") or "warning", "msg": result.get("msg") or "cookie refresh failed", "data": self._profile_rows()}

        cookies = result.get("cookies") or {}
        profile["nid_aut"] = cookies.get("NID_AUT", profile.get("nid_aut", ""))
        profile["nid_ses"] = cookies.get("NID_SES", profile.get("nid_ses", ""))
        profile["nid_jst"] = cookies.get("NID_JST", profile.get("nid_jst", ""))
        profiles = self._replace_profile_preserving_order(profile)
        P.ModelSetting.set("naver_profiles", json.dumps(profiles, ensure_ascii=False))
        return {"ret": "success", "msg": "cookie refreshed", "data": self._profile_rows()}

    def _profile_login_screen(self, user_id):
        block = self._collection_login_block_message()
        if block:
            return block
        warning = self._source_dependency_warning()
        if warning:
            return warning
        np_module = self._np()
        profile = self._find_profile(user_id)
        if not profile:
            return {"ret": "warning", "msg": "profile not found"}
        if not profile.get("password"):
            return {"ret": "warning", "msg": "password is empty"}
        result = np_module.open_login_screen_sync(
            np_module.AccountConfig(user_id=profile["user_id"], password=profile.get("password", ""), keep_login=self._profile_bool(profile.get("keep_login"), True)),
            self._cookie_dir(),
            self._login_proxy_url(),
            log=lambda msg: P.logger.info(f"[NaverPaper] {msg}"),
        )
        if result.get("cookies"):
            self._merge_profile_cookies(profile, result.get("cookies") or {})
            self._close_profile_login_session(profile["user_id"], np_module)
            result["data"] = self._profile_rows()
        return result

    def _profile_submit_captcha(self, user_id, captcha_text):
        block = self._collection_login_block_message()
        if block:
            return block
        warning = self._source_dependency_warning()
        if warning:
            return warning
        np_module = self._np()
        profile = self._find_profile(user_id)
        if not profile:
            return {"ret": "warning", "msg": "profile not found"}
        if not profile.get("password"):
            return {"ret": "warning", "msg": "password is empty"}
        screen = np_module.open_login_screen_sync(
            np_module.AccountConfig(user_id=profile["user_id"], password=profile.get("password", ""), keep_login=self._profile_bool(profile.get("keep_login"), True)),
            self._cookie_dir(),
            self._login_proxy_url(),
            log=lambda msg: P.logger.info(f"[NaverPaper] {msg}"),
        )
        if screen.get("ret") != "success":
            return screen
        if screen.get("cookies"):
            self._merge_profile_cookies(profile, screen.get("cookies") or {})
            self._close_profile_login_session(profile["user_id"], np_module)
            screen["data"] = self._profile_rows()
            return screen
        result = np_module.submit_login_captcha_sync(
            profile["user_id"],
            captcha_text,
            self._cookie_dir(),
            log=lambda msg: P.logger.info(f"[NaverPaper] {msg}"),
        )
        if result.get("cookies"):
            self._merge_profile_cookies(profile, result.get("cookies") or {})
            self._close_profile_login_session(profile["user_id"], np_module)
            result["data"] = self._profile_rows()
        return result

    def _find_profile(self, user_id):
        user_id = str(user_id or "").strip()
        for item in self._profiles():
            if item.get("user_id") == user_id:
                return item
        return None

    def _collection_login_block_message(self):
        with self.collection_lock:
            running = self.collection_running
        if running:
            return {"ret": "warning", "msg": "현재 수집중입니다. 수집이 완료되면 로그인 해 주세요"}
        return None

    def _merge_profile_cookies(self, profile, cookies):
        profile["nid_aut"] = cookies.get("NID_AUT", profile.get("nid_aut", ""))
        profile["nid_ses"] = cookies.get("NID_SES", profile.get("nid_ses", ""))
        profile["nid_jst"] = cookies.get("NID_JST", profile.get("nid_jst", ""))
        profiles = self._replace_profile_preserving_order(profile)
        P.ModelSetting.set("naver_profiles", json.dumps(profiles, ensure_ascii=False))

    def _replace_profile_preserving_order(self, profile):
        profiles = self._profiles()
        replaced = False
        for idx, item in enumerate(profiles):
            if item.get("user_id") == profile.get("user_id"):
                if not profile.get("profile_order"):
                    profile["profile_order"] = item.get("profile_order") or idx + 1
                profiles[idx] = profile
                replaced = True
                break
        if not replaced:
            if not profile.get("profile_order"):
                profile["profile_order"] = self._next_profile_order(profiles)
            profiles.append(profile)
        return profiles

    def _next_profile_order(self, profiles):
        orders = [int(item.get("profile_order") or 0) for item in profiles or []]
        return (max(orders) if orders else 0) + 1

    def _close_profile_login_session(self, user_id, np_module=None):
        try:
            module = np_module or self._np()
            module.close_login_session_sync(user_id)
        except Exception:
            P.logger.warning("[NaverPaper] failed to close login session for %s", user_id)
            P.logger.warning(traceback.format_exc())

    def _profile_delete(self, user_id):
        user_id = str(user_id or "").strip()
        profiles = [item for item in self._profiles() if item.get("user_id") != user_id]
        P.ModelSetting.set("naver_profiles", json.dumps(profiles, ensure_ascii=False))
        cookie_path = os.path.join(self._cookie_dir(), f"{user_id}.json")
        try:
            if user_id and os.path.exists(cookie_path):
                os.remove(cookie_path)
        except Exception:
            P.logger.warning("[NaverPaper] failed to delete cookie file: %s", cookie_path)
        return {"ret": "success", "msg": "profile deleted", "data": self._profile_rows()}

    def _write_profile_cookie(self, profile):
        if not (profile.get("nid_aut") or profile.get("nid_ses") or profile.get("nid_jst")):
            return
        self._ensure_dirs()
        expires = int(time.time()) + 180 * 24 * 60 * 60
        cookies = []
        for name, value in (
            ("NID_AUT", profile.get("nid_aut")),
            ("NID_SES", profile.get("nid_ses")),
            ("NID_JST", profile.get("nid_jst")),
        ):
            if not value:
                continue
            cookies.append(
                {
                    "name": name,
                    "value": value,
                    "domain": ".naver.com",
                    "path": "/",
                    "expires": expires,
                    "httpOnly": True,
                    "secure": True,
                    "sameSite": "Lax",
                }
            )
        storage_state = {"cookies": cookies, "origins": []}
        with open(os.path.join(self._cookie_dir(), f"{profile['user_id']}.json"), "w", encoding="utf-8") as f:
            json.dump(storage_state, f, ensure_ascii=False, indent=2)

    def _sync_profiles_from_cookie_files(self):
        profiles = self._profiles()
        changed = False
        for profile in profiles:
            values = self._cookie_values_from_file(profile.get("user_id", ""))
            for field, cookie_name in (("nid_aut", "NID_AUT"), ("nid_ses", "NID_SES"), ("nid_jst", "NID_JST")):
                value = values.get(cookie_name)
                if value and profile.get(field) != value:
                    profile[field] = value
                    changed = True
        if changed:
            P.ModelSetting.set("naver_profiles", json.dumps(profiles, ensure_ascii=False))

    def _cookie_values_from_file(self, user_id):
        path = os.path.join(self._cookie_dir(), f"{user_id}.json")
        if not user_id or not os.path.exists(path):
            return {}
        try:
            with open(path, "r", encoding="utf-8") as f:
                return self._np().storage_cookie_values(json.load(f))
        except Exception:
            return {}

    def _reward_proxy_url(self):
        reward_enabled = self._bool_setting("proxy_reward_enabled", self._bool_setting("proxy_enabled", False))
        if not reward_enabled:
            return ""
        return str(P.ModelSetting.get("proxy_url") or "").strip()

    def _login_proxy_url(self):
        if not self._bool_setting("proxy_login_enabled", False):
            return ""
        return str(P.ModelSetting.get("proxy_url") or "").strip()

    def _root_data_dir(self):
        return os.path.join(os.path.dirname(__file__), "data")

    def _db_path(self):
        return os.path.join(self._root_data_dir(), "naverpaper.sqlite")

    def _cookie_dir(self):
        return os.path.join(self._root_data_dir(), "cookies")

    def _ensure_dirs(self):
        os.makedirs(self._root_data_dir(), exist_ok=True)
        os.makedirs(self._cookie_dir(), exist_ok=True)

    def _bool_setting(self, key, default=False):
        raw = P.ModelSetting.get(key)
        if raw is None or raw == "":
            return default
        return str(raw).strip().lower() in ("true", "1", "on", "yes")

    def _profile_bool(self, value, default=False):
        if isinstance(value, bool):
            return value
        if value is None or value == "":
            return default
        return str(value).strip().lower() in ("true", "1", "on", "yes")

    def _int_setting(self, key, default):
        try:
            return int(str(P.ModelSetting.get(key) or default).strip())
        except Exception:
            return default

    def _dependency_status(self):
        packages = []
        for item in self.PYTHON_PACKAGES:
            version = ""
            installed = False
            try:
                version = metadata.version(item["dist"])
                installed = True
            except Exception:
                installed = False
            packages.append(
                {
                    "key": item["key"],
                    "name": item["key"],
                    "spec": item["spec"],
                    "installed": installed,
                    "version": version,
                }
            )
        chromium = self._chromium_status()
        return {"packages": packages, "chromium": chromium, "python": sys.executable}

    def _is_dependency_installed(self, key):
        if key == "chromium":
            return bool(self._chromium_status().get("installed"))
        for item in self.PYTHON_PACKAGES:
            if key == item["key"]:
                try:
                    metadata.version(item["dist"])
                    return True
                except Exception:
                    return False
        return False

    def _chromium_status(self):
        spec = "python -m playwright install --with-deps chromium"
        try:
            code = (
                "from playwright.sync_api import sync_playwright\n"
                "with sync_playwright() as p:\n"
                "    print(p.chromium.executable_path or '')\n"
            )
            proc = subprocess.run(
                [sys.executable, "-c", code],
                capture_output=True,
                text=True,
                timeout=15,
            )
            path = (proc.stdout or "").strip().splitlines()[-1] if proc.stdout.strip() else ""
            message = (proc.stderr or "").strip()
            installed = bool(path and os.path.exists(path))
            return {
                "key": "chromium",
                "name": "Playwright Chromium",
                "installed": installed,
                "version": "",
                "path": path or "",
                "spec": spec,
                "message": "" if installed else (message or "Chromium executable was not found."),
            }
        except Exception as e:
            return {
                "key": "chromium",
                "name": "Playwright Chromium",
                "installed": False,
                "version": "",
                "path": "",
                "spec": spec,
                "message": str(e),
            }

    def _install_start(self, key):
        key = str(key or "").strip()
        specs = self._install_specs_for_key(key)
        if not specs:
            return {"ret": "warning", "msg": f"unknown dependency: {key}"}
        job = {
            "idx": str(uuid.uuid4()),
            "key": key,
            "name": "전체 설치" if key == "all" else specs[0]["label"],
            "created_at": time.time(),
            "status": "READY",
            "status_kor": "대기",
            "percent": 0,
            "progress_text": "대기 중",
            "output": "",
            "steps": specs,
            "total_steps": len(specs),
            "current_step": 0,
        }
        with self.install_jobs_lock:
            self.install_jobs.append(job)
            self.install_jobs[:] = self.install_jobs[-30:]
            row = self._install_job_row(job)
        thread = threading.Thread(target=self._run_install_job, args=(job,), daemon=True)
        thread.start()
        self.socketio_callback("add", row)
        return {"ret": "success", "msg": "설치를 시작했습니다.", "data": row}

    def _install_specs_for_key(self, key):
        if key == "all":
            specs = []
            for item in self.PYTHON_PACKAGES:
                specs.append(
                    {
                        "kind": "pip",
                        "key": item["key"],
                        "label": item["key"],
                        "command": [sys.executable, "-m", "pip", "install", item["spec"]],
                        "installed": self._is_dependency_installed(item["key"]),
                    }
                )
            specs.append(
                {
                    "kind": "chromium",
                    "key": "chromium",
                    "label": "Playwright Chromium",
                    "command": [sys.executable, "-m", "playwright", "install", "--with-deps", "chromium"],
                    "installed": self._is_dependency_installed("chromium"),
                }
            )
            return specs
        if key == "chromium":
            return [
                {
                    "kind": "chromium",
                    "key": "chromium",
                    "label": "Playwright Chromium",
                    "command": [sys.executable, "-m", "playwright", "install", "--with-deps", "chromium"],
                    "installed": self._is_dependency_installed("chromium"),
                }
            ]
        for item in self.PYTHON_PACKAGES:
            if key == item["key"]:
                return [
                    {
                        "kind": "pip",
                        "key": item["key"],
                        "label": item["key"],
                        "command": [sys.executable, "-m", "pip", "install", item["spec"]],
                        "installed": self._is_dependency_installed(item["key"]),
                    }
                ]
        return []

    def _install_job_status(self):
        with self.install_jobs_lock:
            return [self._install_job_row(item) for item in self.install_jobs[-30:]]

    def _install_job_row(self, job):
        created_at = float(job.get("created_at") or time.time())
        return {
            "idx": job.get("idx"),
            "key": job.get("key"),
            "name": job.get("name") or "",
            "start_time": time.strftime("%m-%d %H:%M:%S", time.localtime(created_at)),
            "status": job.get("status") or "READY",
            "status_kor": job.get("status_kor") or job.get("status") or "",
            "percent": int(job.get("percent") or 0),
            "progress_text": job.get("progress_text") or "",
            "output": job.get("output") or "",
            "current_step": int(job.get("current_step") or 0),
            "total_steps": int(job.get("total_steps") or 0),
        }

    def _run_install_job(self, job):
        try:
            with self.install_jobs_lock:
                job["status"] = "RUNNING"
                job["status_kor"] = "진행중"
                job["progress_text"] = "시작 중"
                row = self._install_job_row(job)
            self.socketio_callback("status_change", row)

            steps = list(job.get("steps") or [])
            total = max(1, len(steps))
            collected_output = []
            for index, step in enumerate(steps, start=1):
                base_percent = int(((index - 1) / total) * 100)
                end_percent = int(index / total * 100)
                if step.get("installed"):
                    msg = f"{step['label']} already installed - skipped"
                    collected_output.append(msg)
                    with self.install_jobs_lock:
                        job["current_step"] = index
                        job["percent"] = end_percent
                        job["progress_text"] = f"{step['label']} 스킵됨 ({index}/{total})"
                        job["output"] = "\n".join(collected_output)[-6000:]
                        row = self._install_job_row(job)
                    self.socketio_callback("status", row)
                    continue
                with self.install_jobs_lock:
                    job["current_step"] = index
                    job["percent"] = base_percent
                    job["progress_text"] = f"{step['label']} 설치 중 ({index}/{total})"
                    row = self._install_job_row(job)
                self.socketio_callback("status", row)

                ret = self._run_install_command_stream(step["command"], step["label"], job, base_percent, end_percent, collected_output)
                if ret.get("ret") != "success":
                    with self.install_jobs_lock:
                        job["status"] = "FAILED"
                        job["status_kor"] = "실패"
                        job["percent"] = 100
                        job["progress_text"] = f"{step['label']} 설치 실패"
                        job["output"] = "\n".join(collected_output)[-6000:]
                        row = self._install_job_row(job)
                    self.socketio_callback("last", row)
                    return

            with self.install_jobs_lock:
                job["status"] = "COMPLETED"
                job["status_kor"] = "완료"
                job["percent"] = 100
                job["progress_text"] = "설치 완료"
                job["output"] = "\n".join(collected_output)[-6000:]
                row = self._install_job_row(job)
            self.socketio_callback("last", row)
            self.socketio_callback("dependency_status", self._dependency_status())
        except Exception as e:
            P.logger.error(traceback.format_exc())
            with self.install_jobs_lock:
                job["status"] = "FAILED"
                job["status_kor"] = "실패"
                job["percent"] = 100
                job["progress_text"] = "설치 실패"
                job["output"] = str(e)
                row = self._install_job_row(job)
            self.socketio_callback("last", row)

    def _run_install_command_stream(self, command, label, job, start_percent, end_percent, collected_output):
        P.logger.info("[NaverPaper] installing %s: %s", label, " ".join(command))
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
        last_emit = time.time()
        line_count = 0
        output_lines = []
        while True:
            line = process.stdout.readline() if process.stdout else ""
            if line:
                line = line.rstrip()
                output_lines.append(line)
                collected_output.append(line)
                line_count += 1
                if len(collected_output) > 300:
                    del collected_output[:-300]
            if process.poll() is not None:
                remaining = process.stdout.read() if process.stdout else ""
                if remaining:
                    for extra_line in remaining.splitlines():
                        output_lines.append(extra_line)
                        collected_output.append(extra_line)
                break
            now = time.time()
            if now - last_emit >= 0.4:
                span = max(1, end_percent - start_percent)
                pseudo = min(end_percent - 1, start_percent + min(span - 1, line_count % max(1, span)))
                with self.install_jobs_lock:
                    job["percent"] = pseudo
                    job["progress_text"] = f"{label} 설치 중"
                    job["output"] = "\n".join(collected_output)[-6000:]
                    row = self._install_job_row(job)
                self.socketio_callback("status", row)
                last_emit = now
        returncode = process.wait()
        with self.install_jobs_lock:
            job["percent"] = end_percent
            job["progress_text"] = f"{label} 설치 완료" if returncode == 0 else f"{label} 설치 실패"
            job["output"] = "\n".join(collected_output)[-6000:]
            row = self._install_job_row(job)
        self.socketio_callback("status", row)
        if returncode != 0:
            P.logger.warning("[NaverPaper] install failed %s: %s", label, "\n".join(output_lines)[-4000:])
            return {"ret": "error", "returncode": returncode}
        return {"ret": "success"}
