"""
Windows 로컬 디버그 스크립트 - FF 없이 직접 실행

사용법:
  1. 이 파일 상단의 설정값 채우기 (USER_ID, PASSWORD, COOKIE_FILE)
  2. 터미널에서: python debug_happybean.py
  3. Chromium 창이 열리면서 카페/블로그 글쓰기 과정이 보임

필요 패키지: pip install playwright playwright-stealth
브라우저 설치: playwright install chromium
"""

import asyncio
import sys
import os
import json

# ─── 설정 ────────────────────────────────────────────────────────────────────
USER_ID = "여기에_네이버_아이디"
CAFE_URL = "https://cafe.naver.com/sdckong"   # 테스트할 카페 URL
HEADLESS  = False   # False = 창 표시 (개발용), True = 창 숨김

# 쿠키 파일 경로: FF NAS에서 복사하거나 직접 로그인 후 저장
# NAS 경로 예: /data/ff_happybean/cookies/{USER_ID}_storage.json
# Windows에서 복사한 경로로 수정
COOKIE_FILE = rf"C:\Users\user\Desktop\ff_naverpaper\cookies\{USER_ID}_storage.json"
# ─────────────────────────────────────────────────────────────────────────────

sys.path.insert(0, os.path.dirname(__file__))


async def main():
    try:
        from playwright.async_api import async_playwright
        from playwright_stealth import Stealth
    except ImportError:
        print("패키지 없음. 실행: pip install playwright playwright-stealth && playwright install chromium")
        return

    try:
        from source_naverpaper import (
            write_naver_cafe_post,
            write_naver_blog_post,
            HAPPYBEAN_CAFE_BOARD,
            HAPPYBEAN_POST_TITLE,
            HAPPYBEAN_POST_CONTENT,
        )
    except ImportError as e:
        print(f"source_naverpaper 임포트 실패: {e}")
        return

    # 쿠키 로드
    storage_state = None
    if os.path.isfile(COOKIE_FILE):
        with open(COOKIE_FILE, "r", encoding="utf-8") as f:
            storage_state = json.load(f)
        print(f"쿠키 로드: {COOKIE_FILE}")
    else:
        print(f"쿠키 파일 없음: {COOKIE_FILE}")
        print("쿠키 없이 진행 (로그인 안 된 상태로 테스트)")

    def emit(msg):
        print(f"  {msg}")

    async with async_playwright() as playwright:
        browser = await playwright.chromium.launch(
            headless=HEADLESS,
            slow_mo=300 if not HEADLESS else 0,  # 창 표시 시 동작 느리게 해서 잘 보임
        )
        ctx_args = dict(
            locale="ko-KR",
            viewport={"width": 1280, "height": 900},
        )
        if storage_state:
            ctx_args["storage_state"] = storage_state

        context = await browser.new_context(**ctx_args)
        page = await context.new_page()
        await Stealth().apply_stealth_async(page)

        print("\n=== 카페 글쓰기 테스트 ===")
        ok, msg, ss = await write_naver_cafe_post(
            page, CAFE_URL, HAPPYBEAN_CAFE_BOARD,
            HAPPYBEAN_POST_TITLE, HAPPYBEAN_POST_CONTENT,
            USER_ID, emit,
        )
        print(f"결과: {'성공' if ok else '실패'} / {msg}")
        if ss:
            print(f"스크린샷: {ss}")

        await asyncio.sleep(2)

        print("\n=== 블로그 글쓰기 테스트 ===")
        ok2, msg2, ss2 = await write_naver_blog_post(
            page, HAPPYBEAN_POST_TITLE, HAPPYBEAN_POST_CONTENT,
            USER_ID, emit,
        )
        print(f"결과: {'성공' if ok2 else '실패'} / {msg2}")
        if ss2:
            print(f"스크린샷: {ss2}")

        if not HEADLESS:
            print("\n창을 닫으려면 Enter 키를 누르세요...")
            input()

        await context.close()
        await browser.close()


if __name__ == "__main__":
    asyncio.run(main())
