import os
import re
import asyncio
import logging
import json
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from pytz import timezone as pytz_timezone

# ═════════════════════════════
# 基础配置
# ═════════════════════════════

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def must_env(name: str):
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing env: {name}")
    return v

YT_API_KEY        = must_env("YT_API_KEY")
FEISHU_APP_ID     = must_env("FEISHU_APP_ID")
FEISHU_APP_SECRET = must_env("FEISHU_APP_SECRET")
BITABLE_APP_TOKEN = must_env("BITABLE_APP_TOKEN")
BITABLE_TABLE_ID  = must_env("BITABLE_TABLE_ID")

SCHEDULE_CRON = os.getenv("SCHEDULE_CRON", "0 9 * * *")
SCHEDULE_TZ   = os.getenv("SCHEDULE_TZ", "Asia/Shanghai")

YT_BASE = "https://www.googleapis.com/youtube/v3"
FS_BASE = "https://open.feishu.cn/open-apis"

_refresh_lock = asyncio.Lock()
_last_run_cache = {}  # webhook 去重


# ═════════════════════════════
# 工具函数
# ═════════════════════════════

def extract_channel_identifier(url: str):
    url = url.strip()
    patterns = [
        ("id", r"youtube\.com/channel/(UC[\w-]+)"),
        ("handle", r"youtube\.com/@([\w.\-]+)"),
        ("custom", r"youtube\.com/c/([\w.\-]+)"),
        ("user", r"youtube\.com/user/([\w.\-]+)"),
    ]
    for kind, pat in patterns:
        m = re.search(pat, url, re.I)
        if m:
            return kind, m.group(1)
    if url.startswith("@"):
        return "handle", url[1:]
    if re.match(r"^UC[\w-]{22}$", url):
        return "id", url
    return None, None


def parse_social_links(text: str):
    rules = {
        "INS": (r"instagram\.com/([\w.]+)", "https://instagram.com/"),
        "X":   (r"(?:twitter\.com|x\.com)/([\w]+)", "https://x.com/"),
        "FB":  (r"facebook\.com/([\w.]+)", "https://facebook.com/"),
        "TK":  (r"tiktok\.com/@?([\w.]+)", "https://tiktok.com/@"),
    }
    result = {}
    for k, (pat, prefix) in rules.items():
        m = re.search(pat, text, re.I)
        if m:
            result[k] = prefix + m.group(1)
    return result


def fmt_date(iso: str):
    if not iso:
        return None
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    except:
        return None


def now_ts():
    return int(datetime.now(timezone.utc).timestamp() * 1000)


# ═════════════════════════════
# YouTube API（修复 URL 拼接）
# ═════════════════════════════

async def yt_get(client, path: str):
    url = f"{YT_BASE}/{path}"
    url += "&key=" + YT_API_KEY if "?" in url else "?key=" + YT_API_KEY

    r = await client.get(url, timeout=20)
    data = r.json()

    if "error" in data:
        raise RuntimeError(data["error"]["message"])
    return data


# ═════════════════════════════
# yt-dlp 安全版
# ═════════════════════════════

async def fetch_about_page_links(client, channel_url: str):
    about_url = channel_url.rstrip("/") + "/about"

    try:
        proc = await asyncio.create_subprocess_exec(
            "yt-dlp",
            "--dump-single-json",
            "--no-warnings",
            "--skip-download",
            about_url,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )

        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=30)
        except asyncio.TimeoutError:
            proc.kill()
            return {}

        if proc.returncode != 0:
            return {}

        data = json.loads(stdout.decode(errors="ignore"))

        links = data.get("links") or []
        if links:
            text = " ".join(i.get("url", "") for i in links)
            return parse_social_links(text)

        return parse_social_links(
            (data.get("description", "") + " " + " ".join(data.get("tags") or []))
        )

    except Exception as e:
        logger.warning(f"yt-dlp error: {e}")
        return {}


# ═════════════════════════════
# Feishu update（重试）
# ═════════════════════════════

async def retry(fn, times=3):
    for i in range(times):
        try:
            return await fn()
        except Exception:
            if i == times - 1:
                raise
            await asyncio.sleep(1.5 * (i + 1))


async def update_record(client, token, record_id, fields):
    url = f"{FS_BASE}/bitable/v1/apps/{BITABLE_APP_TOKEN}/tables/{BITABLE_TABLE_ID}/records/{record_id}"

    async def _do():
        r = await client.put(
            url,
            headers={"Authorization": f"Bearer {token}"},
            json={"fields": fields},
            timeout=20,
        )
        if r.status_code != 200:
            raise RuntimeError(r.text)

    return await retry(_do)


# ═════════════════════════════
# webhook 防重复
# ═════════════════════════════

def is_duplicate(record_id, url):
    if _last_run_cache.get(record_id) == url:
        return True
    _last_run_cache[record_id] = url
    return False


# ═════════════════════════════
# 定时任务锁 + 并发控制
# ═════════════════════════════

semaphore = asyncio.Semaphore(3)

async def refresh_all_records():
    if _refresh_lock.locked():
        return

    async with _refresh_lock:
        async with httpx.AsyncClient() as client:
            token = await get_feishu_token(client)
            records = await list_all_records(client, token)

            async def worker(i, rec):
                async with semaphore:
                    fields = await fetch_channel_fields(client, rec["channel_url"])
                    await update_record(client, token, rec["record_id"], fields)
                    logger.info(f"{i}/{len(records)} OK")

            tasks = [
                worker(i, rec)
                for i, rec in enumerate(records, 1)
            ]

            await asyncio.gather(*tasks)


# ═════════════════════════════
# FastAPI webhook（防重复）
# ═════════════════════════════

@app.post("/webhook/youtube")
async def webhook(request: Request):
    body = await request.json()

    record_id = body.get("record_id", "").strip()
    raw_url = body.get("channel_url", "")

    channel_url = (
        raw_url.get("link") if isinstance(raw_url, dict)
        else str(raw_url)
    ).strip()

    if not record_id or not channel_url:
        raise HTTPException(400, "missing fields")

    if is_duplicate(record_id, channel_url):
        return {"code": 0, "msg": "duplicate skipped"}

    async with httpx.AsyncClient() as client:
        fields = await fetch_channel_fields(client, channel_url)
        token = await get_feishu_token(client)
        await update_record(client, token, record_id, fields)

    return {"code": 0, "msg": "success"}
