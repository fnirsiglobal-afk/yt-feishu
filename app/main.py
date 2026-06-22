"""
YouTube → 飞书多维表格同步服务
支持：
  1. 飞书按钮触发 Webhook（单条写入）
  2. 定时任务自动刷新全表所有已有数据（周期可在环境变量配置）
"""

import os
import re
import asyncio
import logging
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

YT_API_KEY        = os.environ["YT_API_KEY"]
FEISHU_APP_ID     = os.environ["FEISHU_APP_ID"]
FEISHU_APP_SECRET = os.environ["FEISHU_APP_SECRET"]
BITABLE_APP_TOKEN = os.environ["BITABLE_APP_TOKEN"]
BITABLE_TABLE_ID  = os.environ["BITABLE_TABLE_ID"]

SCHEDULE_CRON = os.getenv("SCHEDULE_CRON", "0 9 * * *")
SCHEDULE_TZ   = os.getenv("SCHEDULE_TZ", "Asia/Shanghai")

YT_BASE = "https://www.googleapis.com/youtube/v3"
FS_BASE = "https://open.feishu.cn/open-apis"

_refresh_lock = asyncio.Lock()


# ══════════════════════════════════════════════════════════════
#  工具函数
# ══════════════════════════════════════════════════════════════

def extract_channel_identifier(url: str):
    url = url.strip()
    patterns = [
        ("id",     r"youtube\.com/channel/(UC[\w-]+)"),
        ("handle", r"youtube\.com/@([\w.\-]+)"),
        ("custom", r"youtube\.com/c/([\w.\-]+)"),
        ("user",   r"youtube\.com/user/([\w.\-]+)"),
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


def parse_social_links(description: str, branding_keywords: str = "") -> dict:
    # 合并简介文本和 brandingSettings keywords 一起搜索
    combined = description + " " + branding_keywords
    rules = {
        "INS": (r"instagram\.com/([\w.]+)",          "https://instagram.com/"),
        "X":   (r"(?:twitter\.com|x\.com)/([\w]+)", "https://x.com/"),
        "FB":  (r"facebook\.com/([\w.]+)",            "https://facebook.com/"),
        "TK":  (r"tiktok\.com/@?([\w.]+)",            "https://tiktok.com/@"),
    }
    result = {}
    for platform, (pat, prefix) in rules.items():
        m = re.search(pat, combined, re.I)
        if m:
            result[platform] = prefix + m.group(1)
    return result

def hyperlink(url: str, text: str = None):
    if not url:
        return None
    return {"text": text or url, "link": url}


def parse_email(description: str):
    m = re.search(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", description)
    return m.group(0) if m else None


def fmt_date(iso: str):
    if not iso:
        return None
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    except Exception:
        return None


def now_ts() -> int:
    return int(datetime.now(timezone.utc).timestamp() * 1000)


# ══════════════════════════════════════════════════════════════
#  YouTube API
# ══════════════════════════════════════════════════════════════

async def yt_get(client: httpx.AsyncClient, path: str) -> dict:
    url = f"{YT_BASE}/{path}&key={YT_API_KEY}"
    r = await client.get(url, timeout=20)
    data = r.json()
    if "error" in data:
        raise RuntimeError(f"YouTube API 错误: {data['error']['message']}")
    return data


async def resolve_channel(client: httpx.AsyncClient, kind: str, value: str) -> dict:
    parts = "snippet,statistics,contentDetails,brandingSettings"
    if kind == "id":
        d = await yt_get(client, f"channels?part={parts}&id={value}")
    elif kind == "handle":
        d = await yt_get(client, f"channels?part={parts}&forHandle={value}")
    else:
        s = await yt_get(client, f"search?part=snippet&type=channel&q={value}&maxResults=1")
        items = s.get("items", [])
        if not items:
            raise RuntimeError("YouTube 搜索未找到该频道")
        cid = items[0]["snippet"]["channelId"]
        d = await yt_get(client, f"channels?part={parts}&id={cid}")
    items = d.get("items", [])
    if not items:
        raise RuntimeError("未找到频道，请确认链接格式")
    return items[0]


async def get_latest_videos(client: httpx.AsyncClient, channel: dict, max_count=6) -> list:
    uploads = (channel.get("contentDetails", {})
               .get("relatedPlaylists", {})
               .get("uploads", ""))
    if not uploads:
        return []
    pl = await yt_get(client, f"playlistItems?part=snippet&playlistId={uploads}&maxResults={max_count}")
    video_ids = [
        i["snippet"]["resourceId"]["videoId"]
        for i in pl.get("items", [])
        if i.get("snippet", {}).get("resourceId")
    ]
    if not video_ids:
        return []
    vd = await yt_get(client, f"videos?part=snippet,statistics&id={','.join(video_ids)}")
    return vd.get("items", [])


# ══════════════════════════════════════════════════════════════
#  核心：组装飞书字段
# ══════════════════════════════════════════════════════════════

async def fetch_about_page_links(
    client: httpx.AsyncClient, channel_url: str
) -> dict:
    """
    爬取 YouTube 频道 about 页面，提取社媒链接。
    YouTube 将 about 页面的链接数据以 JSON 形式嵌入 window.__data__ 或 ytInitialData 中。
    此处直接对整段 HTML 文本运行正则，避免解析复杂 JSON。
    """
    # 构造 about URL：handle / channel / user 格式均兼容
    if "/about" not in channel_url:
        about_url = channel_url.rstrip("/") + "/about"
    else:
        about_url = channel_url

    try:
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        }
        r = await client.get(about_url, headers=headers, timeout=15,
                              follow_redirects=True)
        if r.status_code != 200:
            logger.warning(f"about 页面请求失败 HTTP {r.status_code}: {about_url}")
            return {}
        html = r.text
    except Exception as e:
        logger.warning(f"about 页面请求异常: {e}")
        return {}

    # YouTube 把外链放在 ytInitialData 的 channelExternalLinkViewModel 里，
    # 其中 link.commandRuns[].onTap.innertubeCommand.urlEndpoint.url 是 YouTube 重定向 URL，
    # 格式形如 https://www.youtube.com/redirect?...&q=https%3A%2F%2Finstagram.com%2Fxxx
    # 直接从 HTML 文本中提取 q= 参数里的真实 URL 即可。
    real_urls: list[str] = []

    # 方式1：从 redirect URL 的 q= 参数中提取真实目标
    for encoded in re.findall(r'"url":"https://www\.youtube\.com/redirect\?[^"]*?q=([^&"\\]+)', html):
        try:
            from urllib.parse import unquote
            real_urls.append(unquote(encoded))
        except Exception:
            pass

    # 方式2：直接出现的外链（有些频道直接暴露明文）
    direct = re.findall(
        r'"(https?://(?:(?!youtube\.com|youtu\.be|yt\.be|google\.com)[^"\\]{8,}))"',
        html,
    )
    real_urls.extend(direct)

    combined = " ".join(real_urls)
    return parse_social_links(combined)


# ══════════════════════════════════════════════════════════════
#  飞书 API
# ══════════════════════════════════════════════════════════════

async def get_feishu_token(client: httpx.AsyncClient) -> str:
    r = await client.post(
        f"{FS_BASE}/auth/v3/tenant_access_token/internal",
        json={"app_id": FEISHU_APP_ID, "app_secret": FEISHU_APP_SECRET},
        timeout=10,
    )
    data = r.json()
    token = data.get("tenant_access_token")
    if not token:
        raise RuntimeError(f"飞书鉴权失败: {data}")
    return token


async def update_record(client: httpx.AsyncClient, token: str,
                        record_id: str, fields: dict):
    # 飞书企业版使用 PUT 更新单条记录
    url = (f"{FS_BASE}/bitable/v1/apps/{BITABLE_APP_TOKEN}"
           f"/tables/{BITABLE_TABLE_ID}/records/{record_id}")
    r = await client.put(
        url,
        headers={"Authorization": f"Bearer {token}"},
        json={"fields": fields},
        timeout=20,
    )
    # HTTP 200 即视为成功，兼容飞书企业版返回格式差异
    if r.status_code == 200:
        logger.info(f"飞书写入成功 record={record_id}")
        return {"code": 0}
    # 非 200 才报错
    try:
        data = r.json()
        msg = data.get("msg", r.text[:200])
    except Exception:
        msg = r.text[:200]
    raise RuntimeError(f"飞书写入失败 record={record_id}: HTTP {r.status_code} - {msg}")


async def list_all_records(client: httpx.AsyncClient, token: str) -> list:
    records = []
    page_token = None
    while True:
        params = {"page_size": 100, "field_names": '["频道链接"]'}
        if page_token:
            params["page_token"] = page_token
        url = (f"{FS_BASE}/bitable/v1/apps/{BITABLE_APP_TOKEN}"
               f"/tables/{BITABLE_TABLE_ID}/records")
        r = await client.get(
            url,
            headers={"Authorization": f"Bearer {token}"},
            params=params,
            timeout=20,
        )
        data = r.json()
        if data.get("code") != 0:
            raise RuntimeError(f"飞书拉取记录失败: {data.get('msg')}")
        items = data.get("data", {}).get("items", [])
        for item in items:
            url_field = item.get("fields", {}).get("频道链接")
            if isinstance(url_field, dict):
                ch_url = url_field.get("link", "") or url_field.get("text", "")
            elif isinstance(url_field, str):
                ch_url = url_field
            else:
                ch_url = ""
            if ch_url.strip():
                records.append({"record_id": item["record_id"], "channel_url": ch_url.strip()})
        has_more   = data.get("data", {}).get("has_more", False)
        page_token = data.get("data", {}).get("page_token")
        if not has_more:
            break
    return records


# ══════════════════════════════════════════════════════════════
#  定时任务
# ══════════════════════════════════════════════════════════════

async def refresh_all_records():
    if _refresh_lock.locked():
        logger.warning("定时任务：上一轮仍在执行，跳过本次")
        return
    async with _refresh_lock:
        logger.info("═══ 定时刷新任务开始 ═══")
        start = datetime.now()
        async with httpx.AsyncClient() as client:
            try:
                token = await get_feishu_token(client)
            except Exception as e:
                logger.error(f"定时任务：飞书鉴权失败 → {e}")
                return
            try:
                records = await list_all_records(client, token)
            except Exception as e:
                logger.error(f"定时任务：拉取记录失败 → {e}")
                return
            logger.info(f"定时任务：共找到 {len(records)} 条有效记录，开始逐条刷新")
            ok_count = fail_count = 0
            for i, rec in enumerate(records, 1):
                rid = rec["record_id"]
                url = rec["channel_url"]
                try:
                    fields = await fetch_channel_fields(client, url)
                    if i % 50 == 0:
                        token = await get_feishu_token(client)
                    await update_record(client, token, rid, fields)
                    logger.info(f"  [{i}/{len(records)}] ✓ {url}")
                    ok_count += 1
                except Exception as e:
                    logger.error(f"  [{i}/{len(records)}] ✗ {url} → {e}")
                    fail_count += 1
                await asyncio.sleep(0.5)
        elapsed = (datetime.now() - start).seconds
        logger.info(f"═══ 定时刷新完成：成功 {ok_count} / 失败 {fail_count} / 耗时 {elapsed}s ═══")


# ══════════════════════════════════════════════════════════════
#  FastAPI 应用
# ══════════════════════════════════════════════════════════════

scheduler = AsyncIOScheduler(timezone=SCHEDULE_TZ)


@asynccontextmanager
async def lifespan(app: FastAPI):
    cron_parts = SCHEDULE_CRON.strip().split()
    if len(cron_parts) != 5:
        logger.error(f"SCHEDULE_CRON 格式错误：{SCHEDULE_CRON}")
    else:
        minute, hour, day, month, dow = cron_parts
        scheduler.add_job(
            refresh_all_records,
            CronTrigger(minute=minute, hour=hour, day=day,
                        month=month, day_of_week=dow, timezone=SCHEDULE_TZ),
            id="refresh_all", replace_existing=True, misfire_grace_time=300,
        )
        scheduler.start()
        logger.info(f"✅ 定时任务已启动：Cron={SCHEDULE_CRON}  TZ={SCHEDULE_TZ}")
    yield
    scheduler.shutdown(wait=False)


app = FastAPI(title="YouTube → 飞书多维表格", lifespan=lifespan)


# ══════════════════════════════════════════════════════════════
#  路由
# ══════════════════════════════════════════════════════════════

@app.post("/webhook/youtube")
async def webhook_youtube(request: Request):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(400, "请求体必须是 JSON")

    record_id = (body.get("record_id") or "").strip()
    raw_url   = body.get("channel_url") or ""
    if isinstance(raw_url, dict):
        channel_url = (raw_url.get("link") or raw_url.get("text") or "").strip()
    else:
        channel_url = str(raw_url).strip()

    if not record_id:
        raise HTTPException(400, "缺少 record_id")
    if not channel_url:
        raise HTTPException(400, "缺少 channel_url")

    logger.info(f"Webhook 触发：record={record_id}  url={channel_url}")
    try:
        async with httpx.AsyncClient() as client:
            fields   = await fetch_channel_fields(client, channel_url)
            fs_token = await get_feishu_token(client)
            await update_record(client, fs_token, record_id, fields)
    except RuntimeError as e:
        raise HTTPException(502, str(e))

    logger.info(f"Webhook 完成：record={record_id}")
    return JSONResponse({"code": 0, "msg": "success", "record_id": record_id})


@app.post("/admin/refresh-now")
async def trigger_refresh_now():
    if _refresh_lock.locked():
        return JSONResponse({"code": 1, "msg": "上一轮刷新仍在执行中，请稍后再试"})
    asyncio.create_task(refresh_all_records())
    return JSONResponse({"code": 0, "msg": "全表刷新任务已在后台启动"})


@app.get("/admin/status")
async def status():
    jobs = []
    for job in scheduler.get_jobs():
        next_run = job.next_run_time
        jobs.append({"id": job.id, "next_run": next_run.isoformat() if next_run else None})
    return JSONResponse({
        "scheduler_running": scheduler.running,
        "cron":              SCHEDULE_CRON,
        "timezone":          SCHEDULE_TZ,
        "jobs":              jobs,
        "refresh_running":   _refresh_lock.locked(),
    })


@app.get("/health")
async def health():
    return {"status": "ok"}
