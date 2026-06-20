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

# ── 日志 ────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ── 环境变量 ────────────────────────────────────────────────
YT_API_KEY        = os.environ["YT_API_KEY"]
FEISHU_APP_ID     = os.environ["FEISHU_APP_ID"]
FEISHU_APP_SECRET = os.environ["FEISHU_APP_SECRET"]
BITABLE_APP_TOKEN = os.environ["BITABLE_APP_TOKEN"]   # /base/ 后面的字符串
BITABLE_TABLE_ID  = os.environ["BITABLE_TABLE_ID"]    # tbl 开头

# 定时任务 Cron 表达式，默认每天早上 9:00（Asia/Shanghai）
# 格式：分 时 日 月 周，例如 "0 9 * * *" = 每天9点
# 如需每6小时："0 */6 * * *"  如需每小时："0 * * * *"
SCHEDULE_CRON = os.getenv("SCHEDULE_CRON", "0 9 * * *")
SCHEDULE_TZ   = os.getenv("SCHEDULE_TZ", "Asia/Shanghai")

YT_BASE = "https://www.googleapis.com/youtube/v3"
FS_BASE = "https://open.feishu.cn/open-apis"

# 定时任务运行中防重入锁
_refresh_lock = asyncio.Lock()


# ══════════════════════════════════════════════════════════════
#  工具函数
# ══════════════════════════════════════════════════════════════

def extract_channel_identifier(url: str):
    """从频道 URL 中提取类型和标识符"""
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


def parse_social_links(description: str) -> dict:
    """从简介文本中提取各平台完整链接"""
    rules = {
        "INS": (r"instagram\.com/([\w.]+)",        "https://instagram.com/"),
        "X":   (r"(?:twitter\.com|x\.com)/([\w]+)", "https://x.com/"),
        "FB":  (r"facebook\.com/([\w.]+)",          "https://facebook.com/"),
        "TK":  (r"tiktok\.com/@?([\w.]+)",          "https://tiktok.com/@"),
    }
    result = {}
    for platform, (pat, prefix) in rules.items():
        m = re.search(pat, description, re.I)
        if m:
            result[platform] = prefix + m.group(1)
    return result


def hyperlink(url: str, text: str = None):
    """飞书超链接字段格式"""
    if not url:
        return None
    return {"text": text or url, "link": url}


def parse_email(description: str):
    m = re.search(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", description)
    return m.group(0) if m else None


def fmt_date(iso: str):
    """ISO8601 → 毫秒时间戳（飞书日期字段要求）"""
    if not iso:
        return None
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        return int(dt.timestamp() * 1000)
    except Exception:
        return None


def now_ts() -> int:
    """当前 UTC 时间毫秒时间戳"""
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
    if kind == "id":
        d = await yt_get(client, f"channels?part=snippet,statistics,contentDetails&id={value}")
    elif kind == "handle":
        d = await yt_get(client, f"channels?part=snippet,statistics,contentDetails&forHandle={value}")
    else:
        s = await yt_get(client, f"search?part=snippet&type=channel&q={value}&maxResults=1")
        items = s.get("items", [])
        if not items:
            raise RuntimeError("YouTube 搜索未找到该频道")
        cid = items[0]["snippet"]["channelId"]
        d = await yt_get(client, f"channels?part=snippet,statistics,contentDetails&id={cid}")

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
#  核心：从频道 URL 抓取所有数据，组装飞书字段
# ══════════════════════════════════════════════════════════════

async def fetch_channel_fields(client: httpx.AsyncClient, channel_url: str) -> dict:
    """抓取 YouTube 数据，返回可直接写入飞书的 fields 字典"""
    kind, value = extract_channel_identifier(channel_url)
    if not kind:
        raise RuntimeError(f"无法识别频道链接格式: {channel_url}")

    channel     = await resolve_channel(client, kind, value)
    snippet     = channel.get("snippet", {})
    stats       = channel.get("statistics", {})
    description = snippet.get("description", "")

    videos = await get_latest_videos(client, channel, max_count=6)
    views  = [int(v.get("statistics", {}).get("viewCount", 0)) for v in videos]

    avg_views      = round(sum(views) / len(views)) if views else None
    max_views      = max(views) if views else None
    min_views      = min(views) if views else None
    latest_publish = fmt_date(videos[0]["snippet"].get("publishedAt")) if videos else None

    social = parse_social_links(description)
    email  = parse_email(description)

    fields = {
        "频道链接":         hyperlink(channel_url.strip(), channel_url.strip()),
        "频道名称":         snippet.get("title", ""),
        "国家/地区":        snippet.get("country", ""),
        "邮箱":             {"text": email, "link": f"mailto:{email}"} if email else None,
        "订阅量":           int(stats["subscriberCount"])
                            if not stats.get("hiddenSubscriberCount") and stats.get("subscriberCount")
                            else None,
        "最新视频发布时间": latest_publish,
        "近6条均播":        avg_views,
        "近6条最高播":      max_views,
        "近6条最低播":      min_views,
        "INS":              hyperlink(social.get("INS")),
        "X":                hyperlink(social.get("X")),
        "FB":               hyperlink(social.get("FB")),
        "TK":               hyperlink(social.get("TK")),
        "最后更新时间":     now_ts(),
    }
    # 过滤 None，避免飞书报错
    return {k: v for k, v in fields.items() if v is not None}


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
    url = (f"{FS_BASE}/bitable/v1/apps/{BITABLE_APP_TOKEN}"
           f"/tables/{BITABLE_TABLE_ID}/records/{record_id}")
    r = await client.patch(
        url,
        headers={"Authorization": f"Bearer {token}"},
        json={"fields": fields},
        timeout=20,
    )
    data = r.json()
    if data.get("code") != 0:
        raise RuntimeError(f"飞书写入失败 record={record_id}: {data.get('msg')}")
    return data


async def list_all_records(client: httpx.AsyncClient, token: str) -> list:
    """
    拉取多维表格全部记录（自动翻页）。
    只返回「频道链接」字段有值的行。
    """
    records = []
    page_token = None

    while True:
        params = {
            "page_size": 100,
            "field_names": '["频道链接"]',   # 只拉需要的字段，节省流量
        }
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
            # 超链接字段值是 {"text":..., "link":...} 或字符串
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
#  定时任务：刷新全表
# ══════════════════════════════════════════════════════════════

async def refresh_all_records():
    """
    定时任务主体：
    1. 拉取飞书表格全部有「频道链接」的记录
    2. 逐条查询 YouTube 数据
    3. 写回飞书（顺序执行，避免 API 被限速）
    """
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
                    # 刷新时 token 可能已过期（有效期 2 小时），定期续签
                    if i % 50 == 0:
                        token = await get_feishu_token(client)
                    await update_record(client, token, rid, fields)
                    logger.info(f"  [{i}/{len(records)}] ✓ {url}")
                    ok_count += 1
                except Exception as e:
                    logger.error(f"  [{i}/{len(records)}] ✗ {url} → {e}")
                    fail_count += 1

                # 每条之间短暂等待，避免触发 YouTube API 配额限制
                await asyncio.sleep(0.5)

        elapsed = (datetime.now() - start).seconds
        logger.info(f"═══ 定时刷新完成：成功 {ok_count} / 失败 {fail_count} / 耗时 {elapsed}s ═══")


# ══════════════════════════════════════════════════════════════
#  FastAPI 应用 & 生命周期
# ══════════════════════════════════════════════════════════════

scheduler = AsyncIOScheduler(timezone=SCHEDULE_TZ)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 解析 Cron 表达式并注册定时任务
    cron_parts = SCHEDULE_CRON.strip().split()
    if len(cron_parts) != 5:
        logger.error(f"SCHEDULE_CRON 格式错误（需5段）：{SCHEDULE_CRON}，定时任务不会启动")
    else:
        minute, hour, day, month, dow = cron_parts
        scheduler.add_job(
            refresh_all_records,
            CronTrigger(
                minute=minute, hour=hour,
                day=day, month=month, day_of_week=dow,
                timezone=SCHEDULE_TZ,
            ),
            id="refresh_all",
            replace_existing=True,
            misfire_grace_time=300,   # 允许最多 5 分钟的错过容忍
        )
        scheduler.start()
        logger.info(f"✅ 定时任务已启动：Cron={SCHEDULE_CRON}  TZ={SCHEDULE_TZ}")

    yield   # ← 应用运行期间

    scheduler.shutdown(wait=False)
    logger.info("定时任务已关闭")


app = FastAPI(title="YouTube → 飞书多维表格（含定时刷新）", lifespan=lifespan)


# ══════════════════════════════════════════════════════════════
#  路由
# ══════════════════════════════════════════════════════════════

@app.post("/webhook/youtube")
async def webhook_youtube(request: Request):
    """
    飞书按钮触发 Webhook。
    飞书 POST 的 JSON 格式：
      { "record_id": "recXXXXXX", "channel_url": "https://..." }
    """
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(400, "请求体必须是 JSON")

    record_id   = (body.get("record_id") or "").strip()
    channel_url = (body.get("channel_url") or "").strip()

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
    """
    手动触发一次全表刷新（无需等待定时）。
    调用方式：POST https://你的域名/admin/refresh-now
    """
    if _refresh_lock.locked():
        return JSONResponse({"code": 1, "msg": "上一轮刷新仍在执行中，请稍后再试"})
    # 后台异步执行，不阻塞响应
    asyncio.create_task(refresh_all_records())
    return JSONResponse({"code": 0, "msg": "全表刷新任务已在后台启动"})


@app.get("/admin/status")
async def status():
    """查看定时任务状态和下次执行时间"""
    jobs = []
    for job in scheduler.get_jobs():
        next_run = job.next_run_time
        jobs.append({
            "id":       job.id,
            "next_run": next_run.isoformat() if next_run else None,
        })
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
