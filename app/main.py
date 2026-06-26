"""
YouTube → 飞书多维表格同步服务
支持：
  1. 轮询任务：每隔 POLL_INTERVAL_MINUTES 分钟扫描飞书，满足以下任一条件则刷新：
       a. 「刷新状态」= 待刷新（飞书按钮手动触发）
       b. 「最后更新时间」距今超过 REFRESH_DAYS 天（自动到期）
  2. /webhook/youtube 路由保留（兼容旧流程，但飞书无法直连时不可用）
"""

import os
import re
import asyncio
import logging
from datetime import datetime, timezone, timedelta

import httpx
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler

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

SCHEDULE_TZ            = os.getenv("SCHEDULE_TZ", "Asia/Shanghai")
POLL_INTERVAL_MINUTES  = int(os.getenv("POLL_INTERVAL_MINUTES", "1"))   # 轮询间隔（分钟）
REFRESH_DAYS           = int(os.getenv("REFRESH_DAYS", "7"))           # 自动到期天数

# 飞书「刷新状态」字段的选项值（需与飞书表格中的单选选项名称完全一致）
STATUS_PENDING  = "待刷新"   # 按钮触发时飞书写入此值
STATUS_DONE     = "已完成"   # 刷新完成后写回此值

YT_BASE = "https://www.googleapis.com/youtube/v3"
FS_BASE = "https://open.feishu.cn/open-apis"

_refresh_lock = asyncio.Lock()

COUNTRY_MAP = {
    "AF": "阿富汗", "AL": "阿尔巴尼亚", "DZ": "阿尔及利亚", "AR": "阿根廷",
    "AU": "澳大利亚", "AT": "奥地利", "BE": "比利时", "BR": "巴西",
    "CA": "加拿大", "CL": "智利", "CN": "中国", "CO": "哥伦比亚",
    "HR": "克罗地亚", "CZ": "捷克", "DK": "丹麦", "EG": "埃及",
    "FI": "芬兰", "FR": "法国", "DE": "德国", "GH": "加纳",
    "GR": "希腊", "HK": "香港", "HU": "匈牙利", "IN": "印度",
    "ID": "印度尼西亚", "IE": "爱尔兰", "IL": "以色列", "IT": "意大利",
    "JP": "日本", "KE": "肯尼亚", "KR": "韩国", "MY": "马来西亚",
    "MX": "墨西哥", "NL": "荷兰", "NZ": "新西兰", "NG": "尼日利亚",
    "NO": "挪威", "PK": "巴基斯坦", "PH": "菲律宾", "PL": "波兰",
    "PT": "葡萄牙", "RO": "罗马尼亚", "RU": "俄罗斯", "SA": "沙特阿拉伯",
    "ZA": "南非", "ES": "西班牙", "SE": "瑞典", "CH": "瑞士",
    "TW": "台湾", "TH": "泰国", "TR": "土耳其", "UA": "乌克兰",
    "AE": "阿联酋", "GB": "英国", "US": "美国", "VN": "越南",
}


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


def parse_social_links(text: str) -> dict:
    rules = {
        "INS": (r"instagram\.com/([\w.]+)",          "https://instagram.com/"),
        "X":   (r"(?:twitter\.com|x\.com)/([\w]+)", "https://x.com/"),
        "FB":  (r"facebook\.com/([\w.]+)",            "https://facebook.com/"),
        "TK":  (r"tiktok\.com/@?([\w.]+)",            "https://tiktok.com/@"),
    }
    result = {}
    for platform, (pat, prefix) in rules.items():
        m = re.search(pat, text, re.I)
        if m:
            result[platform] = prefix + m.group(1)
    return result


async def fetch_channel_links(client: httpx.AsyncClient, channel_url: str) -> str:
    """
    抓取 YouTube 频道 about 页面，从 ytInitialData 里提取 channelExternalLinkViewModel 链接。
    这些链接是频道「关于」页的链接卡片，YouTube API 不返回此数据。
    """
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        }
        clean_url = re.sub(r'/(videos|shorts|playlists|community|featured)(/.*)?$', '', channel_url.rstrip("/"))
        r = await client.get(
            clean_url + "/about",
            headers=headers, timeout=15, follow_redirects=True
        )
        html = r.text
        links = re.findall(
            r'"channelExternalLinkViewModel"\s*:\s*\{.*?"link"\s*:\s*\{.*?"content"\s*:\s*"([^"]+)"',
            html, re.DOTALL
        )
        if links:
            logger.info(f"频道外链卡片: {links}")
        return " ".join(links)
    except Exception as e:
        logger.warning(f"抓取频道页面失败（忽略）: {e}")
        return ""


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

    shorts_playlist = "UUSH" + uploads[2:]
    shorts_ids: set = set()
    try:
        sp = await yt_get(client, f"playlistItems?part=snippet&playlistId={shorts_playlist}&maxResults=50")
        shorts_ids = {
            i["snippet"]["resourceId"]["videoId"]
            for i in sp.get("items", [])
            if i.get("snippet", {}).get("resourceId")
        }
    except Exception:
        pass

    regular_ids = []
    page_token = None

    while len(regular_ids) < max_count:
        path = f"playlistItems?part=snippet&playlistId={uploads}&maxResults=50"
        if page_token:
            path += f"&pageToken={page_token}"
        pl = await yt_get(client, path)
        items = pl.get("items", [])
        for i in items:
            vid = (i.get("snippet", {}).get("resourceId", {}).get("videoId"))
            if vid and vid not in shorts_ids:
                regular_ids.append(vid)
                if len(regular_ids) >= max_count:
                    break
        page_token = pl.get("nextPageToken")
        if not page_token or not items:
            break

    if not regular_ids:
        return []

    vd = await yt_get(client, f"videos?part=snippet,statistics&id={','.join(regular_ids[:max_count])}")
    return vd.get("items", [])


# ══════════════════════════════════════════════════════════════
#  核心：组装飞书字段
# ══════════════════════════════════════════════════════════════

async def fetch_channel_fields(client: httpx.AsyncClient, channel_url: str) -> dict:
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

    branding_kw = (channel.get("brandingSettings", {})
                   .get("channel", {})
                   .get("keywords", ""))
    page_links = await fetch_channel_links(client, channel_url)
    social = parse_social_links(description + " " + branding_kw + " " + page_links)
    email  = parse_email(description + " " + page_links)
    country_code = snippet.get("country", "")
    country_name = COUNTRY_MAP.get(country_code, country_code) or None

    fields = {
        "频道":      hyperlink(channel_url.strip()),
        "频道名称":      snippet.get("title", ""),
        "国家/地区":     country_name,
        "邮箱":          email or None,
        "订阅量":        int(stats["subscriberCount"])
                         if not stats.get("hiddenSubscriberCount") and stats.get("subscriberCount")
                         else None,
        "最新发布时间":  latest_publish,
        "均播":     avg_views,
        "最高播":   max_views,
        "最低播":   min_views,
        # ✅ 修复：无社媒链接时不写入该字段，避免空 link 导致飞书静默丢弃整条记录
        "INS":           hyperlink(social.get("INS")),
        "X":             hyperlink(social.get("X")),
        "FB":            hyperlink(social.get("FB")),
        "TK":            hyperlink(social.get("TK")),
        "最后更新时间":  now_ts(),
        # 刷新完成后把状态写回「已完成」
        "刷新状态":      STATUS_DONE,
    }
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
    logger.info(f"写入字段内容 record={record_id}: {fields}")
    url = (f"{FS_BASE}/bitable/v1/apps/{BITABLE_APP_TOKEN}"
           f"/tables/{BITABLE_TABLE_ID}/records/{record_id}")
    r = await client.put(
        url,
        headers={"Authorization": f"Bearer {token}"},
        json={"fields": fields},
        timeout=20,
    )
    # 加这一行
    logger.info(f"飞书响应 record={record_id}: {r.text[:500]}")
    if r.status_code == 200:
        logger.info(f"飞书写入成功 record={record_id}")
        return {"code": 0}
    try:
        data = r.json()
        msg = data.get("msg", r.text[:200])
    except Exception:
        msg = r.text[:200]
    raise RuntimeError(f"飞书写入失败 record={record_id}: HTTP {r.status_code} - {msg}")


async def list_all_records(client: httpx.AsyncClient, token: str) -> list:
    """
    拉取所有记录，返回字段：record_id、channel_url、status、last_updated_ts
    """
    records = []
    page_token = None
    field_names = '["频道","刷新状态","最后更新时间"]'
    while True:
        params = {"page_size": 100, "field_names": field_names}
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
            fields = item.get("fields", {})

            # 解析频道链接
            url_field = fields.get("频道")
            if isinstance(url_field, dict):
                ch_url = url_field.get("link", "") or url_field.get("text", "")
            elif isinstance(url_field, str):
                ch_url = url_field
            else:
                ch_url = ""
            if not ch_url.strip():
                continue

            # 解析刷新状态（单选字段飞书返回字符串）
            status_field = fields.get("刷新状态")
            if isinstance(status_field, dict):
                status = status_field.get("text", "") or status_field.get("value", "")
            else:
                status = status_field or ""

            # 解析最后更新时间（时间戳字段，毫秒）
            last_updated_ts = fields.get("最后更新时间")  # 可能是 int 或 None

            records.append({
                "record_id":      item["record_id"],
                "channel_url":    ch_url.strip(),
                "status":         str(status).strip(),
                "last_updated_ts": int(last_updated_ts) if last_updated_ts else None,
            })

        has_more   = data.get("data", {}).get("has_more", False)
        page_token = data.get("data", {}).get("page_token")
        if not has_more:
            break
    return records


# ══════════════════════════════════════════════════════════════
#  轮询任务（替代原定时全量刷新 + 支持按钮触发 + 支持自动到期）
# ══════════════════════════════════════════════════════════════

async def poll_and_refresh():
    """
    每隔 POLL_INTERVAL_MINUTES 分钟执行一次，刷新满足以下任一条件的记录：
      1. 「刷新状态」= 待刷新（飞书按钮手动触发）
      2. 「最后更新时间」距今超过 REFRESH_DAYS 天（自动到期）
    """
    if _refresh_lock.locked():
        logger.warning("轮询：上一轮仍在执行，跳过本次")
        return

    async with _refresh_lock:
        now = datetime.now(timezone.utc)
        expire_threshold_ts = int((now - timedelta(days=REFRESH_DAYS)).timestamp() * 1000)

        async with httpx.AsyncClient() as client:
            try:
                token = await get_feishu_token(client)
            except Exception as e:
                logger.error(f"轮询：飞书鉴权失败 → {e}")
                return
            try:
                records = await list_all_records(client, token)
            except Exception as e:
                logger.error(f"轮询：拉取记录失败 → {e}")
                return

            # 筛选需要刷新的记录
            to_refresh = []
            for rec in records:
                is_pending = rec["status"] == STATUS_PENDING
                is_expired = (
                    rec["last_updated_ts"] is None or
                    rec["last_updated_ts"] < expire_threshold_ts
                )
                if is_pending or is_expired:
                    reason = []
                    if is_pending:
                        reason.append("手动触发")
                    if is_expired:
                        reason.append(f"超过{REFRESH_DAYS}天未更新")
                    to_refresh.append((rec, "+".join(reason)))

            if not to_refresh:
                logger.info(f"轮询：无需刷新的记录（共扫描 {len(records)} 条）")
                return

            logger.info(f"轮询：共 {len(to_refresh)} 条需要刷新，开始处理")
            ok_count = fail_count = 0

            for i, (rec, reason) in enumerate(to_refresh, 1):
                rid = rec["record_id"]
                url = rec["channel_url"]
                try:
                    fields = await fetch_channel_fields(client, url)
                    # 每50条刷新一次 token
                    if i % 50 == 0:
                        token = await get_feishu_token(client)
                    await update_record(client, token, rid, fields)
                    logger.info(f"  [{i}/{len(to_refresh)}] ✓ {url}（{reason}）")
                    ok_count += 1
                except Exception as e:
                    logger.error(f"  [{i}/{len(to_refresh)}] ✗ {url} → {e}")
                    fail_count += 1
                await asyncio.sleep(0.5)

            logger.info(f"轮询完成：成功 {ok_count} / 失败 {fail_count}")


# ══════════════════════════════════════════════════════════════
#  FastAPI 应用
# ══════════════════════════════════════════════════════════════

scheduler = AsyncIOScheduler(timezone=SCHEDULE_TZ)


@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler.add_job(
        poll_and_refresh,
        "interval",
        minutes=POLL_INTERVAL_MINUTES,
        id="poll_refresh",
        replace_existing=True,
        misfire_grace_time=60,
    )
    scheduler.start()
    logger.info(f"✅ 轮询任务已启动：每 {POLL_INTERVAL_MINUTES} 分钟执行一次，自动到期天数={REFRESH_DAYS}")
    yield
    scheduler.shutdown(wait=False)


app = FastAPI(title="YouTube → 飞书多维表格", lifespan=lifespan)


# ══════════════════════════════════════════════════════════════
#  路由
# ══════════════════════════════════════════════════════════════

@app.post("/webhook/youtube")
async def webhook_youtube(request: Request):
    """保留兼容旧流程，飞书无法直连 Railway 时此路由不可用"""
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


@app.get("/admin/debug-links")
async def debug_links(url: str = "https://www.youtube.com/@bookledge"):
    """调试：分析 YouTube 频道页面里社媒链接的存储位置"""
    import json as jsonlib
    async with httpx.AsyncClient() as client:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
        }
        r = await client.get(url.rstrip("/") + "/about", headers=headers, timeout=15, follow_redirects=True)
        html = r.text
        results = {"html_len": len(html), "found": {}}
        for kw in ["instagram", "facebook", "tiktok", "twitter", "bookl3dge", "goodnews"]:
            if kw.lower() in html.lower():
                idx = html.lower().find(kw.lower())
                results["found"][kw] = html[max(0, idx-80):idx+120]
        m = re.search(r'var ytInitialData\s*=\s*(\{.{100,}?\});\s*(?:var |</script>)', html, re.DOTALL)
        if m:
            results["ytInitialData_found"] = True
            try:
                data = jsonlib.loads(m.group(1))
                raw = jsonlib.dumps(data, ensure_ascii=False)
                results["ytInitialData_len"] = len(raw)
                for kw in ["instagram", "facebook", "tiktok", "channelExternalLink", "bookl3dge"]:
                    if kw.lower() in raw.lower():
                        idx = raw.lower().find(kw.lower())
                        results["found"]["json_" + kw] = raw[max(0,idx-60):idx+120]
            except Exception as e:
                results["ytInitialData_parse_error"] = str(e)
        else:
            results["ytInitialData_found"] = False
        return JSONResponse(results)


@app.get("/health")
async def health():
    return {"status": "ok"}
