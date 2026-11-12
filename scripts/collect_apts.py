"""
공동주택(K-apt) 단지 기본정보 수집기
- 전체 단지 목록(getTotalAptList4)을 받은 뒤
- 단지마다 기본정보(getAphusBassInfoV5: 사용승인일·세대수·동수 등)를 조회해
- data/apts.json 에 누적 저장합니다.

하루 호출 한도(5,000건)를 넘지 않도록 MAX_CALLS(기본 4,500)만큼만 조회하고,
아직 없는 단지(서울 → 경기 → 인천 → 그 외 순)를 먼저, 그다음 오래된 정보를 갱신합니다.

필요한 환경변수
  DATA_GO_KR_KEY : 공공데이터포털 일반 인증키 (Decoding 키 권장, Encoding 키도 자동 처리)
선택 환경변수
  MAX_CALLS      : 이번 실행의 기본정보 조회 최대 건수 (기본 4500)
  API_BASE       : 기본 https://apis.data.go.kr/1613000 (테스트용)
"""
import concurrent.futures as cf
import datetime as dt
import json
import os
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

KEY = urllib.parse.unquote(os.environ.get("DATA_GO_KR_KEY", "").strip())
BASE = os.environ.get("API_BASE", "https://apis.data.go.kr/1613000").rstrip("/")
LIST_PATHS = ["AptListService4/getTotalAptList4", "AptListServiceV4/getTotalAptList4"]
BASIC_PATH = "AptBasisInfoServiceV5/getAphusBassInfoV5"
OUT = os.environ.get("OUT_FILE", "data/apts.json")
MAX_CALLS = int(os.environ.get("MAX_CALLS", "4500"))
WORKERS = int(os.environ.get("WORKERS", "4"))
STALE_DAYS = 180
PRIORITY = ["11", "41", "28"]  # 법정동코드 앞 2자리: 서울, 경기, 인천
TODAY = dt.date.today().isoformat()


class QuotaError(Exception):
    pass


class FatalError(Exception):
    pass


def log(*a):
    print(*a, flush=True)


# ---------------------------------------------------------------- 응답 해석
def xml_to_obj(el):
    kids = list(el)
    if not kids:
        return (el.text or "").strip()
    out = {}
    for k in kids:
        v = xml_to_obj(k)
        if k.tag in out:
            if not isinstance(out[k.tag], list):
                out[k.tag] = [out[k.tag]]
            out[k.tag].append(v)
        else:
            out[k.tag] = v
    return out


def find_key(obj, key):
    """중첩된 dict/list 에서 key 를 처음 찾으면 그 값을 돌려줌"""
    if isinstance(obj, dict):
        if key in obj:
            return obj[key]
        for v in obj.values():
            r = find_key(v, key)
            if r is not None:
                return r
    elif isinstance(obj, list):
        for v in obj:
            r = find_key(v, key)
            if r is not None:
                return r
    return None


def parse(raw):
    text = raw.strip()
    if "LIMITED_NUMBER_OF_SERVICE_REQUESTS" in text:
        raise QuotaError(text[:200])
    for bad in ("SERVICE_KEY_IS_NOT_REGISTERED", "SERVICE_KEY_IS_NULL", "PERMISSION_DENIED",
                "UNREGISTERED_IP", "BLACKLIST_IP", "NO_OPENAPI_SERVICE", "SERVICE ACCESS DENIED",
                "Unauthorized", "API not found"):
        if bad in text:
            raise FatalError(f"공공데이터포털 응답 오류({bad}): {text[:300]}")
    if text.startswith("{") or text.startswith("["):
        obj = json.loads(text)
    elif text.startswith("<"):
        obj = xml_to_obj(ET.fromstring(text))
    else:
        raise FatalError(f"알 수 없는 응답: {text[:300]}")
    code = find_key(obj, "resultCode")
    if code not in (None, "", "00", "000", 0, "0"):
        msg = find_key(obj, "resultMsg")
        if str(code) == "22":
            raise QuotaError(str(msg))
        raise FatalError(f"resultCode={code} {msg}")
    return obj


def items_of(obj):
    """body.items.item / body.item / body.items(배열) 어느 형식이든 단지 목록을 꺼냄"""
    it = find_key(obj, "item")
    if it is None or it == "":
        its = find_key(obj, "items")
        if isinstance(its, list):
            return [x for x in its if isinstance(x, dict)]
        if isinstance(its, dict) and its:
            return [its]
        return []
    return it if isinstance(it, list) else [it]


# ---------------------------------------------------------------- 호출
def call(path, params, tries=3):
    q = dict(params)
    q["serviceKey"] = KEY
    q["_type"] = "json"
    url = f"{BASE}/{path}?" + urllib.parse.urlencode(q)
    err = None
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (zipzip-spot collector)"})
            with urllib.request.urlopen(req, timeout=40) as r:
                return parse(r.read().decode("utf-8", "replace"))
        except (QuotaError, FatalError):
            raise
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", "replace")
            try:
                parse(body)  # 본문에 한도·키 오류가 있으면 그대로 올림
            except (QuotaError, FatalError):
                raise
            except Exception:
                pass
            if e.code in (401, 403, 404):
                raise FatalError(f"HTTP {e.code} ({path}): {body[:300]}")
            err = f"HTTP {e.code}: {body[:200]}"
        except Exception as e:  # 네트워크·일시 오류
            err = repr(e)
        time.sleep(2 * (i + 1))
    raise RuntimeError(f"{path} 호출 실패: {err}")


def fetch_list():
    last = None
    for path in LIST_PATHS:
        try:
            rows, page, total = [], 1, None
            while True:
                obj = call(path, {"pageNo": page, "numOfRows": 1000})
                its = items_of(obj)
                if total is None:
                    t = find_key(obj, "totalCount")
                    total = int(t) if str(t or "").isdigit() else None
                rows += its
                log(f"  단지 목록 {page}페이지: {len(its)}건 (누적 {len(rows)}{'/' + str(total) if total else ''})")
                if not its or (total and len(rows) >= total) or page > 100:
                    break
                page += 1
            return rows
        except FatalError as e:
            last = e
            log(f"  {path} 사용 불가 → 다른 주소 시도: {e}")
    raise FatalError(f"단지 목록 서비스를 호출하지 못했습니다: {last}")


def to_int(v):
    try:
        return int(float(str(v).replace(",", "")))
    except Exception:
        return None


def s(v):
    return "" if v is None else str(v).strip()


def norm_basic(it, code, fallback):
    use = "".join(ch for ch in s(it.get("kaptUsedate")) if ch.isdigit())[:8]
    return {
        "c": code,
        "n": s(it.get("kaptName")) or fallback.get("n", ""),
        "a": s(it.get("kaptAddr")),
        "r": s(it.get("doroJuso")),
        "b": s(it.get("bjdCode")) or fallback.get("b", ""),
        "u": use,
        "h": to_int(it.get("kaptdaCnt")),
        "d": to_int(it.get("kaptDongCnt")),
        "t": s(it.get("codeAptNm")),
        "f": TODAY,
    }


# ---------------------------------------------------------------- 메인
def main():
    if not KEY:
        log("::error::DATA_GO_KR_KEY 가 비어 있습니다. 저장소 Settings → Secrets and variables → Actions 에 등록해 주세요.")
        sys.exit(1)

    old = {}
    if os.path.exists(OUT):
        try:
            with open(OUT, encoding="utf-8") as f:
                for r in json.load(f).get("items", []):
                    old[r["c"]] = r
        except Exception as e:
            log(f"기존 파일을 읽지 못해 새로 만듭니다: {e}")
    log(f"기존 수집 단지: {len(old)}곳")

    log("1) 전체 단지 목록 받는 중…")
    lst = fetch_list()
    listed = {}
    for it in lst:
        code = s(it.get("kaptCode"))
        if code:
            listed[code] = {"n": s(it.get("kaptName")), "b": s(it.get("bjdCode"))}
    log(f"   전국 단지 {len(listed)}곳")
    if not listed:
        raise FatalError("단지 목록이 0건입니다. 응답 형식이 바뀌었을 수 있어 기존 파일은 그대로 둡니다.")

    def prio(code):
        b = listed[code]["b"][:2]
        return PRIORITY.index(b) if b in PRIORITY else len(PRIORITY)

    missing = sorted([c for c in listed if c not in old], key=lambda c: (prio(c), c))
    cutoff = (dt.date.today() - dt.timedelta(days=STALE_DAYS)).isoformat()
    stale = sorted([c for c in listed if c in old and old[c].get("f", "") < cutoff], key=lambda c: old[c].get("f", ""))
    queue = (missing + stale)[:MAX_CALLS]
    log(f"2) 기본정보 조회: 미수집 {len(missing)}곳, 갱신 대상 {len(stale)}곳 → 이번에 {len(queue)}곳")

    result = dict(old)
    lock = threading.Lock()
    stop = threading.Event()
    stats = {"ok": 0, "fail": 0, "quota": False, "fatal": None}

    def work(code):
        if stop.is_set():
            return
        try:
            obj = call(BASIC_PATH, {"kaptCode": code})
            its = items_of(obj)
            if not its:
                with lock:
                    stats["fail"] += 1
                return
            rec = norm_basic(its[0], code, listed.get(code, {}))
            with lock:
                result[code] = rec
                stats["ok"] += 1
                if stats["ok"] % 250 == 0:
                    log(f"   …{stats['ok']}곳 완료")
        except QuotaError:
            stop.set()
            with lock:
                stats["quota"] = True
        except FatalError as e:
            stop.set()
            with lock:
                stats["fatal"] = str(e)
        except Exception as e:
            with lock:
                stats["fail"] += 1
                if stats["fail"] <= 5:
                    log(f"   {code} 실패: {e}")
        time.sleep(0.05)

    with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
        list(ex.map(work, queue))

    # 목록에서 사라진 단지는 정리
    result = {c: r for c, r in result.items() if c in listed}
    os.makedirs(os.path.dirname(OUT) or ".", exist_ok=True)
    payload = {
        "updated": TODAY,
        "source": "국토교통부 공동주택 단지 목록제공 서비스 · 공동주택 기본 정보제공 서비스 (K-apt)",
        "total": len(listed),
        "count": len(result),
        "items": sorted(result.values(), key=lambda r: r["c"]),
    }
    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))

    pct = round(len(result) / max(1, len(listed)) * 100, 1)
    summary = [
        f"### 아파트 기본정보 수집 결과 ({TODAY})",
        f"- 전국 단지: **{len(listed):,}곳**",
        f"- 수집 완료: **{len(result):,}곳 ({pct}%)**",
        f"- 이번 실행: 성공 {stats['ok']:,}곳, 실패 {stats['fail']:,}곳",
    ]
    if stats["quota"]:
        summary.append("- 오늘 호출 한도에 도달해 멈췄습니다. 내일 이어서 수집합니다.")
    if stats["fatal"]:
        summary.append(f"- 오류로 중단: {stats['fatal']}")
    log("\n".join(summary))
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf-8") as f:
            f.write("\n".join(summary) + "\n")
    if stats["fatal"] and stats["ok"] == 0:
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except FatalError as e:
        log(f"::error::{e}")
        sys.exit(1)
