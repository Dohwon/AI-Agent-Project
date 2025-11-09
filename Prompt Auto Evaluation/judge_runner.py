r"""
Judge Runner: Pairwise model comparison (A vs B) with LLM-as-Judge.
- Reads tests from CSV (extended schema)
- Calls Model A / Model B concurrently per query
- Calls Judge with your judge prompt
- Writes per-query JSONL records in your required schema
- Builds an Excel workbook:
  * Sheet1: Summary (행=항목, 우측=값)
  * Sheet2: Results table with "상위 그룹" 병합 헤더 (최상위 키 기준 머지)
No pandas required. Uses httpx + openpyxl only.
"""

import asyncio
import csv
import json
import os
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import httpx
from dotenv import load_dotenv
from tqdm import tqdm
from openpyxl import Workbook
from openpyxl.utils import get_column_letter
from openpyxl.styles import Alignment, Font, PatternFill, Border, Side
from pathlib import Path
from datetime import datetime

# Py3.11/3.12 호환용 UTC 상수
try:
    from datetime import UTC
except Exception:
    from datetime import timezone as _tz
    UTC = _tz.utc


# ---------- Config / ENV ----------

def load_env() -> Dict[str, Any]:
    load_dotenv()
    cfg = {
        # A
        "A_BASE": os.getenv("MODEL_A_BASE_URL", "").strip(),
        "A_KEY": os.getenv("MODEL_A_API_KEY", "").strip(),
        "A_MODEL": os.getenv("MODEL_A_MODEL", "").strip(),
        # B
        "B_BASE": os.getenv("MODEL_B_BASE_URL", "").strip(),
        "B_KEY": os.getenv("MODEL_B_API_KEY", "").strip(),
        "B_MODEL": os.getenv("MODEL_B_MODEL", "").strip(),
        # JUDGE
        "J_BASE": os.getenv("JUDGE_BASE_URL", "").strip(),
        "J_KEY": os.getenv("JUDGE_API_KEY", "").strip(),
        "J_MODEL": os.getenv("JUDGE_MODEL", "").strip(),
        # runtime
        "CONC": int(os.getenv("MAX_CONCURRENCY", "4")),
        "TIMEOUT": int(os.getenv("TIMEOUT", "90")),
        # optional costs
        "COST": {
            "A_IN": os.getenv("MODEL_A_INPUT_COST_PER_1K"),
            "A_OUT": os.getenv("MODEL_A_OUTPUT_COST_PER_1K"),
            "B_IN": os.getenv("MODEL_B_INPUT_COST_PER_1K"),
            "B_OUT": os.getenv("MODEL_B_OUTPUT_COST_PER_1K"),
            "J_IN": os.getenv("JUDGE_INPUT_COST_PER_1K"),
            "J_OUT": os.getenv("JUDGE_OUTPUT_COST_PER_1K"),
        }
    }
    # cast to float if present
    for k in list(cfg["COST"].keys()):
        v = cfg["COST"][k]
        if v is not None and v != "":
            try:
                cfg["COST"][k] = float(v)
            except Exception:
                cfg["COST"][k] = None
        else:
            cfg["COST"][k] = None
    return cfg

def make_chat_url(base_url: str) -> str:
    base = base_url.rstrip("/")
    if base.endswith("/v1/chat/completions"):
        return base
    return base + "/v1/chat/completions"

# ---------- HTTP Clients ----------

def make_client(base_url: str, api_key: str, timeout: int) -> httpx.AsyncClient:
    headers = {}
    if "openai" in base_url:
        headers = {"Authorization": f"Bearer {api_key}"}
    elif "upstage.ai" in base_url:
        headers = {"Authorization": f"Bearer {api_key}"}
    else:
        # 기본 Bearer 가정
        headers = {"Authorization": f"Bearer {api_key}"}
    return httpx.AsyncClient(timeout=timeout, headers=headers)

async def call_chat(
    client: httpx.AsyncClient,
    base_url: str,
    model: str,
    system_prompt: str,
    user_prompt: str,
    force_json: bool = False,
) -> Tuple[bool, Dict[str, Any] or str, Dict[str, Any]]:
    """
    Returns: (ok, payload_or_error, api_meta)
    payload_or_error:
      - ok=True  -> dict {"role": "assistant", "content": "..."} style
      - ok=False -> str error_message
    api_meta: {"duration": float, "usage": {input_tokens, output_tokens, total_tokens}}
    """
    url = make_chat_url(base_url)
    started = time.perf_counter()
    try:
        body = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            "stream": False,
        }
        if force_json:
            # OpenAI 호환 JSON 강제. 비호환 엔드포인트면 무시될 수 있음.
            body["response_format"] = {"type": "json_object"}

        resp = await client.post(url, json=body)
        resp.raise_for_status()
        data = resp.json()

        # 표준 OpenAI 스타일 파싱
        content = None
        try:
            content = data["choices"][0]["message"]["content"]
        except Exception:
            # Upstage도 유사하지만 혹시 모를 스키마 차이 대비
            # 최대한 content 문자열 꺼내기
            content = json.dumps(data, ensure_ascii=False)

        usage = data.get("usage") or {}
        in_tok = usage.get("prompt_tokens") or usage.get("input_tokens")
        out_tok = usage.get("completion_tokens") or usage.get("output_tokens")
        tot_tok = usage.get("total_tokens")
        cached_tok = usage.get("cached_tokens")

        if tot_tok is None and (in_tok is not None or out_tok is not None):
            try:
                tot_tok = (in_tok or 0) + (out_tok or 0)
            except Exception:
                tot_tok = None

        meta = {
            "duration": round(time.perf_counter() - started, 4),
            "usage": {
                "input_tokens": in_tok,
                "output_tokens": out_tok,
                "total_tokens": tot_tok,
                "cached_tokens": cached_tok,   # <- 추가
            }
        }
        return True, {"role": "assistant", "content": content}, meta

    except httpx.HTTPError as e:
        meta = {"duration": round(time.perf_counter() - started, 4), "usage": {}}
        return False, f"HTTPError: {str(e)}", meta
    except Exception as e:
        meta = {"duration": round(time.perf_counter() - started, 4), "usage": {}}
        return False, f"Error: {repr(e)}", meta

# ---------- Parsing & Validation ----------

def try_parse_json_object(text: Optional[str]) -> Optional[dict]:
    if not text:
        return None
    t = text.strip()
    try:
        return json.loads(t)
    except Exception:
        pass
    try:
        s = t.find("{")
        e = t.rfind("}")
        if s != -1 and e != -1 and e > s:
            return json.loads(t[s:e+1])
    except Exception:
        return None
    return None

def validate_case2(payload: dict, func_spec: Optional[dict]) -> Dict[str, Any]:
    out = {
        "function_name": None,
        "available_function": None,
        "function_in_whitelist": None,
        "available_in_whitelist": None,
    }
    if not isinstance(payload, dict) or payload.get("case") != 2:
        return out
    fn = payload.get("function")
    af = payload.get("available_function")
    out["function_name"] = fn
    out["available_function"] = af
    if func_spec:
        out["function_in_whitelist"] = fn in func_spec
        if out["function_in_whitelist"]:
            out["available_in_whitelist"] = (af in func_spec.get(fn, []))
    return out

def validate_case3(payload: dict, min_id: Optional[int], max_id: Optional[int]) -> Dict[str, Any]:
    out = {"prepared_number": None, "prepared_in_range": None}
    if not isinstance(payload, dict) or payload.get("case") != 3:
        return out
    val = payload.get("prepared_question_list_number")
    n = None
    try:
        if val is not None:
            n = int(str(val).strip())
    except Exception:
        n = None
    out["prepared_number"] = val
    if n is not None and min_id is not None and max_id is not None:
        out["prepared_in_range"] = (min_id <= n <= max_id)
    return out

# ---------- Flatten helpers for Excel ----------

def flatten(obj: Any, prefix: str = "", out: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if out is None:
        out = {}
    if isinstance(obj, dict):
        for k, v in obj.items():
            key = f"{prefix}.{k}" if prefix else str(k)
            flatten(v, key, out)
    elif isinstance(obj, list):
        for i, v in enumerate(obj):
            key = f"{prefix}[{i}]"
            flatten(v, key, out)
    else:
        out[prefix] = obj
    return out

def top_group(col: str) -> str:
    # 최상위 그룹 = 첫 번째 점(.) 앞부분
    return col.split(".", 1)[0] if "." in col else col

# ---------- Excel writer ----------

def write_excel_from_jsonl(jsonl_path: Path, xlsx_path: Path, summary: Dict[str, Any]) -> None:
    # Load JSONL
    records: List[dict] = []
    with jsonl_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except Exception:
                continue

    wb = Workbook()
    ws_sum = wb.active
    ws_sum.title = "Summary"

    # --- Summary 작성(기존 로직) ---
    def write_kv(start_row: int, title: str, kv: Dict[str, Any]) -> int:
        ws_sum.cell(row=start_row, column=1, value=title).font = Font(bold=True)
        r = start_row + 1
        for k, v in kv.items():
            ws_sum.cell(row=r, column=1, value=str(k))
            ws_sum.cell(row=r, column=2, value=str(v))
            r += 1
        ws_sum.merge_cells(start_row=start_row, start_column=1, end_row=start_row, end_column=2)
        return r + 1

    row = 1
    scalar_top = {
        "total_evaluations": summary.get("total_evaluations"),
        "successful_evaluations": summary.get("successful_evaluations"),
        "failed_evaluations": summary.get("failed_evaluations"),
        "timestamp": summary.get("timestamp"),
    }
    row = write_kv(row, "Overview", scalar_top)
    row = write_kv(row, "Overall Results", summary.get("overall_results", {}))
    row = write_kv(row, "By Case", {k: v for k, v in summary.get("by_case", {}).items()})
    row = write_kv(row, "Confidence Distribution", summary.get("confidence_distribution", {}))
    row = write_kv(row, "Token Usage (Total & By Model)", summary.get("token_usage", {}))
    row = write_kv(row, "Cost Estimate (By Model)", summary.get("cost_estimate", {}).get("by_model", {}))
    row = write_kv(row, "Performance", summary.get("performance", {}))

    for c in (1, 2):
        ws_sum.column_dimensions[get_column_letter(c)].width = 40

    # --- Results sheet ---
    ws = wb.create_sheet("Results")



    # 1) 플래튼 + 전체 컬럼 수집
    def flatten(obj: Any, prefix: str = "", out: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        if out is None:
            out = {}
        if isinstance(obj, dict):
            for k, v in obj.items():
                key = f"{prefix}.{k}" if prefix else str(k)
                flatten(v, key, out)
        elif isinstance(obj, list):
            for i, v in enumerate(obj):
                key = f"{prefix}[{i}]"
                flatten(v, key, out)
        else:
            out[prefix] = obj
        return out

    flat_rows: List[Dict[str, Any]] = []
    cols_set = set()
    for rec in records:
        fr = flatten(rec)
        flat_rows.append(fr)
        cols_set.update(fr.keys())

    # 2) 고정(선두) 컬럼 정의 — A: user_input, B: query_index, C: expected_case
    pinned = ["user_input", "query_index", "expected_case"]
    # 없는 것은 자동으로 스킵되도록 유지
    pinned_present = [c for c in pinned if c in cols_set]
    for c in pinned_present:
        cols_set.discard(c)
    # 나머지 정렬
    rest_cols = sorted(cols_set)
    cols = pinned_present + rest_cols

    # 3) 헤더 2행 구성
    ws.append(["" for _ in cols])  # row 1 placeholder
    ws.append(cols)                # row 2 keys

    # 3-1) 상단 그룹(첫 점 앞)
    def top_group(col: str) -> str:
        return col.split(".", 1)[0] if "." in col else col

    group_to_cols: Dict[str, List[int]] = {}
    for idx, col in enumerate(cols, start=1):
        g = top_group(col)
        group_to_cols.setdefault(g, []).append(idx)

    # 3-2) 그룹 병합 + 점 없는 단일 컬럼은 행 병합
    for g, idxs in group_to_cols.items():
        start = min(idxs)
        end = max(idxs)
        if g in pinned_present:
            # pinned 컬럼은 top_group==컬럼명(점 없음) → 세로 병합(1~2행)
            ws.cell(row=1, column=start, value=g)
            ws.merge_cells(start_row=1, start_column=start, end_row=2, end_column=start)
        else:
            # 일반 그룹
            ws.cell(row=1, column=start, value=g)
            if end > start:
                ws.merge_cells(start_row=1, start_column=start, end_row=1, end_column=end)

    # 3-3) 스타일링
    for r in (1, 2):
        for c in range(1, len(cols) + 1):
            cell = ws.cell(row=r, column=c)
            cell.alignment = Alignment(horizontal="center", vertical="center", wrap_text=True)
            cell.font = Font(bold=True)
            cell.fill = PatternFill("solid", fgColor="F2F2F2")
            cell.border = Border(
                left=Side(style="thin", color="CCCCCC"),
                right=Side(style="thin", color="CCCCCC"),
                top=Side(style="thin", color="CCCCCC"),
                bottom=Side(style="thin", color="CCCCCC"),
            )

    # 4) 데이터 행
    for fr in flat_rows:
        ws.append([fr.get(k, "") for k in cols])

    # 5) 컬럼 폭 자동 조정(최대 70)
    for i, col in enumerate(cols, start=1):
        max_len = max(len(str(col)), *(len(str(r.get(col, ""))) for r in flat_rows))
        ws.column_dimensions[get_column_letter(i)].width = min(max(12, max_len + 2), 70)


    try:
        wb.save(xlsx_path)
    except PermissionError:
    # 드물게 경로가 잠겨있으면 즉석에서 또 다른 고유 이름으로 재시도
        xlsx_path = _uniquify_path(xlsx_path)
        wb.save(xlsx_path)



# ---------- Summary builder ----------

def safe_int(x) -> int:
    try:
        return int(x)
    except Exception:
        return 0

def build_summary(records: List[dict], costs: Dict[str, Optional[float]]) -> Dict[str, Any]:
    total = len(records)
    successes = sum(1 for r in records if r.get("evaluation_status") == "success")
    fails = total - successes

    model_a_wins = model_b_wins = ties = 0
    by_case: Dict[str, Dict[str, int]] = {}
    conf_dist = {f"confidence_{i}": 0 for i in range(1, 6)}

    # 총합(기존)
    total_input_tok = total_output_tok = total_tok = 0
    total_dur = 0.0

    # 모델별 합계
    agg = {
        "A": {"in":0, "out":0, "tot":0, "cached":0, "dur":0.0},
        "B": {"in":0, "out":0, "tot":0, "cached":0, "dur":0.0},
        "J": {"in":0, "out":0, "tot":0, "cached":0, "dur":0.0},
    }

    for rec in records:
        ev = rec.get("evaluation") or {}
        winner = ev.get("winner")
        if winner == "A": model_a_wins += 1
        elif winner == "B": model_b_wins += 1
        elif winner == "tie": ties += 1

        exp_case = str(rec.get("expected_case"))
        by_case.setdefault(f"case_{exp_case}", {"a_wins":0,"b_wins":0,"ties":0,"total":0})
        by_case[f"case_{exp_case}"]["total"] += 1
        if winner == "A": by_case[f"case_{exp_case}"]["a_wins"] += 1
        elif winner == "B": by_case[f"case_{exp_case}"]["b_wins"] += 1
        elif winner == "tie": by_case[f"case_{exp_case}"]["ties"] += 1

        conf = ev.get("confidence")
        if isinstance(conf, int) and 1 <= conf <= 5:
            conf_dist[f"confidence_{conf}"] += 1

        meta = rec.get("api_call_metadata") or {}
        total_dur += float(meta.get("duration_seconds") or 0.0)
        total_input_tok += int(meta.get("input_tokens") or 0)
        total_output_tok += int(meta.get("output_tokens") or 0)
        total_tok += int(meta.get("total_tokens") or 0)

        ub = rec.get("usage_breakdown") or {}
        for k in ("A","B","J"):
            d = ub.get(k) or {}
            agg[k]["in"]     += int(d.get("input_tokens")  or 0)
            agg[k]["out"]    += int(d.get("output_tokens") or 0)
            agg[k]["tot"]    += int(d.get("total_tokens")  or 0)
            agg[k]["cached"] += int(d.get("cached_tokens") or 0)
            agg[k]["dur"]    += float(d.get("duration")    or 0.0)

    def rate(part, whole): return f"{(part/whole*100):.1f}%" if whole else "0.0%"
    overall_results = {
        "model_a_wins": model_a_wins,
        "model_b_wins": model_b_wins,
        "ties": ties,
        "model_a_win_rate": rate(model_a_wins, total),
        "model_b_win_rate": rate(model_b_wins, total),
        "tie_rate": rate(ties, total),
    }

    token_usage = {
        "total_input_tokens": total_input_tok,
        "total_output_tokens": total_output_tok,
        "total_tokens": total_tok,
        "by_model": {
            "A": {"input": agg["A"]["in"], "output": agg["A"]["out"], "total": agg["A"]["tot"], "cached": agg["A"]["cached"]},
            "B": {"input": agg["B"]["in"], "output": agg["B"]["out"], "total": agg["B"]["tot"], "cached": agg["B"]["cached"]},
            "J": {"input": agg["J"]["in"], "output": agg["J"]["out"], "total": agg["J"]["tot"], "cached": agg["J"]["cached"]},
        }
    }

    def calc(tokens: int, per_1k: Optional[float]) -> Optional[float]:
        if per_1k is None: return None
        return (tokens/1000.0) * per_1k

    # 모델별 비용
    cost_by_model = {}
    for tag, pfx in (("A","A"),("B","B"),("J","J")):
        in_cost  = calc(agg[tag]["in"],  costs.get(f"{pfx}_IN"))
        out_cost = calc(agg[tag]["out"], costs.get(f"{pfx}_OUT"))
        cached_cost = calc(agg[tag]["cached"], costs.get(f"{pfx}_CACHED")) if costs.get(f"{pfx}_CACHED") is not None else None
        total_cost = None
        if in_cost is not None and out_cost is not None:
            total_cost = in_cost + out_cost + (cached_cost or 0)
        cost_by_model[tag] = {
            "input_cost_usd":  f"${in_cost:.2f}"   if in_cost  is not None else "N/A",
            "output_cost_usd": f"${out_cost:.2f}"  if out_cost is not None else "N/A",
            "cached_cost_usd": f"${cached_cost:.2f}" if cached_cost is not None else "N/A",
            "total_cost_usd":  f"${total_cost:.2f}" if total_cost is not None else "N/A",
        }

    # 총합(모형별 합산이 아니라 기존 total_* 기반) — 참고용
    performance = {
        "total_duration_seconds": round(total_dur, 2),
        "average_duration_per_query": round(total_dur / total, 2) if total else 0.0,
    }

    return {
        "total_evaluations": total,
        "successful_evaluations": successes,
        "failed_evaluations": fails,
        "overall_results": overall_results,
        "by_case": by_case,
        "confidence_distribution": conf_dist,
        "token_usage": token_usage,
        "cost_estimate": {
            "by_model": cost_by_model
        },
        "performance": performance,
        "timestamp": datetime.now(UTC).isoformat()
    }


    def rate(part, whole):
        return f"{(part / whole * 100):.1f}%" if whole else "0.0%"

    overall_results = {
        "model_a_wins": model_a_wins,
        "model_b_wins": model_b_wins,
        "ties": ties,
        "model_a_win_rate": rate(model_a_wins, total),
        "model_b_win_rate": rate(model_b_wins, total),
        "tie_rate": rate(ties, total),
    }

    token_usage = {
        "total_input_tokens": total_input_tok,
        "total_output_tokens": total_output_tok,
        "total_tokens": total_tok,
    }

    # Optional cost estimate — 비용 단가가 없으면 N/A
    def calc_cost(tokens: int, per_1k: Optional[float]) -> Optional[float]:
        if per_1k is None:
            return None
        return (tokens / 1000.0) * per_1k

    # 여기서는 총합 토큰 기준의 단순 비용(모델별 분리는 생략)
    total_in_cost = calc_cost(total_input_tok, (costs.get("A_IN") or 0) + (costs.get("B_IN") or 0) + (costs.get("J_IN") or 0))
    total_out_cost = calc_cost(total_output_tok, (costs.get("A_OUT") or 0) + (costs.get("B_OUT") or 0) + (costs.get("J_OUT") or 0))

    def fmt_usd(x: Optional[float]) -> str:
        return f"${x:.2f}" if isinstance(x, (int, float)) else "N/A"

    cost_estimate = {
        "input_cost_usd": fmt_usd(total_in_cost),
        "output_cost_usd": fmt_usd(total_out_cost),
        "total_cost_usd": fmt_usd( (total_in_cost or 0) + (total_out_cost or 0) if (total_in_cost is not None and total_out_cost is not None) else None )
    }

    performance = {
        "total_duration_seconds": round(total_dur, 2),
        "average_duration_per_query": round(total_dur / total, 2) if total else 0.0,
    }

    return {
        "total_evaluations": total,
        "successful_evaluations": successes,
        "failed_evaluations": fails,
        "overall_results": overall_results,
        "by_case": by_case,
        "confidence_distribution": conf_dist,
        "token_usage": token_usage,
        "cost_estimate": cost_estimate,
        "performance": performance,
        "timestamp": datetime.now(UTC).isoformat()
    }

# ---------- Judge payload builder ----------

def build_judge_user_content(
    user_input: str,
    expected_case: Any,
    model_a_output: str,
    model_b_output: str
) -> str:
    # Judge에게 전달되는 user 메시지(간단/명시적)
    return (
        "## Inputs\n"
        f"- User query: {user_input}\n"
        f"- Expected case: {expected_case}\n\n"
        "### Response A\n"
        f"{model_a_output}\n\n"
        "### Response B\n"
        f"{model_b_output}\n"
        "\n"
        "## Task\n"
        "Evaluate per the system instructions and output JSON ONLY."
    )

def _uniquify_path(p: Path) -> Path:
    """이미 존재하면 ' (1)', ' (2)' … 붙여서 충돌 나지 않는 경로를 돌려준다."""
    if not p.exists():
        return p
    i = 1
    stem, suffix = p.stem, p.suffix
    while True:
        cand = p.with_name(f"{stem} ({i}){suffix}")
        if not cand.exists():
            return cand
        i += 1

def make_dated_output_paths(out_dir: Path, base_prefix: str = "results") -> tuple[Path, Path]:
    """
    오늘 날짜(YYMMDD)를 붙인 JSONL/XLSX 경로를 만들고, 이미 있으면 (1) (2) …로 넘버링한다.
    예) results_251106.jsonl, results_251106 (1).jsonl …
    """
    out_dir.mkdir(parents=True, exist_ok=True)
    ymd = datetime.now().strftime("%y%m%d")
    jsonl = _uniquify_path(out_dir / f"{base_prefix}_{ymd}.jsonl")
    xlsx  = _uniquify_path(out_dir / f"{base_prefix}_{ymd}.xlsx")
    return jsonl, xlsx

# ---------- Runner ----------

async def process_one(
    idx: int,
    row: Dict[str, str],
    clients: Dict[str, httpx.AsyncClient],
    cfg: Dict[str, Any],
    prompts: Dict[str, str],
    func_spec: Optional[dict],
    prepared_min: Optional[int],
    prepared_max: Optional[int],
) -> dict:
    query = (row.get("query") or "").strip()
    exp_case = row.get("expected_case")
    try:
        exp_case = int(exp_case) if exp_case not in (None, "") else None
    except Exception:
        exp_case = None

    # (옵션 기대값)
    exp_fn  = (row.get("expected_function") or "").strip() or None
    exp_act = (row.get("expected_available_function") or "").strip() or None
    exp_pq  = (row.get("expected_prepared_question") or "").strip() or None

    # A/B 모델 동시 호출
    a_coro = call_chat(
        clients["A"], cfg["A_BASE"], cfg["A_MODEL"], prompts["A_SYS"], query, force_json=False
    )
    b_coro = call_chat(
        clients["B"], cfg["B_BASE"], cfg["B_MODEL"], prompts["B_SYS"], query, force_json=False
    )
    (a_ok, a_payload, a_meta), (b_ok, b_payload, b_meta) = await asyncio.gather(a_coro, b_coro)

    a_content = a_payload["content"] if a_ok else ""
    b_content = b_payload["content"] if b_ok else ""

    # 로컬 파싱/검증
    def try_parse_json_object(text: Optional[str]) -> Optional[dict]:
        if not text:
            return None
        t = text.strip()
        try:
            return json.loads(t)
        except Exception:
            pass
        try:
            s = t.find("{"); e = t.rfind("}")
            if s != -1 and e != -1 and e > s:
                return json.loads(t[s:e+1])
        except Exception:
            return None
        return None

    a_parsed = try_parse_json_object(a_content) if a_ok else None
    b_parsed = try_parse_json_object(b_content) if b_ok else None

    a_case = (a_parsed or {}).get("case")
    b_case = (b_parsed or {}).get("case")

    def validate_case2(payload: dict, func_spec: Optional[dict]) -> Dict[str, Any]:
        out = {"function_name": None, "available_function": None,
               "function_in_whitelist": None, "available_in_whitelist": None}
        if not isinstance(payload, dict) or payload.get("case") != 2:
            return out
        fn = payload.get("function")
        af = payload.get("available_function")
        out["function_name"] = fn
        out["available_function"] = af
        if func_spec:
            out["function_in_whitelist"] = fn in func_spec
            if out["function_in_whitelist"]:
                out["available_in_whitelist"] = (af in func_spec.get(fn, []))
        return out

    def validate_case3(payload: dict, min_id: Optional[int], max_id: Optional[int]) -> Dict[str, Any]:
        out = {"prepared_number": None, "prepared_in_range": None}
        if not isinstance(payload, dict) or payload.get("case") != 3:
            return out
        val = payload.get("prepared_question_list_number")
        n = None
        try:
            if val is not None:
                n = int(str(val).strip())
        except Exception:
            n = None
        out["prepared_number"] = val
        if n is not None and min_id is not None and max_id is not None:
            out["prepared_in_range"] = (min_id <= n <= max_id)
        return out

    a_c2 = validate_case2(a_parsed or {}, func_spec)
    b_c2 = validate_case2(b_parsed or {}, func_spec)
    a_c3 = validate_case3(a_parsed or {}, prepared_min, prepared_max)
    b_c3 = validate_case3(b_parsed or {}, prepared_min, prepared_max)

    # 기대값 직접 비교 플래그
    def eq(a, b):
        return (str(a).strip() == str(b).strip()) if (a is not None and b is not None) else None

    a_func_eq = eq((a_parsed or {}).get("function"), exp_fn) if a_case == 2 and exp_fn else None
    a_act_eq  = eq((a_parsed or {}).get("available_function"), exp_act) if a_case == 2 and exp_act else None
    b_func_eq = eq((b_parsed or {}).get("function"), exp_fn) if b_case == 2 and exp_fn else None
    b_act_eq  = eq((b_parsed or {}).get("available_function"), exp_act) if b_case == 2 and exp_act else None

    a_pq_eq   = eq((a_parsed or {}).get("prepared_question_list_number"), exp_pq) if a_case == 3 and exp_pq else None
    b_pq_eq   = eq((b_parsed or {}).get("prepared_question_list_number"), exp_pq) if b_case == 3 and exp_pq else None

    # Judge 호출
    def build_judge_user_content(user_input, expected_case, model_a_output, model_b_output) -> str:
        return (
            "## Inputs\n"
            f"- User query: {user_input}\n"
            f"- Expected case: {expected_case}\n\n"
            "### Response A\n"
            f"{model_a_output}\n\n"
            "### Response B\n"
            f"{model_b_output}\n"
            "\n"
            "## Task\n"
            "Evaluate per the system instructions and output JSON ONLY."
        )

    judge_user = build_judge_user_content(query, exp_case, a_content, b_content)
    j_ok, j_payload, j_meta = await call_chat(
        clients["J"], cfg["J_BASE"], cfg["J_MODEL"], prompts["J_SYS"], judge_user, force_json=True
    )

    # ---- 여기서부터 j_meta를 안전하게 사용 ----
    def tok(u, k):
        try:
            return int((u.get("usage") or {}).get(k) or 0)
        except Exception:
            return 0

    # 모델별 usage_breakdown (A/B/J)
    usage_breakdown = {
        "A": {
            "input_tokens":  tok(a_meta, "input_tokens"),
            "output_tokens": tok(a_meta, "output_tokens"),
            "total_tokens":  tok(a_meta, "total_tokens"),
            "cached_tokens": tok(a_meta, "cached_tokens"),
            "duration":      float(a_meta.get("duration") or 0.0),
        },
        "B": {
            "input_tokens":  tok(b_meta, "input_tokens"),
            "output_tokens": tok(b_meta, "output_tokens"),
            "total_tokens":  tok(b_meta, "total_tokens"),
            "cached_tokens": tok(b_meta, "cached_tokens"),
            "duration":      float(b_meta.get("duration") or 0.0),
        },
        "J": {
            "input_tokens":  tok(j_meta, "input_tokens"),
            "output_tokens": tok(j_meta, "output_tokens"),
            "total_tokens":  tok(j_meta, "total_tokens"),
            "cached_tokens": tok(j_meta, "cached_tokens"),
            "duration":      float(j_meta.get("duration") or 0.0),
        },
    }

    # 합계(레코드용)
    total_input_tokens  = usage_breakdown["A"]["input_tokens"] + usage_breakdown["B"]["input_tokens"] + usage_breakdown["J"]["input_tokens"]
    total_output_tokens = usage_breakdown["A"]["output_tokens"] + usage_breakdown["B"]["output_tokens"] + usage_breakdown["J"]["output_tokens"]
    total_tokens        = usage_breakdown["A"]["total_tokens"]  + usage_breakdown["B"]["total_tokens"]  + usage_breakdown["J"]["total_tokens"]
    total_duration      = usage_breakdown["A"]["duration"]      + usage_breakdown["B"]["duration"]      + usage_breakdown["J"]["duration"]

    evaluation_status = "success" if j_ok else "failed"
    evaluation_obj = None
    if j_ok:
        try:
            evaluation_obj = json.loads(j_payload["content"])
        except Exception:
            evaluation_status = "failed"

    record = {
        "query_index": idx,
        "user_input": query,
        "expected_case": exp_case,
        "model_a_output": a_content,
        "model_b_output": b_content,
        "evaluation": evaluation_obj,
        "evaluation_status": evaluation_status,
        "timestamp": datetime.now(UTC).isoformat(),

        "api_call_metadata": {
            "duration_seconds": round(total_duration, 2),
            "input_tokens": total_input_tokens,
            "output_tokens": total_output_tokens,
            "total_tokens": total_tokens
        },

        "usage_breakdown": usage_breakdown,

        # 모델 정보 & 호출 성공/실패
        "model_a": cfg["A_MODEL"],
        "model_b": cfg["B_MODEL"],
        "a_ok": a_ok,
        "b_ok": b_ok,
        "a_error": None if a_ok else a_payload,
        "b_error": None if b_ok else b_payload,
        "judge_ok": j_ok,
        "judge_error": None if j_ok else j_payload,

        # 로컬 파싱/검증 로그
        "a_parse_ok": a_parsed is not None,
        "b_parse_ok": b_parsed is not None,
        "model_a_case": a_case,
        "model_b_case": b_case,

        "a_function_name": a_c2.get("function_name"),
        "a_available_function": a_c2.get("available_function"),
        "a_function_in_whitelist": a_c2.get("function_in_whitelist"),
        "a_available_in_whitelist": a_c2.get("available_in_whitelist"),

        "b_function_name": b_c2.get("function_name"),
        "b_available_function": b_c2.get("available_function"),
        "b_function_in_whitelist": b_c2.get("function_in_whitelist"),
        "b_available_in_whitelist": b_c2.get("available_in_whitelist"),

        "a_prepared_number": a_c3.get("prepared_number"),
        "a_prepared_in_range": a_c3.get("prepared_in_range"),
        "b_prepared_number": b_c3.get("prepared_number"),
        "b_prepared_in_range": b_c3.get("prepared_in_range"),

        "expected_function": exp_fn,
        "expected_available_function": exp_act,
        "expected_prepared_question": exp_pq,

        "a_function_matches_expected": a_func_eq,
        "a_action_matches_expected": a_act_eq,
        "b_function_matches_expected": b_func_eq,
        "b_action_matches_expected": b_act_eq,
        "a_pq_matches_expected": a_pq_eq,
        "b_pq_matches_expected": b_pq_eq,
    }
    return record


async def main_async(args):
    cfg = load_env()
    # 프롬프트 로드
    model_a_sys = Path(args.model_a_prompt).read_text(encoding="utf-8")
    model_b_sys = Path(args.model_b_prompt).read_text(encoding="utf-8")
    judge_sys   = Path(args.judge_prompt).read_text(encoding="utf-8")

    # functions_spec (옵션)
    func_spec = None
    if args.functions_spec:
        p = Path(args.functions_spec)
        if p.exists():
            func_spec = json.loads(p.read_text(encoding="utf-8"))

    # clients
    async with make_client(cfg["A_BASE"], cfg["A_KEY"], cfg["TIMEOUT"]) as cli_a, \
               make_client(cfg["B_BASE"], cfg["B_KEY"], cfg["TIMEOUT"]) as cli_b, \
               make_client(cfg["J_BASE"], cfg["J_KEY"], cfg["TIMEOUT"]) as cli_j:
        clients = {"A": cli_a, "B": cli_b, "J": cli_j}
        prompts = {"A_SYS": model_a_sys, "B_SYS": model_b_sys, "J_SYS": judge_sys}

        # read tests
        rows = []
        with open(args.tests, "r", encoding="utf-8") as f:
            rdr = csv.DictReader(f)
            for r in rdr:
                rows.append(r)

        # 기존: out_dir = Path(args.out); jsonl_path = out_dir / "results.jsonl"; xlsx_path = out_dir / "results.xlsx"
        out_dir = Path(args.out)
        jsonl_path, xlsx_path = make_dated_output_paths(out_dir, base_prefix="results")


        recs: List[dict] = []
        with jsonl_path.open("w", encoding="utf-8") as fw:
            for i, row in enumerate(tqdm(rows, desc="Evaluating", unit="q")):
                rec = await process_one(
                    idx=i,
                    row=row,
                    clients=clients,
                    cfg=cfg,
                    prompts=prompts,
                    func_spec=func_spec,
                    prepared_min=args.prepared_min,
                    prepared_max=args.prepared_max,
                )
                fw.write(json.dumps(rec, ensure_ascii=False) + "\n")
                recs.append(rec)

        # Build summary & write Excel
        summary = build_summary(recs, cfg["COST"])
        write_excel_from_jsonl(jsonl_path, xlsx_path, summary)

        print(f"\nDone.\n- JSONL: {jsonl_path}\n- Excel : {xlsx_path}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--tests", type=str, required=True, help="CSV file with columns: query, expected_case, expected_function, expected_available_function, expected_prepared_question")
    parser.add_argument("--model-a-prompt", type=str, required=True)
    parser.add_argument("--model-b-prompt", type=str, required=True)
    parser.add_argument("--judge-prompt", type=str, required=True)
    parser.add_argument("--functions-spec", type=str, default="", help="JSON mapping of function -> [allowed actions]")
    parser.add_argument("--prepared-min", type=int, default=1)
    parser.add_argument("--prepared-max", type=int, default=60)
    parser.add_argument("--out", type=str, required=True, help="Output directory")
    asyncio.run(main_async(parser.parse_args()))