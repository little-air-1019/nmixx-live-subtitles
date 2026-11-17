"""Offline triage and local audio review. No network clients or paid model calls."""
import argparse
from bisect import bisect_right
from collections import Counter, defaultdict
from datetime import datetime, timezone
import hashlib
import html
import json
from pathlib import Path
import re
import statistics

RATE = 16_000
# Categories correspond to the 2026-09-27 review, not a complete error taxonomy.
# A trigger is a review opportunity, NEVER a proven mistranslation.
RULES = [
    ("R01", "米腸／冬粉等食物詞", r"순대|당면", r"酸菜湯|糯米粉|滷肉", "驗證食物詞 glossary，固定音訊比較加入前後的效果。"),
    ("R02", "遮光簾與裝設語境", r"암막|안마커튼|안 막혔|달았|달면", r"堵|按摩簾|上車", "先回聽辨識是否正確，再檢查裝設動詞及前後文。"),
    ("R03", "否定與限定語意", r"기만|않|없|못 |안 ", r"不只是", "人工核對否定範圍；以短樣本測試翻譯上下文。"),
    ("R04", "主詞與省略主語", r"나는|저는|라도 했으면", r"你|他|她", "合併上下文核對說話對象，避免逐片段自行補主詞。"),
    ("R05", "美甲／皮膚與同音詞", r"내일|네일|아토피|손톱", r"明天|頭皮炎|手指", "回聽美甲同音詞，分開標註辨識與詞義問題。"),
    ("R06", "蘿蔔泡菜等食物詞", r"깍두기", r"蚯蚓", "檢查食物詞轉錄與翻譯，測試 glossary。"),
    ("R07", "髮型詞區別", r"단발|숏컷", r"短髮", "確認不同髮型是否被譯成相同詞。"),
    ("R08", "數字、鬧鐘與時間關係", r"\d+\s*분|알람", r"響了.{0,6}分鐘|關了.{0,6}分鐘", "核對時間點、剩餘時間、持續時間，不只比對數字。"),
    ("R09", "蛋糕品牌、品名與飲料", r"시루|성심당|홍초", r"橘子汁|蛋糕汁|紅酒精", "確認品名後加入有限術語表，保留原詞供核對。"),
    ("R10", "遊戲與慣用語", r"사다리", r"上了梯子", "由房間分配等上下文判斷是否為抽籤遊戲。"),
    ("R11", "節日與四格拍貼", r"추석|4컷|네컷", r"除夕|第四個鏡頭", "核對文化詞與同場譯名一致性。"),
    ("R12", "成員名與近音詞", r"지우로|뒤로 갈", r"智友|智佑", "回聽後比較 terms 開關，勿預設人名一定是辨識偏誤。"),
    ("R13", "個人行程與斷詞", r"걔\s*인|개인\s*스케줄", r"他|她", "回聽斷詞，再核對人稱與行程歸屬。"),
    ("R14", "常見生活詞與語氣", r"송편|수전증|워커|구두|살 순|과일 느낌", r"鬆餅|手癢|跑步鞋|買不起", "分開標註詞義、語氣與可接受的概括翻譯。"),
    ("R15", "不明詞、俚語與低信心辨識", r"잼니|후꿈|부금|세매|헤레나|선차", r"果醬|後咳|布金", "回聽並查粉絲用語；低 confidence 本身不是辨識錯誤。"),
    ("R16", "標點切分與繁體用詞轉換", None, None, "比較原始翻譯、OpenCC 前後和實際送出文字；測試局部分句改善。"),
]


def read_events(directory):
    events, incomplete = [], 0
    path = Path(directory) / "events.jsonl"
    if not path.exists():
        return [], 1
    with path.open(encoding="utf-8") as f:
        for line in f:
            try:
                events.append(json.loads(line))
            except ValueError:
                incomplete += 1  # A crash may leave a partial final JSONL line.
    return events, incomplete


class AudioIndex:
    def __init__(self, events):
        self.received = sorted((e for e in events if e["kind"] == "audio_received"),
                               key=lambda e: e["input_start_sample"])
        self.starts = [e["input_start_sample"] for e in self.received]
        self.sent = defaultdict(list)
        for event in events:
            if event["kind"] == "audio_sent" and event.get("input_start_sample") is not None:
                self.sent[event.get("connection_id")].append(event)
        for items in self.sent.values():
            items.sort(key=lambda e: e["connection_start_sample"])
        self.send_starts = {key: [e["connection_start_sample"] for e in items]
                            for key, items in self.sent.items()}

    def locate(self, connection, ms, *, end=False):
        if not isinstance(ms, (int, float)) or connection not in self.sent:
            return None
        sample = max(0, round(ms * RATE / 1000) - int(end))
        index = bisect_right(self.send_starts[connection], sample) - 1
        if index < 0:
            return None
        sent = self.sent[connection][index]
        offset = sample - sent["connection_start_sample"]
        if offset >= sent["samples"]:
            return None
        original = sent["input_start_sample"] + offset
        index = bisect_right(self.starts, original) - 1
        if index < 0:
            return None
        received = self.received[index]
        offset = original - received["input_start_sample"]
        if offset >= received["samples"]:
            return None
        return ((received["wav_start_sample"] + offset + int(end)) / RATE,
                received["monotonic_ns"])


def source_units(events, audio):
    tokens = [e for e in events if e["kind"] == "soniox_token" and e.get("is_final")
              and e.get("translation_status") != "translation"]
    if not tokens:
        tokens = [e for e in events if e["kind"] == "pipeline_text" and e.get("text_kind") == "source"]
    units, pending = [], []

    def flush():
        if not pending:
            return
        first, last = pending[0], pending[-1]
        positions = [(audio.locate(e.get("connection_id"), e.get("start_ms")),
                      audio.locate(e.get("connection_id"), e.get("end_ms"), end=True)) for e in pending]
        starts = [p[0][0] for p in positions if p[0] is not None]
        ends = [p[1][0] for p in positions if p[1] is not None]
        confidence = [e["confidence"] for e in pending if isinstance(e.get("confidence"), (int, float))]
        units.append({"text": "".join(e.get("text", "") for e in pending),
                      "first_ns": first["monotonic_ns"], "last_ns": last["monotonic_ns"],
                      "event_ids": [e["seq"] for e in pending],
                      "wall_time_utc": first["wall_time_utc"],
                      "audio_start_s": min(starts) if starts else None,
                      "audio_end_s": max(ends) if ends else None,
                      "audio_mapping_partial": len(starts) != len(pending) or len(ends) != len(pending),
                      "confidence_min": min(confidence) if confidence else None})
        pending.clear()

    for token in tokens:
        text = token.get("text", "")
        if text in ("<end>", "<fin>"):
            flush()
            continue
        if not text:
            continue
        if pending and (token["monotonic_ns"] - pending[-1]["monotonic_ns"] > 2_000_000_000
                        or token.get("connection_id") != pending[-1].get("connection_id")):
            flush()
        pending.append(token)
        if re.search(r"[.!?。！？]\s*$", text) or sum(len(e.get("text", "")) for e in pending) >= 250:
            flush()
    flush()
    return units


def load_reviews(directory):
    path = Path(directory) / "reviews.jsonl"
    reviews = {}
    if path.exists():
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                row = json.loads(line)
                reviews[row["candidate_id"]] = row
            except (ValueError, KeyError):
                continue
    return reviews


def analyze_session(directory, *, limit_per_rule=5):
    directory = Path(directory)
    manifest = json.loads((directory / "manifest.json").read_text(encoding="utf-8"))
    events, incomplete = read_events(directory)
    audio = AudioIndex(events)
    units = source_units(events, audio)
    subtitles = [e for e in events if e["kind"] == "discord_accepted"
                 or (e["kind"] == "post_complete" and e.get("destination") == "console")]
    subtitle_times = [e["monotonic_ns"] for e in subtitles]
    subtitle_ids = {e["seq"] for e in subtitles}
    candidates, categories = [], []
    reviews = load_reviews(directory)

    def candidate(rule, unit, nearby, reason, alert, context=None):
        basis = f'{rule}:{unit["event_ids"]}'
        identifier = rule + "-" + hashlib.sha256(basis.encode()).hexdigest()[:12]
        return {"id": identifier, "category": rule, "reason": reason, "known_pattern": alert,
                "source": unit, "source_context": context or [], "nearby_subtitles": nearby,
                "alignment": "receipt_time_window_only_not_verified_translation_pair",
                "review": reviews.get(identifier)}

    for code, title, source_pattern, bad_pattern, suggestion in RULES:
        matches = []
        if source_pattern:
            for unit_index, unit in enumerate(units):
                low = code == "R15" and unit["confidence_min"] is not None and unit["confidence_min"] < 0.65
                if not re.search(source_pattern, unit["text"]) and not low:
                    continue
                a = bisect_right(subtitle_times, unit["first_ns"] - 2_000_000_000)
                b = bisect_right(subtitle_times, unit["last_ns"] + 3_000_000_000)
                nearby = [{"text": e["text"], "event_id": e["seq"], "post_id": e["post_id"]}
                          for e in subtitles[a:b]]
                alert = bool(re.search(bad_pattern, "".join(e["text"] for e in nearby)))
                reason = "命中先前疑似錯譯用詞，需核對配對與原音" if alert else "出現相關語境，供抽樣複核"
                if low:
                    reason += "；有低信心 token（不是錯誤機率）"
                context = units[max(0, unit_index - 2):unit_index + 3]
                matches.append(candidate(code, unit, nearby, reason, alert, context))
        else:
            for event in events:
                reason = None
                if event["seq"] in subtitle_ids and re.fullmatch(r"[.。…\s]+", event["text"]):
                    reason = "可確認送出只有標點的訊息；成因待核對原始 token"
                elif event["seq"] in subtitle_ids and event["text"].lstrip().startswith(('"', '”', '」')):
                    reason = "引號出現在訊息開頭，可能是切句問題"
                elif event["kind"] == "subtitle_transform" and event["before"] != event["after"]:
                    reason = "可確認本機轉換改字，但不代表語意錯誤"
                if reason:
                    unit = dict(text=event.get("before", ""), first_ns=event["monotonic_ns"],
                                last_ns=event["monotonic_ns"], event_ids=[event["seq"]],
                                wall_time_utc=event["wall_time_utc"], audio_start_s=None,
                                audio_end_s=None, audio_mapping_partial=True, confidence_min=None)
                    matches.append(candidate(code, unit, [{"text": event.get("after", event.get("text", "")),
                                                           "event_id": event["seq"]}], reason,
                                             event["kind"] != "subtitle_transform"))
        # Prioritize known suspicious output, but keep retained annotation IDs on rebuild.
        matches.sort(key=lambda c: (not bool(c["review"]), not c["known_pattern"], c["source"]["first_ns"]))
        annotated = [c for c in matches if c["review"]]
        chosen = annotated + [c for c in matches if not c["review"]][:max(0, limit_per_rule - len(annotated))]
        candidates.extend(chosen)
        categories.append(dict(id=code, title=title, opportunities=len(matches), shown=len(chosen),
                               status="有候選，待複核" if matches else "未命中規則；不代表通過", suggestion=suggestion))
    latencies = []
    for event in events:
        if event["kind"] == "soniox_token" and event.get("is_final") and event.get("translation_status") != "translation":
            location = audio.locate(event.get("connection_id"), event.get("end_ms"), end=True)
            if location:
                delay = (event["monotonic_ns"] - location[1]) / 1e6
                if delay >= 0:
                    latencies.append(delay)
    warnings = []
    if incomplete or manifest.get("recording_error") or manifest.get("dropped_records") or manifest.get("status") == "recording" or manifest.get("evidence_complete") is False:
        warnings.append("錄製不完整或未正常關閉；請查看 manifest 與 JSONL 缺口，勿作完整品質統計。")
    if not (directory / "audio.wav").exists():
        warnings.append("沒有可回聽的 audio.wav。")
    verdict_counts = Counter(c["review"]["cause"] for c in candidates
                             if c["review"] and c["review"]["verdict"] == "confirmed")
    result = dict(schema_version=1, session_id=manifest["session_id"], metadata=manifest["metadata"],
                  warnings=warnings, categories=categories, candidates=candidates,
                  subtitle_count=len(subtitles), source_units=len(units), reviewed_causes=dict(verdict_counts),
                  diagnostics={k: sum(e["kind"] == k for e in events) for k in
                               ("audio_dropped", "connection_error", "post_failed", "discord_rate_limit", "pipeline_error")},
                  source_final_after_local_pcm_receipt_median_ms=round(statistics.median(latencies), 1) if latencies else None,
                  latency_note="僅原文定稿到本機 PCM 收到的差值；不是翻譯延遲，也不含平台播放延遲。")
    (directory / "quality.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    write_report(directory, result)
    return result


def write_report(directory, result):
    esc = html.escape
    lines = ["# 直播字幕品質複核", "", f"Session: {result['session_id']}", "",
             "本機離線規則只篩選候選，不判定翻譯正確率。韓中依接收時間粗略配對，請回聽並核對前後文。",
             "未命中不代表沒有問題；相同錯誤的不同說法可能漏掉。低信心不等於辨識錯誤。", "",
             "| 類別 | 可複核片段數 | 本報告樣本數 | 狀態 |", "|---|---:|---:|---|"]
    for category in result["categories"]:
        lines.append(f"| {category['id']} {category['title']} | {category['opportunities']} | {category['shown']} | {category['status']} |")
    lines += ["", "## 收集狀態", "", *result["warnings"],
              f"實際字幕：{result['subtitle_count']} 則。機械式原文片段：{result['source_units']} 段。",
              f"診斷事件：{json.dumps(result['diagnostics'], ensure_ascii=False)}",
              f"人工確認原因計數：{json.dumps(result['reviewed_causes'], ensure_ascii=False)}",
              f"原文定稿計時中位數：{result['source_final_after_local_pcm_receipt_median_ms']} ms。{result['latency_note']}", ""]
    cards = []
    for c in result["candidates"]:
        source = c["source"]
        context_text = "".join(u["text"] for u in c.get("source_context", []))
        subtitle = "\n".join(s["text"] for s in c["nearby_subtitles"])
        start, end = source["audio_start_s"], source["audio_end_s"]
        button = "<p>本片段無完整音訊定位；請使用整場錄音與 JSONL 對照。</p>"
        if start is not None and end is not None:
            context_starts = [u["audio_start_s"] for u in c.get("source_context", []) if u["audio_start_s"] is not None]
            context_ends = [u["audio_end_s"] for u in c.get("source_context", []) if u["audio_end_s"] is not None]
            play_start = max(0, start - 15, min(context_starts or [start]) - 2)
            play_end = min(end + 15, max(context_ends or [end]) + 3)
            button = (f'<button data-start="{play_start:.3f}" data-end="{play_end:.3f}">'
                      f'回聽 {play_start:.1f}–{play_end:.1f} 秒（含前後文）</button>')
        review = json.dumps(c["review"], ensure_ascii=False) if c["review"] else "待人工複核"
        lines += [f"## {c['id']}", "", c["reason"], f"原文：{source['text']}",
                  f"附近原文：{context_text}", f"鄰近字幕（未驗證配對）：{subtitle}", f"音訊秒數：{start}–{end}",
                  f"事件：{source['event_ids']}；時間：{source['wall_time_utc']}", f"複核：{review}", ""]
        title = next(r[1] for r in RULES if r[0] == c["category"])
        cards.append(f'<article data-category="{c["category"]}"><h2>{esc(title)} · {esc(c["id"])}</h2><p>{esc(c["reason"])}</p>{button}'
                     f'<h3>辨識原文</h3><pre>{esc(source["text"])}</pre>'
                     f'<details><summary>附近原文</summary><pre>{esc(context_text)}</pre></details>'
                     f'<h3>附近字幕（推定配對）</h3><pre>{esc(subtitle)}</pre>'
                     f'<p>音訊定位部分缺失：{source["audio_mapping_partial"]}</p><pre>{esc(review)}</pre></article>')
    lines += ["## 改善方向（需要驗證，並未自動修改）", ""]
    for category in result["categories"]:
        if category["opportunities"]:
            lines.append(f"- {category['id']}：{category['suggestion']}")
    (directory / "QUALITY-REVIEW.md").write_text("\n".join(lines), encoding="utf-8")
    document = '''<!doctype html><meta charset="utf-8"><meta name="viewport" content="width=device-width">
<title>直播字幕品質複核</title><style>body{font:16px system-ui;max-width:960px;margin:30px auto;padding:0 20px;background:#f5f6fa;color:#202433}header{position:sticky;top:0;background:#f5f6fa;padding:12px 0}audio{width:100%}article{background:white;border:1px solid #ddd;border-radius:10px;padding:20px;margin:20px 0}pre{white-space:pre-wrap;overflow-wrap:anywhere}button{padding:10px;cursor:pointer}h2{font-size:20px}</style>
<h1>直播字幕品質複核</h1><p>候選不等於錯誤。請核對音訊、原文、前後文；低信心不是錯誤機率。</p>
<p>翻譯沒有音訊時間碼：此處字幕僅依收到時間粗略配對。回聽位置屬於辨識原文。</p>
'''
    document += "".join(f'<p><strong>{esc(w)}</strong></p>' for w in result["warnings"])
    document += '<header><audio id="player" controls preload="metadata" src="audio.wav"></audio><label>檢查類別 <select id="category"><option value="">全部</option>'
    for category in result["categories"]:
        document += (f'<option value="{category["id"]}">{esc(category["title"])} — '
                     f'{category["opportunities"]} 個候選</option>')
    document += '</select></label><p>0 個候選不代表通過檢查。</p></header>'
    document += "".join(cards) or "<p>本場未命中規則。這不代表字幕全部正確。</p>"
    document += '''<script>const player=document.getElementById('player');let stopAt=null;
document.querySelectorAll('[data-start]').forEach(b=>b.onclick=()=>{player.currentTime=Number(b.dataset.start);stopAt=Number(b.dataset.end);player.play().catch(()=>{});});
player.addEventListener('timeupdate',()=>{if(stopAt!==null&&player.currentTime>=stopAt){player.pause();stopAt=null;}});</script>'''
    document += '''<script>document.getElementById('category').onchange=e=>document.querySelectorAll('article').forEach(a=>a.hidden=!!e.target.value&&a.dataset.category!==e.target.value);</script>'''
    (directory / "review.html").write_text(document, encoding="utf-8")
    (directory / "REVIEW-PROMPT.md").write_text(
        "請唯讀分析此 session 的 manifest.json、events.jsonl、quality.json 和錄音。"
        "quality.json 僅是16類已知案例的候選，不是錯誤判決或完整評估。"
        "原始暫定tokens可能重複或被修改，不可全部串接；定稿tokens也不要文字去重。"
        "字幕依接收時間配對，並非已確認的逐句對應；先確認前後文與音訊。"
        "若你的工具不能聽音訊，請明說並請使用者在review.html回聽，勿聲稱已回聽。"
        "分開辨識、翻譯、音源、後處理、風格與誤報；保留事件ID及證據。"
        "同音訊、相同設定才可比較模型；信心值不是正確率。"
        "列出可驗證改善假設，以低成本低延遲為優先；不可改程式或呼叫額外API。"
        "人工確認後可使用 quality_review label 紀錄；不要把尚未確認的候選標成confirmed。\n",
        encoding="utf-8")


def label(directory, candidate_id, *, verdict, cause, note, listened=False):
    directory = Path(directory)
    result = json.loads((directory / "quality.json").read_text())
    if candidate_id not in {c["id"] for c in result["candidates"]}:
        raise ValueError("Unknown candidate ID")
    if verdict == "confirmed" and cause in ("asr", "audio") and not listened:
        raise ValueError("ASR/audio confirmation requires listening to the original audio (--listened)")
    row = dict(candidate_id=candidate_id, verdict=verdict, cause=cause, note=note, listened=listened,
               reviewed_at_utc=datetime.now(timezone.utc).isoformat())
    with (directory / "reviews.jsonl").open("a", encoding="utf-8") as f:
        f.write(json.dumps(row, ensure_ascii=False) + "\n")
    analyze_session(directory)
    summarize(directory.parent)


def summarize(root):
    counts = Counter()
    sessions = 0
    for file in Path(root).glob("*/quality.json"):
        try:
            result = json.loads(file.read_text())
            sessions += 1
            by_id = {c["id"]: c["category"] for c in result["candidates"]}
            for identifier, review in load_reviews(file.parent).items():
                if review["verdict"] == "confirmed" and identifier in by_id:
                    counts[(by_id[identifier], review["cause"])] += 1
        except (ValueError, KeyError, OSError):
            continue
    lines = [f"# 跨場次人工複核彙整（{sessions} 場）", "", "只統計人工確認案例，不是錯誤率；未複核候選不算錯誤。", ""]
    for (category, cause), count in counts.most_common():
        suggestion = next(r[4] for r in RULES if r[0] == category)
        lines.append(f"- {category} / {cause}：{count} 例。後續驗證：{suggestion}")
    if not counts:
        lines.append("尚無人工確認案例，暫不能歸納已證實的主要原因。")
    path = Path(root) / "QUALITY-SUMMARY.md"
    path.write_text("\n\n".join(lines), encoding="utf-8")
    return path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    scan = sub.add_parser("analyze")
    scan.add_argument("session", type=Path)
    scan.add_argument("--limit", type=int, default=5, help="Samples per category (1–50)")
    mark = sub.add_parser("label")
    mark.add_argument("session", type=Path)
    mark.add_argument("candidate_id")
    mark.add_argument("--verdict", choices=("confirmed", "false_alarm", "uncertain"), required=True)
    mark.add_argument("--cause", choices=("translation", "asr", "audio", "postprocess", "style", "unknown"), required=True)
    mark.add_argument("--note", required=True)
    mark.add_argument("--listened", action="store_true")
    summary = sub.add_parser("summary")
    summary.add_argument("root", type=Path)
    args = parser.parse_args()
    try:
        if args.command == "analyze":
            if not 1 <= args.limit <= 50:
                parser.error("--limit must be 1–50")
            analyze_session(args.session, limit_per_rule=args.limit)
            print(args.session / "review.html")
        elif args.command == "label":
            label(args.session, args.candidate_id, verdict=args.verdict, cause=args.cause,
                  note=args.note, listened=args.listened)
            print(args.session / "QUALITY-REVIEW.md")
        else:
            print(summarize(args.root))
    except (ValueError, OSError) as exc:
        parser.exit(1, f"Review failed: {exc}\n")


if __name__ == "__main__":
    main()
