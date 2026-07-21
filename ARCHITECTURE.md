# 架構設計

單一 Python process（FastAPI），跑在 Mac 上由 launchd 常駐，一次只處理一場直播。
整體分三層：**觸發層**（發現直播）、**Pipeline**（音訊 → 字幕）、**狀態與韌性**（重試與重開機恢復）。

```mermaid
flowchart TB
    subgraph EXT["外部服務"]
        YT["YouTube<br/>(NMIXX channel)"]
        HUB["WebSub Hub<br/>(pubsubhubbub)"]
        GEMINI["Gemini API<br/>(GEMINI_MODEL, 預設 flash-lite)"]
        DC["Discord Webhook"]
    end

    subgraph MAC["Mac(launchd 常駐: com.nmixx.subtitles + com.nmixx.ngrok)"]
        NGROK["ngrok tunnel<br/>(PUBLIC_BASE_URL)"]

        subgraph APP["FastAPI process (main.py)"]
            subgraph TRIG["觸發層"]
                WS["POST /youtube/websub<br/>(trigger.py)"]
                WD["watchdog_loop<br/>每 10 分鐘 yt-dlp 掃 /live"]
                RESUB["resubscribe_loop<br/>每 4 天續 WebSub lease"]
                OV["on_video()<br/>videos.list 確認狀態"]
                UP["poll_upcoming<br/>預告場次: 開播前 2 分起每 60 秒查"]
            end

            subgraph PIPE["Pipeline: run_live_job(video_id)"]
                CAP["capture.py<br/>streamlink(退回 yt-dlp) + ffmpeg<br/>→ 16kHz mono PCM, 1 秒 chunks"]
                ASR["asr.py<br/>WhisperLiveKit + mlx-whisper(本機, WHISPER_MODEL=medium)<br/>→ 韓文 Segment(斷句後 commit)"]
                BAT["batcher.py<br/>4 秒視窗收集 segments"]
                TR["translate.py<br/>整批一次 Gemini call<br/>system prompt = 規則 + glossary.md<br/>context deque(近 4 句原文)"]
                POST["discord.py DiscordPoster<br/>緩衝 4 秒/500 字後送出"]
            end

            ST[("state.json<br/>video_id → status/attempt")]
        end
    end

    YT -- "發布/開播事件" --> HUB
    HUB -- "push 通知" --> NGROK --> WS
    WS --> OV
    WD -- "備援發現" --> OV
    YT -. "yt-dlp --simulate" .- WD
    OV -- "live" --> PIPE
    OV -- "upcoming" --> UP -- "轉 live" --> PIPE
    OV <--> ST
    YT -- "HLS 音訊" --> CAP --> ASR --> BAT --> TR --> POST --> DC
    TR <--> GEMINI
    RESUB --> HUB
```

## 觸發層（怎麼知道開播了）

主要靠 **WebSub push**：啟動時向 hub 訂閱頻道（lease 5 天，`resubscribe_loop` 每 4 天續約），
YouTube 有新影片/開播就 push 到 `POST /youtube/websub`（經 ngrok 進來）。收到後 `on_video()`
用 YouTube Data API `videos.list` 確認真實狀態：

- `live` → 直接開 job
- `upcoming` → `poll_upcoming`：排定開播前 2 分鐘開始每 60 秒查一次，2 小時沒開播就放棄
- 其他（已結束/非直播）→ 記錄後忽略

備援是 **watchdog**：每 10 分鐘用 `yt-dlp --simulate` 掃頻道 `/live` 頁，接住 WebSub 漏掉的場次。

## Pipeline（run_live_job，直播期間持續執行）

```
streamlink/ffmpeg → 1s PCM chunks → WhisperLiveKit(本機 Whisper) → 韓文句子
→ 4 秒 micro-batch → Gemini 一次翻整批(glossary 進 system prompt)
→ DiscordPoster 緩衝(4s/500 字) → webhook
```

成本設計：ASR 全本機不花錢；翻譯靠 micro-batch 把 API 呼叫壓到約每分鐘 2–5 次，
model 由 `GEMINI_MODEL` env 控制。翻譯失敗或行數不符時 fallback 成 `[KR] 原文`，永不中斷 job。

## 狀態與韌性

- `state.json`（plain JSON，單 process 夠用）：以 video_id 記錄狀態機
  `upcoming → live → completed`，失敗記 `error` 並帶 attempt 數，重試上限
  `MAX_JOB_ATTEMPTS = 5` 次後標 `failed`（終態）。
- 重開機/重啟：startup 時 `_resume_stale_jobs()` 把殘留的 `live`/`error` 逐一對
  `videos.list` 重查——還在播就續跑，播完就收尾。
- launchd `KeepAlive` + `caffeinate -s` 保證 process 常駐、Mac 不睡。
- 同時間只跑一場：新直播進來時若已有 job 在跑則忽略（`start_live_job`）。

## 設定

`.env`：`DISCORD_WEBHOOK_URL`、`GEMINI_API_KEY`、`GEMINI_MODEL`、`YOUTUBE_API_KEY`、
`PUBLIC_BASE_URL`（ngrok 網址）、`WEBSUB_VERIFY_TOKEN`、`YOUTUBE_CHANNEL_ID`、`WHISPER_MODEL`。
自訂翻譯對照（成員名/slang/世界觀術語）改 `glossary.md` 即可，整份會進每次翻譯的 system prompt。
