# 直播音訊與字幕品質複核

下一次以新版 `main` 啟動 YouTube job，或執行 `mirror` 的 FANS session 時，會自動
保存本機證據。直播期間只有背景寫檔；結束或 Ctrl+C 後才做離線檢查，沒有新增 AI API
或自動換模型，也不會將錄音額外上傳。`preview` 預設不錄製。

## 每場保存什麼

預設位置：`~/Library/Application Support/nmixx-subtitles/sessions/<日期時間-識別碼>/`。
服務 log 會印出 `Quality recording:` 和結束後的 `Quality review:` 路徑。

| 檔案 | 用途 |
|---|---|
| `audio.wav` | 輸入供應商流程的原始 16 kHz、16-bit、單聲道 PCM，含靜音；不是重新錄麥克風 |
| `events.jsonl` | 本機收到 PCM、嘗試及完成送出、丟音訊、連線、token、轉換、Discord 接受訊息等事件 |
| `manifest.json` | 模式、模型、詞彙表內容及 hash、是否完整、錄製錯誤；不保存 API key 或 webhook |
| `quality.json` | 16 類規則的觸發數、候選、原始事件 ID、推定字幕配對及音訊位置 |
| `review.html` | 本機回聽頁面，可選類別、播放片段前後文、看辨識原文及附近字幕 |
| `QUALITY-REVIEW.md` | 可交給 Claude Code 閱讀的檢查報告與待驗證改善方向 |
| `REVIEW-PROMPT.md` | 分析限制與複核指引；不能聽音訊的模型必須明說 |
| `reviews.jsonl` | 人工複核結果，只在標註時建立；重新分析會保留 |

Gemini 也能保存輸入錄音、轉錄、後處理與送出紀錄，但目前沒有可用的 token 音訊
時間碼，因此不會捏造逐句回聽位置。Soniox 同時保存暫定與定稿 token、語言、
confidence、端點、處理進度和實際設定；分析只串接定稿原文，保留真實重複與空白。

依 [Soniox WebSocket 文件](https://soniox.com/docs/api-reference/stt/websocket-api)，
原文 token 可帶 `start_ms` / `end_ms`，**翻譯 token 不帶這兩個欄位**。
所以錄音定位對應辨識原文，原文和字幕仍只用本機接收時間的前後區間做粗略配對，
不能當成確定的一對一翻譯關係。連線 ID、每條連線送出的 sample 偏移、本機輸入
sample 偏移與 WAV 偏移分開記錄；斷線及丟棄音訊不會被當成連續的同一時間軸。

計時使用 `monotonic_ns`，另存 UTC 供找事件。報告的原文定稿計時是「收到定稿
token 減去對應 PCM chunk 抵達程式」，不包括平台播放延遲，也不是翻譯或觀眾
實際看到字幕的延遲。`audio_send_attempt` 沒有相應 `audio_sent` 代表送出未確定，
不保證遠端沒有收到。`discord_accepted` 是 HTTP 成功回應，不是客戶端顯示時間。

## 16 類怎麼判斷

規則對應 2026-09-27 的 16 類代表案例，包括米腸、遮光簾、否定、主詞、美甲、
蘿蔔泡菜、髮型、時間、品名、遊戲、節日、人名、斷詞、生活詞、不明詞及字幕後處理。
實作規則在 `nmixx_subtitles/quality_review.py` 的 `RULES`。

這是低成本篩選：先找到相關韓文／已知可疑中文及低信心 token，每類預設保留
5 個候選，優先展示先前出現過的可疑譯法。**命中不等於錯誤，未命中也不等於通過**；
新詞、新說法與不同語法可能漏掉。低 confidence 是回聽線索，不是校準過的錯誤機率。
只有「純標點訊息」「轉換前後字串不同」這類機械性事實可以直接確認，語意成因仍需複核。

打開 `review.html`，回聽、核對前後文後，把原因分為 `translation`、`asr`、`audio`、
`postprocess`、`style`、`unknown`。沒有聽原音就不能把 ASR／音源標為已確認。
也可以交給 Claude Code 閱讀 `REVIEW-PROMPT.md`，但不能假設它能直接聽 WAV。

```sh
# 重建報告，完全離線；異常終止後也可用，原始檔不會改動
.venv/bin/python -m nmixx_subtitles.quality_review analyze '/完整/session/路徑' --limit 10

# 候選 ID 見報告；記錄人工判斷，ASR/audio 的 confirmed 要加 --listened
.venv/bin/python -m nmixx_subtitles.quality_review label '/完整/session/路徑' R01-實際ID \
  --verdict confirmed --cause translation --note '回聽及前後文核對：原話為米腸，字幕是酸菜湯' --listened

# 可改標 false_alarm / uncertain；同一 ID 以最後一次標註為準
# 彙整所有尚未清理的場次，只計人工確認案例，不算整體錯誤率
.venv/bin/python -m nmixx_subtitles.quality_review summary \
  "$HOME/Library/Application Support/nmixx-subtitles/sessions"
```

每場結束及人工標註後會更新根目錄 `QUALITY-SUMMARY.md`。先看已确认的原因再選改善：
詞彙問題可試 glossary；辨識問題回聽音源或比較 terms；主詞／否定可用小段同音訊
比較翻譯上下文；後處理問題比較原始 token 與 OpenCC／切句。
修改後用相同音訊、相同播放速率和參考答案複測，同時比較成本與延遲。
目前只產生建議，不自動改 glossary、模型或程式。

## 儲存、負擔與異常

16 kHz PCM 約 115 MB／小時，另加事件 Log。錄製由有上限的背景 queue 寫檔，
直播路徑只排入紀錄，不等待磁碟。queue 滿、磁碟故障或錄製額度耗盡時，字幕繼續，
並留下 warning／manifest 不完整標記；不應用有缺口的資料算完整品質統計。
WAV header 每個 chunk 更新，正常停止／取消時收尾；強制殺掉程式仍可能留下最後
一筆不完整事件或無報告，可事後執行 `analyze`。shutdown 最多等 writer 5 秒，逾時
不假裝已完成錄製。

`.env` 設定在程式啟動時載入：

```dotenv
QUALITY_CAPTURE_ENABLED=true
QUALITY_RETENTION_DAYS=7
QUALITY_MAX_BYTES=2147483648
# QUALITY_CAPTURE_DIR=/absolute/path/to/sessions
```

保存政策在下一場開始時清理超過 7 天的**已結束、具有本程式 manifest** 的場次，
接近上限時先清到約 80% 以下，讓新場次有空間。這會一起清除該場音訊及複核資料；
重要場次請複製到別處保存。`recording` 狀態（包括強制終止留下的狀態）、未知目錄與
symlink 不會自動刪除。根目錄 summary 只涵蓋目前仍保留的場次。
單场預算是啟動時的剩餘額度，兩場同時跑及產生報告會有額外用量，2 GB 是近似預算，
不是檔案系統硬配額；用盡時停止本場錄製，不停止翻譯。
`QUALITY_RETENTION_DAYS=0` 關閉天數清理、`QUALITY_MAX_BYTES=0` 關閉容量限制，
兩者都設 0 才是永久保存。

背景服務若跑在固定 runtime snapshot，只有改 workspace 不會自動生效，需在沒有
直播進行時切換到新版 snapshot 並重啟監看。手動 FANS 指令會直接使用所在專案的新版。
