# pipeline — سرویس پردازش فایل مبتنی بر channel و context

سرویس پوشه‌ی `inbox` را رصد می‌کند، هر فایل را از سه مرحله‌ی **scan → convert → compress** عبور می‌دهد و نتیجه را **اتمیک** در `out/` منتشر می‌کند. هر مرحله worker pool مستقل دارد، مراحل با channel‌های bounded به هم وصل‌اند، و timeout / retry / shutdown با `context` مدیریت می‌شود.

- فقط کتابخانه‌ی استاندارد + `golang.org/x/sync/errgroup` (وابستگی `go.uber.org/goleak` فقط در تست‌ها).
- هیچ فایلی کامل در حافظه خوانده نمی‌شود؛ همه‌جا `io.Reader`/`io.Writer` + `io.LimitReader`.
- هیچ `ch <- x` بدون `select` روی `ctx.Done()` وجود ندارد (هر ارسال از `pipeline.send` رد می‌شود).
- هیچ goroutine بدون مسیر خروج و بدون `Wait` در shutdown نیست.

---

## ۱. اجرای سریع

```bash
go build -o bin/pipeline ./cmd/pipeline

# ۱۰۰ فایل تستی + سه فایل طعمه (.tmp/.part/.hidden) + یک فایل خراب
./scripts/make_testdata.sh 100

# اجرای دسته‌ای: بعد از ۱۰۰ job منبع را متوقف کن، صف‌ها را drain کن و خارج شو
./bin/pipeline -config configs/config.example.json -limit 100
```

در ویندوز:

```powershell
go build -o .\bin\pipeline.exe .\cmd\pipeline
.\scripts\make_testdata.ps1 -Count 100
.\bin\pipeline.exe -config .\configs\config.example.json -limit 100

# یا کل دمو (ساخت، تولید داده، اجرا، اعتبارسنجی، اجرای دوباره برای idempotency)
.\scripts\demo.ps1 -Count 100
```

فلگ‌ها: `-config`، `-metrics-addr` (override)، `-limit N` (اجرای دسته‌ای)، `-print-config` (نمایش کانفیگ نهایی بعد از defaults).

بدون `-limit` سرویس تا دریافت `SIGINT`/`SIGTERM` اجرا می‌ماند. متریک‌ها روی `http://127.0.0.1:9090/metrics` و سلامت روی `/healthz`.

---

## ۲. درخت فایل‌ها

| مسیر | توضیح یک‌خطی |
|---|---|
| `cmd/pipeline/main.go` | نقطه‌ی ورود: فلگ‌ها، ساخت و سیم‌کشی اجزا، بازیابی journal در startup، سرور متریک، خلاصه‌ی پایانی |
| `internal/config/config.go` | کانفیگ JSON با decode سختگیرانه، defaults، اعتبارسنجی (از جمله کران ۸..۳۲ برای صف) و helperهای مسیر |
| `internal/job/job.go` | ساختار `Job`، ماشین حالت، پارسر نام فایل (مرز اعتماد) و `SafeFileName` |
| `internal/job/backoff.go` | اینترفیس‌های `Clock`/`Rand`، `RealClock`/`FakeClock` و backoff نمایی با jitter |
| `internal/job/store.go` | journal پایدار: نوشتن اتمیک `jobs/<id>.json` با fsync + rename، قرنطینه‌ی سند خراب |
| `internal/stage/stage.go` | اینترفیس `Stage`، `Kind`، خطاهای `ErrSkip`/`Permanent`/`Fatal`، رجیستری سازنده‌ها و `Build` |
| `internal/stage/scan.go` | باز کردن ورودی، سقف حجم، sha256، ساخت دایرکتوری موقت per-job، probe برای idempotency |
| `internal/stage/convert.go` | نرمال‌ساز streaming (UTF-8/LF/حذف فضای انتهایی) + اجرای ابزار خارجی با `CommandContext` و `WaitDelay` |
| `internal/stage/compress.go` | gzip streaming روی artifact مرحله‌ی قبل، داخل دایرکتوری موقت همان job |
| `internal/pipeline/pipeline.go` | سیم‌کشی channelها، worker pool هر مرحله، retry/backoff، semaphore وزنی بر حسب بایت، شمارش متریک |
| `internal/pipeline/shutdown.go` | shutdown دومرحله‌ای، گرفتن سیگنال، `StopSource`/`ForceCancel`، مستندسازی jobهای نیمه‌کاره |
| `internal/source/watcher.go` | رصد polling پشت اینترفیس `Source` + dedupe + نادیده گرفتن فایل‌های موقت + `Replay` و `Limit` |
| `internal/sink/publish.go` | انتشار اتمیک در `out/<hash[:2]>/<hash>.gz` + manifest JSON، staging داخل `out/.tmp` |
| `internal/sink/dlq.go` | نوشتن `failed/<id>.json` و انتقال فایل ورودی خراب به کنارش |
| `internal/obs/metrics.go` | رجیستری Prometheus دست‌نویس (counter/gauge/histogram + gauge با callback) |
| `internal/obs/logging.go` | `slog` + handlerی که `trace_id` را از context به همه‌ی رکوردها تزریق می‌کند |
| `configs/config.example.json` | کانفیگ نمونه‌ی کامل |
| `scripts/make_testdata.ps1` / `.sh` | ساخت ۱۰۰ فایل تستی با CRLF و فضای انتهایی + طعمه‌ها (+ `-WithBadFile`) |
| `scripts/demo.ps1` / `.sh` | build → تولید داده → اجرا → اعتبارسنجی gzip/نرمال‌سازی → اجرای دوباره برای اثبات idempotency |
| `internal/*/*_test.go` | تست‌های واحد و e2e (fakes، جعل ساعت، goleak، shutdown ۱۰۰۰ job، fuzz) |

---

## ۳. معماری

```
                 ┌──────────────┐
                 │  inbox/      │  (polling هر ۲۰۰ms، فایل‌های .tmp/.part/hidden نادیده)
                 └──────┬───────┘
                        │  Source.Run(emit)   ── dedupe بر اساس (path,size,mtime)
                        │  journal در لحظه‌ی پذیرش (admission)
                        ▼
   ┌────────────────────────────────────────────────────────────────────┐
   │  q0 (bounded 8..32)                                                 │
   └───────────────┬────────────────────────────────────────────────────┘
                   ▼
        ┌─────────────────────┐   workers: N (IO-bound)
        │  scan               │   open → LimitReader → sha256 → probe
        │  Kind = io          │   out: Hash, Size, TempDir
        └──────────┬──────────┘
                   │ q1 (bounded)
                   ▼
        ┌─────────────────────┐   workers: N (CPU-bound)
        │  convert            │   streaming normalise (یا exec خارجی + WaitDelay)
        │  Kind = cpu         │   out: Converted
        └──────────┬──────────┘
                   │ q2 (bounded)
                   ▼
        ┌─────────────────────┐   workers: N (CPU-bound)
        │  compress           │   gzip streaming
        │  Kind = cpu         │   out: Artifact
        └──────────┬──────────┘
                   │ q3 (bounded)
                   ▼
        ┌─────────────────────┐
        │  sink               │   copy → fsync → rename (out/.tmp) → manifest → journal=done
        └──────────┬──────────┘
                   ├─ موفق  → out/<hash[:2]>/<hash>.gz + <hash>.json
                   └─ خطا   → failed/<id>.json + انتقال فایل ورودی
```

نکات کلیدی:

- **backpressure عمدی**: صف‌ها bounded هستند (۸ تا ۳۲، اعتبارسنجی کانفیگ اجازه‌ی بیشتر نمی‌دهد). کندی یک مرحله به مرحله‌ی بالادست و در نهایت به source فشار می‌آورد؛ هیچ صف بی‌نهایتی وجود ندارد.
- **مالکیت job**: هر لحظه فقط یک goroutine مالک `*Job` است و مالکیت با ارسال در channel منتقل می‌شود؛ به همین دلیل mutation بدون lock امن است و journal از یک snapshot (`Clone`) نوشته می‌شود.
- **pool جدا برای CPU و IO**: هر مرحله pool خودش را دارد و `Kind()` پروفایل منابع را مستند می‌کند؛ مراحل CPU-bound (convert/compress) سهم خود را از هسته‌ها می‌گیرند بدون اینکه پشت مراحل IO-bound بلاک شوند.
- **سقف بایت**: علاوه بر تعداد worker، یک semaphore وزنی بر اساس حجم job (`max_inflight_bytes`) مجموع بایت‌های در جریان را محدود می‌کند. job بزرگ‌تر از کل بودجه به‌تنهایی عبور می‌کند تا deadlock نشود.
- **بستن channel از سمت فرستنده**: هر مرحله فقط channel خروجی خودش را می‌بندد و آن هم بعد از `Wait` روی workerهای خودش. به این ترتیب بستن، دقیقاً از بالادست به پایین‌دست حرکت می‌کند و هیچ ارسالی روی channel بسته انجام نمی‌شود.

---

## ۴. مدل تحویل پیام: at-least-once با خروجی idempotent

**تحویل at-least-once است** (نه at-most-once)، اما خروجی «حداکثر یک بار» تولید می‌شود:

1. `sha256` محتوا کلید idempotency است و مسیر خروجی content-addressed است: `out/<hash[:2]>/<hash>.gz`.
2. در scan اگر artefact + manifest از قبل موجود باشند، job با `ErrSkip` تمام می‌شود (بدون پردازش مجدد).
3. در sink، اگر فایل موجود باشد کپی انجام نمی‌شود؛ فقط manifest دوباره نوشته می‌شود.
4. نوشتن خروجی در `out/.tmp` و سپس `os.Rename` در همان فایل‌سیستم است، پس مصرف‌کننده یا فایل کامل می‌بیند یا هیچ چیز.

نتیجه‌ی عملی: **مسیر خروجی «حداکثر یک بار» نوشته می‌شود، اما پردازش می‌تواند بیش از یک بار اتفاق بیفتد** (مثلاً بعد از کرش). برای فایل‌های ورودی که پردازش‌شان side effect خارجی ندارد این ترکیب، بهترین نقطه‌ی تعادل است.

**چرا at-most-once رد شد؟** at-most-once یعنی در کرش، jobهای در جریان رها می‌شوند؛ برای یک pipeline فایل این یعنی کار از دست می‌رود و باید دستی دوباره راه‌اندازی شود. چون عملیات مراحل **خالص و قابل تکرار** است (normalize و gzip روی محتوای ثابت)، هزینه‌ی تکرار فقط چند میلی‌ثانیه CPU است، در حالی که هزینه‌ی از دست دادن کار، مداخله‌ی انسانی است.

---

## ۵. رفتار در کرش

| نقطه‌ی کرش | وضعیت روی دیسک | اجرای بعدی |
|---|---|---|
| قبل از پذیرش job | فایل در inbox، بدون سند journal | source دوباره آن را می‌بیند و می‌پذیرد |
| بعد از پذیرش، قبل از scan | `jobs/<id>.json` با `state=pending` | recovery: requeue → کل زنجیره دوباره اجرا می‌شود |
| وسط یک مرحله | `state=running`، دایرکتوری `tmp/<id>` شاید باقی مانده باشد | recovery دایرکتوری موقت را پاک می‌کند و از scan شروع می‌کند |
| بین compress و publish | `state=running` با `artifact_path` معتبر | در حالت `resume` مستقیماً publish می‌شود؛ در حالت `requeue` دوباره اجرا می‌شود |
| بین rename خروجی و ثبت journal | artefact کامل در `out/`، journal هنوز `running` | probe مرحله‌ی scan آن را می‌بیند، skip می‌کند و job را `done` می‌کند |
| بعد از ثبت `done` | `state=done` | probe در scan مانع پردازش دوباره می‌شود |

در همه‌ی این حالت‌ها یک قاعده برقرار است: **هر job یا `done` است، یا `failed` (در DLQ)، یا در journal به‌صورت ناتمام باقی است.** هیچ jobی بی‌صدا ناپدید نمی‌شود؛ به همین دلیل journal **در لحظه‌ی پذیرش** نوشته می‌شود، نه در اولین مرحله.

### shutdown دومرحله‌ای

- **فاز ۱**: سیگنال → `cancel` روی context منبع → source تمام می‌شود و `q0` را می‌بندد → هر مرحله صف ورودی‌اش را تمام می‌کند و صف خروجی را می‌بندد → sink تخلیه می‌شود. تا `shutdown.grace`.
- **فاز ۲**: اگر مهلت گذشت یا سیگنال دوم رسید → `cancel` سراسری؛ workerها تلاش جاری را رها می‌کنند (کپی‌ها با `ctxReader` فوراً قطع می‌شوند) و فایل staging پاک می‌شود. تا `shutdown.hard`؛ بعد از آن خطای `ErrHardShutdownTimeout` و خروج غیرصفر.

**jobهایی که ممکن است نیمه‌کاره بمانند**: jobهایی که در لحظه‌ی cancel اجباری داخل یک مرحله یا داخل صف بودند. وضعیت‌شان در journal (`running`/`pending`) و فایل ورودی‌شان دست‌نخورده در inbox است. در اجرای بعدی: `recovery.mode=requeue` (پیش‌فرض) آن‌ها را reset و از scan دوباره اجرا می‌کند؛ چون خروجی content-addressed است، اگر کار قبلاً تمام شده باشد فقط یک skip هزینه دارد. در `recovery.mode=resume`، اگر artifact سالم مانده باشد اول publish می‌شود و بقیه requeue می‌شوند.

---

## ۶. افزودن یک stage جدید (سه خط)

۱) فایل جدید `internal/stage/thumb.go` با متدهای `Name`, `Kind`, `Process` (کاملاً مستقل از بقیه‌ی مراحل).

۲) یک خط در رجیستری `internal/stage/stage.go`:

```go
var factories = map[string]Factory{
	"scan": newScanStage, "convert": newConvertStage, "compress": newCompressStage,
	"thumb": newThumbStage, // ← همین
}
```

۳) یک بلوک در آرایه‌ی `stages` کانفیگ، در جای دلخواه زنجیره:

```json
{ "name": "thumb", "workers": 2, "timeout": "10s", "queue_size": 8,
  "backoff": { "base": "100ms", "max": "2s", "factor": 2, "jitter": 0.2, "max_attempts": 3 } }
```

نه wiring جدید لازم است، نه متریک جدید (عمق صف و latency به‌صورت خودکار برای هر مرحله ثبت می‌شود)، نه تغییری در `main`.

---

## ۷. پیکربندی

| کلید | معنا | پیش‌فرض / کران |
|---|---|---|
| `inbox_dir`, `out_dir`, `work_dir`, `failed_dir` | مسیرها | اجباری |
| `max_file_size` | سقف حجم ورودی (بایت)؛ بالاتر → DLQ با خطای permanent | `64MiB`، کران `(0, 1TiB]` |
| `max_inflight_bytes` | بودجه‌ی بایت در جریان (semaphore وزنی)؛ `0` = غیرفعال | `128MiB` |
| `poll_interval` | فاصله‌ی polling | `200ms` |
| `stable_age` | حداقل عمر mtime برای اینکه فایل «کامل» تلقی شود | `300ms` |
| `queue_size` | ظرفیت پیش‌فرض صف هر مرحله | `16`، **کران سخت ۸..۳۲** |
| `stages[].workers` | اندازه‌ی pool | `1`..`256` |
| `stages[].timeout` | timeout هر تلاش (context per attempt) | `30s` |
| `stages[].backoff.{base,max,factor,jitter,max_attempts}` | سیاست retry | `100ms/5s/2/0.2/3` |
| `convert.command` | ابزار خارجی با placeholderهای `{in}`، `{out}`، `{dir}`؛ خالی = نرمال‌ساز داخلی | `[]` |
| `compress.level` | سطح gzip؛ `null`/غایب = ۶، `0` = فقط store | `-1..9` |
| `shutdown.grace` / `shutdown.hard` | مهلت drain / مهلت abort | `15s` / `5s` |
| `recovery.mode` | `requeue` یا `resume` | `requeue` |
| `metrics_addr` | آدرس `/metrics`؛ خالی = غیرفعال | `127.0.0.1:9090` |
| `log_level`, `log_format` | `debug/info/warn/error`، `text/json` | `info`, `text` |

نمونه‌ی کامل: `configs/config.example.json`.

---

## ۸. Observability

متریک‌ها (Prometheus text format، دست‌نویس، بدون وابستگی):

| متریک | نوع | برچسب |
|---|---|---|
| `pipeline_jobs_enqueued_total` | counter | `stage="source"` |
| `pipeline_jobs_completed_total` | counter | `stage="publish"` |
| `pipeline_jobs_failed_total` | counter | `stage` |
| `pipeline_jobs_skipped_total` | counter | `stage="scan"` |
| `pipeline_retries_total` | counter | `stage` |
| `pipeline_bytes_processed_total` | counter | `stage` |
| `pipeline_bytes_written_total` | counter | `stage` |
| `pipeline_queue_depth` | gauge (callback، بدون sampler) | `stage` |
| `pipeline_inflight_bytes` | gauge | — |
| `pipeline_inbox_files` | gauge (callback) | — |
| `pipeline_source_ignored_total` | counter | `reason="name"` |
| `pipeline_stage_duration_seconds` | histogram | `stage` |

لاگ ساخت‌یافته با `slog`؛ هر رکوردی که از context یک job لاگ شود، `trace_id` دارد (`obs.WithTraceID` + `TraceHandler`).

---

## ۹. تست‌ها

```bash
go vet ./...
go test ./... -count=1
CGO_ENABLED=1 go test -race ./... -count=1     # روی لینوکس/مک پیش‌فرض کار می‌کند

# fuzz
go test ./internal/config -run=^$ -fuzz=FuzzParse -fuzztime=30s
go test ./internal/job    -run=^$ -fuzz=FuzzParseFileName -fuzztime=30s
go test ./internal/job    -run=^$ -fuzz=FuzzValidateID -fuzztime=30s
```

پوشش تست‌ها:

- **fake stage با تأخیر و خطای تزریقی**: کل pipeline بدون فایل واقعی تست می‌شود (`internal/pipeline/pipeline_test.go`).
- **ساعت و backoff تزریقی**: `job.FakeClock` + `job.FixedRand`؛ تست retry بدون sleep واقعی و deterministic است و مقادیر دقیق backoff را assert می‌کند.
- **goleak در `TestMain`** برای `internal/pipeline` و `internal/source`.
- **shutdown با ۱۰۰۰ job**: cancel وسط کار؛ سپس `enqueued == done + failed + remaining`، همه‌ی `.gz`ها کامل و معتبر، هیچ `.part`/`.tmp` در `out/`، و در ادامه یک اجرای recovery که همه‌ی ۱۰۰۰ خروجی را تکمیل می‌کند.
- **idempotency**: یک فایل دو بار → یک خروجی و mtime بدون تغییر.
- **DLQ**: فایل با UTF-8 نامعتبر → `failed/<id>.json` معتبر با `stage`, `attempts`, `error`, `at` و انتقال فایل ورودی.
- **fuzz** برای پارسر نام فایل، پارسر کانفیگ و `ValidateID`.

> نکته‌ی ویندوز: `-race` به cgo نیاز دارد. اگر کامپایلر C نصب نباشد پیام `-race requires cgo` می‌گیرید؛ در آن حالت یا `CGO_ENABLED=1` را با یک mingw-w64 تنظیم کنید، یا تست‌ها را بدون `-race` روی ویندوز و با `-race` روی CI لینوکسی اجرا کنید. `go vet ./...` و کل تست‌ها بدون `-race` روی همین مخزن پاس می‌شوند.

---

## ۱۰. تصمیم‌ها (و گزینه‌ای که رد شد)

| # | تصمیم | چرا | جایگزین رد‌شده |
|---|---|---|---|
| ۱ | **at-least-once + خروجی content-addressed** | کرش کار را از بین نمی‌برد؛ تکرار هزینه‌ی چند میلی‌ثانیه‌ای دارد | at-most-once (از دست دادن job در کرش) |
| ۲ | **journal روی دیسک** (`jobs/<id>.json`) | تنها راه اینکه پس از کرش بدانیم چه چیزی ناتمام مانده | state در حافظه (با مرگ process پاک می‌شود) |
| ۳ | **ثبت journal در لحظه‌ی پذیرش** | jobی که در صف مانده هم بازیابی می‌شود؛ متریک `enqueued` و journal همیشه هم‌خوان می‌مانند | ثبت در اولین مرحله (jobهای در صف ناپدید می‌شدند) |
| ۴ | **requeue پیش‌فرض، نه resume** | دایرکتوری موقت بعد از کرش معتبر نیست، پس cursor قابل اعتماد نیست؛ requeue به‌خاطر idempotency ارزان است | resume از cursor (artifactهای نیمه‌کاره/گمشده) |
| ۵ | **صف bounded + بستن از سمت فرستنده** | backpressure واقعی و بدون send-on-closed-channel | صف بی‌نهایت یا بستن از سمت گیرنده (هر دو صریحاً ممنوع) |
| ۶ | **pool جدا برای هر مرحله** | CPU-bound و IO-bound نباید سهم همدیگر را بخورند | یک pool مشترک (head-of-line blocking) |
| ۷ | **retry داخل worker** | backoff و سقف تلاش نزدیک به جایی است که خطا رخ می‌دهد؛ ساده و قابل تست | retry queue جداگانه (state اضافه، احتمال گم شدن) |
| ۸ | **مسیر خروجی `out/<hash[:2]>/<hash>.gz`** | idempotency و dedupe رایگان؛ shard برای تعداد زیاد فایل | نام اصلی فایل (تصادم، dedupe غیرممکن) |
| ۹ | **staging داخل `out/.tmp`** | `os.Rename` فقط внутри یک فایل‌سیستم اتمیک است؛ `out` و `work` ممکن است روی volumeهای مختلف باشند | temp در `work_dir` (rename غیراتمیک / خطای cross-device) |
| ۱۰ | **تفکیک خطای permanent از fatal** | فایل بزرگ یا محتوای خراب نباید کل pipeline را متوقف کند، ولی «دیسک پر» باید | یک نوع خطا (یا job بد همه را می‌خواباند، یا دیسک پر بی‌صدا DLQ می‌شود) |
| ۱۱ | **semaphore وزنی بر حسب بایت** | تعداد worker حافظه را محدود نمی‌کند؛ ۳۲ worker با فایل ۱GiB یعنی OOM | فقط سقف تعداد job |
| ۱۲ | **polling پشت اینترفیس** | صفر وابستگی و رفتار قابل پیش‌بینی روی شبکه/ویندوز؛ جای‌گذاری با fsnotify فقط تغییر constructor در `main` | fsnotify (وابستگی + پیچیدگی race در rename) |
| ۱۳ | **dedupe با (path,size,mtime) + stable_age** | فایل نیمه‌کاره ساخته نمی‌شود و رویداد تکراری یک بار پردازش می‌شود | فقط mtime (فایل‌های هم‌اندازه‌ی سریع) یا فقط رویداد watch |
| ۱۴ | **JSON با `DisallowUnknownFields` + fuzz** | صفر وابستگی، خطای زودهنگام روی typo، و fuzz معنادار | YAML (وابستگی سوم، سطح حمله‌ی بزرگ‌تر) |
| ۱۵ | **متریک دست‌نویس** | طبق نیاز: بدون `client_golang`، با gauge callback برای عمق صف | کتابخانه‌ی رسمی (وابستگی سنگین‌تر) |
| ۱۶ | **`slog` + TraceHandler** | `trace_id` بدون تکرار در هر call site | فیلد دستی در هر لاگ (فراموش می‌شود) |
| ۱۷ | **gzip با level پیش‌فرض ۶ و فیلد pointer** | تفکیک «تنظیم نشده» از `0` صریح | مقدار ساده (۰ کاربر را بی‌صدا override می‌کرد) |
| ۱۸ | **خطای journal هنگام shutdown غیر‌fatal** | نوشتن در حال cancel شکست می‌خورد؛ job با سند قبلی بازیابی می‌شود | fatal کردن (shutdown سالم را به خطا تبدیل می‌کرد) |
| ۱۹ | **panic در stage → خطای permanent** | یک فایل بد نباید process را بکشد | crash process یا retry بی‌پایان |

---

## ۱۱. محدودیت‌ها (آگاهانه)

1. **یک instance به ازای یک inbox.** هیچ lock بین‌processی وجود ندارد؛ اجرای هم‌زمان دو نمونه روی یک `work_dir` مجاز نیست.
2. **polling**: هزینه‌ی هر tick با تعداد فایل‌های inbox خطی است و تأخیر تا `poll_interval` وجود دارد. برای میلیون‌ها فایل باید اینترفیس `Source` را با fsnotify + index جای‌گذاری کرد.
3. **حافظه‌ی dedupe** با تعداد فایل‌های موجود در inbox رشد می‌کند (نه با تعداد کل jobها، چون entries حذف‌شده پاک می‌شوند).
4. **journal هرگز خودش را پاک نمی‌کند**؛ یک سند به ازای هر job باقی می‌ماند. پاک‌سازی/آرشیو تاریخچه خارج از محدوده است (کار برای cron).
5. **پنجره‌ی یک‌jobی orphan**: اگر کرش دقیقاً بین `rename` خروجی و ثبت `done` بیفتد، artefact کامل بدون سند `done` می‌ماند؛ اجرای بعدی آن را idempotent تکمیل می‌کند (خروجی تکراری ساخته نمی‌شود).
6. **`recovery.mode=resume`** فقط jobهایی را مستقیم publish می‌کند که `artifact_path` آن‌ها هنوز روی دیسک باشد؛ بقیه requeue می‌شوند.
7. **ابزار خارجی**: `CommandContext` فرزند مستقیم را می‌کشد و `WaitDelay` انتظار pipeها را محدود می‌کند، اما در ویندوز process group وجود ندارد؛ نوه‌ها ممکن است زنده بمانند. واحد پاک‌سازی، دایرکتوری موقت per-job است نه درخت process.
8. **fsync دایرکتوری** روی ویندوز best-effort است (باز کردن دایرکتوری برای `Sync` ممکن نیست)؛ خود فایل‌ها fsync می‌شوند.
9. **`/metrics` احراز هویت ندارد**؛ روی `127.0.0.1` گوش می‌دهد و برای exposure عمومی باید پشت proxy قرار بگیرد.
10. **دو فایل با محتوای یکسان** یک artefact تولید می‌کنند (به‌خاطر content-addressing) و job دوم `done` می‌شود بدون خروجی جداگانه. اگر per-file خروجی لازم است، کلید idempotency باید `hash+name` شود.
11. **سقف حجم** روی حجم ورودی است؛ خروجی gzip در بدترین حالت کمی بزرگ‌تر می‌شود (سربار gzip) و کنترل جداگانه ندارد.
12. **throughput**: هر گذار وضعیت یک fsync دارد (چند صد job در ثانیه روی دیسک معمولی). اگر throughput مهم‌تر از دوام لحظه‌ای باشد، باید journal را batch کرد.
13. **چرخه‌ی عمر دایرکتوری موقت**: `sink` آن را بعد از انتشار موفق، `dlq` بعد از ثبت شکست، و startup بعد از بازیابی پاک می‌کند. اگر process با `kill -9` بمیرد دایرکتوری باقی می‌ماند و در اجرای بعدی پاک می‌شود؛ عمداً در مسیر `cancel` پاک نمی‌شود تا حالت `recovery.mode=resume` بتواند از artifact سالم استفاده کند.
14. **`-limit N`** تعداد jobهای *پذیرفته‌شده* را می‌شمارد (شامل jobهای replay‌شده). اگر N از تعداد فایل‌های موجود بیشتر باشد، منبع خودش تمام نمی‌شود و سرویس تا سیگنال بعدی اجرا می‌ماند؛ برای اجرای دسته‌ای، N را دقیقاً برابر تعداد فایل‌های معتبر inbox بگذارید (اسکریپت دمو همین کار را می‌کند).
