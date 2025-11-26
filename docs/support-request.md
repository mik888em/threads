# Черновик письма в поддержку GitHub

Ниже — текст, который можно вставить в форму обращения по блокировке GitHub Actions. Текст учитывает текущее расписание, таймауты и имена workflow в репозитории `mik888em/threads`.

## Тема письма (Subject)
`GitHub Actions false-positive block for account "mik888em"`

## Текст обращения
```
Hello GitHub Support,

My personal account "mik888em" shows the banner "GitHub Actions is currently disabled for your account." The issue reappeared recently; the account was previously unblocked after review.

Context (all workflows are in mik888em/threads):
- "Threads Metrics" (`threads-metrics.yml`): scheduled via cron `0 */2 * * *` (every 2 hours, UTC), collects public Threads metrics and writes them to Google Sheets. Concurrency is set with `cancel-in-progress: true`; job timeout is 60 minutes.
- "Cancel queued Threads Metrics runs" (`cancel-pending.yml`): safety workflow to cancel overlapping runs of "Threads Metrics". Scheduled via cron `*/15 * * * *` (every 15 minutes, UTC) with concurrency + `cancel-in-progress: true`, job timeout 15 minutes, internal check interval 120 seconds.
- "Sync Google Sheets" (`sheets-sync.yml`): syncs aggregated data to Google Sheets, scheduled via cron `*/30 * * * *` (every 30 minutes, UTC) with `cancel-in-progress: true` and a 20-minute timeout.
- Automation for pull requests (`automerge-codex.yml`) only adds a label/merges when a PR is labeled `automerge`; no mining or heavy compute is involved.

Mitigation already applied:
- Reduced cron frequency (2h/15m/30m) and shortened timeouts across all scheduled workflows.
- Enabled `cancel-in-progress: true` to avoid run queues and potential bursts.
- Additional guard logic cancels overlapping runs of "Threads Metrics" before the main job starts.

All workflows operate on public data only; there is no crypto-mining or prohibited workload. If you need more details or sample run URLs, I will gladly provide them.

Could you please review and re-enable GitHub Actions for my account?

Thank you for your help!
Mikhail (mik888em)
Email: mikhailinvest4@gmail.com
```

## Подсказки по заполнению формы
- В поле **Subject** вставьте тему из раздела выше.
- В поле описания скопируйте текст обращения.
- В выпадающем списке продукта выберите **GitHub Actions** (если доступно).
- Для вопроса «Is this request related to GitHub Actions?» выберите **Yes**.
- Укажите основной e-mail, связанный с аккаунтом: `mikhailinvest4@gmail.com`.
- Если есть поле про срочность, выберите обычный/normal priority.
- Прикрепите, при необходимости, скриншот баннера «GitHub Actions is currently disabled for your account».
