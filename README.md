# Adding Bot

Adding-only Telegram bot for saving and updating media-name mappings into MongoDB.

This repo is designed for **Render Web Service** deployment first, with optional polling mode for non-Render/local use.

## What this bot does

- Manual save with `/save`
- Auto-save from:
  - supported inline bots
  - supported forwarded source channels
  - target chat save-only mode
- Writes media metadata into MongoDB
- Sends save/update logs to `ADDED_LOG_CHANNEL`
- Supports:
  - `@Character_Catcher_Bot` -> `/catch`
  - `@Character_Seizer_Bot` -> `/seize`
  - `@CaptureCharacterBot` -> `/capture`
  - `@Takers_character_bot` -> `/take`
  - `@Grab_Your_Waifu_Bot` -> `/grab`
- Supports forwarded source mapping such as:
  - `@CaptureDatabase` -> `/capture`
  - `@Seizer_Database` -> `/seize`

## Files

- `app.py` - main adding bot
- `requirements.txt` - Python dependencies
- `.env.example` - environment template
- `render.yaml` - optional Render Blueprint
- `.gitignore`

## Render deploy

### Option A: Deploy with `render.yaml`
1. Push this repo to GitHub.
2. In Render, create a new Blueprint or Web Service from the repo.
3. Fill in the secret env vars when Render prompts for them:
   - `BOT_TOKEN`
   - `MONGO_URI`
   - `OWNER_IDS`
   - `DEFAULT_TARGET_CHAT`
   - `ADDED_LOG_CHANNEL`
4. Deploy.

### Option B: Manual Web Service setup
Create a **Web Service** in Render and use:

- **Build Command**
  ```bash
  pip install -r requirements.txt
  ```

- **Start Command**
  ```bash
  python app.py
  ```

- **Health Check Path**
  ```text
  /healthz
  ```

Then add the environment variables from `.env.example`.

## Important env vars

### Required
- `BOT_TOKEN`
- `MONGO_URI`
- `OWNER_IDS`
- `DEFAULT_TARGET_CHAT`

### Recommended
- `ADDED_LOG_CHANNEL`
- `USE_WEBHOOK=true`
- `WEBHOOK_SECRET`
- `INLINE_SOURCE_BOTS`
- `FORWARD_SOURCE_COMMANDS`

### Webhook URL
On Render, you can usually leave `PUBLIC_URL` empty because the app will fall back to Render's `RENDER_EXTERNAL_URL`.

## UptimeRobot

Optional only.

If you want an external health monitor, point it to:

```text
https://your-service.onrender.com/healthz
```

A simple HTTP monitor is enough.

## Local run

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python app.py
```

For local polling mode, set:

```env
USE_WEBHOOK=false
```

## Notes

- This is an **adding bot only** repo.
- Public lookup commands are intentionally not included here.
- MongoDB database/collections are reused from your main setup.
