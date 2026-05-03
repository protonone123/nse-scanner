# NSE Scanner

NSE Live Pattern Scanner v3.1 — Production Grade Stock Market Scanning

## Features

- 13 Detectors + 5 Confirmation Signals
- Hourly + Daily scanning modes
- Telegram alerts integration
- Web dashboard
- GitHub Actions automation

## Quick Start

### Prerequisites
- Python 3.11+
- Telegram Bot Token (from [@BotFather](https://t.me/botfather))

### Installation

```bash
git clone https://github.com/trashpandak/nse-scanner.git
cd nse-scanner
pip install -r requirements.txt
```

### Configuration

1. Copy `.env.example` to `.env`:
```bash
cp .env.example .env
```

2. Edit `.env` and add your credentials:
```
TG_BOT_TOKEN=your_bot_token_from_botfather
TG_CHAT_ID=your_telegram_chat_id
```

### Usage

```bash
# Full daily scan
python scanner.py --daily

# Hourly scan (during market hours)
python scanner.py --hourly

# With Telegram alerts
python scanner.py --daily --telegram

# Launch web dashboard
python scanner.py --dashboard

# Health check
python scanner.py --healthcheck

# Quick test (10 stocks)
python scanner.py --test
```

## Deployment

### Local Development
```bash
python scanner.py --test --telegram
```

### GitHub Actions (Automated)
Scans run automatically:
- **Daily**: 4:30 PM IST (Mon-Fri)
- **Hourly**: 10:15 AM - 3:15 PM IST (market hours)

Set secrets in GitHub repo settings:
- `TG_BOT_TOKEN`
- `TG_CHAT_ID`

## Architecture

- **DAILY (4:30 PM IST)**: Full scan of all NSE stocks → builds watchlist
- **HOURLY (market hours)**: Checks watchlist only → triggers & alerts
- **Dashboard**: Real-time web interface

## Security

⚠️ **Never commit secrets to git!**
- Use `.env` for local development (ignored by git)
- Use GitHub Secrets for Actions
- See `.env.example` for required variables

## License

Private - Internal Use Only
