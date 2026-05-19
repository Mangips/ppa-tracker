# PPA Tracker — Setup Guide

## What you need before starting

- GitHub account (free)
- Google account → Gemini API key (free, via Google AI Studio)
- NewsAPI key (free tier, newsapi.org)
- rclone configured for OneDrive (one-time local setup)

---

## Step 1 — Get your API keys

### Gemini (Google AI Studio)
1. Go to https://aistudio.google.com
2. Sign in with your Google account
3. Click "Get API key" → "Create API key"
4. Copy the key, save it somewhere safe

### NewsAPI
1. Go to https://newsapi.org/register
2. Register with your email
3. Your API key is shown on the dashboard after registration

---

## Step 2 — Create the GitHub repository

1. Go to https://github.com/new
2. Create a **private** repository named `ppa-tracker`
3. Do not initialise with README (you'll push the code yourself)

### Push the code

On your machine (or in any terminal with git and Python available):

```bash
git clone https://github.com/YOUR_USERNAME/ppa-tracker.git
cd ppa-tracker
# Copy all files from this project into the folder
git add .
git commit -m "initial commit"
git push
```

---

## Step 3 — Add secrets to GitHub

Go to your repo → Settings → Secrets and variables → Actions → New repository secret

Add these three secrets:

| Name | Value |
|------|-------|
| `NEWSAPI_KEY` | your NewsAPI key |
| `GEMINI_KEY` | your Gemini API key |
| `RCLONE_CONFIG` | contents of your rclone config file (see Step 4) |

---

## Step 4 — Configure rclone for OneDrive

Do this on your local machine. rclone portable requires no installation.

### Download rclone portable (Windows, no admin needed)

1. Go to https://rclone.org/downloads/
2. Download the Windows zip for your architecture (amd64 for most machines)
3. Extract the zip anywhere, e.g. `C:\Users\yourname\rclone\`

### Authenticate with OneDrive

Open a terminal (Command Prompt or PowerShell) in the rclone folder:

```cmd
rclone.exe config
```

Follow the prompts:
- `n` for new remote
- Name it `onedrive`
- Choose `Microsoft OneDrive` from the list (type the number)
- Leave client_id and client_secret blank (press Enter)
- Choose your region (global for most EU institutional accounts)
- `y` to use auto config (opens a browser window)
- Log in with your Microsoft account in the browser
- Confirm the permissions
- Choose your OneDrive type (personal or business)
- Select the drive
- Confirm and quit config

### Get the config content for GitHub

```cmd
rclone.exe config show
```

Copy the entire output. It looks like:

```
[onedrive]
type = onedrive
token = {"access_token":"...","expiry":"..."}
drive_id = ...
drive_type = personal
```

Paste this entire block as the value of the `RCLONE_CONFIG` secret in GitHub (Step 3).

**Note:** rclone tokens expire. If the OneDrive upload starts failing after a few months,
re-run `rclone config` locally and update the secret with the new token.

---

## Step 5 — Create the data folder in the repo

```bash
mkdir data
touch data/.gitkeep
git add data/.gitkeep
git commit -m "add data folder"
git push
```

---

## Step 6 — Test the pipeline manually

In your GitHub repo, go to Actions → PPA Tracker Daily Run → Run workflow.

Watch the logs. On first run you should see:
- Articles collected from NewsAPI and GDELT
- Gemini extraction running per article
- "New deals" and "Updates" count at the end
- CSV committed back to the repo
- OneDrive upload status

---

## Step 7 — Access your data

**In GitHub:** go to your repo → `data/ppa_deals.csv` → Download

**In OneDrive:** the file appears at `PPA_Tracker/ppa_deals.csv` in your OneDrive

**In Excel:** open directly from OneDrive or download and open locally.
The CSV is UTF-8 encoded. If accented characters look wrong in Excel,
use Data → From Text/CSV and select UTF-8 encoding.

---

## CSV column reference

| Column | Description |
|--------|-------------|
| id | Internal row id |
| date_agreement | Date the deal was signed (YYYY-MM-DD, or partial if unknown) |
| date_found | Date our tool found and recorded this deal |
| buyer | Offtaker / energy buyer |
| seller | Developer / IPP / generator |
| capacity_mw | Contracted capacity in MW |
| energy_gwh | Contracted energy in GWh/year (if reported) |
| tenure_years | Contract duration in years |
| country | Country where energy is delivered |
| technology | solar / wind onshore / wind offshore / hydro / mixed / other |
| price_eur_mwh | Strike price in EUR/MWh (rarely disclosed) |
| source_url | Direct link to the source article |
| source_outlet | News outlet or domain |
| publication_date | Date the article was published |
| notes | Additional details: project name, grid connection, special terms, etc. |
| is_update | 1 if this row is an update to a previously recorded deal |
| original_deal_id | id of the original deal row if is_update = 1 |

---

## Tuning search queries

Edit `SEARCH_QUERIES` in `src/pipeline.py` to add or remove terms.
More specific queries = fewer false positives but may miss deals.
Broader queries = more coverage but more noise for Gemini to filter.

The current queries are a good starting point for the first few weeks.
After 2-3 weeks, review what's being found and adjust accordingly.

---

## Cost

- NewsAPI free tier: 100 requests/day. Current config uses ~3 requests/day. Fine.
- GDELT: free, no limits.
- Gemini Flash free tier: 1500 requests/day. Current config uses ~20-60/day. Fine.
- GitHub Actions free tier: 2000 minutes/month. Each run takes ~3-5 minutes. Fine.

Total cost: €0.
