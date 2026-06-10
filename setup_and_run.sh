#!/bin/bash

# ── Step 2: Environment variables — replace with your actual keys ──
export NEWSAPI_KEY=09dc36bc64a640e09c505edb68bf576b
export MISTRAL_KEY=fxdBAx3SLW1zcgUat1JowUbBtT5eCo4C
export LOOKBACK_DAYS=0
export MAX_ARTICLES=100000
export NOTIFY_EMAIL_ENABLED=
export ENVIRONMENT=Test

# ── Step 3: Install dependencies ──
pip install -r requirements.txt

# ── Step 4: Git config ──
git config user.name "ppa-tracker-bot"
git config user.email "bot@ppa-tracker"

# ── Step 5: Historical loop ──
START="2025-07-22"
END="2025-12-31"
CURRENT=$START

while [[ "$CURRENT" < "$END" ]]; do
  WEEK_END=$(date -d "$CURRENT +6 days" +%Y-%m-%d)
  if [[ "$WEEK_END" > "$END" ]]; then WEEK_END=$END; fi

  echo "=========================================="
  echo "Processing $CURRENT to $WEEK_END"
  echo "=========================================="

  export SEARCH_FROM_DATE=$CURRENT
  export SEARCH_TO_DATE=$WEEK_END

  python src/pipeline.py

  git add data/ppa_deals.db data/ppa_deals.csv data/logs/
  git diff --staged --quiet || git commit -m "chore: historical $CURRENT to $WEEK_END"

  CURRENT=$(date -d "$WEEK_END +1 day" +%Y-%m-%d)
  sleep 5
done

git push
