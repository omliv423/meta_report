import requests
import json
import gspread
import os
import base64
from datetime import datetime, timedelta
from oauth2client.service_account import ServiceAccountCredentials

# === 環境変数から取得 ===
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
SPREADSHEET_ID = "1Jrgz4T7pEisKl9sIBNNBBy8ZDPTWOBBJOZJhFVvkitE"
SHEET_NAME = "広告report"
AD_ACCOUNT_ID = "act_2830099530543264"
IMPERSONATE_USER = "m.ogasahara@proreach.co.jp"

# === credentials.json をSecretsから生成 ===
gsheet_base64 = os.getenv("GSHEET_JSON_BASE64")

print("ENV CHECK")
print("GSHEET_JSON_BASE64 is None:", gsheet_base64 is None)
print("ACCESS_TOKEN is None:", ACCESS_TOKEN is None)

if gsheet_base64 is None or ACCESS_TOKEN is None:
    print("❌ 環境変数が見つかりません。Secretsを確認してください。")
    exit(1)

with open("credentials.json", "wb") as f:
    f.write(base64.b64decode(gsheet_base64))

# === スプレッドシート認証 ===
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
with open('credentials.json') as f:
    creds_data = json.load(f)

creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_data, scopes=SCOPES)
delegated_creds = creds.create_delegated(IMPERSONATE_USER)
client = gspread.authorize(delegated_creds)
sheet = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)

# === 今日の日付（JST）
today = datetime.utcnow() + timedelta(hours=9)
yesterday = today - timedelta(days=1)
date_str = yesterday.strftime('%Y-%m-%d')
print(f"📅 Fetching {date_str}...")

# === 既存のキー（date + ad_id）を取得 ===
existing = sheet.get_all_values()[1:]  # ヘッダーを除く
existing_keys = set(f"{r[0]}_{r[18]}" for r in existing if len(r) > 18)

params = {
    'fields': ','.join([
        'date_start', 'campaign_name', 'adset_name', 'ad_name', 'campaign_id', 'adset_id', 'ad_id',
        'clicks', 'impressions', 'spend', 'cpc', 'reach', 'frequency', 'cpm',
        'inline_link_clicks', 'video_play_actions', 'video_avg_time_watched_actions',
        'video_p25_watched_actions', 'video_p50_watched_actions',
        'video_p75_watched_actions', 'video_p95_watched_actions',
        'video_p100_watched_actions'
    ]),
    'level': 'ad',
    'time_range[since]': date_str,
    'time_range[until]': date_str,
    'limit': 100,
    'access_token': ACCESS_TOKEN
}

res = requests.get(f"https://graph.facebook.com/v18.0/{AD_ACCOUNT_ID}/insights", params=params)
data = res.json()

if "error" in data:
    print("❌ API Error:", data["error"])
    exit(1)

def get_first_value(arr):
    if isinstance(arr, list) and len(arr) > 0:
        return float(arr[0].get("value", 0))
    return 0

def get_ratio(numerator_arr, denominator_arr):
    num = get_first_value(numerator_arr)
    den = get_first_value(denominator_arr)
    return round(num / den, 4) if den else 0

buffer = []

for row in data.get('data', []):
    if int(row.get('impressions', 0)) == 0:
        continue  # ⏭ 配信されていない広告はスキップ

    key = f"{row.get('date_start', '')}_{row.get('ad_id', '')}"
    if key in existing_keys:
        print("⏭ Skip (already exists):", key)
        continue

    v25 = get_ratio(row.get('video_p25_watched_actions'), row.get('video_play_actions'))
    v50 = get_ratio(row.get('video_p50_watched_actions'), row.get('video_play_actions'))
    v75 = get_ratio(row.get('video_p75_watched_actions'), row.get('video_play_actions'))
    v95 = get_ratio(row.get('video_p95_watched_actions'), row.get('video_play_actions'))
    v100 = get_ratio(row.get('video_p100_watched_actions'), row.get('video_play_actions'))

    row_data = [
        row.get('date_start', ''),
        row.get('campaign_name', ''),
        row.get('adset_name', ''),
        row.get('ad_name', ''),
        float(row.get('spend', 0)),
        int(row.get('reach', 0)),
        float(row.get('frequency', 0)),
        int(row.get('impressions', 0)),
        float(row.get('cpm', 0)),
        int(row.get('clicks', 0)),
        int(row.get('inline_link_clicks', 0)),
        float(row.get('cpc', 0)),
        get_first_value(row.get('video_avg_time_watched_actions')),
        v25, v50, v75, v95, v100,
        row.get('campaign_id', ''),
        row.get('adset_id', ''),
        row.get('ad_id', '')
    ]

    buffer.append(row_data)

if buffer:
    sheet.append_rows(buffer)
    print(f"✅ {date_str}: {len(buffer)} 行を一括書き込み")
else:
    print(f"ℹ️ {date_str}: データなし or 全て重複")

print("🎉 当日のデータ取得完了！")
