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

gsheet_base64 = os.getenv("GSHEET_JSON_BASE64")

if gsheet_base64 is None or ACCESS_TOKEN is None:
    print("❌ 環境変数が見つかりません。Secretsを確認してください。")
    exit(1)

with open("credentials.json", "wb") as f:
    f.write(base64.b64decode(gsheet_base64))

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
with open('credentials.json') as f:
    creds_data = json.load(f)

creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_data, scopes=SCOPES)
delegated_creds = creds.create_delegated(IMPERSONATE_USER)
client = gspread.authorize(delegated_creds)
sheet = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)

existing = sheet.get_all_values()[1:]
existing_keys = set(f"{r[0]}_{r[18]}" for r in existing if len(r) > 18)

start_date = datetime.strptime("2025-05-01", "%Y-%m-%d")
end_date = datetime.strptime("2025-05-27", "%Y-%m-%d")

def get_first_value(arr):
    return float(arr[0].get('value', 0)) if arr else 0

while start_date <= end_date:
    date_str = start_date.strftime('%Y-%m-%d')
    print(f"📅 Fetching {date_str}...")

    params = {
        'fields': ','.join([
            'date_start', 'campaign_name', 'adset_name', 'ad_name', 'campaign_id', 'adset_id', 'ad_id',
            'clicks', 'impressions', 'spend', 'cpc', 'ctr', 'reach', 'frequency', 'cpm',
            'inline_link_clicks', 'video_play_actions', 'video_avg_time_watched_actions',
            'video_p25_watched_actions', 'video_p50_watched_actions', 'video_p75_watched_actions',
            'video_p95_watched_actions', 'video_p100_watched_actions',
            'conversions', 'objective', 'objective_results'
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
        start_date += timedelta(days=1)
        continue

    buffer = []

    for row in data.get('data', []):
        if int(row.get('impressions', 0)) == 0:
            continue  # 配信されていない広告はスキップ

        key = f"{row.get('date_start', '')}_{row.get('ad_id', '')}"
        if key in existing_keys:
            print("⏭ Skip (already exists):", key)
            continue

        v25 = get_first_value(row.get('video_p25_watched_actions'))
        v50 = get_first_value(row.get('video_p50_watched_actions'))
        v75 = get_first_value(row.get('video_p75_watched_actions'))
        v95 = get_first_value(row.get('video_p95_watched_actions'))
        v100 = get_first_value(row.get('video_p100_watched_actions'))

        results = row.get('conversions') or row.get('objective_results')
        result_count = get_first_value(results)
        spend = float(row.get('spend', 0))
        cost_per_result = spend / result_count if result_count else 0

        row_data = [
            row.get('date_start', ''),
            row.get('campaign_name', ''),
            row.get('adset_name', ''),
            row.get('ad_name', ''),
            spend,
            result_count,
            cost_per_result,
            int(row.get('reach', 0)),
            float(row.get('frequency', 0)),
            int(row.get('impressions', 0)),
            float(row.get('cpm', 0)),
            float(row.get('ctr', 0)),
            int(row.get('clicks', 0)),
            int(row.get('inline_link_clicks', 0)),
            float(row.get('cpc', 0)),
            v25, v50, v75, v95, v100,
            row.get('campaign_id', ''),
            row.get('adset_id', ''),
            row.get('ad_id', ''),
            get_first_value(row.get('video_avg_time_watched_actions'))
        ]

        buffer.append(row_data)

    if buffer:
        sheet.append_rows(buffer)
        print(f"✅ {date_str}: {len(buffer)} 行を一括書き込み")
    else:
        print(f"ℹ️ {date_str}: データなし or 全て重複")

    start_date += timedelta(days=1)

print("🎉 5/1〜5/27のデータ取得完了！")
