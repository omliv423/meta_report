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

# === 既存のキー（date + ad_id）を取得 ===
existing = sheet.get_all_values()[1:]  # ヘッダーを除く
existing_keys = set(f"{r[0]}_{r[18]}" for r in existing if len(r) > 18)

# === 日別ループ（5/1〜5/27） ===
start_date = datetime.strptime("2025-05-01", "%Y-%m-%d")
end_date = datetime.strptime("2025-05-27", "%Y-%m-%d")

while start_date <= end_date:
    date_str = start_date.strftime('%Y-%m-%d')
    print(f"📅 Fetching {date_str}...")

    params = {
        'fields': ','.join([
            'date_start', 'campaign_name', 'adset_name', 'ad_name', 'campaign_id', 'adset_id', 'ad_id',
            'clicks', 'impressions', 'spend', 'cpc', 'ctr', 'reach', 'frequency', 'cpm',
            'inline_link_clicks', 'video_play_actions', 'video_avg_time_watched_actions',
            'actions', 'cost_per_action_type'
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

    def get_action_value(arr, action_type):
        if not arr:
            return 0
        for a in arr:
            if a.get('action_type') == action_type:
                return float(a.get('value', 0))
        return 0

    for row in data.get('data', []):
        video_total = get_action_value(row.get('video_play_actions'), 'video_view')
        key = f"{row.get('date_start', '')}_{row.get('ad_id', '')}"
        if key in existing_keys:
            print("⏭ Skip (already exists):", key)
            continue

        row_data = [
            row.get('date_start', ''),
            row.get('campaign_name', ''),
            row.get('adset_name', ''),
            row.get('ad_name', ''),
            int(row.get('clicks', 0)),
            int(row.get('impressions', 0)),
            float(row.get('spend', 0)),
            float(row.get('cpc', 0)),
            float(row.get('ctr', 0)),
            int(row.get('reach', 0)),
            float(row.get('frequency', 0)),
            float(row.get('cpm', 0)),
            int(row.get('inline_link_clicks', 0)),
            0,
            get_action_value(row.get('actions'), 'offsite_conversion.fb_pixel_custom'),
            get_action_value(row.get('cost_per_action_type'), 'offsite_conversion.fb_pixel_custom'),
            row.get('campaign_id', ''),
            row.get('adset_id', ''),
            row.get('ad_id', ''),
            0, 0, 0, 0, 0,
            float(row.get('video_avg_time_watched_actions', [{}])[0].get('value', 0)) if row.get('video_avg_time_watched_actions') else 0,
            get_action_value(row.get('actions'), 'landing_page_view')
        ]

        buffer.append(row_data)

    if buffer:
        sheet.append_rows(buffer)
        print(f"✅ {date_str}: {len(buffer)} 行を一括書き込み")
    else:
        print(f"ℹ️ {date_str}: データなし or 全て重複")

    start_date += timedelta(days=1)

print("🎉 5/1〜5/27の日別データ取得完了！")
