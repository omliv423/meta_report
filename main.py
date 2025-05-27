import requests
import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime, timedelta

# === 環境変数またはSecretsから取得 ===
ACCESS_TOKEN = "EAAMIqzU3ZCCEBOwRfIZCCMCqjelbYjcWk1fXI6OnA1jZAmylczkZB07KcKGceLA9LB9zZAcujZA1PMwFqNLStOPbZBRAsPLixj6mIcEtAiU6P9XuZCdkZCn5zfjrMEnIfIyhlDw5GeC583FhwwE82WKKOZBlUt9ZAXXeMBIRQuxiGW79NQyZCJEPp4k5ECXv"
SPREADSHEET_ID = "1Jrgz4T7pEisKl9sIBNNBBy8ZDPTWOBBJOZJhFVvkitE"
SHEET_NAME = "広告report"
AD_ACCOUNT_ID = "act_2830099530543264"

# === 日付の取得 ===
yesterday = (datetime.utcnow() + timedelta(hours=9)) - timedelta(days=1)
date_str = yesterday.strftime('%Y-%m-%d')

# === スプレッドシート認証（なりすまし含む） ===
import json

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
IMPERSONATE_USER = "m.ogasahara@proreach.co.jp"  # ← あなたのGoogle Workspaceユーザー

with open('credentials.json') as f:
    creds_data = json.load(f)

creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_data, scopes=SCOPES)
delegated_creds = creds.create_delegated(IMPERSONATE_USER)

client = gspread.authorize(delegated_creds)
sheet = client.open_by_key(SPREADSHEET_ID).worksheet(SHEET_NAME)


# === 既存の日付チェック ===
existing_dates = sheet.col_values(1)[1:]  # skip header
if date_str in existing_dates:
    print(f"✅ {date_str} は既に存在しています。スキップします。")
    exit()

# === Meta API 取得 ===
url = f"https://graph.facebook.com/v18.0/{AD_ACCOUNT_ID}/insights"
params = {
    'fields': ','.join([
        'date_start', 'campaign_name', 'adset_name', 'ad_name', 'campaign_id', 'adset_id', 'ad_id',
        'clicks', 'impressions', 'spend', 'cpc', 'ctr', 'reach', 'frequency', 'cpm',
        'inline_link_clicks', 'video_play_actions', 'video_3_sec_watched_actions',
        'video_25_watched_actions', 'video_50_watched_actions', 'video_75_watched_actions',
        'video_95_watched_actions', 'video_100_watched_actions', 'video_avg_time_watched_actions',
        'actions', 'cost_per_action_type'
    ]),
    'level': 'ad',
    'time_range[since]': date_str,
    'time_range[until]': date_str,
    'limit': 100,
    'access_token': ACCESS_TOKEN
}

def get_action_value(arr, action_type):
    if not arr:
        return 0
    for a in arr:
        if a.get('action_type') == action_type:
            return float(a.get('value', 0))
    return 0

def get_video_value(arr, key):
    if not arr:
        return 0
    for a in arr:
        if a.get('action_type') == key:
            return float(a.get('value', 0))
    return 0

while True:
    res = requests.get(url, params=params)
    data = res.json()

    for row in data.get('data', []):
        video_total = get_action_value(row.get('video_play_actions'), 'video_view')
        video_3s = get_action_value(row.get('video_3_sec_watched_actions'), 'video_view')
        three_sec_rate = round(video_3s / video_total, 3) if video_total else 0

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
            three_sec_rate,
            get_action_value(row.get('actions'), 'offsite_conversion.fb_pixel_custom'),
            get_action_value(row.get('cost_per_action_type'), 'offsite_conversion.fb_pixel_custom'),
            row.get('campaign_id', ''),
            row.get('adset_id', ''),
            row.get('ad_id', ''),
            round(get_action_value(row.get('video_25_watched_actions'), 'video_view') / video_total, 3) if video_total else 0,
            round(get_action_value(row.get('video_50_watched_actions'), 'video_view') / video_total, 3) if video_total else 0,
            round(get_action_value(row.get('video_75_watched_actions'), 'video_view') / video_total, 3) if video_total else 0,
            round(get_action_value(row.get('video_95_watched_actions'), 'video_view') / video_total, 3) if video_total else 0,
            round(get_action_value(row.get('video_100_watched_actions'), 'video_view') / video_total, 3) if video_total else 0,
            float(row.get('video_avg_time_watched_actions', [{}])[0].get('value', 0)) if row.get('video_avg_time_watched_actions') else 0,
            get_action_value(row.get('actions'), 'landing_page_view')
        ]

        sheet.append_row(row_data)

    # 次ページがあれば更新
    if 'paging' in data and 'next' in data['paging']:
        url = data['paging']['next']
        params = {}  # nextにはすでにパラメータが含まれている
    else:
        break

print("✅ 完了しました")
