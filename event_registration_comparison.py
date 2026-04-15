import pandas as pd
import matplotlib.pyplot as plt

# ============================================
# SECTION 1: DATA LOADING & CLEANING
# ============================================

def load_data():
    df = pd.read_csv('event_registration_2025.csv')
    df = df.drop(columns=['注文ID', 'チケットNo.', 'Unnamed: 11', 'Unnamed: 12'])
    df = df.rename(columns={
        '注文時刻': 'registered_at',
        '注文内容': 'ticket_type',
        'イベントID': 'event_id',
        'イベントタイトル': 'event_title',
        '年代': 'age_group',
        'フリーフォーム1 所属(学校名または会社名)\n \n ※所属が無い方は「無し」とご記入ください。': 'affiliation',
        'オプション1 イベントを知ったきっかけ': 'acquisition_channel',
        'オプション2 Club 2025に期待することをご選択ください。': 'expectations',
        'オプション3 属性(最も当てはまるもの一つを選択してください)': 'attribute'
    })
    df['registered_at'] = pd.to_datetime(df['registered_at']).dt.tz_localize(None)
    return df

def load_data2():
    df_2026 = pd.read_csv('event_registration_2026.csv')
    df_2026 = df_2026.rename(columns={
        '注文時刻': 'registered_at',
        '注文内容': 'ticket_type',
        '年代': 'age_group',
        'お住まいの地域': 'residence',
        '所属(学校名または会社名)\n ※所属が無い方は「無し」とご記入ください。': 'affiliation',
        '属性（最も当てはまるものを一つ選択してください）': 'attribute',
        'イベントを知ったきっかけ': 'acquisition_channel',
        'Club 2026に期待することをご選択ください。': 'expectations'
    })
    df_2026['registered_at'] = pd.to_datetime(df_2026['registered_at'], utc=True).dt.tz_convert('Asia/Tokyo').dt.tz_localize(None)
    return df_2026

keep = ['registered_at', 'ticket_type', 'age_group', 'affiliation',
        'acquisition_channel', 'expectations', 'attribute', 'year']

df_2025 = load_data()
df_2025['year'] = 2025

df_2026 = load_data2()
df_2026['year'] = 2026

df_combined = pd.concat([df_2025[keep], df_2026[keep]], ignore_index=True)

# ============================================
# SECTION 2: REGISTRATION GROWTH
# ============================================

df_combined['first_day'] = df_combined.groupby('year')['registered_at'].transform('min')
df_combined['days_since_open'] = (df_combined['registered_at'] - df_combined['first_day']).dt.days

cumulative = df_combined.groupby(['days_since_open', 'year']).size().unstack().fillna(0).cumsum()

fig, ax = plt.subplots(figsize=(12, 6))
cumulative.plot(ax=ax)
ax.set_xlabel('Days Since Registration Opened')
ax.set_ylabel('Cumulative Registrations')
ax.set_title('Club Registration Growth: 2025 vs 2026')
ax.legend(['2025', '2026'])
plt.tight_layout()
plt.savefig('registration_growth.png')
plt.close()

# ============================================
# SECTION 3: ACQUISITION CHANNEL ANALYSIS
# ============================================

rename_map_channel = {
    'Club Instagram': 'INSTAGRAM',
    'Club Facebook': 'FACEBOOK',
    'Club ホームページ': 'HOMEPAGE',
    'Club ホームページ・メール': 'HOMEPAGE',
    'Club以外のSNS': 'OTHER SNS',
    'Club以外の各種SNS媒体': 'OTHER SNS',
    'Club X': 'X',
    'スタッフからの紹介': 'INTRODUCED BY STAFF',
    'その他': 'OTHERS',
    '所属校・会社からの紹介': 'INTRODUCED BY INSTITUTION',
    '登壇者・出展者からの紹介': 'INTRODUCED BY SPEAKERS',
    '駅広告': 'TRAIN STATION ADS'
}

df_combined['acquisition_channel'] = df_combined['acquisition_channel'].str.split(',')
df_combined = df_combined.explode('acquisition_channel')
df_combined['acquisition_channel'] = df_combined['acquisition_channel'].map(rename_map_channel)

acq = df_combined.groupby(['acquisition_channel', 'year']).size().unstack().fillna(0)

fig, ax = plt.subplots(figsize=(12, 6))
acq.sort_values(2025, ascending=False).plot(kind='bar', ax=ax)
ax.set_xlabel('Acquisition Channel')
ax.set_ylabel('Number of Registrations')
ax.set_title('Club Acquisition Channel: 2025 vs 2026')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.savefig('acquisition_comparison.png')
plt.close()

# ============================================
# SECTION 4: ATTENDEE TYPE ANALYSIS
# ============================================

rename_map_attr = {
    '大学生大学院生': 'UNIVERSITY STUDENT',
    '大学生・大学院生': 'UNIVERSITY STUDENT',
    '社会人': 'WORKING PEOPLE',
    '高校生': 'HIGH SCHOOL STUDENT',
    '中学生以下': 'MIDDLE SCHOOL STUDENT AND YOUNGER',
}

df_combined['attribute'] = df_combined['attribute'].str.split(',')
df_combined = df_combined.explode('attribute')
df_combined['attribute'] = df_combined['attribute'].map(rename_map_attr)

attr = df_combined.groupby(['attribute', 'year']).size().unstack().fillna(0)

fig, ax = plt.subplots(figsize=(10, 6))
attr.plot(kind='bar', ax=ax)
ax.set_xlabel('Attribute')
ax.set_ylabel('Number of Registrations')
ax.set_title('Club Attendee Type: 2025 vs 2026')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
plt.savefig('attribute_comparison.png')
plt.close()

# ============================================
# SECTION 5: CHANNEL GROWTH OVER TIME
# ============================================

channels_to_plot = ['INTRODUCED BY STAFF', 'INTRODUCED BY INSTITUTION', 'INSTAGRAM', 'HOMEPAGE']

fig, axes = plt.subplots(2, 2, figsize=(14, 10))
axes = axes.flatten()

for i, channel in enumerate(channels_to_plot):
    df_ch = df_combined[df_combined['acquisition_channel'] == channel].copy()
    df_ch['days_since_open'] = (df_ch['registered_at'] - df_ch.groupby('year')['registered_at'].transform('min')).dt.days
    cumul = df_ch.groupby(['days_since_open', 'year']).size().unstack().fillna(0).cumsum()
    cumul.plot(ax=axes[i], title=channel)
    axes[i].set_xlabel('Days Since Registration Opened')
    axes[i].set_ylabel('Cumulative Registrations')

plt.suptitle('Club Acquisition Channel Growth: 2025 vs 2026', fontsize=14)
plt.tight_layout()
plt.savefig('channel_growth.png')
plt.close()

# ============================================
# SECTION 6: CHANNEL PERFORMANCE VS LAST YEAR
# ============================================

day_cutoff = 47

for channel in channels_to_plot:
    df_ch = df_combined.copy()
    df_ch['days_since_open'] = (df_ch['registered_at'] - df_ch.groupby('year')['registered_at'].transform('min')).dt.days

    count_2025 = df_ch[(df_ch['year'] == 2025) & (df_ch['days_since_open'] <= day_cutoff)].shape[0]
    count_2026 = df_ch[(df_ch['year'] == 2026) & (df_ch['days_since_open'] <= day_cutoff)].shape[0]

    if count_2025 > 0:
        pct_change = round((count_2026 - count_2025) / count_2025 * 100, 1)
        print(f"{channel}: 2025={count_2025}, 2026={count_2026}, change={pct_change}%")