import pandas as pd
import matplotlib.pyplot as plt

# ============================================
# SECTION 1: DATA LOADING & CLEANING
# ============================================

def load_data():
    df = pd.read_csv('../data/event_registration_2025.csv')
    col = df.columns
    df = df.rename(columns={
        col[1]: 'registered_at',
        col[2]: 'ticket_type',
        col[4]: 'event_title',
        col[6]: 'age_group',
        col[8]: 'acquisition_channel',
        col[9]: 'expectations',
        col[10]: 'attribute'
    })
    df['registered_at'] = pd.to_datetime(df['registered_at']).dt.tz_localize(None)
    return df

def load_data2():
    df_2026 = pd.read_csv('../data/event_registration_2026.csv')
    col = df_2026.columns
    df_2026 = df_2026.rename(columns={
        col[1]: 'registered_at',
        col[2]: 'ticket_type',
        col[10]: 'age_group',
        col[13]: 'residence',
        col[15]: 'attribute',
        col[16]: 'acquisition_channel',
        col[17]: 'expectations'
    })
    df_2026['registered_at'] = pd.to_datetime(df_2026['registered_at'], utc=True).dt.tz_convert('Asia/Tokyo').dt.tz_localize(None)
    return df_2026

keep = ['registered_at', 'ticket_type', 'age_group',
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
plt.savefig('../outputs/registration_growth.png')
plt.close()

# ============================================
# SECTION 3: ACQUISITION CHANNEL ANALYSIS
# ============================================

rename_map_channel = {
    # 2025 names
    'Club Instagram': 'INSTAGRAM',
    'Club Facebook': 'FACEBOOK',
    'Club ホームページ': 'HOMEPAGE',
    'Club ホームページ・メール': 'HOMEPAGE',
    'Club以外のSNS': 'OTHER SNS',
    'Club以外の各種SNS媒体': 'OTHER SNS',
    'Club X': 'X',
    # 2026 names
    'TEDxNagoyaU Instagram': 'INSTAGRAM',
    'TEDxNagoyaU Facebook': 'FACEBOOK',
    'TEDxNagoyaU ホームページ・メール': 'HOMEPAGE',
    'TEDxNagoyaU以外の各種SNS媒体': 'OTHER SNS',
    'TEDxNagoyaU X': 'X',
    # Common
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
plt.savefig('../outputs/acquisition_comparison.png')
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
plt.savefig('../outputs/attribute_comparison.png')
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
    cumul_pct = cumul.div(cumul.max()) * 100
    cumul_pct.plot(ax=axes[i], title=channel)
    axes[i].set_xlabel('Days Since Registration Opened')
    axes[i].set_ylabel('% of Total Registrations')
    axes[i].set_ylim(0, 105)

plt.suptitle('Club Acquisition Channel Growth: 2025 vs 2026', fontsize=14)
plt.tight_layout()
plt.savefig('../outputs/channel_growth.png')
plt.close()

# ============================================
# SECTION 6: CHANNEL PERFORMANCE VS LAST YEAR
# ============================================

for channel in channels_to_plot:
    df_ch = df_combined[df_combined['acquisition_channel'] == channel]

    count_2025 = df_ch[df_ch['year'] == 2025].shape[0]
    count_2026 = df_ch[df_ch['year'] == 2026].shape[0]

    if count_2025 > 0:
        pct_change = round((count_2026 - count_2025) / count_2025 * 100, 1)
        print(f"{channel}: 2025={count_2025}, 2026={count_2026}, change={pct_change}%")