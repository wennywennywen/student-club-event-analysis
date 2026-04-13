import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import sqlite3

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
    return df

# ============================================
# SECTION 2: PIPELINE — LOAD INTO SQLITE
# ============================================
df = load_data()
conn = sqlite3.connect(":memory:")

# Main table
df.to_sql("event_registration", conn, index=False, if_exists="replace")

# Acquisition channel — exploded
df_acq = load_data()
df_acq['acquisition_channel'] = df_acq['acquisition_channel'].str.split(',')
df_acq = df_acq.explode('acquisition_channel')
df_acq.to_sql("event_acquisition", conn, index=False, if_exists="replace")

# Expectations — exploded
df_exp = load_data()
df_exp['expectations'] = df_exp['expectations'].str.split(',')
df_exp = df_exp.explode('expectations')
df_exp.to_sql("event_expectations", conn, index=False, if_exists="replace")

# Attribute — filtered (no multi-select)
df_att = load_data()
df_att = df_att[~df_att['attribute'].str.contains(',', na=False)]
df_att.to_sql("event_attribute", conn, index=False, if_exists="replace")

st.title('Student club main event registration analysis')
st.write('An analysis of a student club main event registration data — exploring attendance trends, acquisition channels, demographic breakdown, and attendee expectations to inform future event promotion strategy.')

# ============================================
# CHART 1: REGISTRATION TIMELINE
# ============================================
st.header('Daily Registration Trend')
df_time = load_data()
df_time['registered_at'] = pd.to_datetime(df_time['registered_at'], utc=True)
df_time['registered_at'] = df_time['registered_at'].dt.tz_localize(None)
df_time['day'] = df_time['registered_at'].dt.to_period('D').astype(str)
trend = df_time.groupby('day')['event_id'].count().reset_index()
trend.columns = ['day', 'registrations']

fig, ax = plt.subplots(figsize=(12, 6))
trend.plot(x='day', y='registrations', kind='line', ax=ax, color='crimson')
ax.set_xlabel('Day')
ax.set_ylabel('Number of registrations')
plt.xticks(rotation=45, ha='right')
st.pyplot(fig)

# ============================================
# CHART 2: ACQUISITION CHANNEL RANKING
# ============================================
st.header('Acquisition channel ranking')

rename_map = {
    'Club Instagram': 'INSTAGRAM',
    'Club Facebook': 'FACEBOOK',
    'Club ホームページ': 'HOMEPAGE',
    'Club以外のSNS': 'OTHER SNS',
    'Club X': 'X',
    'スタッフからの紹介': 'INTRODUCED BY STAFF',
    'その他': 'OTHERS',
    '所属校・会社からの紹介': 'INTRODUCED BY INSTITUTION',
    '登壇者・出展者からの紹介': 'INTRODUCED BY SPEAKERS',
    '駅広告': 'TRAIN STATION ADS'
}

acquisition = pd.read_sql("""
    SELECT acquisition_channel, COUNT(event_id) as count
    FROM event_acquisition
    WHERE acquisition_channel IS NOT NULL
    GROUP BY acquisition_channel
    ORDER BY count DESC
""", conn)

acquisition['acquisition_channel'] = acquisition['acquisition_channel'].map(rename_map).fillna(acquisition['acquisition_channel'])
fig, ax = plt.subplots(figsize=(12, 6))
acquisition.plot(x='acquisition_channel', y='count', kind='bar', ax=ax, color='crimson')
ax.set_xlabel('Acquisition channel')
ax.set_ylabel('Number of registrations')
plt.xticks(rotation=45, ha='right')
st.pyplot(fig)

# ============================================
# CHART 3: ATTRIBUTE BREAKDOWN
# ============================================
st.header('Attribute breakdown')

rename_map = {
    '大学生大学院生': 'UNIVERSITY STUDENT',
    '社会人': 'WORKING PEOPLE',
    '高校生': 'HIGH SCHOOL STUDENT',
    '中学生以下': 'MIDDLE SCHOOL STUDENT AND YOUNGER',
}

attribute = pd.read_sql("""
    SELECT attribute, COUNT(event_id) as count
    FROM event_attribute
    WHERE attribute IS NOT NULL
    GROUP BY attribute
    ORDER BY count DESC
""", conn)


attribute['attribute'] = attribute['attribute'].map(rename_map).fillna(attribute['attribute'])
fig, ax = plt.subplots(figsize=(12, 6))
attribute.plot(x='attribute', y='count', kind='bar', ax=ax, color='crimson')
ax.set_xlabel('Attribute')
ax.set_ylabel('Number of registrations')
plt.xticks(rotation=45, ha='right')
st.pyplot(fig)

# ============================================
# CHART 4: CHANNEL BY ATTRIBUTE CROSS ANALYSIS
# ============================================

rename_map = {
    'Club Instagram': 'INSTAGRAM',
    'Club Facebook': 'FACEBOOK',
    'Club ホームページ': 'HOMEPAGE',
    'Club以外のSNS': 'OTHER SNS',
    'Club X': 'X',
    'スタッフからの紹介': 'INTRODUCED BY STAFF',
    'その他': 'OTHERS',
    '所属校・会社からの紹介': 'INTRODUCED BY INSTITUTION',
    '登壇者・出展者からの紹介': 'INTRODUCED BY SPEAKERS',
    '駅広告': 'TRAIN STATION ADS'
}

st.header('Channel Effectiveness by Audience Type')

df_acq_att = df_acq[~df_acq['attribute'].str.contains(',', na=False)]
att_channel = df_acq_att.groupby(['acquisition_channel', 'attribute'])['event_id'].count().reset_index()

att_channel['acquisition_channel'] = att_channel['acquisition_channel'].map(rename_map).fillna(att_channel['acquisition_channel'])
att_channel['attribute'] = att_channel['attribute'].map({
    '大学生大学院生': 'UNIVERSITY STUDENT',
    '社会人': 'WORKING PEOPLE',
    '高校生': 'HIGH SCHOOL STUDENT',
    '中学生以下': 'MIDDLE SCHOOL AND BELOW'
})

att_channel_top = att_channel.sort_values(by='event_id', ascending=False).head(20)
st.dataframe(att_channel_top[['acquisition_channel', 'attribute', 'event_id']].rename(columns={'event_id': 'count'}))

# ============================================
# CHART 5: ATTRIBUTE CHANNEL EFFECTIVENESS OVER TIME
# ============================================
rename_map = {
    'ClubInstagram': 'INSTAGRAM',
    'スタッフからの紹介': 'INTRODUCED BY STAFF',
    '登壇者・出展者からの紹介': 'SPEAKERS',
    'その他': 'OTHERS',
    '駅広告': 'TRAIN STATION ADS',
    'Club X' : 'X',
    '所属校・会社からの紹介' : 'INTRODUCED BY OWN INSTITUTION',
    'Club ホームページ' : 'HOMEPAGE',
    'Club Facebook' : 'FACEBOOK',
    'Club以外のSNS' : 'OTHER SNS'
}

df_time_acq = df_time.copy()
df_time_acq['acquisition_channel'] = df_time_acq['acquisition_channel'].str.split(r',|，')
df_time_acq = df_time_acq.explode('acquisition_channel')
df_time_acq['acquisition_channel'] = df_time_acq['acquisition_channel'].str.replace(' ', '', regex=False)
time_acq = df_time_acq.groupby(['day','acquisition_channel'])['event_id'].count().unstack().fillna(0)
time_acq.columns = ['FACEBOOK', 'INSTAGRAM', 'X', 'WEBSITE', 'OTHER SNS','OTHERS','INTRODUCED BY STAFF', 'INTRODUCED BY OWN INSTITUTION', 'INTRODUCED BY SPEAKERS AND EXHIBITORS','TRAIN STATION AD']
keep_channels = ['INTRODUCED BY STAFF', 'INTRODUCED BY OWN INSTITUTION', 'INTRODUCED BY SPEAKERS AND EXHIBITORS']
time_acq_filtered = time_acq[keep_channels]

st.header('Word of Mouth Channel Effectiveness Over Time')
fig, ax = plt.subplots(figsize=(14, 6))
time_acq_filtered.plot(kind='line', ax=ax)
ax.set_xlabel('Date')
ax.set_ylabel('Number of registrations')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
st.pyplot(fig)

social_channels = ['INSTAGRAM', 'FACEBOOK', 'X', 'OTHER SNS', 'WEBSITE']
time_social = time_acq[social_channels]

st.header('Social Media Channel Effectiveness Over Time')
fig, ax = plt.subplots(figsize=(14, 6))
time_social.plot(kind='line', ax=ax)
ax.set_xlabel('Date')
ax.set_ylabel('Registrations')
plt.xticks(rotation=45, ha='right')
plt.tight_layout()
st.pyplot(fig)
