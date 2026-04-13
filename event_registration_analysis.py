import pandas as pd
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
df.to_sql("event", conn, index=False, if_exists="replace")

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

# ============================================
# SECTION 3: ANALYSIS
# ============================================

# Q1: Acquisition channels
acquisition = pd.read_sql("""
    SELECT acquisition_channel, COUNT(event_id) as count
    FROM event_acquisition
    WHERE acquisition_channel IS NOT NULL
    GROUP BY acquisition_channel
    ORDER BY count DESC
""", conn)
print("Acquisition Channels:")
print(acquisition)

# Q2: Age breakdown
age = pd.read_sql("""
    SELECT age_group, COUNT(event_id) as count
    FROM event
    GROUP BY age_group
    ORDER BY count DESC
""", conn)
print("\nAge Breakdown:")
print(age)

# Q3: Attribute breakdown
attribute = pd.read_sql("""
    SELECT attribute, COUNT(event_id) as count
    FROM event_attribute
    WHERE attribute IS NOT NULL
    GROUP BY attribute
    ORDER BY count DESC
""", conn)
print("\nAttribute Breakdown:")
print(attribute)

# Q4: Expectations
expectations = pd.read_sql("""
    SELECT expectations, COUNT(event_id) as count
    FROM event_expectations
    WHERE expectations IS NOT NULL
    GROUP BY expectations
    ORDER BY count DESC
""", conn)
print("\nExpectations:")
print(expectations)

# ============================================
# SECTION 5: REGISTRATION TREND
# ============================================
df_time = load_data()
df_time['registered_at'] = pd.to_datetime(df_time['registered_at'], utc=True)
df_time['registered_at'] = df_time['registered_at'].dt.tz_localize(None)
df_time['day'] = df_time['registered_at'].dt.to_period('D').astype(str)
trend = df_time.groupby('day')['event_id'].count().reset_index()
trend.columns = ['day', 'registrations']
print(trend)

# ============================================
# SECTION 6: ATTRIBUTE AND ACQUISITION CHANNEL
# ============================================
df_acq_att = df_acq[~df_acq['attribute'].str.contains(',', na=False)]
att_channel = df_acq_att.groupby(['acquisition_channel','attribute'])['event_id'].count().reset_index()
att_channel.columns = ['channel', 'attribute','count']
print(att_channel.sort_values(by='count', ascending=False).head(15))

# ============================================
# SECTION 7: ACQUISITION CHANNEL OVER TIME
# ============================================
rename_map = {
    'Club Instagram': 'INSTAGRAM',
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
pd.set_option('display.max_columns', None)
print(time_acq.astype(int))

