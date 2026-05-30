# ============================================================
# pipeline.py
# Runs anomaly detection + business analysis in one command
# Usage: python pipeline.py
# ============================================================
 
import psycopg2
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use('Agg')  
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import LabelEncoder
from sqlalchemy import create_engine
import warnings
import time
warnings.filterwarnings('ignore')


# ── DB config ───────────────────────────────────────────────
DB = dict(dbname="postgres", user="postgres",
          password="postgres123", host="localhost", port=5433)
ENGINE_URL = 'postgresql+psycopg2://postgres:postgres123@localhost:5433/postgres'
 
def get_conn():
    return psycopg2.connect(**DB)
 
def get_engine():
    return create_engine(ENGINE_URL)
 

# ============================================================
# STEP 1 — ANOMALY DETECTION
# reads raw_data → trains IsolationForest → saves anomaly_data
# ============================================================
def run_anomaly_detection():
    print("\n" + "="*55)
    print("  STEP 1 — ANOMALY DETECTION")
    print("="*55)
 
    conn   = get_conn()
    engine = get_engine()
 
    # ── Load raw data ────────────────────────────────────────
    print("\n[1/5] Loading raw_data from PostgreSQL...")
    df_raw = pd.read_sql("SELECT * FROM raw_data", conn)
    print(f"      Loaded {len(df_raw):,} rows")
 
    # ── Prepare features ─────────────────────────────────────
    print("[2/5] Preparing features for ML...")
    df_ml = df_raw.copy()
 
    df_ml['price_clean'] = pd.to_numeric(
        df_ml['price'].astype(str).str.replace(r'[^\d.-]', '', regex=True),
        errors='coerce'
    ).fillna(0)
 
    df_ml['quantity_clean'] = pd.to_numeric(
        df_ml['quantity'], errors='coerce'
    ).fillna(0)
 
    df_ml['revenue_clean'] = df_ml['price_clean'] * df_ml['quantity_clean']
 
    df_ml['category_clean']   = df_ml['category'].fillna('Unknown')
    df_ml['city_clean']       = df_ml['city'].fillna('Unknown')
    df_ml['event_type_clean'] = df_ml['event_type'].fillna('Unknown')
    df_ml['payment_clean']    = df_ml['payment_method'].fillna('Unknown')
 
    le_category   = LabelEncoder()
    le_city       = LabelEncoder()
    le_event_type = LabelEncoder()
    le_payment    = LabelEncoder()
 
    df_ml['category_encoded']   = le_category.fit_transform(df_ml['category_clean'])
    df_ml['city_encoded']       = le_city.fit_transform(df_ml['city_clean'])
    df_ml['event_type_encoded'] = le_event_type.fit_transform(df_ml['event_type_clean'])
    df_ml['payment_encoded']    = le_payment.fit_transform(df_ml['payment_clean'])
 
    features = df_ml[[
        'price_clean', 'quantity_clean', 'revenue_clean',
        'category_encoded', 'city_encoded',
        'event_type_encoded', 'payment_encoded'
    ]]
 
    # ── Train IsolationForest ────────────────────────────────
    print("[3/5] Training IsolationForest model...")
    model = IsolationForest(contamination=0.10, random_state=42,
                            n_estimators=100, max_samples='auto')
    df_ml['anomaly_score'] = model.fit_predict(features)
    df_ml['anomaly_label'] = df_ml['anomaly_score'].map({1:'Normal', -1:'Anomaly'})
 
    total_anomalies = (df_ml['anomaly_label'] == 'Anomaly').sum()
    total_normal    = (df_ml['anomaly_label'] == 'Normal').sum()
    actual_rate = total_anomalies / len(df_ml) * 100

    print(f"      Normal:    {total_normal:,} ({total_normal/len(df_ml)*100:.1f}%)")
    print(f"      Anomalies: {total_anomalies:,} ({total_anomalies/len(df_ml)*100:.1f}%)")

    # ── ALERT BLOCK ──────────────────────────────────────
    ALERT_THRESHOLD = 5.0

    if actual_rate > ALERT_THRESHOLD:
        print(f"\n{'!'*50}")
        print(f"  ALERT: Anomaly rate {actual_rate:.1f}% exceeds {ALERT_THRESHOLD}%")
        print(f"{'!'*50}\n")
        alert_df = pd.DataFrame({
            'alert_time':    [pd.Timestamp.now()],
            'anomaly_rate':  [round(actual_rate, 2)],
            'threshold':     [ALERT_THRESHOLD],
            'total_rows':    [len(df_ml)],
            'anomaly_count': [total_anomalies],
            'message':       [f"High anomaly rate: {actual_rate:.1f}%"]
        })
        alert_df.to_sql('anomaly_alerts', engine,
                        if_exists='append', index=False)
        print("  Alert saved to anomaly_alerts table ✓")
    else:
        print(f"  Anomaly rate {actual_rate:.1f}% is within normal range ✓")
    # ── END ALERT BLOCK ──────────────────────────────────
 
    # ── Assign anomaly reasons ───────────────────────────────
    df_ml['anomaly_reason'] = 'Normal'
    a = df_ml['anomaly_label'] == 'Anomaly'
 
    df_ml.loc[a & (df_ml['price_clean'] < 0),    'anomaly_reason'] = 'Negative Price'
    df_ml.loc[a & (df_ml['price_clean'] == 0),   'anomaly_reason'] = 'Zero Price'
    df_ml.loc[a & (df_ml['price_clean'] > 80000),'anomaly_reason'] = 'Price Outlier (Currency Mismatch)'
    df_ml.loc[a & (df_ml['quantity_clean'] == 0),'anomaly_reason'] = 'Zero Quantity (Bot Traffic)'
    df_ml.loc[a & (df_ml['quantity_clean'] > 3), 'anomaly_reason'] = 'Quantity Outlier (Data Entry Error)'
    df_ml.loc[a & (~df_ml['city_clean'].isin(['Delhi','Mumbai','Kolkata','Bangalore'])),
              'anomaly_reason'] = 'Unknown City (Corrupt/Missing)'
    df_ml.loc[a & (~df_ml['event_type_clean'].isin(['view','add_to_cart','purchase'])),
              'anomaly_reason'] = 'Invalid Event Type (Schema Drift)'
    df_ml.loc[a & (df_ml['payment_clean'] == 'Unknown'),
              'anomaly_reason'] = 'Missing Payment Method'
    df_ml.loc[a & (df_ml['anomaly_reason'] == 'Normal') &
              (df_ml['revenue_clean'] > df_ml['revenue_clean'].mean() + 2*df_ml['revenue_clean'].std()),
              'anomaly_reason'] = 'Unusually High Revenue'
    df_ml.loc[a & (df_ml['anomaly_reason'] == 'Normal'),
              'anomaly_reason'] = 'ML Pattern Anomaly'
 
    # ── Save charts ──────────────────────────────────────────
    print("[4/5] Saving anomaly charts...")
    _save_anomaly_charts(df_ml, model, features)
 
    # ── Save to PostgreSQL ───────────────────────────────────
    print("[5/5] Saving anomaly_data to PostgreSQL...")
    save_df = df_ml[[
        'order_id', 'user_id', 'product_id', 'product_name',
        'category_clean', 'price', 'price_clean',
        'quantity', 'quantity_clean', 'revenue_clean',
        'event_type_clean', 'timestamp', 'payment_clean',
        'city_clean', 'anomaly_label', 'anomaly_reason', 'anomaly_score'
    ]].rename(columns={
        'category_clean':   'category',
        'price':            'price_raw',
        'quantity':         'quantity_raw',
        'revenue_clean':    'revenue',
        'event_type_clean': 'event_type',
        'payment_clean':    'payment_method',
        'city_clean':       'city',
    })
    save_df.to_sql('anomaly_data', engine, if_exists='replace', index=False)
    print(f"      Saved {len(save_df):,} rows to anomaly_data table")
 
    conn.close()
    print("\n  ANOMALY DETECTION COMPLETE")
    return total_anomalies, total_normal
 
 
def _save_anomaly_charts(df_ml, model, features):
    """Save all 7 anomaly charts to anomaly_report.png"""
    # sample for memory efficiency on large datasets
    # ML already ran on full data — charts only need a representative sample
    df_ml = df_ml.sample(min(20000, len(df_ml)), random_state=42)

    title_color = 'white'
    label_color = '#B0C4DE'
    bg_color    = '#1B2A4A'
 
    fig = plt.figure(figsize=(12, 8))
    fig.patch.set_facecolor('#0D1B2A')
    gs  = gridspec.GridSpec(3, 3, figure=fig, hspace=0.5, wspace=0.4)
    fig.suptitle('Anomaly Detection Report — Raw E-Commerce Data\nIsolationForest (Unsupervised ML)',
                 fontsize=18, fontweight='bold', color=title_color, y=1.02)
 
    ax1 = fig.add_subplot(gs[0, 0])
    ax1.set_facecolor(bg_color)
    anomaly_counts = df_ml['anomaly_label'].value_counts()
    ax1.pie(anomaly_counts.values, labels=anomaly_counts.index, autopct='%1.1f%%',
            colors=['#2ecc71','#e74c3c'], textprops={'color':title_color,'fontsize':11},
            startangle=90, wedgeprops={'edgecolor':bg_color,'linewidth':2})
    ax1.set_title('Anomaly vs Normal', color=title_color, fontsize=13, pad=15)
 
    ax2 = fig.add_subplot(gs[0, 1:])
    ax2.set_facecolor(bg_color)
    reason_counts = df_ml[df_ml['anomaly_label']=='Anomaly']['anomaly_reason'].value_counts()
    bars = ax2.barh(reason_counts.index, reason_counts.values,
                    color='#e74c3c', edgecolor='#c0392b', linewidth=0.5)
    ax2.set_title('Anomaly Reasons', color=title_color, fontsize=13, pad=15)
    ax2.set_xlabel('Count', color=label_color, fontsize=10)
    ax2.tick_params(colors=label_color, labelsize=9)
    ax2.spines[:].set_color('#2C3E50')
    for bar in bars:
        ax2.text(bar.get_width() + max(reason_counts.values)*0.01,
                 bar.get_y() + bar.get_height()/2,
                 f'{int(bar.get_width()):,}', va='center', color=title_color, fontsize=9)
 
    ax3 = fig.add_subplot(gs[1, 0])
    ax3.set_facecolor(bg_color)
    anomaly_by_cat = df_ml[df_ml['anomaly_label']=='Anomaly']['category_clean'].value_counts()
    ax3.bar(anomaly_by_cat.index, anomaly_by_cat.values,
            color=['#e74c3c','#c0392b','#a93226','#922b21','#7b241c'][:len(anomaly_by_cat)])
    ax3.set_title('Anomalies by Category', color=title_color, fontsize=13, pad=15)
    ax3.tick_params(colors=label_color, rotation=30, labelsize=9)
    ax3.spines[:].set_color('#2C3E50')
 
    ax4 = fig.add_subplot(gs[1, 1])
    ax4.set_facecolor(bg_color)
    anomaly_by_city = df_ml[df_ml['anomaly_label']=='Anomaly']['city_clean'].value_counts()
    ax4.bar(anomaly_by_city.index, anomaly_by_city.values, color='#f39c12')
    ax4.set_title('Anomalies by City', color=title_color, fontsize=13, pad=15)
    ax4.tick_params(colors=label_color, rotation=30, labelsize=9)
    ax4.spines[:].set_color('#2C3E50')
 
    ax5 = fig.add_subplot(gs[1, 2])
    ax5.set_facecolor(bg_color)

    # sample to avoid memory error on large datasets
    normal_df  = df_ml[df_ml['anomaly_label']=='Normal'].sample(
                    min(5000, len(df_ml[df_ml['anomaly_label']=='Normal'])),
                    random_state=42)
    anomaly_df = df_ml[df_ml['anomaly_label']=='Anomaly'].sample(
                    min(5000, len(df_ml[df_ml['anomaly_label']=='Anomaly'])),
                    random_state=42)

    normal_prices  = normal_df['price_clean'].clip(0, 100000)
    anomaly_prices = anomaly_df['price_clean'].clip(0, 100000)
    ax5.hist(normal_prices,  bins=50, alpha=0.6, color='#2ecc71', label='Normal',  density=True)
    ax5.hist(anomaly_prices, bins=50, alpha=0.6, color='#e74c3c', label='Anomaly', density=True)
    ax5.set_title('Price Distribution', color=title_color, fontsize=13, pad=15)
    ax5.tick_params(colors=label_color, labelsize=9)
    ax5.spines[:].set_color('#2C3E50')
    ax5.legend(facecolor=bg_color, labelcolor=title_color)
 
    ax6 = fig.add_subplot(gs[2, 0])
    ax6.set_facecolor(bg_color)
    anomaly_by_event = df_ml[df_ml['anomaly_label']=='Anomaly']['event_type_clean'].value_counts()
    ax6.bar(anomaly_by_event.index, anomaly_by_event.values, color='#9b59b6')
    ax6.set_title('Anomalies by Event Type', color=title_color, fontsize=13, pad=15)
    ax6.tick_params(colors=label_color, rotation=30, labelsize=9)
    ax6.spines[:].set_color('#2C3E50')
 
    ax7 = fig.add_subplot(gs[2, 1:])
    ax7.set_facecolor(bg_color)
    sample = df_ml.sample(min(5000, len(df_ml)), random_state=42)
    normal_s  = sample[sample['anomaly_label'] == 'Normal']
    anomaly_s = sample[sample['anomaly_label'] == 'Anomaly']
    ax7.scatter(normal_s['price_clean'],  normal_s['revenue_clean'],
                c='#2ecc71', alpha=0.3, s=3,  label=f"Normal ({len(normal_s):,})")
    ax7.scatter(anomaly_s['price_clean'], anomaly_s['revenue_clean'],
                c='#e74c3c', alpha=0.9, s=15, label=f"Anomaly ({len(anomaly_s):,})", zorder=5)
    ax7.set_xlim(-10000, 85000)
    ax7.set_ylim(-5000,  300000)
    ax7.set_title('Price vs Revenue\n(Green=Normal, Red=Anomaly)',
                  color=title_color, fontsize=13, pad=15)
    ax7.tick_params(colors=label_color, labelsize=9)
    ax7.spines[:].set_color('#2C3E50')
    ax7.legend(facecolor=bg_color, labelcolor=title_color, fontsize=8)
 
    plt.savefig('anomaly_report.png', dpi=80, bbox_inches='tight', facecolor='#0D1B2A')
    plt.close()
    print("Charts saved to anomaly_report.png")


# ============================================================
# STEP 2 — BUSINESS ANALYSIS
# reads cleaned_data → computes insights → saves business_insights
# ============================================================
def run_analysis():
    print("\n" + "="*55)
    print("  STEP 2 — BUSINESS ANALYSIS")
    print("="*55)
 
    conn   = get_conn()
    engine = get_engine()
 
    # ── Load cleaned data ────────────────────────────────────
    print("\n[1/4] Loading cleaned_data from PostgreSQL...")
    df = pd.read_sql("SELECT * FROM cleaned_data", conn)
    print(f"      Loaded {len(df):,} rows")
 
    # ── Compute insights ─────────────────────────────────────
    print("[2/4] Computing business insights...")
 
    # Top category
    category_revenue = df.groupby('category')['revenue'].sum().sort_values(ascending=False)
    top_category = category_revenue.idxmax()
 
    # Peak hour
    df['hour'] = pd.to_datetime(df['event_timestamp'], errors='coerce').dt.hour
    hour_revenue = df.groupby('hour')['revenue'].sum()
    peak_hour = hour_revenue.idxmax()
 
    # Conversion rate
    total_views     = len(df[df['event_type'] == 'view'])
    total_cart      = len(df[df['event_type'] == 'add_to_cart'])
    total_purchases = len(df[df['event_type'] == 'purchase'])
    conversion = round(total_purchases / total_views * 100, 2) if total_views > 0 else 0
 
    # Top city
    city_revenue = df.groupby('city')['revenue'].sum()
    top_city = city_revenue.idxmax()
 
    print(f"      Top Category:    {top_category}")
    print(f"      Peak Hour:       {peak_hour}:00")
    print(f"      Conversion Rate: {conversion}%")
    print(f"      Top City:        {top_city}")
 
    # ── Save charts ──────────────────────────────────────────
    print("[3/4] Saving analysis charts...")
    _save_analysis_charts(df, category_revenue, hour_revenue,
                          total_views, total_cart, total_purchases,
                          city_revenue)
 
    # ── Save insights to PostgreSQL ──────────────────────────
    print("[4/4] Saving business_insights to PostgreSQL...")
    insights = pd.DataFrame({
        'metric': ['Top Category', 'Peak Hour', 'Conversion Rate',
                   'Top City', 'Total Orders', 'Total Revenue'],
        'value':  [top_category,
                   str(peak_hour) + ':00',
                   str(conversion) + '%',
                   top_city,
                   str(len(df[df['event_type'] == 'purchase'])),
                   str(df['revenue'].sum())]
    })
    insights.to_sql('business_insights', engine, if_exists='replace', index=False)
    print(f"      Saved {len(insights)} insights to business_insights table")
 
    conn.close()
    print("\n  BUSINESS ANALYSIS COMPLETE")
    return insights
 
 
def _save_analysis_charts(df, category_revenue, hour_revenue,
                           total_views, total_cart, total_purchases,
                           city_revenue):
    """Save all analysis charts to analysis_report.png"""
    fig, axes = plt.subplots(2, 3, figsize=(14, 8))
    fig.patch.set_facecolor('white')
    fig.suptitle('Business Insights Report', fontsize=16, fontweight='bold')
 
    # Category revenue
    axes[0,0].bar(category_revenue.index, category_revenue.values,
                  color=['#e74c3c','#3498db','#2ecc71','#f39c12','#9b59b6'])
    axes[0,0].set_title('Revenue by Category')
    axes[0,0].tick_params(rotation=30)
 
    # Peak hour
    axes[0,1].plot(hour_revenue.index, hour_revenue.values,
                   marker='o', color='#3498db', linewidth=2)
    axes[0,1].set_title('Revenue by Hour')
    axes[0,1].set_xlabel('Hour (0-23)')
    axes[0,1].grid(True, alpha=0.3)
 
    # Funnel
    stages = ['Views', 'Add to Cart', 'Purchase']
    counts = [total_views, total_cart, total_purchases]
    bars = axes[0,2].bar(stages, counts,
                          color=['#3498db','#f39c12','#2ecc71'], width=0.5)
    axes[0,2].set_title('Customer Journey Funnel')
    for bar, count in zip(bars, counts):
        axes[0,2].text(bar.get_x() + bar.get_width()/2,
                       bar.get_height() + max(counts)*0.01,
                       f'{count:,}', ha='center', fontweight='bold', fontsize=9)
 
    # City revenue
    city_sorted = city_revenue.sort_values()
    axes[1,0].barh(city_sorted.index, city_sorted.values, color='#9b59b6')
    axes[1,0].set_title('Revenue by City')
 
    # Payment method
    payment_counts = df['payment_method'].value_counts()
    axes[1,1].pie(payment_counts.values, labels=payment_counts.index,
                  autopct='%1.1f%%', colors=['#3498db','#e74c3c','#2ecc71'])
    axes[1,1].set_title('Payment Method Distribution')
 
    # Hide empty subplot
    axes[1,2].axis('off')
 
    plt.tight_layout()
    plt.savefig('analysis_report.png', dpi=80, bbox_inches='tight', facecolor='white')
    plt.close()
    print("      Charts saved to analysis_report.png")
 
 
# ============================================================
# MAIN — run both steps in sequence
# ============================================================
if __name__ == '__main__':
    start = time.time()
 
    print("\n" + "="*55)
    print("  PIPELINE STARTED")
    print("="*55)
    print("  Steps: Anomaly Detection → Business Analysis")
 
    try:
        # Step 1
        total_anomalies, total_normal = run_anomaly_detection()
 
        # Step 2
        insights = run_analysis()
 
        # Final summary
        elapsed = round(time.time() - start, 1)
        print("\n" + "="*55)
        print("  PIPELINE COMPLETE")
        print("="*55)
        print(f"""
  Time taken       : {elapsed} seconds
  Anomalies found  : {total_anomalies:,}
  Normal rows      : {total_normal:,}
 
  Tables updated:
    anomaly_data       ← IsolationForest results
    business_insights  ← KPI metrics
 
  Charts saved:
    anomaly_report.png
    analysis_report.png
        """)
 
    except Exception as e:
        print(f"\n  PIPELINE FAILED: {e}")
        raise
 
 