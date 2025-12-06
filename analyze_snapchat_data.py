"""
Snapchat Cohort Analysis Script
================================

This script analyzes extracted Snapchat data to generate cohort-level insights.
It processes CSV files from `extracted_csvs/` and produces:
- Per-user metrics (messages, friends, memories)
- Temporal trends (yearly, seasonal, hourly, weekly)
- NLP analysis (wordclouds for conversations and My AI)
- Visualizations saved to `analysis_plots/`
- Comprehensive report in `analysis_report.md`

Usage:
    python analyze_snapchat_data.py

"""

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from wordcloud import WordCloud
import os
import logging
import glob
import numpy as np

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# =============================================================================
# CONFIGURATION
# =============================================================================
DATA_DIR = "extracted_csvs"    # Input: extracted CSV files
OUTPUT_DIR = "analysis_plots"  # Output: generated visualizations
REPORT_FILE = "analysis_report.md"  # Output: analysis report

# Create output directory
os.makedirs(OUTPUT_DIR, exist_ok=True)

# =============================================================================
# DATA LOADING & CLEANING
# =============================================================================

def load_data():
    """
    Loads all necessary CSV files from the extracted data directory.
    
    Returns:
        Dictionary with keys: 'chats', 'myai', 'snap_history', 'friends', 
        'memories', 'calls' - each containing a pandas DataFrame.
    """
    data = {}
    files = {
        "chats": "chats.csv",
        "myai": "myai.csv",
        "snap_history": "snap_history_log.csv",
        "friends": "friends.csv",
        "memories": "memories.csv"
    }
    
    for key, filename in files.items():
        path = os.path.join(DATA_DIR, filename)
        if os.path.exists(path):
            try:
                df = pd.read_csv(path, low_memory=False)
                data[key] = df
                logging.info(f"Loaded {filename}: {len(df)} records")
            except Exception as e:
                logging.error(f"Error loading {filename}: {e}")
        else:
            logging.warning(f"File not found: {filename}")
            data[key] = pd.DataFrame()
    
    # Load talk history files for call duration
    talk_files = glob.glob(os.path.join(DATA_DIR, "talk_history_*.csv"))
    data['calls'] = pd.DataFrame()
    if talk_files:
        dfs = []
        for f in talk_files:
            try:
                df = pd.read_csv(f)
                dfs.append(df)
            except Exception as e:
                logging.error(f"Error loading {f}: {e}")
        if dfs:
            data['calls'] = pd.concat(dfs, ignore_index=True)
            logging.info(f"Loaded {len(talk_files)} talk history files: {len(data['calls'])} records")

    return data

# =============================================================================
# DATA CLEANING
# =============================================================================

def clean_data(data):
    """
    Cleans loaded data: converts timestamps to datetime, removes duplicates.
    
    Args:
        data: Dictionary of DataFrames from load_data()
    
    Returns:
        Cleaned dictionary of DataFrames
    """
    
    # Clean Chats - convert timestamps, drop duplicates
    if not data['chats'].empty:
        df = data['chats']
        if 'timestamp' in df.columns:
             df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        df = df.drop_duplicates()
        data['chats'] = df

    # Clean MyAI
    if not data['myai'].empty:
        df = data['myai']
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        data['myai'] = df
        
    # Clean Calls
    if not data['calls'].empty:
        df = data['calls']
        if 'Length (Sec)' in df.columns:
            df['Length (Sec)'] = pd.to_numeric(df['Length (Sec)'], errors='coerce').fillna(0)
        data['calls'] = df

    # Clean Friends
    if not data['friends'].empty:
        df = data['friends']
        if 'creation_timestamp' in df.columns:
            df['creation_timestamp'] = pd.to_datetime(df['creation_timestamp'], errors='coerce')
        data['friends'] = df

    # Clean Memories
    if not data['memories'].empty:
        df = data['memories']
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
        data['memories'] = df

    # Clean Snap History
    if not data['snap_history'].empty:
        df = data['snap_history']
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        data['snap_history'] = df
        
    return data

# =============================================================================
# PER-USER METRICS
# =============================================================================

def calculate_user_metrics(data):
    """
    Calculates metrics for each unique user in the dataset.
    
    Metrics computed:
    - msg_count: Total messages sent
    - active_days: Number of unique days with activity
    - days_range: Span between first and last activity
    - friend_count: Number of friends
    - memory_count: Number of saved memories
    - call_duration_hours: Total call time
    - myai_interactions: Number of My AI messages
    
    Args:
        data: Dictionary of cleaned DataFrames
    
    Returns:
        DataFrame with one row per user and computed metrics
    """
    user_metrics = {}
    
    # Get all unique user_ids from all datasets
    all_users = set()
    for key, df in data.items():
        if 'user_id' in df.columns:
            all_users.update(df['user_id'].dropna().unique())
            
    logging.info(f"Identified {len(all_users)} unique users.")
    
    for user_id in all_users:
        metrics = {'user_id': user_id}
        
        # Chats
        chats_df = data['chats'][data['chats']['user_id'] == user_id]
        metrics['msg_count'] = len(chats_df)
        if not chats_df.empty and 'timestamp' in chats_df.columns:
            metrics['active_days'] = chats_df['timestamp'].dt.date.nunique()
            min_date = chats_df['timestamp'].min()
            max_date = chats_df['timestamp'].max()
            metrics['days_range'] = (max_date - min_date).days if pd.notnull(min_date) and pd.notnull(max_date) else 0
        else:
            metrics['active_days'] = 0
            metrics['days_range'] = 0
            
        # Friends
        friends_df = data['friends'][data['friends']['user_id'] == user_id]
        metrics['friend_count'] = len(friends_df)
        
        # Memories
        memories_df = data['memories'][data['memories']['user_id'] == user_id]
        metrics['memory_count'] = len(memories_df)
        
        # Calls
        calls_df = data['calls'][data['calls']['user_id'] == user_id]
        metrics['call_duration_hours'] = calls_df['Length (Sec)'].sum() / 3600
        
        # My AI
        myai_df = data['myai'][data['myai']['user_id'] == user_id]
        metrics['myai_interactions'] = len(myai_df)
        
        user_metrics[user_id] = metrics
        
    return pd.DataFrame.from_dict(user_metrics, orient='index')

# =============================================================================
# COHORT ANALYSIS
# =============================================================================

def analyze_cohort(metrics_df, report_lines):
    """
    Analyzes the cohort of users and generates distribution visualizations.
    
    Produces:
    - Message distribution histogram
    - Friend distribution histogram
    - My AI adoption statistics
    
    Args:
        metrics_df: DataFrame from calculate_user_metrics()
        report_lines: List to append report markdown content
    """
    logging.info("Analyzing cohort...")
    report_lines.append("## Cohort Analysis (N={})\n".format(len(metrics_df)))
    
    # Message Distribution
    report_lines.append("### Messaging Activity\n")
    msg_stats = metrics_df['msg_count'].describe()
    report_lines.append(f"- **Average Messages/User**: {msg_stats['mean']:.1f}")
    report_lines.append(f"- **Median Messages/User**: {msg_stats['50%']:.1f}")
    report_lines.append(f"- **Max Messages**: {msg_stats['max']:.0f}")
    
    plt.figure(figsize=(10, 6))
    sns.histplot(metrics_df['msg_count'], bins=30, kde=True, color='skyblue')
    plt.title('Distribution of Messages per User')
    plt.xlabel('Number of Messages')
    plt.ylabel('Count of Users')
    plt.savefig(os.path.join(OUTPUT_DIR, "msg_distribution.png"))
    plt.close()
    report_lines.append("![Message Distribution](analysis_plots/msg_distribution.png)\n")
    
    # Friends Distribution
    report_lines.append("### Friend Networks\n")
    friend_stats = metrics_df['friend_count'].describe()
    report_lines.append(f"- **Average Friends/User**: {friend_stats['mean']:.1f}")
    report_lines.append(f"- **Median Friends/User**: {friend_stats['50%']:.1f}")
    
    plt.figure(figsize=(10, 6))
    sns.histplot(metrics_df['friend_count'], bins=30, kde=True, color='green')
    plt.title('Distribution of Friends per User')
    plt.xlabel('Number of Friends')
    plt.ylabel('Count of Users')
    plt.savefig(os.path.join(OUTPUT_DIR, "friend_distribution.png"))
    plt.close()
    report_lines.append("![Friend Distribution](analysis_plots/friend_distribution.png)\n")

    # My AI Adoption
    report_lines.append("### My AI Adoption\n")
    ai_users = metrics_df[metrics_df['myai_interactions'] > 0]
    adoption_rate = len(ai_users) / len(metrics_df) * 100
    report_lines.append(f"- **Adoption Rate**: {adoption_rate:.1f}% ({len(ai_users)}/{len(metrics_df)} users)")
    if not ai_users.empty:
        avg_interactions = ai_users['myai_interactions'].mean()
        report_lines.append(f"- **Avg Interactions (Adopters)**: {avg_interactions:.1f}")

    report_lines.append("\n")

import matplotlib.dates as mdates

# ... (imports remain the same)

def analyze_global_trends(data, report_lines):
    """Analyzes global trends across all users."""
    logging.info("Analyzing global trends...")
    report_lines.append("## Global Activity Trends\n")
    
    if data['chats'].empty or 'timestamp' not in data['chats'].columns:
        return

    df = data['chats'].copy()
    df = df.dropna(subset=['timestamp'])
    
    # Yearly Trend (Aggregate)
    df['Year'] = df['timestamp'].dt.year
    yearly_counts = df['Year'].value_counts().sort_index()
    
    plt.figure(figsize=(10, 6))
    plt.plot(yearly_counts.index, yearly_counts.values, marker='o', color='purple')
    plt.title('Aggregate Message Activity Over Time (Yearly)')
    plt.xlabel('Year')
    plt.ylabel('Total Messages')
    plt.grid(True)
    plt.xticks(yearly_counts.index, rotation=45)
    plt.savefig(os.path.join(OUTPUT_DIR, "global_yearly_trend.png"))
    plt.close()
    report_lines.append("![Global Yearly Trend](analysis_plots/global_yearly_trend.png)\n")

    # Average User Trend (Yearly)
    # Group by Year and User to get counts per user per year
    user_yearly = df.groupby(['Year', 'user_id']).size().reset_index(name='count')
    # Group by Year to get mean and std across users
    yearly_stats = user_yearly.groupby('Year')['count'].agg(['mean', 'std']).fillna(0)
    
    plt.figure(figsize=(10, 6))
    plt.plot(yearly_stats.index, yearly_stats['mean'], color='blue', label='Average User')
    plt.fill_between(yearly_stats.index, 
                     yearly_stats['mean'] - yearly_stats['std'], 
                     yearly_stats['mean'] + yearly_stats['std'], 
                     color='blue', alpha=0.2, label='Std Dev')
    plt.title('Average User Activity Over Time (Yearly)')
    plt.xlabel('Year')
    plt.ylabel('Messages per User')
    plt.xticks(yearly_stats.index, rotation=45)
    plt.legend()
    plt.grid(True)
    plt.savefig(os.path.join(OUTPUT_DIR, "average_user_yearly_trend.png"))
    plt.close()
    report_lines.append("![Average User Yearly Trend](analysis_plots/average_user_yearly_trend.png)\n")

    # Seasonality (Month of Year)
    df['Month'] = df['timestamp'].dt.month_name()
    months_order = ['January', 'February', 'March', 'April', 'May', 'June', 
                    'July', 'August', 'September', 'October', 'November', 'December']
    
    plt.figure(figsize=(10, 6))
    sns.countplot(x='Month', data=df, order=months_order, palette='coolwarm')
    plt.title('Seasonality: Activity by Month of Year')
    plt.xlabel('Month')
    plt.ylabel('Total Messages')
    plt.xticks(rotation=45)
    plt.savefig(os.path.join(OUTPUT_DIR, "seasonality_trend.png"))
    plt.close()
    report_lines.append("![Seasonality Trend](analysis_plots/seasonality_trend.png)\n")

    # Aggregate Hourly Trend
    df['Hour'] = df['timestamp'].dt.hour
    plt.figure(figsize=(10, 6))
    sns.histplot(df['Hour'], bins=24, kde=False, color='skyblue')
    plt.title('Aggregate Message Frequency by Hour (All Users)')
    plt.xlabel('Hour (0-23)')
    plt.ylabel('Total Messages')
    plt.savefig(os.path.join(OUTPUT_DIR, "global_hourly_trend.png"))
    plt.close()
    report_lines.append("![Global Hourly Trend](analysis_plots/global_hourly_trend.png)\n")
    
    # Aggregate Weekly Trend
    df['DayOfWeek'] = df['timestamp'].dt.day_name()
    days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    plt.figure(figsize=(10, 6))
    sns.countplot(x='DayOfWeek', data=df, order=days_order, palette='viridis')
    plt.title('Aggregate Message Frequency by Day (All Users)')
    plt.xlabel('Day of Week')
    plt.ylabel('Total Messages')
    plt.savefig(os.path.join(OUTPUT_DIR, "global_weekly_trend.png"))
    plt.close()
    report_lines.append("![Global Weekly Trend](analysis_plots/global_weekly_trend.png)\n")

    report_lines.append("\n")

def analyze_memories(data, report_lines):
    """Analyzes memories data."""
    logging.info("Analyzing memories...")
    report_lines.append("## Memories Analysis\n")
    
    if data['memories'].empty:
        report_lines.append("No memories data available.\n")
        return
        
    df = data['memories']
    report_lines.append(f"- **Total Memories**: {len(df):,}")
    
    if 'media_type' in df.columns:
        media_counts = df['media_type'].value_counts()
        report_lines.append(f"- **Media Types**:\n{media_counts.to_markdown()}")
        
    if 'date' in df.columns:
        # Yearly Trend
        df['Year'] = df['date'].dt.year
        yearly_mems = df['Year'].value_counts().sort_index()
        
        plt.figure(figsize=(10, 6))
        plt.plot(yearly_mems.index, yearly_mems.values, marker='o', color='orange')
        plt.title('Memories Saved Over Time (Yearly)')
        plt.xlabel('Year')
        plt.ylabel('Memories Saved')
        plt.grid(True)
        plt.xticks(yearly_mems.index, rotation=45)
        plt.savefig(os.path.join(OUTPUT_DIR, "memories_yearly_trend.png"))
        plt.close()
        report_lines.append("![Memories Yearly Trend](analysis_plots/memories_yearly_trend.png)\n")
    
    report_lines.append("\n")

def analyze_nlp(data, report_lines):
    """Analyzes text content of conversations."""
    logging.info("Analyzing conversation content...")
    report_lines.append("## Conversation Content Analysis\n")
    
    if data['chats'].empty or 'content' not in data['chats'].columns:
        report_lines.append("No content data available for analysis.\n")
        return

    df = data['chats'].dropna(subset=['content'])
    report_lines.append(f"- **Messages with Content**: {len(df):,} (subset of total)\n")
    
    text_data = " ".join(df['content'].astype(str))
    if text_data.strip():
        wordcloud = WordCloud(width=800, height=400, background_color='white', max_words=200).generate(text_data)
        plt.figure(figsize=(10, 5))
        plt.imshow(wordcloud, interpolation='bilinear')
        plt.axis('off')
        plt.title('Common Words in User Conversations')
        plt.savefig(os.path.join(OUTPUT_DIR, "conversation_wordcloud.png"))
        plt.close()
        report_lines.append("![Conversation Wordcloud](analysis_plots/conversation_wordcloud.png)\n")
    else:
        report_lines.append("Not enough text data for wordcloud.\n")
    
    report_lines.append("\n")

def analyze_myai(data, report_lines):
    """Analyzes My AI interactions."""
    logging.info("Analyzing My AI...")
    report_lines.append("## My AI Engagement\n")
    
    if data['myai'].empty:
        report_lines.append("No My AI data available.\n")
        return

    df = data['myai']
    
    # Attribution: IP Address present = User, Missing = AI
    df['Sender_Type'] = df['ip_address'].apply(lambda x: 'User' if pd.notna(x) and str(x).strip() != '' else 'My AI')
    
    # Metrics
    total_interactions = len(df)
    user_msgs = df[df['Sender_Type'] == 'User']
    ai_msgs = df[df['Sender_Type'] == 'My AI']
    
    report_lines.append(f"- **Total Interactions**: {total_interactions:,}")
    report_lines.append(f"- **User Messages**: {len(user_msgs):,} ({len(user_msgs)/total_interactions*100:.1f}%)")
    report_lines.append(f"- **My AI Responses**: {len(ai_msgs):,} ({len(ai_msgs)/total_interactions*100:.1f}%)")
    
    # Avg Response Length (Word count)
    if 'content' in df.columns:
        ai_msgs['word_count'] = ai_msgs['content'].astype(str).apply(lambda x: len(x.split()))
        avg_len = ai_msgs['word_count'].mean()
        report_lines.append(f"- **Avg AI Response Length**: {avg_len:.1f} words")

    # Plot split
    sender_counts = df['Sender_Type'].value_counts()
    plt.figure(figsize=(6, 6))
    sender_counts.plot(kind='pie', autopct='%1.1f%%', startangle=90, colors=['lightskyblue', 'lightcoral'])
    plt.title('My AI vs User Interactions')
    plt.ylabel('')
    plt.savefig(os.path.join(OUTPUT_DIR, "myai_split.png"))
    plt.close()
    report_lines.append("![My AI Split](analysis_plots/myai_split.png)\n")

    # Word Cloud (AI Content)
    if 'content' in df.columns:
        text_data = " ".join(ai_msgs['content'].dropna().astype(str))
        if text_data.strip():
            wordcloud = WordCloud(width=800, height=400, background_color='white').generate(text_data)
            plt.figure(figsize=(10, 5))
            plt.imshow(wordcloud, interpolation='bilinear')
            plt.axis('off')
            plt.title('Common Words in My AI Responses')
            plt.savefig(os.path.join(OUTPUT_DIR, "myai_wordcloud.png"))
            plt.close()
            report_lines.append("![My AI Wordcloud](analysis_plots/myai_wordcloud.png)\n")
    
    report_lines.append("\n")

def generate_report(report_lines):
    """Writes the report to a file."""
    with open(REPORT_FILE, "w") as f:
        f.writelines([line + "\n" for line in report_lines])
    logging.info(f"Report generated: {REPORT_FILE}")

def main():
    logging.info("Starting cohort analysis...")
    data = load_data()
    data = clean_data(data)
    
    metrics_df = calculate_user_metrics(data)
    
    report_lines = ["# Snapchat Cohort Analysis Report\n"]
    
    analyze_cohort(metrics_df, report_lines)
    analyze_global_trends(data, report_lines)
    analyze_memories(data, report_lines)
    analyze_nlp(data, report_lines)
    analyze_myai(data, report_lines)
    
    # Data Limitations & Assumptions
    report_lines.append("## Data Limitations & Assumptions\n")
    report_lines.append("- **Cohort Definition**: Analysis is based on 55 extracted user profiles.\n")
    report_lines.append("- **Aggregation**: Global trends represent the sum of activity across all users.\n")
    report_lines.append("- **Content Analysis**: Wordcloud is based on the subset of messages (~52k) that contain text content.\n")
    report_lines.append("- **My AI Attribution**: Distinguished User vs AI based on IP address presence heuristic.\n")

    generate_report(report_lines)
    logging.info("Analysis complete.")

if __name__ == "__main__":
    main()
