# Snapchat Data Extraction & Analysis

A Python toolkit for extracting and analyzing Snapchat data exports. This project processes raw Snapchat data (JSON/HTML) from multiple users and generates comprehensive cohort-level analytics.

## Project Structure

```
SnapAnalysis_Extraction/
├── Snapchat_Data/           # Raw data directory (not tracked by git)
│   └── Extracted_Users/     # Extracted user directories
├── extracted_csvs/          # Processed CSV files
├── analysis_plots/          # Generated visualizations
├── extract_snapchat_data.py # Data extraction script
├── analyze_snapchat_data.py # Analysis script
├── analysis_report.md       # Generated analysis report
├── requirements.txt         # Python dependencies
└── README.md                # This file
```

## Quick Start

### 1. Setup Environment

```bash
# Create conda environment
conda create -n snap_env python=3.10 -y
conda activate snap_env

# Install dependencies
pip install -r requirements.txt
```

### 2. Prepare Data

Place your Snapchat data exports in `Snapchat_Data/Extracted_Users/`. Each user should have their own directory containing `html/` and optionally `json/` folders.

### 3. Extract Data

```bash
python extract_snapchat_data.py
```

This parses all user data and generates CSV files in `extracted_csvs/`:
- `chats.csv` - Chat message metadata
- `friends.csv` - Friend connections
- `memories.csv` - Saved memories/snaps
- `myai.csv` - My AI interactions
- `snap_history_log.csv` - Snap send/receive history
- `talk_history_*.csv` - Call logs

### 4. Run Analysis

```bash
python analyze_snapchat_data.py
```

This generates:
- `analysis_report.md` - Comprehensive cohort analysis
- `analysis_plots/` - Visualizations (trends, distributions, wordclouds)

## Output Files

### Extracted CSVs

| File | Description |
|------|-------------|
| `chats.csv` | Message metadata (sender, timestamp, type) |
| `friends.csv` | Friend list with timestamps |
| `memories.csv` | Saved snaps with location/media type |
| `myai.csv` | My AI conversation logs |
| `snap_history_log.csv` | Snap send/receive history |

### Analysis Visualizations

| Plot | Description |
|------|-------------|
| `global_yearly_trend.png` | Total messages per year |
| `average_user_yearly_trend.png` | Mean messages/user per year |
| `seasonality_trend.png` | Activity by month of year |
| `global_hourly_trend.png` | Activity by hour of day |
| `global_weekly_trend.png` | Activity by day of week |
| `msg_distribution.png` | Distribution of messages per user |
| `friend_distribution.png` | Distribution of friends per user |
| `conversation_wordcloud.png` | Common words in conversations |
| `myai_wordcloud.png` | Common words in My AI responses |

## Requirements

- Python 3.10+
- pandas, matplotlib, seaborn, wordcloud, beautifulsoup4, tqdm

## License

For educational/research purposes only.
