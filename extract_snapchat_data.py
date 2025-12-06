"""
Snapchat Data Extraction Script
================================

This script extracts and normalizes data from Snapchat data exports.
It processes both JSON and HTML formats, handling:
- Chat history (messages, media types)
- My AI conversations
- Friends lists
- Memories (saved snaps)
- Snap history logs
- Call logs (talk history)

The output is a set of CSV files in the `extracted_csvs/` directory,
ready for analysis.

Usage:
    python extract_snapchat_data.py
"""

import os
import zipfile
import json
import shutil
import glob
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import logging
import re
from tqdm import tqdm

# =============================================================================
# CONFIGURATION
# =============================================================================
# Paths to raw data and extraction output
DATA_DIR = "/Volumes/X9_Pro/Grad_AI_Consultant/SnapAnalysis_Extraction/Snapchat_Data/Snapchat donated data"
EXTRACTED_DIR = "/Volumes/X9_Pro/Grad_AI_Consultant/SnapAnalysis_Extraction/Snapchat_Data/Extracted_Users"
INPUT_DIR = "Snapchat_Data"
OUTPUT_DIR = "extracted_csvs"  # Where processed CSVs are saved
LOG_FILE = "extraction_output.log"
ERROR_LOG_FILE = "extraction_errors.log"

# Setup Logging - Errors are logged to file for debugging
logging.basicConfig(filename=LOG_FILE, level=logging.ERROR, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

# =============================================================================
# DATA CONTAINERS
# =============================================================================
# These lists accumulate data from all users before being exported to CSV
all_chats = []           # Chat messages
all_friends = []         # Friend connections
all_memories = []        # Saved memories/snaps
all_myai = []            # My AI conversations
all_snap_history = []    # Snap send/receive logs
all_generic_data = {}    # Catch-all for other HTML tables (Key: table_name, Value: list of rows)
verification_stats = []  # Tracks input vs output counts for data validation

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def setup_directories():
    """Creates the extraction output directory if it doesn't exist."""
    if not os.path.exists(EXTRACTED_DIR):
        os.makedirs(EXTRACTED_DIR)

def extract_zip(zip_path, user_id):
    """
    Extracts a Snapchat data zip file to a user-specific directory.
    
    Args:
        zip_path: Path to the zip file
        user_id: Unique identifier for the user
    
    Returns:
        Path to the extracted directory, or None on failure
    """
    user_dir = os.path.join(EXTRACTED_DIR, user_id)
    if not os.path.exists(user_dir):
        try:
            with zipfile.ZipFile(zip_path, 'r') as z:
                z.extractall(user_dir)
        except zipfile.BadZipFile:
            logging.error(f"Bad zip file for user {user_id}")
            return None
    return user_dir

# =============================================================================
# PARSING FUNCTIONS - JSON
# =============================================================================

def parse_json_chat(json_path, user_id):
    """
    Parses chat history from a JSON file.
    
    JSON structure expected:
    {
        "conversation_title": [
            {"From": "user", "Created": "timestamp", "Content": "text", "Media Type": "TEXT"}
        ]
    }
    
    Args:
        json_path: Path to the JSON file
        user_id: User identifier to tag each record
    
    Returns:
        Tuple of (list of chat dicts, input message count)
    """
    chats = []
    input_count = 0
    try:
        with open(json_path, 'r') as f:
            data = json.load(f)
            
        # Handle dictionary structure: keys are conversation titles
        if isinstance(data, dict):
            for title, messages in data.items():
                if isinstance(messages, list):
                    input_count += len(messages)
                    for msg in messages:
                        chats.append({
                            'user_id': user_id,
                            'conversation_title': title,
                            'sender': msg.get('From'),
                            'recipient': None,  # Not available in JSON format
                            'timestamp': msg.get('Created'),
                            'content': msg.get('Content'),
                            'media_type': msg.get('Media Type')
                        })
    except Exception as e:
        logging.error(f"Error parsing JSON chat for {user_id}: {e}")
    return chats, input_count

def parse_html_chat(html_path, user_id):
    chats = []
    input_count = 0
    
    base_dir = os.path.dirname(html_path)
    chat_history_dir = os.path.join(base_dir, "chat_history")
    
    if os.path.exists(chat_history_dir):
        subpages = glob.glob(os.path.join(chat_history_dir, "subpage_*.html"))
        
        for subpage in subpages:
            try:
                with open(subpage, 'r') as f:
                    soup = BeautifulSoup(f, 'html.parser')
                
                # Extract Conversation Title from filename or header
                # Filename format: subpage_username.html
                filename = os.path.basename(subpage)
                conversation_title = filename.replace("subpage_", "").replace(".html", "")
                
                # Check for header in file
                header = soup.find('h1') or soup.find('h2') or soup.find('title')
                if header:
                    # Clean up header text if needed
                    pass

                # Extract messages
                # Heuristic: Look for rows or blocks with timestamps
                # Based on audit: <tr><td>Timestamp</td><td>IP</td><td>Type</td><td>Content</td></tr> for some
                # OR div based structure for others.
                
                # Strategy 1: Table based (common in older exports or specific sections)
                rows = soup.find_all('tr')
                if len(rows) > 1:
                    # Check headers
                    headers = [th.get_text(strip=True) for th in rows[0].find_all('th')]
                    if "Content" in headers and "Type" in headers:
                        # It's a table!
                        input_count += len(rows) - 1
                        for row in rows[1:]:
                            cols = row.find_all('td')
                            if len(cols) >= 4:
                                # Assuming order: Timestamp, IP, Type, Content (based on MyAI audit, might vary for chats)
                                pass
                
                # Strategy 2: Div based (seen in audit for chats)
                msg_blocks = soup.find_all('div', style=lambda s: s and 'background: #f2f2f2' in s)
                input_count += len(msg_blocks)
                
                for block in msg_blocks:
                    sender_tag = block.find('h4')
                    sender = sender_tag.get_text(strip=True) if sender_tag else None
                    
                    time_tag = block.find('h6')
                    timestamp = time_tag.get_text(strip=True) if time_tag else None
                    
                    # Content/Type
                    # Type is often an icon or text.
                    # Content might be missing if not saved.
                    
                    # Check for "TEXT", "SNAP", "CHAT" labels often next to icons
                    type_span = block.find('span', style=lambda s: s and 'position: absolute' in s)
                    media_type = type_span.get_text(strip=True) if type_span else "UNKNOWN"
                    
                    content = None
                    
                    chats.append({
                        'user_id': user_id,
                        'conversation_title': conversation_title,
                        'sender': sender,
                        'recipient': None,
                        'timestamp': timestamp,
                        'content': content, # Might need refinement
                        'media_type': media_type
                    })

            except Exception as e:
                logging.error(f"Error parsing HTML subpage {subpage} for {user_id}: {e}")

    return chats, input_count

def parse_myai(user_dir, user_id):
    myai_data = []
    
    # 1. JSON
    json_path = os.path.join(user_dir, "json", "snapchat_ai.json")
    
    # 2. HTML
    html_path = os.path.join(user_dir, "html", "snapchat_ai.html")
    if os.path.exists(html_path):
        try:
            with open(html_path, 'r') as f:
                soup = BeautifulSoup(f, 'html.parser')
            
            rows = soup.find_all('tr')
            # Header: Timestamp, IP Address, Type, Content
            if len(rows) > 1:
                headers = [th.get_text(strip=True) for th in rows[0].find_all('th')]
                # Map indices
                try:
                    ts_idx = headers.index("Timestamp")
                    ip_idx = headers.index("IP Address")
                    type_idx = headers.index("Type")
                    content_idx = headers.index("Content")
                    
                    for row in rows[1:]:
                        cols = row.find_all('td')
                        if len(cols) == len(headers):
                            myai_data.append({
                                'user_id': user_id,
                                'timestamp': cols[ts_idx].get_text(strip=True),
                                'ip_address': cols[ip_idx].get_text(strip=True),
                                'type': cols[type_idx].get_text(strip=True),
                                'content': cols[content_idx].get_text(strip=True)
                            })
                except ValueError:
                    pass # Headers didn't match expectation
        except Exception as e:
            logging.error(f"Error parsing My AI HTML for {user_id}: {e}")

    return myai_data

def parse_profile(user_dir, user_id):
    profile = {'user_id': user_id}
    
    # 1. JSON: user_profile.json
    json_path = os.path.join(user_dir, "json", "user_profile.json")
    if os.path.exists(json_path):
        try:
            with open(json_path, 'r') as f:
                data = json.load(f)
            app_profile = data.get('App Profile', {})
            profile['creation_time'] = app_profile.get('Creation Time')
            profile['country'] = app_profile.get('Country')
            # Check top level for Email/Phone just in case
            if 'Email' in data: profile['email'] = data['Email']
            if 'Phone Number' in data: profile['phone'] = data['Phone Number']
        except Exception as e:
            logging.error(f"Error parsing user_profile.json for {user_id}: {e}")

    # 2. JSON: account.json (Often contains Email/Phone)
    account_json_path = os.path.join(user_dir, "json", "account.json")
    if os.path.exists(account_json_path):
        try:
            with open(account_json_path, 'r') as f:
                data = json.load(f)
            # Basic Information might be top level or nested
            if 'Email' in data: profile['email'] = data['Email']
            if 'Phone Number' in data: profile['phone'] = data['Phone Number']
            if 'Basic Information' in data:
                basic = data['Basic Information']
                if 'Email' in basic: profile['email'] = basic['Email']
                if 'Phone Number' in basic: profile['phone'] = basic['Phone Number']
        except Exception as e:
            logging.error(f"Error parsing account.json for {user_id}: {e}")

    # 3. HTML: account.html (Fallback)
    if 'email' not in profile or 'phone' not in profile:
        account_html_path = os.path.join(user_dir, "html", "account.html")
        if os.path.exists(account_html_path):
            try:
                with open(account_html_path, 'r') as f:
                    soup = BeautifulSoup(f, 'html.parser')
                # Look for "Basic Information" table
                for row in soup.find_all('tr'):
                    cells = row.find_all(['th', 'td'])
                    if len(cells) >= 2:
                        key = cells[0].get_text(strip=True).lower()
                        val = cells[1].get_text(strip=True)
                        if 'email' in key:
                            profile['email'] = val
                        elif 'phone' in key:
                            profile['phone'] = val
                        elif 'creation date' in key and 'creation_time' not in profile:
                             profile['creation_time'] = val
            except Exception as e:
                logging.error(f"Error parsing account.html for {user_id}: {e}")
                
    return [profile]

def parse_friends(user_dir, user_id):
    friends_data = []
    
    # 1. JSON
    json_path = os.path.join(user_dir, "json", "friends.json")
    if os.path.exists(json_path):
        try:
            with open(json_path, 'r') as f:
                data = json.load(f)
            # Structure: List of friend objects
            if isinstance(data, list):
                for friend in data:
                    friends_data.append({
                        'user_id': user_id,
                        'username': friend.get('Username'),
                        'display_name': friend.get('Display Name'),
                        'creation_timestamp': friend.get('Creation Timestamp'),
                        'last_modified_timestamp': friend.get('Last Modified Timestamp'),
                        'source': friend.get('Source')
                    })
            elif isinstance(data, dict) and 'Friends' in data:
                 for friend in data['Friends']:
                    friends_data.append({
                        'user_id': user_id,
                        'username': friend.get('Username'),
                        'display_name': friend.get('Display Name'),
                        'creation_timestamp': friend.get('Creation Timestamp'),
                        'last_modified_timestamp': friend.get('Last Modified Timestamp'),
                        'source': friend.get('Source')
                    })
        except Exception as e:
            logging.error(f"Error parsing friends.json for {user_id}: {e}")
            
    # 2. HTML
    if not friends_data:
        html_path = os.path.join(user_dir, "html", "friends.html")
        if os.path.exists(html_path):
            try:
                with open(html_path, 'r') as f:
                    soup = BeautifulSoup(f, 'html.parser')
                
                # Look for "Friends" table
                # Headers: Username, Display Name, Creation Timestamp, Last Modified Timestamp, Source
                tables = soup.find_all('table')
                for table in tables:
                    rows = table.find_all('tr')
                    if len(rows) > 1:
                        headers = [th.get_text(strip=True) for th in rows[0].find_all('th')]
                        if "Username" in headers and "Display Name" in headers:
                            # Map indices
                            try:
                                u_idx = headers.index("Username")
                                d_idx = headers.index("Display Name")
                                c_idx = headers.index("Creation Timestamp") if "Creation Timestamp" in headers else -1
                                l_idx = headers.index("Last Modified Timestamp") if "Last Modified Timestamp" in headers else -1
                                s_idx = headers.index("Source") if "Source" in headers else -1
                                
                                for row in rows[1:]:
                                    cols = row.find_all('td')
                                    if len(cols) == len(headers):
                                        friends_data.append({
                                            'user_id': user_id,
                                            'username': cols[u_idx].get_text(strip=True),
                                            'display_name': cols[d_idx].get_text(strip=True),
                                            'creation_timestamp': cols[c_idx].get_text(strip=True) if c_idx != -1 else None,
                                            'last_modified_timestamp': cols[l_idx].get_text(strip=True) if l_idx != -1 else None,
                                            'source': cols[s_idx].get_text(strip=True) if s_idx != -1 else None
                                        })
                            except ValueError:
                                continue
            except Exception as e:
                logging.error(f"Error parsing friends.html for {user_id}: {e}")

    return friends_data

def parse_memories(user_dir, user_id):
    memories_data = []
    
    # 1. JSON
    json_path = os.path.join(user_dir, "json", "memories_history.json")
    if os.path.exists(json_path):
        try:
            with open(json_path, 'r') as f:
                data = json.load(f)
            # Structure: List of memory objects
            if isinstance(data, list):
                for mem in data:
                    memories_data.append({
                        'user_id': user_id,
                        'date': mem.get('Date'),
                        'media_type': mem.get('Media Type'),
                        'location': mem.get('Location'), # Might need parsing "Latitude, Longitude: ..."
                        'download_link': mem.get('Download Link')
                    })
            elif isinstance(data, dict) and 'Saved Media' in data:
                 for mem in data['Saved Media']:
                    memories_data.append({
                        'user_id': user_id,
                        'date': mem.get('Date'),
                        'media_type': mem.get('Media Type'),
                        'location': mem.get('Location'),
                        'download_link': mem.get('Download Link')
                    })
        except Exception as e:
            logging.error(f"Error parsing memories_history.json for {user_id}: {e}")
            
    # 2. HTML
    if not memories_data:
        html_path = os.path.join(user_dir, "html", "memories_history.html")
        if os.path.exists(html_path):
            try:
                with open(html_path, 'r') as f:
                    soup = BeautifulSoup(f, 'html.parser')
                
                # Headers: Date, Media Type, Location, (Download Link column is empty header)
                rows = soup.find_all('tr')
                if len(rows) > 1:
                    headers = [th.get_text(strip=True) for th in rows[0].find_all('th')]
                    # Map indices
                    try:
                        date_idx = headers.index("Date")
                        type_idx = headers.index("Media Type")
                        loc_idx = headers.index("Location")
                        
                        for row in rows[1:]:
                            cols = row.find_all('td')
                            if len(cols) >= 3:
                                date = cols[date_idx].get_text(strip=True)
                                media_type = cols[type_idx].get_text(strip=True)
                                location = cols[loc_idx].get_text(strip=True)
                                
                                # Extract Download Link
                                link_col = cols[-1] # Assuming last column
                                download_link = None
                                a_tag = link_col.find('a')
                                if a_tag and 'href' in a_tag.attrs:
                                    href = a_tag['href']
                                    # href="javascript:downloadMemories('URL');"
                                    match = re.search(r"downloadMemories\('([^']+)'\)", href)
                                    if match:
                                        download_link = match.group(1)
                                
                                memories_data.append({
                                    'user_id': user_id,
                                    'date': date,
                                    'media_type': media_type,
                                    'location': location,
                                    'download_link': download_link
                                })
                    except ValueError:
                        pass
            except Exception as e:
                logging.error(f"Error parsing memories_history.html for {user_id}: {e}")

    return memories_data

def parse_html_table(html_path):
    """
    Parses generic HTML tables.
    Returns a dictionary where keys are table names (derived from preceding headers)
    and values are lists of dictionaries (rows).
    """
    data = {}
    try:
        with open(html_path, 'r') as f:
            soup = BeautifulSoup(f, 'html.parser')
        
        tables = soup.find_all('table')
        for i, table in enumerate(tables):
            # Try to find a preceding header
            header = table.find_previous(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
            table_name = header.get_text(strip=True) if header else f"Table_{i+1}"
            
            rows = table.find_all('tr')
            if not rows:
                continue
                
            # Extract headers
            headers = [th.get_text(strip=True).title() for th in rows[0].find_all(['th', 'td'])]
            # Deduplicate headers if necessary
            seen_headers = {}
            unique_headers = []
            for h in headers:
                if h in seen_headers:
                    seen_headers[h] += 1
                    unique_headers.append(f"{h}_{seen_headers[h]}")
                else:
                    seen_headers[h] = 1
                    unique_headers.append(h)
            headers = unique_headers

            table_data = []
            for row in rows[1:]:
                cols = row.find_all('td')
                if len(cols) == len(headers):
                    row_dict = {}
                    for h, c in zip(headers, cols):
                        row_dict[h] = c.get_text(strip=True)
                    table_data.append(row_dict)
            
            if table_data:
                if table_name in data:
                    data[table_name].extend(table_data)
                else:
                    data[table_name] = table_data
                    
    except Exception as e:
        logging.error(f"Error parsing HTML table in {html_path}: {e}")
        
    return data

def parse_snap_history_subpage(html_path, user_id):
    """Parses a single snap history subpage."""
    snaps = []
    try:
        with open(html_path, 'r') as f:
            soup = BeautifulSoup(f, 'html.parser')
            
        content_divs = soup.find_all('div', style=lambda value: value and 'background: #f2f2f2' in value)
        
        for div in content_divs:
            try:
                sender_tag = div.find('h4')
                sender = sender_tag.get_text(strip=True) if sender_tag else "Unknown"
                
                media_span = div.find('span', style=lambda value: value and 'font-weight: bold' in value)
                media_type = media_span.get_text(strip=True) if media_span else "UNKNOWN"
                
                timestamp_tag = div.find('h6')
                timestamp = timestamp_tag.get_text(strip=True) if timestamp_tag else None
                
                snaps.append({
                    'user_id': user_id,
                    'sender': sender,
                    'media_type': media_type,
                    'timestamp': timestamp,
                    'source_file': os.path.basename(html_path)
                })
            except AttributeError:
                continue 
                
    except Exception as e:
        logging.error(f"Error parsing snap history subpage {html_path}: {e}")
        
    return snaps

def parse_chat_history_subpage(html_path, user_id):
    """Parses a single chat history subpage."""
    chats = []
    try:
        with open(html_path, 'r') as f:
            soup = BeautifulSoup(f, 'html.parser')
            
        content_divs = soup.find_all('div', style=lambda value: value and 'background: #f2f2f2' in value)
        
        for div in content_divs:
            try:
                sender_tag = div.find('h4')
                sender = sender_tag.get_text(strip=True) if sender_tag else "Unknown"
                
                media_span = div.find('span', style=lambda value: value and 'font-weight: bold' in value)
                media_type = media_span.get_text(strip=True) if media_span else "TEXT"
                
                content_tag = div.find('p')
                content = content_tag.get_text(strip=True) if content_tag else ""
                
                timestamp_tag = div.find('h6')
                timestamp = timestamp_tag.get_text(strip=True) if timestamp_tag else None
                
                chats.append({
                    'user_id': user_id,
                    'sender': sender,
                    'media_type': media_type,
                    'content': content,
                    'timestamp': timestamp,
                    'source_file': os.path.basename(html_path)
                })
            except AttributeError:
                continue
                
    except Exception as e:
        logging.error(f"Error parsing chat history subpage {html_path}: {e}")
        
    return chats

def process_user(user_dir, user_id):
    # --- Chats (JSON/HTML Metadata) ---
    chats = []
    input_count = 0
    
    # Try JSON first
    json_chat_path = os.path.join(user_dir, "json", "chat_history.json")
    if os.path.exists(json_chat_path):
        c, i = parse_json_chat(json_chat_path, user_id)
        chats.extend(c)
        input_count += i
    else:
        # Try HTML Metadata
        html_chat_path = os.path.join(user_dir, "html", "chat_history.html")
        if os.path.exists(html_chat_path):
            c, i = parse_html_chat(html_chat_path, user_id)
            chats.extend(c)
            input_count += i
            
    all_chats.extend(chats)
    
    # Verification Stats
    verification_stats.append({
        'user_id': user_id,
        'input_count': input_count,
        'output_count': len(chats),
        'diff': input_count - len(chats)
    })

    # --- My AI ---
    myai = parse_myai(user_dir, user_id)
    all_myai.extend(myai)
    
    # --- Friends ---
    friends = parse_friends(user_dir, user_id)
    all_friends.extend(friends)
    
    # --- Memories ---
    memories = parse_memories(user_dir, user_id)
    all_memories.extend(memories)

    # --- Generic HTML Tables ---
    generic_files = [
        "talk_history.html", "snap_map_places_history.html"
    ]
    
    for filename in generic_files:
        html_path = os.path.join(user_dir, "html", filename)
        if os.path.exists(html_path):
            tables = parse_html_table(html_path)
            for table_name, rows in tables.items():
                # Normalize table name for CSV key
                clean_name = f"{os.path.splitext(filename)[0]}_{table_name}"
                clean_name = re.sub(r'[^a-zA-Z0-9_]', '_', clean_name).lower()
                
                # Add user_id
                for row in rows:
                    row['user_id'] = user_id
                
                if clean_name in all_generic_data:
                    all_generic_data[clean_name].extend(rows)
                else:
                    all_generic_data[clean_name] = rows

    # --- Snap History Subpages ---
    snap_history_dir = os.path.join(user_dir, "html", "snap_history")
    if os.path.exists(snap_history_dir):
        for html_file in glob.glob(os.path.join(snap_history_dir, "subpage_*.html")):
            all_snap_history.extend(parse_snap_history_subpage(html_file, user_id))

def main():
    setup_directories()
    
    # Iterate over extracted user directories
    extracted_users_dir = EXTRACTED_DIR
    if os.path.exists(extracted_users_dir):
        user_dirs = [d for d in glob.glob(os.path.join(extracted_users_dir, "*")) if os.path.isdir(d)]
        print(f"Found {len(user_dirs)} extracted users.")
        
        for user_dir in tqdm(user_dirs, desc="Processing Users"):
            user_id = os.path.basename(user_dir)
            try:
                process_user(user_dir, user_id)
            except Exception as e:
                logging.error(f"Critical error processing {user_id}: {e}")
                print(f"Failed {user_id}")
    else:
        print(f"No Extracted_Users directory found at {extracted_users_dir}")
        return

    # Export
    print("Exporting data...")
    pd.DataFrame(all_chats).to_csv(os.path.join(OUTPUT_DIR, "chats.csv"), index=False)
    pd.DataFrame(all_myai).to_csv(os.path.join(OUTPUT_DIR, "myai.csv"), index=False)
    pd.DataFrame(all_friends).to_csv(os.path.join(OUTPUT_DIR, "friends.csv"), index=False)
    pd.DataFrame(all_memories).to_csv(os.path.join(OUTPUT_DIR, "memories.csv"), index=False)
    pd.DataFrame(all_snap_history).to_csv(os.path.join(OUTPUT_DIR, "snap_history_log.csv"), index=False)
    
    # Export Generic Tables
    for table_name, rows in all_generic_data.items():
        if rows:
            csv_name = f"{table_name}.csv"
            df = pd.DataFrame(rows)
            
            # Ensure user_id is the first column
            cols = ['user_id'] + [c for c in df.columns if c != 'user_id']
            df = df[cols]
            
            df.to_csv(os.path.join(OUTPUT_DIR, csv_name), index=False)
            
    pd.DataFrame(verification_stats).to_csv(os.path.join(OUTPUT_DIR, "verification_report.csv"), index=False)
    
    print("Done!")

if __name__ == "__main__":
    main()
