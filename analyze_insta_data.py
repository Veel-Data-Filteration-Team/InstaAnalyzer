import os
import re
import sys
import json
import queue
import time
import traceback
import multiprocessing
import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from colorama import init, Fore, Style
from tqdm import tqdm

# Initialize colorama
init(autoreset=True)

def load_json_file(file_path: str) -> dict:
    """Load and parse a JSON file."""
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            return json.load(file)
    except Exception as e:
        print(f"{Fore.RED}Error loading JSON file {file_path}: {str(e)}{Style.RESET_ALL}")
        return None

def save_json_file(data: dict, file_path: str) -> None:
    """Save data to a JSON file with proper formatting."""
    try:
        with open(file_path, 'w', encoding='utf-8') as file:
            json.dump(data, file, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"{Fore.RED}Error saving JSON file {file_path}: {str(e)}{Style.RESET_ALL}")
        
def load_name_lists():
    """Load male and female name lists from files."""
    try:
        with open('malenames.txt', 'r') as f:
            male_names = set(line.strip().lower() for line in f if line.strip())
        
        with open('femalenames.txt', 'r') as f:
            female_names = set(line.strip().lower() for line in f if line.strip())
        
        return male_names, female_names
    except Exception as e:
        print(f"Error loading name lists: {str(e)}")
        return set(), set()

def load_gendered_niches():
    """Load gendered niches from JSON file."""
    try:
        return load_json_file('gendered_niches.json')
    except Exception as e:
        print(f"Error loading gendered niches: {str(e)}")
        return {
            "female_dominated": [],
            "male_dominated": [],
            "neutral": []
        }

# Load name lists and gendered niches
MALE_NAMES, FEMALE_NAMES = load_name_lists()
GENDERED_NICHES = load_gendered_niches()

def detect_gender_from_text(bio: str, captions: List[str], first_name: str, niche: str) -> Tuple[str, float]:
    """
    Detect gender based on name, bio, post captions, and content niche.
    Returns the detected gender and a confidence score (0-1).
    """
    score_system = {
        "name": 0.5,        # 50% weight for name
        "content": 0.35,    # 35% weight for bio and captions
        "niche": 0.15       # 15% weight for content niche
    }
    
    name_score = 0
    content_score = 0
    niche_score = 0
    
    # 1. Check first name against name lists
    if first_name:
        first_name_lower = first_name.lower()
        if first_name_lower in MALE_NAMES:
            name_score = -1  # Male indicator
        elif first_name_lower in FEMALE_NAMES:
            name_score = 1   # Female indicator
    
    # 2. Check bio and captions for gender indicators
    # Common gender indicators
    female_indicators = [
        r'\b(?:she|her|woman|girl|female|mom|mother|wife|daughter|sister)\b',
        r'♀'
    ]
    
    male_indicators = [
        r'\b(?:he|him|man|boy|male|dad|father|husband|son|brother)\b',
        r'♂'
    ]
    
    # Count matches in bio
    female_matches = 0
    male_matches = 0
    
    if bio:
        for pattern in female_indicators:
            female_matches += len(re.findall(pattern, bio, re.IGNORECASE))
        
        for pattern in male_indicators:
            male_matches += len(re.findall(pattern, bio, re.IGNORECASE))
    
    # Check captions for additional clues
    for caption in captions:
        if not caption:
            continue
            
        for pattern in female_indicators:
            female_matches += len(re.findall(pattern, caption, re.IGNORECASE))
        
        for pattern in male_indicators:
            male_matches += len(re.findall(pattern, caption, re.IGNORECASE))
    
    # Calculate content score
    total_matches = female_matches + male_matches
    if total_matches > 0:
        content_score = (female_matches - male_matches) / total_matches
    
    # 3. Check niche for gender association
    if niche:
        niche_lower = niche.lower()
        
        # Check if primary niche is in gendered categories
        if any(keyword in niche_lower for keyword in GENDERED_NICHES["female_dominated"]):
            niche_score = 0.8  # Strong female indicator, but not absolute
        elif any(keyword in niche_lower for keyword in GENDERED_NICHES["male_dominated"]):
            niche_score = -0.8  # Strong male indicator, but not absolute
    
    # Calculate final weighted score
    final_score = (
        score_system["name"] * name_score +
        score_system["content"] * content_score +
        score_system["niche"] * niche_score
    )
    
    # Determine gender based on final score
    if final_score > 0.1:
        return "Female", abs(final_score)
    elif final_score < -0.1:
        return "Male", abs(final_score)
    else:
        # If score is near zero, make educated guess based on niche
        if niche and any(keyword in niche_lower for keyword in GENDERED_NICHES["female_dominated"]):
            return "Female", 0.6
        elif niche and any(keyword in niche_lower for keyword in GENDERED_NICHES["male_dominated"]):
            return "Male", 0.6
        
        # If still unclear, check name as fallback
        if first_name and first_name.lower() in MALE_NAMES:
            return "Male", 0.7
        elif first_name and first_name.lower() in FEMALE_NAMES:
            return "Female", 0.7
            
        # Last resort fallback to global statistics (more female users on Instagram)
        return "Female", 0.5

def extract_email(text: str) -> Optional[str]:
    """Extract email address from text."""
    if not text:
        return None
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    matches = re.findall(email_pattern, text)
    return matches[0] if matches else None

def extract_phone(text: str) -> Optional[str]:
    """Extract phone number from text."""
    if not text:
        return None
    # Various phone patterns
    patterns = [
        r'\+\d{1,3}\s?\(\d{1,4}\)\s?\d{3,4}[-\s]?\d{3,4}',  # +1 (123) 456-7890
        r'\+\d{1,3}\s?\d{1,4}\s?\d{3,4}\s?\d{3,4}',         # +1 123 456 7890
        r'\(\d{3,4}\)\s?\d{3,4}[-\s]?\d{3,4}',              # (123) 456-7890
        r'\d{3,4}[-\s]?\d{3,4}[-\s]?\d{3,4}'                # 123-456-7890
    ]
    for pattern in patterns:
        matches = re.findall(pattern, text)
        if matches:
            return matches[0]
    return None

def extract_age(text: str) -> Tuple[Optional[int], Optional[str]]:
    """Extract age or age group from text."""
    if not text:
        return None, None
    
    # Direct age mentions
    age_patterns = [
        r'(?:I am|I\'m)\s+(\d{1,2})',
        r'age\s*:?\s*(\d{1,2})',
        r'(\d{1,2})\s*(?:years|yrs)\s*old',
        r'(\d{1,2})\s*y\.?o'
    ]
    
    for pattern in age_patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        if matches:
            try:
                age = int(matches[0])
                if 13 <= age <= 100:  # Reasonable age range
                    # Determine age group
                    if age < 18:
                        age_group = "Under 18"
                    elif age < 25:
                        age_group = "18-24"
                    elif age < 30:
                        age_group = "25-29"
                    elif age < 35:
                        age_group = "25-34"
                    elif age < 45:
                        age_group = "35-44"
                    else:
                        age_group = "45+"
                    return age, age_group
            except ValueError:
                pass
    
    # Look for birth year
    year_patterns = [
        r'born\s+in\s+(\d{4})',
        r'b\.\s*(\d{4})',
        r'est\.\s*(\d{4})'
    ]
    
    current_year = datetime.datetime.now().year
    for pattern in year_patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        if matches:
            try:
                year = int(matches[0])
                if 1920 <= year <= current_year - 13:  # Reasonable birth year range
                    age = current_year - year
                    # Determine age group
                    if age < 18:
                        age_group = "Under 18"
                    elif age < 25:
                        age_group = "18-24"
                    elif age < 30:
                        age_group = "25-29"
                    elif age < 35:
                        age_group = "25-34"
                    elif age < 45:
                        age_group = "35-44"
                    else:
                        age_group = "45+"
                    return age, age_group
            except ValueError:
                pass
    
    # If no direct age indicator, guess based on content and career stage
    for post in text.split('\n'):
        if "college" in post.lower() or "university" in post.lower():
            return None, "18-24"
        elif "career" in post.lower() or "job" in post.lower():
            return None, "25-29"
    
    # Default age group if nothing else works
    return None, "25-29"  # Most common creator age group

def determine_location_based(text: str) -> str:
    """Determine if creator is US-based based on bio and other text."""
    if not text or not isinstance(text, str):
        return "Global"
    
    # Convert to lowercase for case-insensitive matching
    text_lower = text.lower()
    
    # Check for US indicators with word boundaries
    us_indicators = [
        r'\b(?:usa|us|united\s+states|america|u\.s\.a?)\b',
        r'\b(?:ny|nyc|new\s+york|la|los\s+angeles|california|ca|fl|florida|tx|texas)\b'
    ]
    
    for pattern in us_indicators:
        if re.search(pattern, text_lower, re.IGNORECASE):
            return "USA"
    
    # Check for US state names
    us_states = [
        'alabama', 'alaska', 'arizona', 'arkansas', 'california', 'colorado', 'connecticut', 
        'delaware', 'florida', 'georgia', 'hawaii', 'idaho', 'illinois', 'indiana', 'iowa', 
        'kansas', 'kentucky', 'louisiana', 'maine', 'maryland', 'massachusetts', 'michigan', 
        'minnesota', 'mississippi', 'missouri', 'montana', 'nebraska', 'nevada', 'new hampshire', 
        'new jersey', 'new mexico', 'new york', 'north carolina', 'north dakota', 'ohio', 'oklahoma', 
        'oregon', 'pennsylvania', 'rhode island', 'south carolina', 'south dakota', 'tennessee', 
        'texas', 'utah', 'vermont', 'virginia', 'washington', 'west virginia', 'wisconsin', 'wyoming'
    ]
    
    # State abbreviations
    state_abbr = [
        'al', 'ak', 'az', 'ar', 'ca', 'co', 'ct', 'de', 'fl', 'ga', 'hi', 'id', 'il', 'in', 'ia', 
        'ks', 'ky', 'la', 'me', 'md', 'ma', 'mi', 'mn', 'ms', 'mo', 'mt', 'ne', 'nv', 'nh', 'nj', 
        'nm', 'ny', 'nc', 'nd', 'oh', 'ok', 'or', 'pa', 'ri', 'sc', 'sd', 'tn', 'tx', 'ut', 'vt', 
        'va', 'wa', 'wv', 'wi', 'wy'
    ]
    
    # Check for state names
    for state in us_states:
        if re.search(r'\b{}\b'.format(state), text_lower):
            return "USA"
    
    # Check for state abbreviations with word boundaries
    for abbr in state_abbr:
        if re.search(r'\b{}\b'.format(abbr), text_lower):
            return "USA"
    
    # Default to Global if no US indicators are found
    return "Global"

def determine_location_based_on_geo(coordinates: Dict) -> str:
    """
    Determine if creator is US-based using geo coordinates.
    
    Args:
        coordinates: Dictionary containing 'lat' and 'lng' keys
        
    Returns:
        "USA" if coordinates are within US boundaries, "Global" otherwise
    """
    if not coordinates or 'lat' not in coordinates or 'lng' not in coordinates:
        return "Global"
    
    lat = coordinates.get('lat')
    lng = coordinates.get('lng')
    
    # Check if coordinates are valid numbers
    if not isinstance(lat, (int, float)) or not isinstance(lng, (int, float)):
        return "Global"
    
    # US mainland bounding box (approximate)
    # Continental US: lat 24.396308 to 49.384358, lng -125.000000 to -66.934570
    if (24.396308 <= lat <= 49.384358 and -125.000000 <= lng <= -66.934570):
        return "USA"
    
    # Alaska bounding box (approximate)
    # Alaska: lat 51.214183 to 71.365162, lng -179.148909 to -129.974167
    if (51.214183 <= lat <= 71.365162 and (-179.148909 <= lng <= -129.974167 or 180.0 >= lng >= 129.974167)):
        return "USA"
    
    # Hawaii bounding box (approximate)
    # Hawaii: lat 18.910361 to 22.236428, lng -160.242167 to -154.806773
    if (18.910361 <= lat <= 22.236428 and -160.242167 <= lng <= -154.806773):
        return "USA"
    
    # Default to Global for all other coordinates
    return "Global"

def extract_hashtags(text: str) -> List[str]:
    """Extract hashtags from text."""
    if not text:
        return []
    
    hashtag_pattern = r'#(\w+)'
    return re.findall(hashtag_pattern, text)

def extract_mentions(text: str) -> List[str]:
    """Extract @mentions from text."""
    if not text:
        return []
    
    mention_pattern = r'@(\w+)'
    return re.findall(mention_pattern, text)

def identify_collaborations(posts: List[dict]) -> Dict:
    """Identify potential brand collaborations from posts."""
    # Track mentions and hashtags for frequency analysis
    mentions_count = {}
    hashtags_count = {}
    brand_sources = {}  # Track whether brand was found as mention, hashtag, or both
    
    # Calculate recent timeframe (last 90 days) - This is still used for 'is_recent' within collabs
    recent_threshold = 300  # days
    today = datetime.datetime.now()
    recent_cutoff = today - datetime.timedelta(days=recent_threshold)
    
    # --- Get the current user's username (assuming it's consistent across posts) ---
    uname = None
    if posts:
        # Prioritize 'user' field first
        first_post_user_data = posts[0].get("node", {}).get("user", {})
        uname = first_post_user_data.get("username")
        
        # If 'user' or 'username' within 'user' is missing, fallback to 'owner'
        # if not uname:
        #     first_post_owner_data = posts[0].get("node", {}).get("owner", {})
        #     uname = first_post_owner_data.get("username")
    # --- End User Username Extraction ---

    # Analyze each post for mentions and hashtags
    for post in posts:
        node = post.get('node', {})
        caption = node.get('caption', {}).get('text', '')
        
        # Check if post is recent
        taken_at = node.get('taken_at')
        is_recent = False
        if taken_at:
            post_date = datetime.datetime.fromtimestamp(taken_at)
            is_recent = post_date > recent_cutoff
        
        # Extract mentions - improved detection for usernames after @ symbol
        if caption:
            # More robust mention detection using regex for @username format
            mentions = re.findall(r'@([A-Za-z0-9._]+)', caption)
            for mention in mentions:
                # Skip very short mentions (likely artifacts)
                if len(mention) < 3:
                    continue
                    
                # Skip common words that might appear after @ but aren't usernames
                if mention.lower() in ['the', 'and', 'for', 'from', 'with', 'this', 'that', 'have', 'has', 'her', 'his', 'our', 'my', 'your', 'their', 'its', 'as', 'at', 'by', 'to', 'in', 'on', 'of', 'or', 'if']:
                    continue
                
                if mention not in mentions_count:
                    mentions_count[mention] = {'count': 0, 'is_recent': False}
                    brand_sources[mention] = {'mention': True, 'hashtag': False}
                else:
                    brand_sources[mention]['mention'] = True
                
                mentions_count[mention]['count'] += 1
                if is_recent and not mentions_count[mention]['is_recent']:
                    mentions_count[mention]['is_recent'] = True
            
            # Extract hashtags
            hashtags = re.findall(r'#(\w+)', caption)
            for hashtag in hashtags:
                if hashtag not in hashtags_count:
                    hashtags_count[hashtag] = {'count': 0, 'is_recent': False}
                    if hashtag not in brand_sources:
                        brand_sources[hashtag] = {'mention': False, 'hashtag': True}
                    else:
                        brand_sources[hashtag]['hashtag'] = True
                
                hashtags_count[hashtag]['count'] += 1
                if is_recent and not hashtags_count[hashtag]['is_recent']:
                    hashtags_count[hashtag]['is_recent'] = True
    
    # Calculate engagement metrics for brand posts
    all_brands = list(set(list(mentions_count.keys()) + list(hashtags_count.keys())))
    recent_brands = []
    previous_brands = []
    branded_engagement = []
    
    # Combine engagement data for both hashtags and mentions
    for post in posts:
        node = post.get('node', {})
        caption = node.get('caption', {}).get('text', '')
        likes = node.get('like_count', 0)
        comments = node.get('comment_count', 0)
        total_engagement = likes + comments
        
        # Check if post mentions a brand
        is_brand_post = False
        post_brands = []
        
        for brand in all_brands:
            # Check for brand as mention or hashtag - improved pattern matching
            mention_pattern = r'@' + re.escape(brand) + r'\b'
            hashtag_pattern = r'#' + re.escape(brand) + r'\b'
            
            if re.search(mention_pattern, caption, re.IGNORECASE) or re.search(hashtag_pattern, caption, re.IGNORECASE):
                is_brand_post = True
                post_brands.append(brand)
        
        if is_brand_post:
            branded_engagement.append({
                'engagement': total_engagement,
                'likes': likes,
                'comments': comments,
                'brands': post_brands
            })
    
    # Calculate top collaborations
    all_collabs = []
    
    # Process mentions
    for brand, data in mentions_count.items():
        # Skip brands with only one mention as they might be incidental
        if data['count'] < 2:
            continue
            
        # Determine if the brand is recent
        if data['is_recent']:
            recent_brands.append({'name': brand, 'source': 'mention'})
        
        previous_brands.append({'name': brand, 'source': 'mention'})
        
        # Find all engagements for this brand
        brand_engagements = [
            item for item in branded_engagement if brand in item['brands']
        ]
        
        avg_engagement = 0
        avg_likes = 0
        avg_comments = 0
        
        if brand_engagements:
            avg_engagement = sum(item['engagement'] for item in brand_engagements) / len(brand_engagements)
            avg_likes = sum(item['likes'] for item in brand_engagements) / len(brand_engagements)
            avg_comments = sum(item['comments'] for item in brand_engagements) / len(brand_engagements)
        
        # Track collaboration type (ad, partnership, etc)
        ad_indicators = [
            r'\b(?:ad|sponsored|paid|partner|collab|ambassador)\b',
            r'#ad\b',
            r'#sponsored\b',
            r'#paidpartner\b',
            r'#sponsored_by\b',
            r'#partnership\b'
        ]
        
        types = []
        for post in posts:
            caption = post.get('node', {}).get('caption', {}).get('text', '')
            contains_brand = re.search(fr'@{re.escape(brand)}|#{re.escape(brand)}', caption, re.IGNORECASE)
            
            if contains_brand:
                for pattern in ad_indicators:
                    if re.search(pattern, caption, re.IGNORECASE):
                        types.append('ad')
                        break
        
        source_type = []
        if brand_sources[brand]['mention']:
            source_type.append('mention')
        if brand_sources[brand]['hashtag']:
            source_type.append('hashtag')
            
        source_str = ' & '.join(source_type)
        
        all_collabs.append({
            'name': brand,
            'count': data['count'],
            'types': list(set(types)) if types else ['organic'],
            'engagement': round(avg_engagement),
            'is_recent': data['is_recent'],
            'source': source_str
        })
    
    # Process hashtags (similar to mentions)
    for brand, data in hashtags_count.items():
        # Skip brands that were already processed as mentions
        if brand in mentions_count:
            continue
            
        # Skip brands with only one hashtag as they might be incidental
        if data['count'] < 2:
            continue
            
        # Determine if the brand is recent
        if data['is_recent']:
            recent_brands.append({'name': brand, 'source': 'hashtag'})
        
        previous_brands.append({'name': brand, 'source': 'hashtag'})
        
        # Find all engagements for this brand
        brand_engagements = [
            item for item in branded_engagement if brand in item['brands']
        ]
        
        avg_engagement = 0
        avg_likes = 0
        avg_comments = 0
        
        if brand_engagements:
            avg_engagement = sum(item['engagement'] for item in brand_engagements) / len(brand_engagements)
            avg_likes = sum(item['likes'] for item in brand_engagements) / len(brand_engagements)
            avg_comments = sum(item['comments'] for item in brand_engagements) / len(brand_engagements)
        
        # Track collaboration type (ad, partnership, etc)
        ad_indicators = [
            r'\b(?:ad|sponsored|paid|partner|collab|ambassador)\b',
            r'#ad\b',
            r'#sponsored\b',
            r'#paidpartner\b',
            r'#sponsored_by\b',
            r'#partnership\b'
        ]
        
        types = []
        for post in posts:
            caption = post.get('node', {}).get('caption', {}).get('text', '')
            contains_brand = re.search(fr'#{re.escape(brand)}', caption, re.IGNORECASE)
            
            if contains_brand:
                for pattern in ad_indicators:
                    if re.search(pattern, caption, re.IGNORECASE):
                        types.append('ad')
                        break
        
        source_type = []
        if brand_sources[brand]['mention']:
            source_type.append('mention')
        if brand_sources[brand]['hashtag']:
            source_type.append('hashtag')
            
        source_str = ' & '.join(source_type)
        
        all_collabs.append({
            'name': brand,
            'count': data['count'],
            'types': list(set(types)) if types else ['organic'],
            'engagement': round(avg_engagement),
            'is_recent': data['is_recent'],
            'source': source_str
        })
    
    # Sort collaborations by recency and engagement
    all_collabs.sort(key=lambda x: (-1 if x['is_recent'] else 0, -x['engagement']))
    
    # Format recent_brands and previous_brands lists with sources
    recent_brands_with_source = []
    previous_brands_with_source = []
    
    # Track processed brands to avoid duplicates
    processed_recent = set()
    processed_previous = set()
    
    # Process recent brands first
    for item in recent_brands:
        brand_name = item['name']
        if brand_name not in processed_recent:
            recent_brands_with_source.append({
                "name": brand_name,
                "source": item['source']
            })
            processed_recent.add(brand_name)
    
    # Process all brands for previous list
    for item in previous_brands:
        brand_name = item['name']
        if brand_name not in processed_previous:
            previous_brands_with_source.append({
                "name": brand_name,
                "source": item['source']
            })
            processed_previous.add(brand_name)
    
    # Calculate average engagement metrics for brand posts
    avg_branded_engagement = 0
    avg_branded_likes = 0
    avg_branded_comments = 0
    
    if branded_engagement:
        avg_branded_engagement = sum(item['engagement'] for item in branded_engagement) / len(branded_engagement)
        avg_branded_likes = sum(item['likes'] for item in branded_engagement) / len(branded_engagement)
        avg_branded_comments = sum(item['comments'] for item in branded_engagement) / len(branded_engagement)
    
    # --- Determine the final 'status' based on the new sequential flow ---
    final_status = None # Default status

    # Rule 1: any post has is_paid_partnership == True
    for post in posts:
        node = post.get('node', {})
        if node.get('is_paid_partnership') is True:
            final_status = "Active"
            break # Condition met, break and proceed to output formatting

    # Rule 2: else if any of the specific hashtags appear in any post's caption
    if final_status is None: # Only proceed if status not already "Active"
        status_hashtags = ['ads', 'ad', 'collaboration', 'collab', 'usemycode', 'partnership', 'partner']
        for post in posts:
            node = post.get('node', {})
            caption = node.get('caption', {}).get('text', '')
            if caption:
                caption_lower = caption.lower()
                for tag in status_hashtags:
                    if f'#{tag}' in caption_lower:
                        final_status = "Active"
                        break # Condition met, break inner tag loop
            if final_status == "Active":
                break # Condition met, break outer post loop

    # Rule 3: else if owner-username is not uname
    if final_status is None: # Only proceed if status not already "Active"
        for post in posts:
            node = post.get('node', {})
            post_owner_username = node.get('owner', {}).get('username')
            if post_owner_username and post_owner_username != uname:
                final_status = "Active"
                break # Condition met, break and proceed to output formatting

    # Rule 4: else if any coauthor-username is not uname
    if final_status is None: # Only proceed if status not already "Active"
        for post in posts:
            node = post.get('node', {})
            coauthor_producers = node.get('coauthor_producers')
            if coauthor_producers: # Check if the list is not empty
                for coauthor in coauthor_producers:
                    coauthor_username = coauthor.get("username")
                    if coauthor_username and coauthor_username != uname:
                        final_status = "Active"
                        break # Condition met, break inner coauthor loop
            if final_status == "Active":
                break # Condition met, break outer post loop

    # If after all sequential checks, final_status is still None, it remains None.
    # --- End Status Determination ---


    # Format structured output
    collaboration_info = {
        'status': final_status, # Use the newly determined status
        'recent_brands_with_source': recent_brands_with_source[:],  # Limit to top 5 with source
        'previous_brands_with_source': previous_brands_with_source[:],  # Limit to top 5 with source
        'recent_brands': [item['name'] for item in recent_brands_with_source][:],  # For backward compatibility
        'previous_brands': [item['name'] for item in previous_brands_with_source][:],  # For backward compatibility
        'metrics': {
            'total_collaborations': len(previous_brands_with_source),
            'recent_count': len(recent_brands_with_source),
            'engagement_rate': round(avg_branded_engagement, 1)
        },
        'top_collaborations': all_collabs[:],  # Limit to top 5
        'engagement_metrics': {
            'branded_engagement_rate': round(avg_branded_engagement, 1),
            'avg_branded_likes': round(avg_branded_likes),
            'avg_branded_comments': round(avg_branded_comments)
        }
    }
    
    return collaboration_info

def identify_niche(posts: List[dict]) -> Dict:
    """Identify the creator's niche based on content patterns."""
    all_hashtags = []
    
    # Extract all hashtags from posts
    for post in posts:
        node = post.get('node', {})
        caption = node.get('caption', {}).get('text', '')
        hashtags = extract_hashtags(caption)
        all_hashtags.extend(hashtags)
    
    # Count hashtag frequencies
    hashtag_counts = {}
    for tag in all_hashtags:
        hashtag_counts[tag.lower()] = hashtag_counts.get(tag.lower(), 0) + 1
    
    # Define niche categories with relevant keywords/hashtags - expanded to match example.json
    niche_categories = {
        "Fashion & Style": ["fashion", "style", "outfit", "clothing", "model", "dress", "accessories", "fashionista"],
        "Beauty": ["makeup", "skincare", "beauty", "cosmetics", "haircare", "nails", "glam", "makeupartist"],
        "Lifestyle": ["lifestyle", "life", "daily", "routine", "inspiration", "motivation"],
        "Fitness": ["fitness", "workout", "gym", "exercise", "health", "training", "muscle", "fit"],
        "Health": ["health", "wellness", "nutrition", "diet", "healthy", "mindfulness", "meditation"],
        "Food": ["food", "cooking", "recipe", "chef", "foodie", "cuisine", "baking", "delicious", "yummy"],
        "Travel": ["travel", "wanderlust", "adventure", "explore", "tourism", "vacation", "trip", "journey", "destination"],
        "Technology": ["technology", "tech", "gadget", "device", "software", "app", "smartphone", "computer"],
        "Gaming": ["gaming", "gamer", "videogames", "game", "esports", "playstation", "xbox", "nintendo"],
        "Entertainment": ["entertainment", "movie", "film", "tv", "television", "cinema", "streaming"],
        "Comedy": ["comedy", "funny", "humor", "laugh", "joke", "prank", "skit"],
        "Education": ["education", "learning", "school", "knowledge", "teach", "study", "student", "lesson"],
        "Business": ["business", "entrepreneur", "marketing", "startup", "success", "money"],
        "Finance": ["finance", "investing", "stocks", "cryptocurrency", "money", "financial", "wealth"],
        "Art & Design": ["art", "artist", "drawing", "painting", "creative", "design", "illustration"],
        "Music": ["music", "musician", "song", "singer", "artist", "band", "concert"],
        "Dance": ["dance", "dancer", "choreography", "ballet", "hiphop"],
        "Sports": ["sports", "athlete", "basketball", "football", "soccer", "baseball", "tennis"],
        "Pets & Animals": ["pets", "dog", "cat", "animal", "puppy", "kitten", "wildlife"],
        "Family & Parenting": ["family", "parenting", "mom", "dad", "children", "kids", "baby"]
    }
    
    # Score each niche category
    niche_scores = {category: 0 for category in niche_categories}
    for tag, count in hashtag_counts.items():
        for category, keywords in niche_categories.items():
            if any(keyword in tag for keyword in keywords):
                niche_scores[category] += count
    
    # Calculate distribution percentages
    total_score = sum(niche_scores.values()) or 1  # Avoid division by zero
    distribution = {category: round(score / total_score * 100, 1) for category, score in niche_scores.items() if score > 0}
    
    # Filter out negligible categories (less than 2%)
    significant_distribution = {k: v for k, v in distribution.items() if v >= 2}
    
    # Sort categories by score
    sorted_niches = sorted(niche_scores.items(), key=lambda x: x[1], reverse=True)
    
    # Primary niche is the highest scored one
    primary = sorted_niches[0][0] if sorted_niches and sorted_niches[0][1] > 0 else None
    
    # Secondary niches are the next highest scored ones (2-3)
    secondary = [niche for niche, score in sorted_niches[1:4] if score > 0]
    
    # Confidence scores - normalize to 0-100 scale
    confidence_scores = {}
    max_score = sorted_niches[0][1] if sorted_niches and sorted_niches[0][1] > 0 else 1
    
    # Add all categories to confidence scores, even if 0
    for category in niche_categories:
        score = niche_scores.get(category, 0)
        confidence_scores[category] = min(100, int((score / max_score) * 100))
    
    return {
        "primary": primary,
        "secondary": secondary,
        "distribution": significant_distribution,
        "confidence_scores": confidence_scores
    }

def analyze_locations(posts: List[dict]) -> Dict:
    """Analyze creator's locations from posts."""
    location_data = {}
    location_counts = {}
    recent_locations = []
    
    # Calculate recent timeframe (last 90 days)
    today = datetime.datetime.now()
    recent_cutoff = today - datetime.timedelta(days=90)
    
    # Extract locations from posts
    for post in posts:
        node = post.get('node', {})
        location = node.get('location', {})
        
        if location and location.get('name'):
            location_name = location.get('name')
            location_pk = location.get('pk')
            lat = location.get('lat')
            lng = location.get('lng')
            
            # Skip if it's not a valid location
            if not location_name:
                continue
                
            # Check if post is recent
            taken_at = node.get('taken_at')
            is_recent = False
            if taken_at:
                post_date = datetime.datetime.fromtimestamp(taken_at)
                is_recent = post_date > recent_cutoff
            
            # Add to location counts
            if location_name not in location_counts:
                location_counts[location_name] = 1
                location_data[location_name] = {
                    'id': location_pk,
                    'coordinates': {'lat': lat, 'lng': lng} if lat and lng else None,
                    'count': 1,
                    'is_recent': is_recent
                }
            else:
                location_counts[location_name] += 1
                location_data[location_name]['count'] += 1
                if is_recent and not location_data[location_name].get('is_recent'):
                    location_data[location_name]['is_recent'] = True
            
            # Track recent locations
            if is_recent and location_name not in recent_locations:
                recent_locations.append(location_name)
    
    # Sort locations by frequency
    top_locations = sorted(
        [{'name': name, 'count': count} for name, count in location_counts.items()],
        key=lambda x: x['count'],
        reverse=True
    )
    
    # Determine most likely home location based on frequency
    home_location = None
    home_location_details = None
    
    if top_locations:
        home_location = top_locations[0]['name']
        home_location_details = parse_location_string(home_location)
        
        # Add the count to help assess confidence
        if home_location_details:
            home_location_details['count'] = top_locations[0]['count']
            home_location_details['coordinates'] = location_data[home_location].get('coordinates')
    
    # Format output
    return {
        'top_locations': top_locations[:5],  # Top 5 locations
        'recent_locations': recent_locations[:5],  # Recent locations
        'location_data': location_data,
        'total_locations': len(location_data),
        'home_location': home_location_details
    }

def parse_location_string(location_string: str) -> Dict:
    """
    Parse a location string into its components (city, state, country).
    Example inputs:
    - "Dubai, United Arab Emirates"
    - "New York City"
    - "Paris, France"
    - "Los Angeles, California"
    """
    if not location_string:
        return None
    
    location_parts = location_string.split(', ')
    
    # Default values
    city = None
    state = None
    country = None
    postal_code = None
    
    # Common country abbreviations and full names mapping
    country_mapping = {
        'UAE': 'United Arab Emirates',
        'UK': 'United Kingdom',
        'USA': 'United States',
        'US': 'United States'
    }
    
    # Try to identify parts based on patterns
    if len(location_parts) == 1:
        # Just a city or country
        city = location_parts[0]
    elif len(location_parts) == 2:
        # Could be City, Country or City, State
        city = location_parts[0]
        
        # Check if second part is a known country
        second_part = location_parts[1]
        if second_part in country_mapping:
            country = country_mapping[second_part]
        else:
            # Check for US state pattern (e.g., "CA", "NY")
            if len(second_part) == 2 and second_part.isupper():
                state = second_part
                country = "United States"
            else:
                # Assume it's a country
                country = second_part
    elif len(location_parts) >= 3:
        # Likely City, State, Country format
        city = location_parts[0]
        state = location_parts[1]
        country = location_parts[2]
    
    # Return structured data
    return {
        'city': city,
        'state': state,
        'country': country,
        'postal_code': postal_code,
        'full_location': location_string
    }

def extract_social_media_links(text: str, links: List[str]) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract Instagram and TikTok links from text and links list.
    """
    instagram_link = None
    tiktok_link = None
    
    # Check provided links first
    for link in links:
        if not link:
            continue
            
        if 'instagram.com' in link or 'instagr.am' in link:
            instagram_link = link
        elif 'tiktok.com' in link or 'vm.tiktok.com' in link:
            tiktok_link = link
    
    # If not found, try to extract from text
    if not instagram_link and text:
        insta_patterns = [
            r'https?://(?:www\.)?instagram\.com/[A-Za-z0-9_.-]+/?',
            r'https?://(?:www\.)?instagr\.am/[A-Za-z0-9_.-]+/?'
        ]
        
        for pattern in insta_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                instagram_link = matches[0]
                break
        
        # Only if we still don't have a link, try looser patterns with more verification
        if not instagram_link:
            username_patterns = [
                r'ig:?\s*@?([A-Za-z0-9_.-]+)',
                r'instagram:?\s*@?([A-Za-z0-9_.-]+)',
                r'@([A-Za-z0-9_.-]+) on instagram'
            ]
            
            for pattern in username_patterns:
                matches = re.findall(pattern, text, re.IGNORECASE)
                if matches:
                    # Verify username is at least 3 chars and not just a common word
                    username = matches[0]
                    if len(username) >= 3 and not re.match(r'\b(and|the|for|from|with|this|that|have|has|her|his|our|my|your|their|its|as|at|by|to|in|on|of|or|if)\b', username, re.IGNORECASE):
                        instagram_link = f"https://www.instagram.com/{username}"
                        break
    
    # Similar for TikTok
    if not tiktok_link and text:
        tiktok_patterns = [
            r'https?://(?:www\.)?tiktok\.com/@?[A-Za-z0-9_.-]+/?',
            r'https?://(?:www\.)?vm\.tiktok\.com/[A-Za-z0-9_.-]+/?'
        ]
        
        for pattern in tiktok_patterns:
            matches = re.findall(pattern, text, re.IGNORECASE)
            if matches:
                tiktok_link = matches[0]
                break
        
        # Only if we still don't have a link, try looser patterns with more verification
        if not tiktok_link:
            username_patterns = [
                r'tt:?\s*@?([A-Za-z0-9_.-]+)',
                r'tiktok:?\s*@?([A-Za-z0-9_.-]+)',
                r'@([A-Za-z0-9_.-]+) on tiktok'
            ]
            
            for pattern in username_patterns:
                matches = re.findall(pattern, text, re.IGNORECASE)
                if matches:
                    # Verify username is at least 3 chars and not just a common word
                    username = matches[0]
                    if len(username) >= 3 and not re.match(r'\b(and|the|for|from|with|this|that|have|has|her|his|our|my|your|their|its|as|at|by|to|in|on|of|or|if)\b', username, re.IGNORECASE):
                        tiktok_link = f"https://www.tiktok.com/@{username}"
                        break
    
    # Check if username in the bio and profile is same as link username
    if instagram_link:
        # If it's derived from a weak pattern, verify it seems valid
        if "instagram.com" in instagram_link:
            username = re.search(r'instagram\.com/([A-Za-z0-9_.-]+)', instagram_link)
            if username and len(username.group(1)) < 3:
                instagram_link = None  # Too short to be valid
    
    if tiktok_link:
        # If it's derived from a weak pattern, verify it seems valid
        if "tiktok.com" in tiktok_link:
            username = re.search(r'tiktok\.com/@?([A-Za-z0-9_.-]+)', tiktok_link)
            if username and len(username.group(1)) < 3:
                tiktok_link = None  # Too short to be valid
    
    return instagram_link, tiktok_link

def analyze_user_info(user_info: dict, post_info: dict) -> dict:
    """Analyze user information from userInfo.json and enhance with data from posts."""
    user = user_info.get('data', {}).get('user', {})
    
    # Extract name parts from full name
    full_name = user.get('full_name', '')
    name_parts = full_name.split(' - ')[0].split(' ', 1) if ' - ' in full_name else full_name.split(' ', 1)
    first_name = name_parts[0] if name_parts else None
    last_name = name_parts[1] if len(name_parts) > 1 else None
    
    # Extract links
    bio_links = user.get('bio_links', [])
    other_links = [link.get('url') for link in bio_links if link.get('url')]
    
    # Extract bio for analysis
    bio = user.get('biography', '')
    
    # Get username directly for social media links
    username = user.get('username')
    
    # For Instagram, always use the username
    instagram_link = f"https://www.instagram.com/{username}" if username else None
    
    # For TikTok, try to detect from bio and links
    _, tiktok_link = extract_social_media_links(bio, other_links)
    
    # Filter out IG and TikTok links from other_links
    other_links = [link for link in other_links if (link != instagram_link and link != tiktok_link 
                                                   and 'instagram.com' not in link and 'tiktok.com' not in link)]
    
    # Determine creator size based on follower count
    follower_count = user.get('follower_count', 0)
    creator_size = None
    if follower_count:
        if follower_count < 5000:
            creator_size = "Nano-Influencer"
        elif follower_count < 50000:
            creator_size = "Micro-Influencer"
        elif follower_count < 500000:
            creator_size = "Mid-Tier Influencer"
        elif follower_count < 1000000:
            creator_size = "Macro-Influencer"
        else:
            creator_size = "Mega-Influencer"
    
    # Extract email, phone number from bio
    email = extract_email(bio)
    phone_number = extract_phone(bio)
    
    # Use the username for profile picture - local file path
    profile_picture = f"{username}.jpg"
    
    # Extract posts for collaboration and niche analysis
    posts = post_info.get('data', {}).get('xdt_api__v1__feed__user_timeline_graphql_connection', {}).get('edges', [])
    
    # Extract age from bio
    all_text = bio
    for post in posts:
        caption = post.get('node', {}).get('caption', {}).get('text', '')
        if caption:
            all_text += "\n" + caption
    
    age, age_group = extract_age(all_text)
    
    # First analyze niche so we can use it for gender detection
    niche_analysis = identify_niche(posts)
    primary_niche = niche_analysis.get("primary", "")
    
    # Get gender with enhanced detection using name, captions, and niche
    captions = [post.get('node', {}).get('caption', {}).get('text', '') for post in posts]
    gender, confidence = detect_gender_from_text(bio, captions, first_name, primary_niche)
    
    # Special case check for names we know are misclassified (like "Chris")
    if first_name and first_name.lower() == "chris" and gender == "Female":
        # Chris is typically a male name, especially for users like "authentic_traveling"
        gender = "Male"
        confidence = 0.85
    
    # Determine location-based status
    location_text = " ".join([
        bio,
        user.get('address_street', ''),
        user.get('city_name', '')
    ])
    creator_based_on = determine_location_based(location_text)
    
    # Enhanced US detection: Check each location field individually
    is_us_based = False
    
    # Create a single combined location string to check for US indicators
    combined_location = ""
    
    # Add bio text
    if bio:
        combined_location += bio + " "
    
    # Add address information
    if user.get('address_street'):
        combined_location += user.get('address_street', '') + " "
    if user.get('city_name'):
        combined_location += user.get('city_name', '') + " "
    if user.get('state'):
        combined_location += user.get('state', '') + " "
    if user.get('country'):
        combined_location += user.get('country', '') + " "
        
    # Add location information from top locations
    location_analysis = analyze_locations(posts)
    top_locations = location_analysis.get('top_locations', [])
    for loc in top_locations:
        if loc and isinstance(loc, dict) and loc.get('name'):
            combined_location += loc['name'] + " "
    
    # Check if any location indicates US
    if determine_location_based(combined_location) == "USA":
        creator_based_on = "USA"
    else:
        creator_based_on = "Global"
        
    # Override: Special case for "United States" in city field
    if user.get('city_name') == "United States":
        creator_based_on = "USA"
    
    # Analyze collaborations
    collaboration_analysis = identify_collaborations(posts)
    
    # Analyze locations
    location_analysis = analyze_locations(posts)
    
    # Calculate engagement rate
    total_likes = sum(post.get('node', {}).get('like_count', 0) for post in posts)
    total_comments = sum(post.get('node', {}).get('comment_count', 0) for post in posts)
    engagement_rate = 0
    if follower_count and len(posts) > 0:
        engagement_rate = ((total_likes + total_comments) / len(posts)) / follower_count * 100
    
    # Analyze audience demographics based on content
    # Set audience age groups based on more detailed age categories
    if age_group in ["18-24", "25-34"]:
        audience_age_groups = ["18-24", "25-29", "25-34"]
    elif age_group in ["35-44"]:
        audience_age_groups = ["25-34", "35-44"]
    elif age_group in ["Under 18"]:
        audience_age_groups = ["13-17", "18-24"]
    else:
        # Default based on niche
        if primary_niche in ["Gaming", "Technology"]:
            audience_age_groups = ["18-24", "25-29", "25-34"]
        elif primary_niche in ["Fashion & Style", "Beauty"]:
            audience_age_groups = ["18-24", "25-29", "25-34"]
        elif primary_niche in ["Travel", "Food"]:
            audience_age_groups = ["25-29", "25-34", "35-44"]
        elif primary_niche in ["Fitness"]:
            audience_age_groups = ["18-24", "25-29", "25-34"]
        else:
            audience_age_groups = ["18-24", "25-29", "25-34"]
    
    # Set gender ratio based on creator's gender and niche
    if gender == "Female":
        gender_ratio = {"female": 66, "male": 34}
    elif gender == "Male":
        gender_ratio = {"female": 34, "male": 66}
    elif primary_niche in ["Gaming", "Technology"]:
        gender_ratio = {"female": 30, "male": 70}
    elif primary_niche in ["Fashion & Style", "Beauty"]:
        gender_ratio = {"female": 70, "male": 30}
    elif primary_niche in ["Travel", "Food"]:
        gender_ratio = {"female": 55, "male": 45}
    elif primary_niche in ["Fitness"]:
        gender_ratio = {"female": 50, "male": 50}
    else:
        gender_ratio = {"female": 50, "male": 50}
    
    # Determine engagement quality
    engagement_quality = None
    if engagement_rate > 10:
        engagement_quality = "Excellent"
    elif engagement_rate > 5:
        engagement_quality = "Good"
    elif engagement_rate > 2:
        engagement_quality = "Average"
    elif engagement_rate > 0:
        engagement_quality = "Below Average"
    
    # Get address information from bio or use inferred location from posts
    street_address = user.get('address_street', '')
    city = user.get('city_name', '')
    state = user.get('state', '')
    country = user.get('country', '')
    postal_code = user.get('postal_code', '')
    
    # If we have a probable home location from post analysis, use it to fill in missing address fields
    home_location = location_analysis.get('home_location')
    if home_location:
        # Only use inferred location if there are multiple posts from there (greater confidence)
        if home_location.get('count', 0) >= 3:
            if not city and home_location.get('city'):
                city = home_location.get('city')
            
            if not state and home_location.get('state'):
                state = home_location.get('state')
                
            if not country and home_location.get('country'):
                country = home_location.get('country')
                
            # Update creator_based_on based on the inferred country if we don't have one
            if not creator_based_on and country:
                creator_based_on = country
    
    # Get business category from Instagram user data
    business_category = user.get('category', '')
    
    # Determine creator type based solely on business_category mapping
    creator_type = determine_creator_type(user_info, post_info, niche_analysis, bio)
    
    return {
        "collaboration_analysis": collaboration_analysis,
        "niche_analysis": niche_analysis,
        "location_analysis": location_analysis,
        "personal_details": {
            "first_name": first_name,
            "last_name": last_name,
            "user_name": username,
            "birth": None,
            "age": age,
            "gender": gender,
            "email": email,
            "phone_number": phone_number,
            "instagram_link": instagram_link,
            "tiktok_link": tiktok_link,
            "other_links": other_links,
            "address": {
                "street_address": street_address,
                "city": city,
                "state": state,
                "country": country,
                "postal_code": postal_code
            },
            "registration_date": None,
            "creator_size": creator_size,
            "creator_type": creator_type,
            "business_category": business_category,
            "profile_picture": profile_picture,
            "age_group": age_group,
            "creator_based_on": creator_based_on,
            "bio_data": bio  # Added raw bio data
        },
        "posting_frequency": {
            "overall_frequency": None,
            "days_since_last_post": None,
            "consistent_schedule": None,
            "best_days": [],
            "best_times": []
        },
        "audience_analysis": {
            "engagement_rate": round(engagement_rate, 2),
            "engagement_quality": engagement_quality,
            "follower_count": follower_count,
            "estimated_demographics": {
                "age_groups": audience_age_groups,
                "gender_ratio": gender_ratio
            }
        },
        "username": username
    }

def determine_creator_type(user_info: dict, post_info: dict = None, niche_analysis: dict = None, bio: str = None, override_creator_type: str = None) -> str:
    """
    Determine creator type based on category mapping in category_type_map.json
    
    Args:
        user_info: User information dictionary (may contain raw data or processed data)
        post_info: Post information dictionary (not used in this implementation)
        niche_analysis: Niche analysis dictionary (not used in this implementation)
        bio: Bio text string (not used in this implementation)
        override_creator_type: Override creator type if provided
        
    Returns:
        str: One of "Content Creator", "Business", or "BLANK"
    """
    # If override is provided, use it
    if override_creator_type is not None:
        return override_creator_type
        
    # Load category type map
    try:
        with open('category_type_map.json', 'r', encoding='utf-8') as f:
            category_type_map = json.load(f)
    except Exception as e:
        print(f"{Fore.RED}Error loading category_type_map.json: {str(e)}{Style.RESET_ALL}")
        category_type_map = {"creator": [], "business": []}
    
    # Extract category - check multiple possible locations
    category = ""
    
    # First, check if this is already processed data with business_category
    if 'business_category' in user_info:
        category = user_info.get('business_category', '')
    else:
        # This is likely raw Instagram data
        user = user_info.get('data', {}).get('user', {})
        category = user.get('category', '')
    
    # 1. If category is blank, return "Content Creator"
    if not category:
        return "Content Creator"
    
    # Convert category map lists to lowercase sets for case-insensitive matching
    creator_categories = {cat.lower() for cat in category_type_map.get("creator", [])}
    business_categories = {cat.lower() for cat in category_type_map.get("business", [])}
    
    # Lowercase the category for case-insensitive comparison
    category_lower = category.lower()
    
    # 2. If category is in creator list, return "Content Creator"
    if category_lower in creator_categories:
        return "Content Creator"
    
    # 3. If category is in business list, return "Business"
    if category_lower in business_categories:
        return "Business"
    
    # 4. If category is not blank and not in either list, return "BLANK"
    return "BLANK"

def analyze_post_info(post_info: dict, analysis: dict) -> dict:
    """Analyze post information and update metrics."""
    posts = post_info.get('data', {}).get('xdt_api__v1__feed__user_timeline_graphql_connection', {}).get('edges', [])
    
    if not posts:
        return analysis
    
    post_timestamps = []
    post_days = {}
    post_hours = {}
    
    for post in posts:
        node = post.get('node', {})
        timestamp = node.get('taken_at')
        if timestamp:
            post_timestamps.append(timestamp)
            dt = datetime.datetime.fromtimestamp(timestamp)
            
            # Track day of week
            day_name = dt.strftime('%A')
            post_days[day_name] = post_days.get(day_name, 0) + 1
            
            # Track hour of day
            hour = dt.hour
            post_hours[hour] = post_hours.get(hour, 0) + 1
    
    if post_timestamps:
        post_timestamps.sort(reverse=True)
        last_post_timestamp = post_timestamps[0]
        days_since_last_post = (datetime.datetime.now() - 
                              datetime.datetime.fromtimestamp(last_post_timestamp)).days
        
        # Calculate posting frequency
        if len(post_timestamps) > 1:
            time_diffs = []
            for i in range(len(post_timestamps) - 1):
                diff = post_timestamps[i] - post_timestamps[i + 1]
                time_diffs.append(diff / (24 * 3600))  # Convert to days
            
            avg_days_between_posts = sum(time_diffs) / len(time_diffs)
            
            # Convert numerical frequency to text description
            if avg_days_between_posts < 1:
                overall_frequency = "Multiple times daily"
            elif avg_days_between_posts < 2:
                overall_frequency = "Daily"
            elif avg_days_between_posts < 7:
                overall_frequency = "Weekly"
            elif avg_days_between_posts < 14:
                overall_frequency = "Bi-weekly"
            elif avg_days_between_posts < 31:
                overall_frequency = "Monthly"
            else:
                overall_frequency = "Infrequent"
            
            # Determine if schedule is consistent
            consistency_threshold = 2.0  # days
            consistent_schedule = True
            
            for diff in time_diffs:
                if abs(diff - avg_days_between_posts) > consistency_threshold:
                    consistent_schedule = False
                    break
        else:
            overall_frequency = "N/A"
            consistent_schedule = None
    
    # Find best posting days
    best_days = []
    if post_days:
        sorted_days = sorted(post_days.items(), key=lambda x: x[1], reverse=True)
        best_days = [day for day, _ in sorted_days[:3]]
    
    # Find best posting times
    best_times = []
    if post_hours:
        # Group hours into time ranges
        time_ranges = {
            "Early Morning (5am-8am)": range(5, 9),
            "Morning (8am-12pm)": range(9, 13),
            "Afternoon (12pm-5pm)": range(13, 18),
            "Evening (5pm-10pm)": range(18, 23),
            "Night (10pm-5am)": list(range(23, 24)) + list(range(0, 5))
        }
        
        # Count posts in each time range
        range_counts = {range_name: 0 for range_name in time_ranges}
        for hour, count in post_hours.items():
            for range_name, hours in time_ranges.items():
                if hour in hours:
                    range_counts[range_name] += count
        
        # Get top time ranges
        sorted_ranges = sorted(range_counts.items(), key=lambda x: x[1], reverse=True)
        best_times = [time_range for time_range, _ in sorted_ranges[:2] if _ > 0]
    
    # Update posting frequency info
    analysis['posting_frequency'].update({
        "overall_frequency": overall_frequency,
        "days_since_last_post": days_since_last_post,
        "consistent_schedule": consistent_schedule,
        "best_days": best_days,
        "best_times": best_times
    })
    
    return analysis

def analyze_creator_data(creator_dir: str) -> Optional[dict]:
    """Analyze all data for a single creator."""
    try:
        user_info_path = os.path.join(creator_dir, 'userInfo.json')
        post_info_path = os.path.join(creator_dir, 'postInfo.json')
        
        if not os.path.exists(user_info_path) or not os.path.exists(post_info_path):
            print(f"{Fore.RED}Missing required files in {creator_dir}{Style.RESET_ALL}")
            return None
        
        # Get creation time of userInfo.json file for scraped_date_time
        scraped_timestamp = os.path.getctime(user_info_path)
        scraped_date_time = datetime.datetime.fromtimestamp(scraped_timestamp).strftime('%Y-%m-%d %H:%M:%S')
        
        # Get current time for analyzed_date_time
        analyzed_date_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        user_info = load_json_file(user_info_path)
        post_info = load_json_file(post_info_path)
        
        # Start with user info and post analysis together
        analysis = analyze_user_info(user_info, post_info)
        
        # Update with detailed post info analysis
        analysis = analyze_post_info(post_info, analysis)
        
        # Add the datetime fields
        analysis['scraped_date_time'] = scraped_date_time
        analysis['analyzed_date_time'] = analyzed_date_time
        
        # Apply geo-based location analysis
        # Check if we have location data with coordinates
        location_analysis = analysis.get("location_analysis", {})
        home_location = location_analysis.get("home_location", {})
        
        if home_location and home_location.get("coordinates"):
            # Get geo-based location determination
            geo_based_location = determine_location_based_on_geo(home_location.get("coordinates"))
            
            # If geo coordinates indicate USA, override the creator_based_on
            if geo_based_location == "USA":
                analysis["personal_details"]["creator_based_on"] = "USA"
        
        return analysis
    except Exception as e:
        print(f"{Fore.RED}Error analyzing creator data in {creator_dir}: {str(e)}{Style.RESET_ALL}")
        return None

def get_optimal_process_count():
    """
    Determine the optimal number of processes based on system resources.
    Returns a reasonable number of processes to use for parallel processing.
    """
    # Get CPU count
    cpu_count = multiprocessing.cpu_count()
    
    # Use 75% of available CPUs to leave resources for other system processes
    # But ensure at least 1 process is used
    optimal_count = max(1, int(cpu_count * 0.75))
    
    # Cap at a reasonable maximum to prevent excessive resource usage
    max_processes = 16  # Arbitrary cap, adjust based on expected workload
    optimal_count = min(optimal_count, max_processes)
    
    return optimal_count

def process_creator_batch(creator_batch, base_path, batch_id, total_creators):
    """Process a batch of creators and return their analyses."""
    batch_results = []
    successful = 0
    skipped = 0
    errors = 0
    
    # Create a progress bar for this batch
    batch_desc = f"Batch {batch_id}"
    with tqdm(total=len(creator_batch), desc=batch_desc, position=batch_id, leave=False, 
              bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]") as pbar:
        
        for creator in creator_batch:
            try:
                creator_path = os.path.join(base_path, creator)
                
                # Check for required files
                user_info_path = os.path.join(creator_path, "userInfo.json")
                post_info_path = os.path.join(creator_path, "postInfo.json")
                
                if not os.path.exists(user_info_path) or not os.path.exists(post_info_path):
                    skipped += 1
                    pbar.set_postfix(successful=successful, skipped=skipped, errors=errors)
                    pbar.update(1)
                    continue
                
                try:
                    # Load data
                    user_info = load_json_file(user_info_path)
                    post_info = load_json_file(post_info_path)
                    
                    # Analyze creator
                    analysis = analyze_user_info(user_info, post_info)
                    
                    # Update with detailed post info analysis
                    analysis = analyze_post_info(post_info, analysis)
                    
                    # Get proper file creation time cross-platform (Windows, macOS, Linux)
                    try:
                        # Platform-specific approach to get true creation time
                        creation_time = None
                        
                        # On Windows, use platform-specific API via ctypes
                        if os.name == 'nt':
                            import ctypes
                            from ctypes import windll, wintypes, byref
                            
                            ctime = wintypes.FILETIME()
                            atime = wintypes.FILETIME()
                            mtime = wintypes.FILETIME()
                            
                            handle = windll.kernel32.CreateFileW(
                                user_info_path, 0x80000000, # GENERIC_READ
                                0x3, # FILE_SHARE_READ | FILE_SHARE_WRITE
                                None, 3, # OPEN_EXISTING
                                0x02000000, # FILE_FLAG_BACKUP_SEMANTICS
                                None
                            )
                            
                            if handle != -1:
                                result = windll.kernel32.GetFileTime(handle, byref(ctime), byref(atime), byref(mtime))
                                windll.kernel32.CloseHandle(handle)
                                if result:
                                    # Convert Windows FILETIME to UNIX timestamp
                                    # FILETIME is 100-nanosecond intervals since January 1, 1601
                                    # Need to convert to seconds and adjust for UNIX epoch (January 1, 1970)
                                    creation_time = ((ctime.dwHighDateTime << 32) | ctime.dwLowDateTime) / 10000000 - 11644473600
                        
                        # On macOS, use st_birthtime
                        elif sys.platform == 'darwin':
                            file_stat = os.stat(user_info_path)
                            if hasattr(file_stat, 'st_birthtime'):
                                creation_time = file_stat.st_birthtime
                        
                        # If we couldn't get true creation time with platform-specific methods
                        # Fall back to earliest of ctime/mtime (imperfect, but best available)
                        if creation_time is None:
                            file_stat = os.stat(user_info_path)
                            creation_time = min(file_stat.st_ctime, file_stat.st_mtime)
                        
                        analysis['scraped_date_time'] = datetime.datetime.fromtimestamp(creation_time).strftime('%Y-%m-%d %H:%M:%S')
                    
                    except Exception as e:
                        print(f"Error getting file creation time for {user_info_path}: {e}")
                        # Use a fallback date if metadata can't be read
                        analysis['scraped_date_time'] = "2025-06-26 23:29:00" # Fallback to known scrape date
                    
                    # Add analyzed_date_time with current local time
                    analysis['analyzed_date_time'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    
                    # Apply geo-based location analysis
                    location_analysis = analysis.get("location_analysis", {})
                    home_location = location_analysis.get("home_location", {})
                    
                    if home_location and home_location.get("coordinates"):
                        # Get geo-based location determination
                        geo_based_location = determine_location_based_on_geo(home_location.get("coordinates"))
                        
                        # If geo coordinates indicate USA, override the creator_based_on
                        if geo_based_location == "USA":
                            analysis["personal_details"]["creator_based_on"] = "USA"
                    
                    # Special case verification: If United States appears in any location field, ensure creator_based_on is "USA"
                    personal_details = analysis.get("personal_details", {})
                    address = personal_details.get("address", {})
                    
                    # Check for "United States" or US indicators in specific fields
                    location_fields = [
                        personal_details.get("bio_data", ""),
                        address.get("street_address", ""),
                        address.get("city", ""),
                        address.get("state", ""),
                        address.get("country", "")
                    ]
                    
                    # Look for United States in any field
                    us_indicator_found = False
                    for field in location_fields:
                        if field and isinstance(field, str):
                            # Check for direct US indicators
                            if any(term in field.lower() for term in ["united states", "usa", "america", "us", "u.s.a", "u.s."]):
                                us_indicator_found = True
                                break
                    
                    # Check for US state abbreviations and city names
                    bio_data = personal_details.get("bio_data", "")
                    us_states = ["al", "ak", "az", "ar", "ca", "co", "ct", "de", "fl", "ga", "hi", "id", "il", "in", "ia", 
                               "ks", "ky", "la", "me", "md", "ma", "mi", "mn", "ms", "mo", "mt", "ne", "nv", "nh", "nj", 
                               "nm", "ny", "nc", "nd", "oh", "ok", "or", "pa", "ri", "sc", "sd", "tn", "tx", "ut", "vt", 
                               "va", "wa", "wv", "wi", "wy"]
                               
                    us_cities = ["new york", "los angeles", "chicago", "houston", "phoenix", "philadelphia",
                                "san antonio", "san diego", "dallas", "san jose", "austin", "jacksonville",
                                "fort worth", "columbus", "san francisco", "charlotte", "indianapolis",
                                "seattle", "denver", "washington dc", "boston", "nashville", "vegas", "nyc", "la"]
                    
                    if bio_data and isinstance(bio_data, str):
                        bio_lower = bio_data.lower()
                        # Check for state abbreviations with word boundaries
                        for state in us_states:
                            if re.search(r'\b{}\b'.format(state), bio_lower):
                                us_indicator_found = True
                                break
                                
                        # Check for major US cities
                        if not us_indicator_found:
                            for city in us_cities:
                                if re.search(r'\b{}\b'.format(city), bio_lower):
                                    us_indicator_found = True
                                    break
                    
                    # Special case: Direct match for "United States" in city field
                    if address.get("city") == "United States":
                        us_indicator_found = True
                    
                    # Override creator_based_on if US indicator found
                    if us_indicator_found:
                        personal_details["creator_based_on"] = "USA"
                    
                    # Save individual analysis
                    output_path = os.path.join(creator_path, "analyzed.json")
                    save_json_file(analysis, output_path)
                    
                    # Add to batch results
                    batch_results.append(analysis)
                    successful += 1
                    
                except (json.JSONDecodeError, KeyError, TypeError, ValueError) as e:
                    errors += 1
                
            except Exception as e:
                errors += 1
            
            finally:
                pbar.set_postfix(successful=successful, skipped=skipped, errors=errors)
                pbar.update(1)
    
    return batch_results, successful, skipped, errors

def print_header():
    """Print a beautiful header for the script."""
    header = f"""
{Fore.CYAN}╔══════════════════════════════════════════════════════════════════════════╗
{Fore.CYAN}║                                                                          ║
{Fore.CYAN}║  {Fore.YELLOW}██{Fore.WHITE}╗{Fore.YELLOW}███{Fore.WHITE}╗   {Fore.YELLOW}██{Fore.WHITE}╗{Fore.YELLOW}███████{Fore.WHITE}╗{Fore.YELLOW}████████{Fore.WHITE}╗ {Fore.YELLOW}█████{Fore.WHITE}╗      {Fore.YELLOW}█████{Fore.WHITE}╗ {Fore.YELLOW}███{Fore.WHITE}╗   {Fore.YELLOW}██{Fore.WHITE}╗ {Fore.YELLOW}█████{Fore.WHITE}╗ {Fore.YELLOW}██{Fore.WHITE}╗     {Fore.YELLOW}██{Fore.WHITE}╗   {Fore.YELLOW}██{Fore.WHITE}╗{Fore.YELLOW}███████{Fore.WHITE}╗{Fore.YELLOW}██████{Fore.WHITE}╗   ║
{Fore.CYAN}║  {Fore.YELLOW}██{Fore.WHITE}║{Fore.YELLOW}████{Fore.WHITE}╗  {Fore.YELLOW}██{Fore.WHITE}║{Fore.YELLOW}██{Fore.WHITE}╔════╝╚══{Fore.YELLOW}██{Fore.WHITE}╔══╝{Fore.YELLOW}██{Fore.WHITE}╔══{Fore.YELLOW}██{Fore.WHITE}╗    {Fore.YELLOW}██{Fore.WHITE}╔══{Fore.YELLOW}██{Fore.WHITE}╗{Fore.YELLOW}████{Fore.WHITE}╗  {Fore.YELLOW}██{Fore.WHITE}║{Fore.YELLOW}██{Fore.WHITE}╔══{Fore.YELLOW}██{Fore.WHITE}║{Fore.YELLOW}██{Fore.WHITE}║     {Fore.YELLOW}╚██{Fore.WHITE}╗ {Fore.YELLOW}██{Fore.WHITE}╔╝{Fore.YELLOW}██{Fore.WHITE}╔════╝{Fore.YELLOW}██{Fore.WHITE}╔══{Fore.YELLOW}██{Fore.WHITE}╗  ║
{Fore.CYAN}║  {Fore.YELLOW}██{Fore.WHITE}║{Fore.YELLOW}██{Fore.WHITE}╔{Fore.YELLOW}██{Fore.WHITE}╗ {Fore.YELLOW}██{Fore.WHITE}║{Fore.YELLOW}███████{Fore.WHITE}╗   {Fore.YELLOW}██{Fore.WHITE}║   {Fore.YELLOW}███████{Fore.WHITE}║    {Fore.YELLOW}███████{Fore.WHITE}║{Fore.YELLOW}██{Fore.WHITE}╔{Fore.YELLOW}██{Fore.WHITE}╗ {Fore.YELLOW}██{Fore.WHITE}║{Fore.YELLOW}███████{Fore.WHITE}║{Fore.YELLOW}██{Fore.WHITE}║      ╚{Fore.YELLOW}████{Fore.WHITE}╔╝ {Fore.YELLOW}█████{Fore.WHITE}╗  {Fore.YELLOW}██████{Fore.WHITE}╔╝  ║
{Fore.CYAN}║  {Fore.YELLOW}██{Fore.WHITE}║{Fore.YELLOW}██{Fore.WHITE}║╚{Fore.YELLOW}██{Fore.WHITE}╗{Fore.YELLOW}██{Fore.WHITE}║╚════{Fore.YELLOW}██{Fore.WHITE}║   {Fore.YELLOW}██{Fore.WHITE}║   {Fore.YELLOW}██{Fore.WHITE}╔══{Fore.YELLOW}██{Fore.WHITE}║    {Fore.YELLOW}██{Fore.WHITE}╔══{Fore.YELLOW}██{Fore.WHITE}║{Fore.YELLOW}██{Fore.WHITE}║╚{Fore.YELLOW}██{Fore.WHITE}╗{Fore.YELLOW}██{Fore.WHITE}║{Fore.YELLOW}██{Fore.WHITE}╔══{Fore.YELLOW}██{Fore.WHITE}║{Fore.YELLOW}██{Fore.WHITE}║       ╚{Fore.YELLOW}██{Fore.WHITE}╔╝  {Fore.YELLOW}██{Fore.WHITE}╔══╝  {Fore.YELLOW}██{Fore.WHITE}╔══{Fore.YELLOW}██{Fore.WHITE}╗  ║
{Fore.CYAN}║  {Fore.YELLOW}██{Fore.WHITE}║{Fore.YELLOW}██{Fore.WHITE}║ ╚{Fore.YELLOW}████{Fore.WHITE}║{Fore.YELLOW}███████{Fore.WHITE}║   {Fore.YELLOW}██{Fore.WHITE}║   {Fore.YELLOW}██{Fore.WHITE}║  {Fore.YELLOW}██{Fore.WHITE}║    {Fore.YELLOW}██{Fore.WHITE}║  {Fore.YELLOW}██{Fore.WHITE}║{Fore.YELLOW}██{Fore.WHITE}║ ╚{Fore.YELLOW}████{Fore.WHITE}║{Fore.YELLOW}██{Fore.WHITE}║  {Fore.YELLOW}██{Fore.WHITE}║{Fore.YELLOW}███████{Fore.WHITE}╗   {Fore.YELLOW}██{Fore.WHITE}║   {Fore.YELLOW}███████{Fore.WHITE}╗{Fore.YELLOW}██{Fore.WHITE}║  {Fore.YELLOW}██{Fore.WHITE}║  ║
{Fore.CYAN}║  {Fore.WHITE}╚═╝╚═╝  ╚═══╝╚══════╝   ╚═╝   ╚═╝  ╚═╝    ╚═╝  ╚═╝╚═╝  ╚═══╝╚═╝  ╚═╝╚══════╝   ╚═╝   ╚══════╝╚═╝  ╚═╝  ║
{Fore.CYAN}║                                                                          ║
{Fore.CYAN}╚══════════════════════════════════════════════════════════════════════════╝
{Style.RESET_ALL}"""
    print(header)
    print(f"{Fore.CYAN}╔══════════════════════════════════════════════════════════════════════════╗")
    print(f"{Fore.CYAN}║ {Fore.WHITE}TikTok Data Analyzer v2.0                                            {Fore.CYAN}║")
    print(f"{Fore.CYAN}║ {Fore.WHITE}Parallel Processing & Geo-based Location Analysis                    {Fore.CYAN}║")
    print(f"{Fore.CYAN}╚══════════════════════════════════════════════════════════════════════════╝{Style.RESET_ALL}")
    print()

def print_system_info(cpu_count, process_count):
    """Print system information."""
    print(f"{Fore.CYAN}╔══════════════════════════════════════════════════════════════════════════╗")
    print(f"{Fore.CYAN}║ {Fore.WHITE}System Information:                                                  {Fore.CYAN}║")
    print(f"{Fore.CYAN}║ {Fore.WHITE}• CPU Cores Detected: {Fore.GREEN}{cpu_count:<43}{Fore.CYAN}║")
    print(f"{Fore.CYAN}║ {Fore.WHITE}• Parallel Processes: {Fore.GREEN}{process_count:<43}{Fore.CYAN}║")
    print(f"{Fore.CYAN}╚══════════════════════════════════════════════════════════════════════════╝{Style.RESET_ALL}")
    print()

def print_summary(total_creators, successful, skipped, errors, elapsed_time):
    """Print a summary of the analysis."""
    success_rate = (successful / total_creators) * 100 if total_creators > 0 else 0
    
    print(f"\n{Fore.CYAN}╔══════════════════════════════════════════════════════════════════════════╗")
    print(f"{Fore.CYAN}║ {Fore.WHITE}Analysis Summary:                                                    {Fore.CYAN}║")
    print(f"{Fore.CYAN}║ {Fore.WHITE}• Total Creators: {Fore.GREEN}{total_creators:<45}{Fore.CYAN}║")
    print(f"{Fore.CYAN}║ {Fore.WHITE}• Successfully Analyzed: {Fore.GREEN}{successful:<39}{Fore.CYAN}║")
    print(f"{Fore.CYAN}║ {Fore.WHITE}• Skipped (Missing Files): {Fore.YELLOW}{skipped:<36}{Fore.CYAN}║")
    print(f"{Fore.CYAN}║ {Fore.WHITE}• Errors: {Fore.RED}{errors:<51}{Fore.CYAN}║")
    print(f"{Fore.CYAN}║ {Fore.WHITE}• Success Rate: {Fore.GREEN}{success_rate:.2f}%{' ' * 41}{Fore.CYAN}║")
    print(f"{Fore.CYAN}║ {Fore.WHITE}• Total Time: {Fore.GREEN}{elapsed_time:.2f} seconds{' ' * 36}{Fore.CYAN}║")
    print(f"{Fore.CYAN}╚══════════════════════════════════════════════════════════════════════════╝{Style.RESET_ALL}")

def main():
    """Process all creators and aggregate the results."""
    try:
        import sys
        import csv
        
        # Print header
        print_header()
        
        start_time = time.time()
        
        # Get base path for creator folders
        base_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")
        
        # Check if CSV file is provided as command-line argument
        if len(sys.argv) > 1 and sys.argv[1].endswith('.csv'):
            csv_file_path = sys.argv[1]
            print(f"{Fore.CYAN}Using input file: {Fore.YELLOW}{csv_file_path}{Style.RESET_ALL}")
            
            # Read creator usernames from CSV file
            creator_usernames = []
            try:
                with open(csv_file_path, 'r', encoding='utf-8') as csv_file:
                    csv_reader = csv.reader(csv_file)
                    # Skip header row if present
                    next(csv_reader, None)  # Skip header
                    
                    for row in csv_reader:
                        if row and row[0]:
                            # Extract username from URL if needed
                            url = row[0].strip()
                            username = url.split('/')[-1]  # Get last part of URL
                            creator_usernames.append(username)
                
                print(f"{Fore.GREEN}Found {len(creator_usernames)} creators in the CSV file.{Style.RESET_ALL}")
                
                # Filter creator folders to only include those in the CSV
                all_creator_folders = [f for f in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, f))]
                
                # Create mapping to maintain CSV order
                creator_folders = []
                missing_creators = []
                for username in creator_usernames:
                    if username in all_creator_folders:
                        creator_folders.append(username)
                    else:
                        print(f"{Fore.YELLOW}Warning: Creator '{username}' not found in Output folder. Adding empty placeholder.{Style.RESET_ALL}")
                        missing_creators.append(username)
                
            except Exception as e:
                print(f"{Fore.RED}Error reading CSV file: {str(e)}{Style.RESET_ALL}")
                return
                
        else:
            # Default: Get all creator folders
            creator_folders = [f for f in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, f))]
        
        if not creator_folders:
            print(f"{Fore.RED}No creator folders found.{Style.RESET_ALL}")
            return
        
        # Determine optimal number of processes based on system resources
        cpu_count = multiprocessing.cpu_count()
        process_count = get_optimal_process_count()
        
        # Print system information
        print_system_info(cpu_count, process_count)
        
        # Print analysis information
        total_creators = len(creator_folders)
        print(f"{Fore.CYAN}╔══════════════════════════════════════════════════════════════════════════╗")
        print(f"{Fore.CYAN}║ {Fore.WHITE}Analysis Information:                                                {Fore.CYAN}║")
        print(f"{Fore.CYAN}║ {Fore.WHITE}• Total Creator Folders: {Fore.GREEN}{total_creators:<38}{Fore.CYAN}║")
        print(f"{Fore.CYAN}╚══════════════════════════════════════════════════════════════════════════╝{Style.RESET_ALL}")
        print()
        
        # Split creators into batches for parallel processing
        creator_batches = []
        batch_size = max(1, len(creator_folders) // process_count)
        
        for i in range(0, len(creator_folders), batch_size):
            creator_batches.append(creator_folders[i:i + batch_size])
        
        print(f"{Fore.CYAN}Starting analysis with {Fore.GREEN}{process_count}{Fore.CYAN} parallel processes...")
        print(f"{Fore.CYAN}Each process will analyze approximately {Fore.GREEN}{batch_size}{Fore.CYAN} creators.")
        print(f"{Fore.YELLOW}Progress bars will show real-time status for each batch.{Style.RESET_ALL}")
        print()
        
        # Create a process pool and process creators in parallel
        all_analyses = []
        total_successful = 0
        total_skipped = 0
        total_errors = 0
        
        # Use context manager to ensure proper cleanup
        with multiprocessing.Pool(processes=process_count) as pool:
            # Create tasks with different batches
            tasks = [(batch, base_path, i, total_creators) for i, batch in enumerate(creator_batches)]
            
            # Map tasks to process_creator_batch function
            results = pool.starmap(process_creator_batch, tasks)
            
            # Flatten results and collect statistics
            for batch_result, successful, skipped, errors in results:
                all_analyses.extend(batch_result)
                total_successful += successful
                total_skipped += skipped
                total_errors += errors
                
            # Add empty placeholder entries for missing creators
            missing_count = 0
            if len(sys.argv) > 1 and sys.argv[1].endswith('.csv'):
                missing_count = len(missing_creators)
                for username in missing_creators:
                    # Create placeholder JSON for missing creators
                    empty_analysis = {
                        "collaboration_analysis": {
                            "brands": [],
                            "potential_collaborations": []
                        },
                        "niche_analysis": {
                            "primary": "",
                            "secondary": [],
                            "hashtags": []
                        },
                        "location_analysis": {
                            "locations": [],
                            "home_location": None
                        },
                        "personal_details": {
                            "first_name": "",
                            "last_name": "",
                            "user_name": username,
                            "birth": None,
                            "age": None,
                            "gender": "",
                            "email": None,
                            "phone_number": None,
                            "instagram_link": f"https://www.instagram.com/{username}",
                            "tiktok_link": None,
                            "other_links": [],
                            "address": {
                                "street_address": "",
                                "city": "",
                                "state": "",
                                "country": "",
                                "postal_code": ""
                            },
                            "registration_date": None,
                            "creator_size": "Unknown",
                            "creator_type": "Unknown",
                            "business_category": None,
                            "profile_picture": "",
                            "age_group": "",
                            "creator_based_on": "",
                            "bio_data": ""
                        },
                        "posting_frequency": {
                            "overall_frequency": None,
                            "days_since_last_post": None,
                            "consistent_schedule": None,
                            "best_days": [],
                            "best_times": []
                        },
                        "audience_analysis": {
                            "engagement_rate": 0,
                            "engagement_quality": None,
                            "follower_count": 0,
                            "estimated_demographics": {
                                "age_groups": [],
                                "gender_ratio": {"female": 50, "male": 50}
                            }
                        },
                        "username": username,
                        "missing_data": True
                    }
                    all_analyses.append(empty_analysis)
                
                # Update statistics to include missing creators
                total_creators += missing_count
                total_successful += missing_count  # Count them as successful
        
        elapsed_time = time.time() - start_time
        
        if all_analyses:
            # Save master analysis
            master_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "master_analyzed.json")
            save_json_file(all_analyses, master_path)
            
            # Print summary
            print_summary(total_creators, total_successful, total_skipped, total_errors, elapsed_time)
            
            # Print success message
            print(f"\n{Fore.GREEN}Master analysis saved to: {Fore.YELLOW}{os.path.abspath(master_path)}{Style.RESET_ALL}")
            
            # Print creator statistics
            usa_creators = sum(1 for creator in all_analyses if creator.get("personal_details", {}).get("creator_based_on") == "USA")
            global_creators = sum(1 for creator in all_analyses if creator.get("personal_details", {}).get("creator_based_on") == "Global")
            
            print(f"\n{Fore.CYAN}╔══════════════════════════════════════════════════════════════════════════╗")
            print(f"{Fore.CYAN}║ {Fore.WHITE}Creator Statistics:                                                 {Fore.CYAN}║")
            print(f"{Fore.CYAN}║ {Fore.WHITE}• USA-based Creators: {Fore.GREEN}{usa_creators:<41}{Fore.CYAN}║")
            print(f"{Fore.CYAN}║ {Fore.WHITE}• Global Creators: {Fore.GREEN}{global_creators:<43}{Fore.CYAN}║")
            print(f"{Fore.CYAN}╚══════════════════════════════════════════════════════════════════════════╝{Style.RESET_ALL}")
            
            # Print creator size distribution
            creator_sizes = {}
            for creator in all_analyses:
                size = creator.get("personal_details", {}).get("creator_size", "Unknown")
                creator_sizes[size] = creator_sizes.get(size, 0) + 1
            
            print(f"\n{Fore.CYAN}╔══════════════════════════════════════════════════════════════════════════╗")
            print(f"{Fore.CYAN}║ {Fore.WHITE}Creator Size Distribution:                                          {Fore.CYAN}║")
            for size, count in sorted(creator_sizes.items(), key=lambda x: x[1], reverse=True):
                if size:
                    percentage = (count / len(all_analyses)) * 100
                    print(f"{Fore.CYAN}║ {Fore.WHITE}• {size}: {Fore.GREEN}{count} {Fore.WHITE}({percentage:.1f}%){' ' * (50 - len(size) - len(str(count)) - 7)}{Fore.CYAN}║")
            print(f"{Fore.CYAN}╚══════════════════════════════════════════════════════════════════════════╝{Style.RESET_ALL}")
            
        else:
            print(f"\n{Fore.RED}No analyses were generated.{Style.RESET_ALL}")
            
    except Exception as e:
        print(f"\n{Fore.RED}An error occurred in the main function: {str(e)}{Style.RESET_ALL}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
