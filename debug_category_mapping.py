#!/usr/bin/env python3
import json
import os
from colorama import init, Fore, Style

# Initialize colorama
init(autoreset=True)

def load_json_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"{Fore.RED}Error loading file {file_path}: {str(e)}{Style.RESET_ALL}")
        return None

def main():
    # Load the master analyzed data
    master_data = load_json_file('master_analyzed.json')
    if not master_data:
        return
    
    # Load category type map
    category_type_map = load_json_file('category_type_map.json')
    if not category_type_map:
        return
    
    # Convert lists to sets for faster lookups
    creator_categories = set(category_type_map.get("creator", []))
    business_categories = set(category_type_map.get("business", []))
    
    print(f"{Fore.CYAN}{'Username':<30} | {'Category':<40} | {'Creator Type':<20} | {'Expected Type':<15} | {'Match?'}")
    print("-" * 120)
    
    match_count = 0
    mismatch_count = 0
    blank_count = 0
    
    for user in master_data:
        if "creator_type" in user:
            username = user.get("username", "N/A")
            category = user.get("business_category", "")
            creator_type = user.get("creator_type", "N/A")
            
            # Determine expected type based on category_type_map
            expected_type = "BLANK"
            if not category:
                expected_type = "Content Creator"
            elif category in creator_categories:
                expected_type = "Content Creator"
            elif category in business_categories:
                expected_type = "Business"
            
            match = creator_type == expected_type
            match_status = f"{Fore.GREEN}✓" if match else f"{Fore.RED}✗"
            
            # Count matches/mismatches
            if match:
                match_count += 1
            else:
                mismatch_count += 1
                
            # Count blank categories
            if expected_type == "BLANK" and category:
                blank_count += 1
            
            print(f"{username:<30} | {category[:38]:<40} | {creator_type:<20} | {expected_type:<15} | {match_status}")
    
    print("\n" + "=" * 120)
    print(f"{Fore.YELLOW}Summary:")
    print(f"{Fore.GREEN}Matches: {match_count}")
    print(f"{Fore.RED}Mismatches: {mismatch_count}")
    print(f"{Fore.BLUE}Categories resulting in BLANK: {blank_count}")
    
    # Print some examples of categories that aren't in the mapping
    unmapped_categories = set()
    for user in master_data:
        category = user.get("business_category", "")
        if category and category not in creator_categories and category not in business_categories:
            unmapped_categories.add(category)
    
    print(f"\n{Fore.YELLOW}Sample of unmapped categories (max 10):")
    for i, category in enumerate(list(unmapped_categories)[:10]):
        print(f"{i+1}. {category}")

if __name__ == "__main__":
    main()
