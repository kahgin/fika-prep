import pandas as pd
import os
import glob
from collections import defaultdict

os.chdir('/home/kahgin/fika/fika-prep')

INPUT_DIR = 'data/map'

SAVE_TO_FILE = False

# csv_files = glob.glob(os.path.join(INPUT_DIR, "**", "*.csv"), recursive=True) # recursively find CSV files
csv_files = glob.glob(os.path.join(INPUT_DIR, "*.csv"), recursive=True)

def extract_country(address_str):
    """Extract country code from complete_address field"""
    if pd.isna(address_str):
        return None
    
    address_str = str(address_str)
    
    if '"country":"SG"' in address_str or '"countryCode":"SG"' in address_str:
        return 'SG'
    elif '"country":"MY"' in address_str or '"countryCode":"MY"' in address_str:
        return 'MY'
    # elif '"country":"' in address_str:
    #     start_idx = address_str.find('"country":"') + 11
    #     if start_idx > 10:
    #         end_idx = address_str.find('"', start_idx)
    #         if end_idx > start_idx:
    #             return address_str[start_idx:end_idx]
    return None

def clean_df(file_path):
    pois = pd.read_csv(file_path)
    
    # Extract country from address
    pois['country'] = pois['complete_address'].apply(extract_country)
    
    # Add helper columns
    pois['base_name'] = pois['title'].apply(lambda x: x.split(' ')[0].strip().lower() if ' ' in str(x) else str(x).lower())
    # pois['base_name'] = pois['title'].apply(lambda x: str(x).lower())
    pois['is_flagship'] = pois['title'].str.contains('flagship', case=False, na=False)
    pois['exceeds_threshold'] = (pois['review_count'] > 50) & (pois['review_rating'] >= 3.0)
    
    # Create a combined key for duplicate detection: brand + country
    # Only consider entries with valid country for duplicate detection
    pois['brand_country_key'] = pois.apply(
        lambda row: f"{row['base_name']}_{row['country']}" 
        if row['country'] is not None else None, 
        axis=1
    )
    
    # Mark duplicates based on brand_country_key (within same country)
    pois['is_duplicate'] = pois.duplicated(subset='brand_country_key', keep=False) & pois['brand_country_key'].notna()
    
    # Get duplicate groups (now grouped by brand within same country)
    duplicate_groups = pois[pois['is_duplicate']].groupby('brand_country_key')

    KEEP_INDICES = []
    REMOVE_INDICES = []
    LOG = []

    # Process each duplicate brand-country group
    for brand_country_key, group in duplicate_groups:
        if len(group) == 1:
            continue  # Not actually duplicate after grouping
        
        base_name, country = brand_country_key.split('_', 1)
        
        # Skip specific names if needed
        if base_name in ['restoran al', 'international', 'traditional', 'cornerstone' ] or len(base_name) <= 5:
            continue

        # Separate by threshold
        above_threshold = group[group['exceeds_threshold']]
        below_threshold = group[~group['exceeds_threshold']]
        
        # Keep ALL above-threshold entries
        if not above_threshold.empty:
            for idx, row in above_threshold.iterrows():
                KEEP_INDICES.append(idx)
                LOG.append((brand_country_key, idx, row['title'], row['country'], "KEEP", "exceeds threshold", True, False))
        
        # For below-threshold, keep only if flagship, otherwise keep best one
        if not below_threshold.empty:
            # Check for flagship
            flagship_entries = below_threshold[below_threshold['is_flagship']]
            non_flagship = below_threshold[~below_threshold['is_flagship']]
            
            # Keep ALL flagship entries (even below threshold)
            for idx, row in flagship_entries.iterrows():
                KEEP_INDICES.append(idx)
                LOG.append((brand_country_key, idx, row['title'], row['country'], "KEEP", "flagship", False, True))
            
            # For non-flagship below-threshold, keep only the best one
            if not non_flagship.empty:
                # Sort to find best: highest reviews, then highest rating
                best_entry = non_flagship.sort_values(
                    ['review_count', 'review_rating'], 
                    ascending=[False, False]
                ).iloc[0]
                
                best_idx = best_entry.name
                KEEP_INDICES.append(best_idx)
                LOG.append((brand_country_key, best_idx, best_entry['title'], best_entry['country'], "KEEP", "best below-threshold", False, False))
                
                # Remove other non-flagship below-threshold entries
                for idx, row in non_flagship.iterrows():
                    if idx != best_idx:
                        REMOVE_INDICES.append(idx)
                        LOG.append((brand_country_key, idx, row['title'], row['country'], "REMOVE", "not best below-threshold", False, False))

    # Keep all non-duplicate entries
    non_duplicate_mask = ~pois['is_duplicate']
    non_duplicate_indices = pois[non_duplicate_mask].index.tolist()
    KEEP_INDICES.extend(non_duplicate_indices)

    # Apply filtering
    pois_cleaned = pois.loc[list(set(KEEP_INDICES))].copy()

    # print(f"\nSUMMARY for {os.path.basename(file_path)}:")
    # print(f"Original: {len(pois)} rows")
    # print(f"Cleaned: {len(pois_cleaned)} rows")
    # print(f"Removed: {len(REMOVE_INDICES)} rows")
    
    # Country distribution in original and cleaned
    # print(f"\nCountry distribution (original):")
    # country_counts_orig = pois['country'].value_counts()
    # for country, count in country_counts_orig.items():
    #     print(f"  {country}: {count}")
    
    # print(f"\nCountry distribution (cleaned):")
    # country_counts_clean = pois_cleaned['country'].value_counts()
    # for country, count in country_counts_clean.items():
    #     print(f"  {country}: {count}")

    # Show detailed log for removed entries
    if REMOVE_INDICES:
        # print(f"\n{'='*80}")
        # print("DETAILED REMOVAL LOG (grouped by brand-country):")
        # print('='*80)
        
        # Group log entries by brand-country
        brand_country_logs = defaultdict(list)
        
        for log_entry in LOG:
            brand_country_key, idx, title, country, action, reason, exceeds, flagship = log_entry
            if action in ["KEEP", "REMOVE"]:
                brand_country_logs[brand_country_key].append((action, title, country, reason, exceeds, flagship))
        
        # Print grouped by brand-country
        for brand_country_key in sorted(brand_country_logs.keys()):
            base_name, country = brand_country_key.split('_', 1)
            entries = brand_country_logs[brand_country_key]
            keep_entries = [e for e in entries if e[0] == "KEEP"]
            remove_entries = [e for e in entries if e[0] == "REMOVE"]
            
            if remove_entries:  # Only show brand-countries with removals
                print(f"\n{base_name.upper()} ({country}):")
                
                # Show kept entries first
                if keep_entries:
                    print("  KEPT:")
                    for action, title, country, reason, exceeds, flagship in keep_entries:
                        print(f"    ✓ {title}")

                # Show removed entries
                if remove_entries:
                    print("  REMOVED:")
                    for action, title, country, reason, exceeds, flagship in remove_entries:
                        print(f"    ✗ {title} ({reason})")

    # Clean up columns
    columns_to_drop = ['base_name', 'is_flagship', 'exceeds_threshold', 'is_duplicate', 'brand_country_key', 'country']
    pois_cleaned = pois_cleaned.drop(columns=columns_to_drop)

    # Save
    if SAVE_TO_FILE:
        pois_cleaned.to_csv(file_path, index=False)
    else:
        # Return cleaned dataframe for inspection
        return pois_cleaned

# Read and clean all CSV files
cleaned_dfs = []
for file in csv_files:
    cleaned_df = clean_df(file)
    if cleaned_df is not None:
        cleaned_dfs.append(cleaned_df)