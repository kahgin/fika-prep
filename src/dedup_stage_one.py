import pandas as pd
import os
import glob
from collections import defaultdict

os.chdir('/home/kahgin/fika/fika-prep')

INPUT_DIR = 'data/map'
OUTPUT_DIR = 'output'
SAVE_TO_FILE = True

csv_files = glob.glob(os.path.join(INPUT_DIR, "*.csv"), recursive=True)

def extract_country(address_str):
    """Extract country code from complete_address field"""
    if pd.isna(address_str):
        return None
    
    address_str = str(address_str)
    
    if '"country":"SG"' in address_str:
        return 'SG'
    elif '"state":"Johor"' in address_str:
        return 'Johor'
    elif '"country":"MY"' in address_str:
        return 'MY'
    
    return None

def combine_dataframes(dfs):
    """Concat dataframes, dedup by name, keep highest review_rating"""
    combined = pd.concat(dfs, ignore_index=True)
    
    num_duplicates = combined.duplicated(subset=['title']).sum()
    print(f"Duplicates found: {num_duplicates}")
    
    combined = combined.sort_values('review_rating', ascending=False)
    combined = combined.drop_duplicates(subset=['title'], keep='first')
    
    return combined

def clean_df(pois, file_path=f'{OUTPUT_DIR}/data.csv'):
    print(f"\n{'='*80}")
    print(f"STARTING DEDUPLICATION")
    print(f"{'='*80}")
    print(f"Original dataset: {len(pois)} rows")
    
    # Extract country from address
    pois['country'] = pois['complete_address'].apply(extract_country)
    
    # Diagnostic: Country distribution
    print(f"\nCountry distribution (including None):")
    country_dist = pois['country'].value_counts(dropna=False)
    for country, count in country_dist.items():
        print(f"  {country}: {count}")
    
    # Add helper columns
    # pois['base_name'] = pois['title'].apply(lambda x: x.rsplit(" ", 1)[0].strip().lower() if ' ' in str(x) else str(x).lower())
    pois['base_name'] = pois['title'].apply(lambda x: x.split('@')[0].strip().lower() if '@' in str(x) else str(x).lower()) # a, - , (
    # pois['base_name'] = pois['title'].apply(lambda x: str(x).lower())
    pois['is_flagship'] = pois['title'].str.contains('flagship', case=False, na=False)
    pois['exceeds_threshold'] = (pois['review_count'] > 10**2) & (pois['review_rating'] >= 3.0)
    
    # Create brand_country_key for duplicate detection
    pois['brand_country_key'] = pois.apply(
        lambda row: f"{row['base_name']}_{row['country']}" 
        if row['country'] is not None else None, 
        axis=1
    )
    
    # Mark duplicates based on brand_country_key
    pois['is_duplicate'] = pois.duplicated(subset='brand_country_key', keep=False) & pois['brand_country_key'].notna()
    
    print(f"\nDuplicate analysis:")
    print(f"  Marked as duplicate: {pois['is_duplicate'].sum()}")
    print(f"  Marked as non-duplicate: {(~pois['is_duplicate']).sum()}")
    
    # Get duplicate groups
    duplicate_groups = pois[pois['is_duplicate']].groupby('brand_country_key')

    KEEP_INDICES = []
    REMOVE_INDICES = []
    LOG = []

    # Process each duplicate brand-country group
    for brand_country_key, group in duplicate_groups:
        if len(group) == 1:
            continue
        
        base_name, country = brand_country_key.split('_', 1)
        
        # Skip specific names
        if base_name in ['restoran al', 'swiss', 'universal studios', 'jade', 'yishun town', 'pasar malam taman', 'pasar malam jalan'] or len(base_name) <= 10:
            for idx in group.index:
                KEEP_INDICES.append(idx)
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
            flagship_entries = below_threshold[below_threshold['is_flagship']]
            non_flagship = below_threshold[~below_threshold['is_flagship']]
            
            # Keep ALL flagship entries
            for idx, row in flagship_entries.iterrows():
                KEEP_INDICES.append(idx)
                LOG.append((brand_country_key, idx, row['title'], row['country'], "KEEP", "flagship", False, True))
            
            # For non-flagship below-threshold, keep only the best one
            if not non_flagship.empty:
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

    # Diagnostic: Index accounting
    print(f"\nIndex accounting:")
    print(f"  Total KEEP indices (with duplicates): {len(KEEP_INDICES)}")
    print(f"  Unique KEEP indices: {len(set(KEEP_INDICES))}")
    print(f"  REMOVE indices: {len(REMOVE_INDICES)}")
    print(f"  Expected result: {len(pois) - len(REMOVE_INDICES)} rows")
    
    # Check for missing indices
    all_original_indices = set(pois.index)
    all_keep_indices = set(KEEP_INDICES)
    all_remove_indices = set(REMOVE_INDICES)
    
    missing_indices = all_original_indices - all_keep_indices - all_remove_indices
    if missing_indices:
        print(f"  WARNING: {len(missing_indices)} indices are neither kept nor removed!")
        sample_missing = list(missing_indices)[:5]
        print(f"  Sample missing indices: {sample_missing}")
        print(f"  Sample missing POI names:")
        for idx in sample_missing:
            print(f"    - {pois.loc[idx, 'title']} (country: {pois.loc[idx, 'country']}, base_name: {pois.loc[idx, 'base_name']})")

    # Apply filtering
    pois_cleaned = pois.loc[list(set(KEEP_INDICES))].copy()

    print(f"\nFINAL RESULTS:")
    print(f"  Original: {len(pois)} rows")
    print(f"  Cleaned: {len(pois_cleaned)} rows")
    print(f"  Removed: {len(pois) - len(pois_cleaned)} rows")
    
    # Country distribution in cleaned data
    print(f"\nCountry distribution (cleaned):")
    country_counts_clean = pois_cleaned['country'].value_counts(dropna=False)
    for country, count in country_counts_clean.items():
        print(f"  {country}: {count}")

    # Show detailed log for removed entries
    if REMOVE_INDICES:
        print(f"\n{'='*80}")
        print("DETAILED REMOVAL LOG:")
        print('='*80)
        
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
            
            if remove_entries:
                print(f"\n{base_name.upper()} ({country}):")
                
                if keep_entries:
                    print("  KEPT:")
                    for action, title, country, reason, exceeds, flagship in keep_entries:
                        print(f"    ✓ {title}")

                if remove_entries:
                    print("  REMOVED:")
                    for action, title, country, reason, exceeds, flagship in remove_entries:
                        print(f"    ✗ {title} ({reason})")

    # Clean up helper columns
    columns_to_drop = ['base_name', 'is_flagship', 'exceeds_threshold', 'is_duplicate', 'brand_country_key', 'country']
    pois_cleaned = pois_cleaned.drop(columns=columns_to_drop)

    # Save
    if SAVE_TO_FILE:
        pois_cleaned.to_csv(file_path, index=False)
        print(f"\n✓ Saved to: {file_path}")
    
    return pois_cleaned

# Main execution
df = [pd.read_csv(filename, low_memory=False) for filename in csv_files]
df = combine_dataframes(df)
clean_df(df)