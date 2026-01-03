import pandas as pd
import os

os.chdir('/home/kahgin/fika/fika-prep')

INPUT_FILE = 'data/map/data.csv'
OUTPUT_FILE = 'data/map/data.csv'
REMOVAL_LIST_FILE = 'text/removal_list.txt'  # One name per line
DRY_RUN = False  # Set to False to actually remove

# Toggle options
KEEP_FLAGSHIP = True  # Keep entries with 'flagship' in the name
KEEP_EXACT_NAME_MATCH = True  # Keep exact name matches from removal list
RANK_BY_REVIEWS = True  # Rank by review_count and review_rating, keep first occurrence

def extract_country(address_str):
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

def load_removal_list(file_path):
    """Load list of POI names to remove from text file"""
    if not os.path.exists(file_path):
        print(f"ERROR: Removal list file not found: {file_path}")
        return []
    
    with open(file_path, 'r', encoding='utf-8') as f:
        names = [line.strip() for line in f if line.strip()]
    
    return names
def remove_pois(
    df,
    removal_names,
    dry_run=True,
    keep_flagship=True,
    keep_exact=True,
    rank_by_reviews=True
):
    print(f"\n{'='*80}")
    print(f"POI BULK REMOVAL {'(DRY RUN)' if dry_run else '(LIVE RUN)'}")
    print(f"{'='*80}")

    # Extract country (same as code 2)
    df = df.copy()
    df['country'] = df['complete_address'].apply(extract_country)

    all_indices_to_remove = set()
    all_indices_to_keep = set()

    for country in ['SG', 'MY', 'Johor']:
        country_df = df[df['country'] == country]

        if country_df.empty:
            continue

        print(f"\n{'-'*80}")
        print(f"PROCESSING COUNTRY: {country}")
        print(f"{'-'*80}")

        for name in removal_names:
            matched = country_df[
                country_df['title'].str.contains(name, case=False, na=False, regex=False)
            ]

            if matched.empty:
                continue

            print(f"\n📍 Pattern: '{name}' → {len(matched)} matches")

            if rank_by_reviews:
                matched = matched.sort_values(
                    ['review_count', 'review_rating'],
                    ascending=[False, False]
                )

            kept_count = 0

            for idx, row in matched.iterrows():
                should_keep = False
                reasons = []

                title_lower = row['title'].lower()

                if keep_flagship and 'flagship' in title_lower:
                    should_keep = True
                    reasons.append("flagship")

                if keep_exact and title_lower == name.lower():
                    should_keep = True
                    reasons.append("exact match")

                if rank_by_reviews and kept_count == 0 and not should_keep:
                    should_keep = True
                    reasons.append("highest ranked")

                if should_keep:
                    print(f"   ✓ KEEP: {row['title']} [{', '.join(reasons)}]")
                    all_indices_to_keep.add(idx)
                    kept_count += 1
                else:
                    print(f"   ✗ REMOVE: {row['title']}")
                    all_indices_to_remove.add(idx)

    print(f"\n{'='*80}")
    print("SUMMARY")
    print(f"  Total KEEP: {len(all_indices_to_keep)}")
    print(f"  Total REMOVE: {len(all_indices_to_remove)}")
    print(f"  Remaining: {len(df) - len(all_indices_to_remove)}")
    print(f"{'='*80}")

    if dry_run:
        print("\n⚠️ DRY RUN — no rows removed")
        return df.drop(columns=['country'])

    df_cleaned = df.drop(index=all_indices_to_remove)
    return df_cleaned.drop(columns=['country'])

# Main execution
if __name__ == "__main__":
    # Load data
    print(f"Loading data from: {INPUT_FILE}")
    df = pd.read_csv(INPUT_FILE, low_memory=False)
    
    # Load removal list
    removal_names = load_removal_list(REMOVAL_LIST_FILE)
    
    if not removal_names:
        print("No names to remove. Exiting.")
        exit()
    
    # Process removals
    df_cleaned = remove_pois(
        df, 
        removal_names, 
        dry_run=DRY_RUN,
        keep_flagship=KEEP_FLAGSHIP,
        keep_exact=KEEP_EXACT_NAME_MATCH,
        rank_by_reviews=RANK_BY_REVIEWS
    )
    
    # Save if not dry run
    if not DRY_RUN:
        df_cleaned.to_csv(OUTPUT_FILE, index=False)
        print(f"\n✓ Saved to: {OUTPUT_FILE}")
    else:
        print(f"\nTo remove these POIs, set DRY_RUN = False in the script")