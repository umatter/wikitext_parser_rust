#!/usr/bin/env python3
import sys
import pyarrow.parquet as pq

if len(sys.argv) < 3:
    print("Usage: extract_article.py <parquet_file> <page_id>")
    sys.exit(1)

parquet_file = sys.argv[1]
target_page_id = sys.argv[2]

# Read parquet file
table = pq.read_table(parquet_file)
df = table.to_pandas()

# Find the row with matching page_id
row = df[df['page_id'] == target_page_id]

if row.empty:
    print(f"Page ID {target_page_id} not found")
    sys.exit(1)

print(f"Page ID: {row.iloc[0]['page_id']}")
print(f"Title: {row.iloc[0]['page_title']}")
print(f"\nOfficial text length: {len(str(row.iloc[0]['official_text']))}")
print(f"Clone text length: {len(str(row.iloc[0]['clone_text']))}")
print("\n" + "="*60)
print("OFFICIAL TEXT:")
print("="*60)
print(row.iloc[0]['official_text'][:5000])  # First 5000 chars
if len(str(row.iloc[0]['official_text'])) > 5000:
    print(f"\n... (truncated, total {len(str(row.iloc[0]['official_text']))} characters)")
