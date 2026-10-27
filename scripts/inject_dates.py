import json
import os

input_path = "data/data.txt"
db_path = "data/viet69_database.json"
js_path = "data/viet69_database.js"

print("=== VIET69 ORIGINAL DATE INJECTOR ===")
if not os.path.exists(input_path):
    print(f"[!] Error: Raw data file '{input_path}' not found!")
    exit(1)

if not os.path.exists(db_path):
    print(f"[!] Error: Database file '{db_path}' not found!")
    exit(1)

print(f"[*] Loading raw data from '{input_path}'...")
with open(input_path, "r", encoding="utf-8") as f:
    raw_data = json.load(f)

print(f"[*] Loading database from '{db_path}'...")
with open(db_path, "r", encoding="utf-8") as f:
    db_data = json.load(f)

# Create a mapping from video ID to original publish date
print("[*] Mapping video IDs to original dates...")
date_map = {}
for item in raw_data:
    v_id = item.get("id")
    if v_id and "date" in item:
        date_map[str(v_id)] = item["date"]

# Inject dates into the scraped database
print("[*] Injecting dates into database records...")
injected_count = 0
for item in db_data:
    v_id = str(item.get("id"))
    if v_id in date_map:
        item["date"] = date_map[v_id]
        injected_count += 1

# Save the updated database back to JSON
print(f"[*] Saving updated database to '{db_path}'...")
with open(db_path, "w", encoding="utf-8") as f:
    json.dump(db_data, f, ensure_ascii=False, indent=2)

# Save the updated database back to JS
print(f"[*] Saving updated database to '{js_path}'...")
with open(js_path, "w", encoding="utf-8") as f:
    f.write("window.viet69_data = ")
    json.dump(db_data, f, ensure_ascii=False, indent=2)
    f.write(";")

print(f"[🎉 SUCCESS] Successfully injected original publish dates for {injected_count} / {len(db_data)} videos!")
