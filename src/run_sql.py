import os
import sys
import psycopg2
from dotenv import load_dotenv

load_dotenv()

supabase_url = os.environ.get("SUPABASE_URL")
db_password = os.environ.get("SUPABASE_PASSWORD")

if not supabase_url:
    print("❌ Missing SUPABASE_URL in .env")
    exit(1)
if not db_password:
    print("❌ Missing SUPABASE_PASSWORD in .env")
    exit(1)

project_ref = supabase_url.replace("https://", "").replace(".supabase.co", "")

conn_string = f"postgresql://postgres.{project_ref}:{db_password}@aws-1-ap-southeast-1.pooler.supabase.com:5432/postgres"

sql_file = sys.argv[1] if len(sys.argv) > 1 else "sql/31_function_search_locations.sql"
try:
    with open(sql_file, 'r') as f:
        sql_content = f.read()
except FileNotFoundError:
    print(f"❌ Error: SQL file not found at {sql_file}")
    exit(1)

print(f"Connecting to Direct Connection (Port 5432) as user: postgres...")
print(f"Executing SQL from: {sql_file}\n")

try:
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()
    cur.execute(sql_content)
    conn.commit()
    print(f"✅ Successfully executed and committed {sql_file}")
    cur.close()
    conn.close()
except Exception as e:
    print(f"❌ Execution Error: {e}")
