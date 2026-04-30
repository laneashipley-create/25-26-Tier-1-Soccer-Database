"""
Quick connection test: run a read query against each Supabase table.

Run before re-running the full pipeline to confirm the connection works.
"""

from config import USE_SUPABASE


def main():
    if not USE_SUPABASE:
        print("USE_SUPABASE is False. Add SUPABASE_KEY to config_local.py")
        return
    import db
    client = db.get_client()
    tables = [
        db.T_COMPETITIONS,
        db.T_SEASONS,
        db.T_GAMES,
        db.T_TIMELINES,
        "own_goals",
    ]
    print("Testing Supabase connection...")
    for table in tables:
        try:
            r = client.table(table).select("*").limit(5).execute()
            n = len(r.data or [])
            print(f"  {table}: OK ({n} row(s) sampled)")
        except Exception as e:
            print(f"  {table}: ERROR — {e}")
    print("Done.")


if __name__ == "__main__":
    main()
