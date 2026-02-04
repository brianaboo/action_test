import csv
from pathlib import Path
import sqlglot
from sqlglot.errors import SqlglotError

# --- SETTINGS ---
BASE_DIR = Path(__file__).resolve().parent
SRC_DIR = BASE_DIR / "input"
OUT_BASE_DIR = BASE_DIR / "output"
REPORT_NAME = "conversion_report.csv"
READ_DIALECT = "athena"
WRITE_DIALECT = "databricks"
# ----------------

def main():
    if not SRC_DIR.exists():
        print(f"Error: Source directory not found at {SRC_DIR}")
        return

    sql_out_path = OUT_BASE_DIR / "queries"
    sql_out_path.mkdir(parents=True, exist_ok=True)
    
    files = sorted(SRC_DIR.rglob("*.sql"))
    results = []

    if not files:
        print("No SQL files found.")
        return

    for i, f in enumerate(files, 1):
        relative_path = f.relative_to(SRC_DIR)
        target_file_path = sql_out_path / relative_path
        target_file_path.parent.mkdir(parents=True, exist_ok=True)
        
        entry = {"file_path": str(relative_path), "status": "success", "error": ""}
        
        try:
            raw_sql = f.read_text(encoding="utf-8")
            
            converted = sqlglot.transpile(
                raw_sql
                , read=READ_DIALECT
                , write=WRITE_DIALECT
                , pretty=True
            )[0]
            
            target_file_path.write_text(converted, encoding="utf-8")
            print(f"Success: {relative_path}")
            
        except (SqlglotError, Exception) as e:
            print(f"Fail: {relative_path} - {str(e)}")
            entry["status"], entry["error"] = "fail", str(e)
        
        results.append(entry)

    # 보고서 작성
    report_path = OUT_BASE_DIR / REPORT_NAME
    with open(report_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["file_path", "status", "error"])
        writer.writeheader()
        writer.writerows(results)

if __name__ == "__main__":
    main()
