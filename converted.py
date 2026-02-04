import csv
from pathlib import Path
import sqlglot
from sqlglot.errors import SqlglotError

# --- SETTINGS ---
SRC_DIR = Path("input")
OUT_BASE_DIR = Path("output/converted")
REPORT_NAME = "conversion_report.csv"
READ_DIALECT = "athena"
WRITE_DIALECT = "databricks"
# ----------------

def main():
    sql_out_path = OUT_BASE_DIR / "queries"
    sql_out_path.mkdir(parents=True, exist_ok=True)
    
    # rglob("*.sql")로 하위 폴더의 모든 sql 파일 탐색
    files = sorted(SRC_DIR.rglob("*.sql"))
    results = []

    for i, f in enumerate(files, 1):
        # input 폴더 이후의 상대 경로 계산 (예: folder1/sub/test.sql)
        relative_path = f.relative_to(SRC_DIR)
        target_file_path = sql_out_path / relative_path
        
        # 출력될 폴더 생성
        target_file_path.parent.mkdir(parents=True, exist_ok=True)
        
        print(f"[{i}/{len(files)}] Processing: {relative_path}")
        
        entry = {"file_path": str(relative_path), "status": "success", "error": ""}
        
        try:
            sql_text = f.read_text(encoding="utf-8")
            converted = sqlglot.transpile(
                sql_text, 
                read=READ_DIALECT, 
                write=WRITE_DIALECT, 
                pretty=True
            )[0]
            
            target_file_path.write_text(converted, encoding="utf-8")
            
        except (SqlglotError, Exception) as e:
            entry["status"] = "fail" if isinstance(e, SqlglotError) else "error"
            entry["error"] = str(e)
        
        results.append(entry)

    # 보고서 저장
    report_path = OUT_BASE_DIR / REPORT_NAME
    with open(report_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["file_path", "status", "error"])
        writer.writeheader()
        writer.writerows(results)

    print(f"Finished. Check output folder.")

if __name__ == "__main__":
    main()
