import csv
import re
import os
from pathlib import Path
import sqlglot
from sqlglot.errors import SqlglotError

# --- SETTINGS ---
# 스크립트 파일의 위치를 기준으로 루트 경로를 잡습니다.
BASE_DIR = Path(__file__).resolve().parent
SRC_DIR = BASE_DIR / "input"
OUT_BASE_DIR = BASE_DIR / "output"
REPORT_NAME = "conversion_report.csv"
READ_DIALECT = "athena"
WRITE_DIALECT = "databricks"
# ----------------

def main():
    # 1. 경로 존재 확인 (디버깅용)
    if not SRC_DIR.exists():
        print(f"Error: Source directory not found at {SRC_DIR}")
        return

    sql_out_path = OUT_BASE_DIR / "queries"
    sql_out_path.mkdir(parents=True, exist_ok=True)
    
    # rglob으로 하위 모든 폴더의 .sql 탐색
    files = sorted(SRC_DIR.rglob("*.sql"))
    results = []

    if not files:
        print("No SQL files found in input directory.")
        return

    for i, f in enumerate(files, 1):
        # input 폴더 대비 상대 경로 추출 (폴더 구조 유지의 핵심)
        relative_path = f.relative_to(SRC_DIR)
        target_file_path = sql_out_path / relative_path
        
        # 출력될 하위 폴더 생성
        target_file_path.parent.mkdir(parents=True, exist_ok=True)
        
        print(f"[{i}/{len(files)}] Processing: {relative_path}")
        entry = {"file_path": str(relative_path), "status": "success", "error": ""}
        
        try:
            raw_sql = f.read_text(encoding="utf-8")

            # [Step 1] SQL 변환
            converted = sqlglot.transpile(
                raw_sql, 
                read=READ_DIALECT, 
                write=WRITE_DIALECT, 
                pretty=True
            )[0]
            
            target_file_path.write_text(converted, encoding="utf-8")
            
        except (SqlglotError, Exception) as e:
            entry["status"] = "fail" if isinstance(e, SqlglotError) else "error"
            entry["error"] = str(e)
        
        results.append(entry)

    # 변환 보고서 저장
    report_path = OUT_BASE_DIR / REPORT_NAME
    with open(report_path, "w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["file_path", "status", "error"])
        writer.writeheader()
        writer.writerows(results)

    print(f"Finished. Output saved to: {OUT_BASE_DIR}")
