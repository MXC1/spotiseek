
import os
import json
from datetime import datetime

LOGS_DIR = os.path.join(os.path.dirname(__file__), "..", "observability", "logs")

def scan_logs(logs_dir):
    total_errors = 0
    total_warnings = 0
    total_infos = 0
    total_debugs = 0
    most_recent_time = None
    log_file_count = 0
    error_messages = {}
    warning_messages = {}
    for root, _, files in os.walk(logs_dir):
        for file in files:
            if file.endswith(".log"):
                path = os.path.join(root, file)
                log_file_count += 1
                with open(path, encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            entry = json.loads(line)
                        except Exception:
                            # Not a JSON line, skip
                            continue
                        level = entry.get("level", "").upper()
                        msg = entry.get("message", "")
                        # Count by level
                        if level == "ERROR":
                            total_errors += 1
                            error_messages[msg] = error_messages.get(msg, 0) + 1
                        elif level == "WARNING":
                            total_warnings += 1
                            warning_messages[msg] = warning_messages.get(msg, 0) + 1
                        elif level == "INFO":
                            total_infos += 1
                        elif level == "DEBUG":
                            total_debugs += 1
                        # Most recent timestamp (if present)
                        ts_str = entry.get("timestamp")
                        if ts_str:
                            # Try to parse as ISO or fallback to yyyymmdd_HHMMSS_mmmmmm
                            try:
                                if "-" in ts_str and ":" in ts_str:
                                    ts = datetime.fromisoformat(ts_str.replace(" ", "T"))
                                else:
                                    # Format: 20251109_171331_449493
                                    ts = datetime.strptime(ts_str.split("_")[0] + ts_str.split("_")[1], "%Y%m%d%H%M%S")
                                if (most_recent_time is None) or (ts > most_recent_time):
                                    most_recent_time = ts
                            except Exception:
                                pass
    return {
        "total_errors": total_errors,
        "total_warnings": total_warnings,
        "total_infos": total_infos,
        "total_debugs": total_debugs,
        "log_file_count": log_file_count,
        "error_messages": error_messages,
        "warning_messages": warning_messages
    }

def print_report(summary):
    print("=== Log Summary ===")
    print(f"Log files processed: {summary['log_file_count']}")
    print(f"Total ERRORs: {summary['total_errors']}")
    print(f"Total WARNINGs: {summary['total_warnings']}")
    print(f"Total INFOs: {summary['total_infos']}")
    print(f"Total DEBUGs: {summary['total_debugs']}")
    if summary.get("error_messages"):
        if summary["error_messages"]:
            print("\n--- Error Summary ---")
            for msg, count in sorted(summary["error_messages"].items(), key=lambda x: -x[1]):
                print(f"[{count}x] {msg}")
    if summary.get("warning_messages"):
        if summary["warning_messages"]:
            print("\n--- Warning Summary ---")
            for msg, count in sorted(summary["warning_messages"].items(), key=lambda x: -x[1]):
                print(f"[{count}x] {msg}")

if __name__ == "__main__":
    report = scan_logs(LOGS_DIR)
    print_report(report)