import os
import re
from datetime import datetime

LOGS_DIR = os.path.join(os.path.dirname(__file__), '..', 'logs')

def scan_logs(logs_dir):
    total_errors = 0
    total_warnings = 0
    total_infos = 0
    most_recent_time = None
    log_file_count = 0
    timestamp_regex = re.compile(r'^(\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2})')
    for root, _, files in os.walk(logs_dir):
        for file in files:
            if file.endswith('.log'):
                path = os.path.join(root, file)
                log_file_count += 1
                with open(path, encoding='utf-8') as f:
                    for line in f:
                        if 'ERROR' in line:
                            total_errors += 1
                        if 'WARNING' in line:
                            total_warnings += 1
                        if 'INFO' in line:
                            total_infos += 1
                        # Try to extract timestamp for most recent entry
                        match = timestamp_regex.match(line)
                        if match:
                            try:
                                ts = datetime.fromisoformat(match.group(1).replace(' ', 'T'))
                                if (most_recent_time is None) or (ts > most_recent_time):
                                    most_recent_time = ts
                            except Exception:
                                pass
    
    return {
        'total_errors': total_errors,
        'total_warnings': total_warnings,
        'total_infos': total_infos,
        'log_file_count': log_file_count
    }

def print_report(summary):
    print("=== Log Summary ===")
    print(f"Log files processed: {summary['log_file_count']}")
    print(f"Total ERRORs: {summary['total_errors']}")
    print(f"Total WARNINGs: {summary['total_warnings']}")
    print(f"Total INFOs: {summary['total_infos']}")

if __name__ == "__main__":
    report = scan_logs(LOGS_DIR)
    print_report(report)