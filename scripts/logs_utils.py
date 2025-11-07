# import standard libraries
import logging
import os
from datetime import datetime

_LOGGING_INITIALIZED = False

def setup_logging(logs_dir=None, log_level=logging.INFO, log_name_prefix="run"):
	"""
	Sets up logging to both console and a unique file per invocation.
	Log files are stored in scripts/logs by default.
	This function is idempotent: calling it multiple times has no effect after the first call.
	"""
	global _LOGGING_INITIALIZED
	if _LOGGING_INITIALIZED:
		return

	if logs_dir is None:
		logs_dir = os.path.join(os.path.dirname(__file__), "_logs")
	os.makedirs(logs_dir, exist_ok=True)

	timestamp = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
	log_filename = f"{log_name_prefix}_{timestamp}.log"
	log_path = os.path.join(logs_dir, log_filename)

	# Root logger
	logger = logging.getLogger()
	logger.setLevel(log_level)

	# Remove any existing handlers (avoid duplicate logs)
	for handler in logger.handlers[:]:
		logger.removeHandler(handler)

	# Console handler
	ch = logging.StreamHandler()
	ch.setLevel(log_level)
	ch_formatter = logging.Formatter('[%(levelname)s] %(message)s')
	ch.setFormatter(ch_formatter)
	logger.addHandler(ch)

	# File handler
	fh = logging.FileHandler(log_path, encoding="utf-8")
	fh.setLevel(log_level)
	fh_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s')
	fh.setFormatter(fh_formatter)
	logger.addHandler(fh)

	logger.info(f"Logging initialized. Log file: {log_path}")
	_LOGGING_INITIALIZED = True
