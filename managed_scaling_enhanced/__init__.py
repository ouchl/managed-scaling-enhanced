import sys
import logging.handlers
from pathlib import Path
from botocore.config import Config


formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setFormatter(formatter)
stdout_handler.setLevel(logging.INFO)
log_dir = Path('log')
log_dir.mkdir(parents=True, exist_ok=True)
file_handler = logging.handlers.RotatingFileHandler(
    'log/managed-scaling.log',
    maxBytes=100*1024*1024,
    backupCount=10,
)
file_handler.formatter = formatter
file_handler.setLevel(logging.INFO)
logging.getLogger().addHandler(file_handler)
logging.getLogger().addHandler(stdout_handler)
logging.getLogger().setLevel(logging.INFO)

boto3_config = Config(
    retries={
        'max_attempts': 10,  # Maximum number of retries
        'mode': 'adaptive'   # Retry mode (standard or adaptive)
    }
)
