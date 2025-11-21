"""
scripts/config.example.py
Database Configuration Template

Copy this file to config.py and fill in your database credentials.
Never commit config.py to version control!
"""

# Database Configuration
# Uncomment the section you want to use

# Option 1: SQLite (Recommended for local testing/portability)
# No installation required, creates a file in the data directory
DB_CONFIG = {
    'type': 'sqlite',
    'filename': 'f1_analytics.db'
}

# Option 2: MySQL
# DB_CONFIG = {
#     'type': 'mysql',
#     'host': 'localhost',
#     'port': 3306,
#     'user': 'root',
#     'password': 'your_password_here',
#     'database': 'F1_RedBull_Analytics'
# }

# API Configuration (if needed in future)
API_CONFIG = {
    'base_url': 'http://ergast.com/api/f1',
    'rate_limit_delay': 0.5  # seconds between requests
}

# Data Paths
DATA_PATHS = {
    'raw_data': 'data/raw/',
    'processed_data': 'data/processed/'
}

# Extraction Settings
EXTRACTION_CONFIG = {
    'start_year': 2005,
    'end_year': 2024
}

