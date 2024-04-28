"""
File to store constants
"""

from enum import Enum

# Child classes of Enum class
class S3FileTypes():
    """
    Supported file types for S3BucketConnector
    """
    CSV = 'csv'
    PARQUET = 'parquet'

class MetaProcessFormat(Enum):
    """
    Formats for MetaProcess class
    """
    META_DATE_FORMAT = '%Y-%m-%d'
    META_PROCESS_DATE_FORMAT = '%Y-%m-%d %H:%M:S'
    META_SOURCE_DATE_COL = 'source_date'
    META_PROCESS_COL = 'datetime_of_processing'
    META_FILE_FORMAT = 'csv'
