"""Connector and methods accessing S3"""
from io import StringIO
import os
import logging
import boto3
import pandas as pd

class S3BucketConnector():
    """
    Class for interacting with S3 buckets
    """
    def __init__(self, access_key: str, secret_key: str, endpoint_url: str, bucket: str):
        """
        Constructor for S3BucketConnector()
        :param access_key: access key for accessing S3
        :param secret_key: secret key for accessing S3
        :param endpoint_url: endpoint url to S3
        :param bucket: S3 bucket name
        """
        self._logger = logging.getLogger(__name__)
        self.endpoint_url = endpoint_url
        self.session = boto3.Session(aws_access_key_id=os.environ[access_key],
                                     aws_secret_access_key=os.environ[secret_key])
        # Single underscore _ in front of the variable name signalizes protected variable.
        # Double underscore __ in front of the variable name signalizes private variable.
        self._s3 = self.session.resource(service_name='s3', endpoint_url=endpoint_url)
        self._bucket = self._s3.Bucket(bucket)

    def list_files_in_prefix(self, prefix: str):
        """
        Lists all files with a prefix on the S3 bucket

        :param prefix: prefix used to filter files on the S3 bucket

        returns:
          files: list of all the file names containing the prefix in the key
        """
        files = [obj.key for obj in self._bucket.objects.filter(Prefix=prefix)]
        return files

    def read_csv_to_df(self, key: str, encoding = 'utf-8', sep = ','):
        """
        Reads csv file from source and converts it to the DataFrame

        :param key: key of the file tha should be read
        :param encoding: encoding of the data inside the csv file
        :param sep: csv file separator

        returns:
          data_frame: Pandas DataFrame containing the data of the csv file
        """
        self._logger.info('Reading file %s/%s/%s', self.endpoint_url, self._bucket.name, key)
        csv_obj = self._bucket.Object(key=key).get().get('Body').read().decode(encoding)
        data = StringIO(csv_obj)
        data_frame = pd.read_csv(data, delimiter=sep)
        return data_frame

    def write_df_to_s3(self):
        """
        """
        pass
