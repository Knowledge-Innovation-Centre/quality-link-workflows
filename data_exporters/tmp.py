from mage_ai.data_preparation.shared.secrets import get_secret_value
from minio import Minio
from minio.error import S3Error
import requests
import psycopg2
from datetime import datetime
from typing import List
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data(curr_data, *args, **kwargs):

    pass
    # extract all content? (LOI, LOS) for the previous day
    # filter, dedup only courses and its learning opportunities
    # prep with frame.json
    # push to meili