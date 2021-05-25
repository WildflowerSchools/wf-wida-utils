import wf_core_data.utils
import requests
import pandas as pd
# from collections import OrderedDict
# import pickle
# import json
# import datetime
import logging
# import os

logger = logging.getLogger(__name__)

class AirtableClient:
    def __init__(
        self,
        api_key=None,
        url_base='https://api.airtable.com/v0/'
    ):
        self.api_key = api_key
        self.url_base = url_base
        if self.api_key is None:
            self.api_key = os.getenv('AIRTABLE_API_KEY')

    def get(
        self,
        base_id,
        endpoint,
        params=None,
        auth=None
    ):
        headers = dict()
        if self.api_key is not None:
            headers['Authorization'] = 'Bearer {}'.format(self.api_key)

        r = requests.get(
            '{}{}/{}'.format(
                self.url_base,
                base_id,
                endpoint
            ),
            params=params,
            headers=headers,
            auth=auth
        )
        if r.status_code != 200:
            error_message = 'Airtable GET request returned status code {}'.format(r.status_code)
            r.raise_for_status()
        return r.json()
