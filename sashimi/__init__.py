import requests
from urllib.parse import urljoin
from typing import List
import json

__version__ = '0.0.1'

user_agent = f'sashimi_client/{__version__}'


class SashimiClient():
    base_url = None
    
    def __init__(self, project_url: str, token: str):
        self.project_url = project_url
        self.token = token

        self.headers = {
            'User-Agent': user_agent,
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json'
        }
    
    def ds_url(self, ds_name):        
        return urljoin(self.project_url + '/', ds_name)

    def info(self):
        headers = self.headers
        del headers['Content-Type']

        r = requests.get(self.project_url, headers=headers)
        r.raise_for_status()
        return(r.json())

    def rm(self, ds_name: str):

        if ds_name is None :
            raise ValueError('Need ds_name')

        payload = dict(name=ds_name)

        r = requests.delete(self.project_url, headers=self.headers, data=json.dumps(payload))
        r.raise_for_status()        
        return r.text

    def put(self, ds_name: str, dataset: list):

        if ds_name is None or dataset is None:
            raise ValueError('Need both ds_name and dataset')

        payload = dict(ds=dataset, name=ds_name)

        r = requests.put(self.project_url, headers=self.headers, data=json.dumps(payload))
        r.raise_for_status()
        return r.text

    def query(self, ds_name, filter=None, expr='True', 
              sort: str=None, reverse: bool=False,
              limit: int=None, offset: int=None,
              aggregate: List[str] = None,
              fields: List[str] = None,
              discard: bool=None):
    
        url = self.ds_url(ds_name)
        payload = dict()

        if filter:
            payload['filter'] = filter
        else:
            payload['expr'] = expr or 'True'
        
        if sort:
            payload['sort'] = sort
        
        if reverse:
            payload['reverse'] = reverse

        if limit:
            payload['limit'] = limit

        if offset:
            payload['offset'] = offset

        if aggregate:
            payload['aggregate'] = aggregate

        if fields:
            payload['fields'] = fields

        if discard:
            payload['discard'] = discard

        # print(payload)

        headers = self.headers
        del headers['Authorization']

        r = requests.post(url, data=json.dumps(payload), headers=headers)
        r.raise_for_status()
        return(r.json())

    def delete(self, ds_name: str, expr: str): 
        url = self.ds_url(ds_name)
        payload = {
            'op': 'delete',
            'expr': expr
        }
        r = requests.patch(url, data=json.dumps(payload), headers=self.headers)
        r.raise_for_status()
        return r.text

    def update(self, ds_name: str, field: str, update_expr: str, where_expr: str):
        url = self.ds_url(ds_name)
        payload = {
            'op': 'update',
            'update': field,
            'expr': where_expr,
            'update_expr': update_expr
        }
        r = requests.patch(url, data=json.dumps(payload), headers=self.headers)
        r.raise_for_status()
        return r.text

    def insert(self, ds_name: str, data: dict):
        url = self.ds_url(ds_name)
        payload = {
            'data': json.dumps(data)
        }
        r = requests.put(url, data=json.dumps(payload), headers=self.headers)
        r.raise_for_status()
        return r.text
