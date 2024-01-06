import requests
from urllib.parse import urljoin
from typing import List
from pathlib import Path
import json
import yaml
import os

__version__ = '0.0.8'

user_agent = f'sashimi_client/{__version__}'


class SashimiClient():
    base_url = None
    
    def __init__(self, project_url: str, token: str = None):
        self.project_url = project_url
        self.token = token or os.getenv('SASHIMI_TOKEN')

        self.headers = {
            'User-Agent': user_agent,
            'Authorization': f'Bearer {self.token}',
            'Content-Type': 'application/json'
        }
    
    def ds_url(self, ds_name):        
        return urljoin(self.project_url + '/', ds_name)
    
    def ds_config_url(self, ds_name):        
        return urljoin(self.ds_url(ds_name) + '/', '_config')

    def project_config_url(self):        
        return urljoin(self.project_url + '/', '_config')
        # return 'http://localhost:8000/ds/q/sandbox/_config'



    def info(self):
        headers = dict(self.headers)
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

    def put(self, ds_name: str, dataset: list, secret: str = None):

        if ds_name is None or dataset is None:
            raise ValueError('Need both ds_name and dataset')

        payload = dict(ds=dataset, name=ds_name, secret = secret)

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

        if limit is not None:
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

        headers = dict(self.headers)
        del headers['Authorization']

        r = requests.post(url, data=json.dumps(payload), headers=headers)
        r.raise_for_status()
        return(r.json())

    def named_query(self, ds_name: str, name: str):
    
        url = urljoin(self.ds_url(ds_name)+'/', name)
        headers = {
            'User-Agent': user_agent,
        }

        r = requests.get(url)
        return r.json()


    def delete(self, ds_name: str, expr: str): 
        url = self.ds_url(ds_name)
        payload = {
            'op': 'delete',
            'expr': expr
        }
        r = requests.patch(url, data=json.dumps(payload), headers=self.headers)
        r.raise_for_status()
        return r.text

    def update(self, ds_name: str, expr: str, data: dict):
        url = self.ds_url(ds_name)
        payload = {
            'op': 'update',
            'expr': expr,
            'update': data
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

    def get_ds_config(self, ds_name: str):
        url = self.ds_config_url(ds_name)
        r = requests.get(url, headers=self.headers)
        r.raise_for_status()
        return r.text

    def set_ds_config(self, ds_name: str, path: Path = None, config: str = None):
        """
        set new config for dataset. Config is either in path or yaml-string in config
        """
        url = self.ds_config_url(ds_name)
        
        # test load
        #with open(path) as fh:
        #    yaml.safe_load(fh)
            
        headers = dict(self.headers)
        del headers['Content-Type']




        if config:
            # validate
            yaml.safe_load(config)
            r = requests.post(url, headers=headers, data=config)
        else:
            with open(path) as fh:
                yaml.safe_load(fh)
            with open(path) as fh:
                r = requests.post(url, headers=headers, data=fh)

        r.raise_for_status()
        return r.text

    def get_project_config(self):
        url = self.project_config_url()
        r = requests.get(url, headers=self.headers)
        r.raise_for_status()
        return r.text

    def set_project_config(self, path: Path = None, config: str = None):
        """
        set new config for project. Config is either in path or in config
        """
        url = self.project_config_url()
        
        assert path or config

        headers = dict(self.headers)
        del headers['Content-Type']


        if config:            
            yaml.safe_load(config)
            r = requests.post(url, headers=headers, data=config)
        else:
            with open(path) as fh:
                yaml.safe_load(fh)

            with open(path) as fh:
                r = requests.post(url, headers=headers, data=fh)


        r.raise_for_status()
        return r.text
