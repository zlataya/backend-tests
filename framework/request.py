# -*- coding: utf-8 -*-
import json
import logging
import requests
import urllib3

log = logging.getLogger(__name__)


class Request(object):
    def __init__(self, url, cookies='', headers=None):
        self.url = url
        self.cookies = cookies
        self.headers = headers

    def get(self, url_param='', params='', no_check=False):
        """
        :param params: additional parameters for request
        :param url_param: part of url to add to domain url
        :param no_check: True - no need to check the status code, False - check the request response
        :return:
        """
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        url_param = ['/' + url_param, url_param][url_param == '' or url_param.startswith('?')]
        response = requests.get(self.url + url_param, params=params, cookies=self.cookies, headers=self.headers,
                                timeout=5, verify=False)
        if no_check:
            return response

        if response.status_code in [200, 202]:
            return json.loads(response.content, encoding='utf-8')
        else:
            log.exception('GET request failure: status %s, url: %s, message %s' %
                          (response.status_code, response.url, response.content))
            raise requests.RequestException

    def post(self, body=None, url_param='', no_check=False, files=None):

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        params = {'files': files, 'verify': False, 'headers': self.headers.copy()}
        # do not send content type for files
        if files:
            del params['headers']['Content-Type']
        response = requests.post(self.url + url_param, data=body, **params)

        if no_check:
            return response

        if response.status_code in [200, 202]:
            if response.content:
                return json.loads(response.content)
            else:
                return response.content
        else:
            log.exception('POST request failure: status %s, url: %s, message %s' %
                          (response.status_code, response.url, response.content))
            raise requests.RequestException

    def put(self, body=None, url_param='', no_check=False):

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        url_param = ['/' + url_param, url_param][url_param == '']

        response = requests.put(self.url + url_param, data=body, headers=self.headers, verify=False)

        if no_check:
            return response

        if response.status_code in [200, 202]:
            if response.content:
                return json.loads(response.content)
            else:
                return response.content
        else:
            log.exception('PUT request failure: status %s, url: %s, message %s' %
                          (response.status_code, response.url, response.content))
            raise requests.RequestException

    def delete(self, url_param='', params='', no_check=False):
        """
        :param params: additional parameters for request
        :param url_param: part of url to add to domain url
        :param no_check: True - no need to check the status code, False - check the request response
        :return:
        """
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

        url_param = ['/' + url_param, url_param][url_param == '']

        response = requests.delete(self.url + url_param, params=params, cookies=self.cookies, headers=self.headers,
                                   verify=False)

        if no_check:
            return response

        if response.status_code in [200, 202, 204]:
            if response.content:
                return json.loads(response.content)
            else:
                return response.content
        else:
            log.exception('DELETE request failure: status %s, url: %s, message %s' %
                          (response.status_code, response.url, response.content))
            raise requests.RequestException
