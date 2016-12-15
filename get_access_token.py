import json
import requests

def exchange_code_for_access_token(cls, code, redirect_uri, **kwargs):
    url = u'https://api.instagram.com/oauth/access_token'
    data = {
        u'client_id': cls.get_client_id(),
        u'client_secret': cls.get_client_secret(),
        u'code': code,
        u'grant_type': u'authorization_code',
        u'redirect_uri': redirect_uri
    }

    response = requests.post(url, data=data)

    account_data = json.loads(response.content)

    return account_data