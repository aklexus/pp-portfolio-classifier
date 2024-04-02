import json
import os
import re

import requests

from utils.CONSTANTS import DOMAIN_DEFAULT


class Isin2secid:
    mapping = dict()

    @staticmethod
    def load_cache():
        if os.path.exists("isin2secid.json"):
            with open("isin2secid.json", "r") as f:
                try:
                    Isin2secid.mapping = json.load(f)
                except json.JSONDecodeError:
                    print("Invalid json file")

    @staticmethod
    def save_cache():
        with open("isin2secid.json", "w") as f:
            json.dump(Isin2secid.mapping, f, indent=1, sort_keys=True)

    @staticmethod
    def get_secid(isin):
        cached_secid = Isin2secid.mapping.get(isin, "-")
        if cached_secid == "-" or len(cached_secid.split("|")) < 3:
            url = f"https://www.morningstar.{DOMAIN_DEFAULT}/en/util/SecuritySearch.ashx"
            payload = {'q': isin, 'preferedList': '', 'source': 'nav', 'moduleId': 6, 'ifIncludeAds': False,
                       'usrtType': 'v'}
            headers = {'accept': '*/*', 'accept-encoding': 'gzip, deflate, br',
                       'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36', }
            resp = requests.post(url, data=payload, headers=headers)
            response = resp.content.decode('utf-8')
            if response:
                secid = re.search('\{"i":"([^"]+)"', response).group(1)
                secid_type = response.split("|")[2].lower()
                secid_type_domain = secid + "|" + secid_type + "|" + DOMAIN_DEFAULT
                Isin2secid.mapping[isin] = secid_type_domain
            else:
                secid_type_domain = '||'
        else:
            secid_type_domain = Isin2secid.mapping[isin]
        return secid_type_domain.split("|")
