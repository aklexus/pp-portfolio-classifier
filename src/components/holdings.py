import re
from collections import defaultdict
from typing import NamedTuple
from xml.sax.saxutils import escape

import requests
from bs4 import BeautifulSoup
from jsonpath_ng import parse

from src.components.isin2secid import Isin2secid
from src.utils.CONSTANTS import DOMAIN_DEFAULT
from src.utils.taxonomies import taxonomies


class Security:

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)
        self.holdings = []

    def load_holdings(self):
        if len(self.holdings) == 0:
            self.holdings = SecurityHoldingReport()
            self.holdings.load(isin=self.ISIN, secid=self.secid)
        return self.holdings


class SecurityHolding(NamedTuple):
    name: str
    isin: str
    country: str
    industry: str
    currency: str
    percentage: float


class Holding(NamedTuple):
    name: str
    percentage: float


class SecurityHoldingReport:
    def __init__(self):
        self.secid = ''
        pass

    def get_bearer_token(self, secid, domain):
        # the secid can change for retrieval purposes
        # find the retrieval secid
        headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36'}
        url = f'https://www.morningstar.{domain}/{domain}/funds/snapshot/snapshot.aspx?id={secid}'
        response = requests.get(url, headers=headers)
        secid_regexp = r"var FC =  '(.*)';"
        matches = re.findall(secid_regexp, response.text)
        if len(matches) > 0:
            secid_to_search = matches[0]
        else:
            secid_to_search = secid

        # get the bearer token for the new secid
        url = f'https://www.morningstar.{domain}/Common/funds/snapshot/PortfolioSAL.aspx'
        payload = {'FC': secid_to_search}
        response = requests.get(url, headers=headers, params=payload)
        token_regex = r"const maasToken \=\s\"(.+)\""
        resultstringtoken = re.findall(token_regex, response.text)[0]
        return resultstringtoken, secid_to_search

    def calculate_grouping(self, categories, percentages, grouping_name, long_equity):
        for category_name, percentage in zip(categories, percentages):
            self.grouping[grouping_name][escape(category_name)] = self.grouping[grouping_name].get(
                escape(category_name), 0) + percentage

        if grouping_name != 'Asset-Type':
            self.grouping[grouping_name] = {k: v * long_equity for k, v in self.grouping[grouping_name].items()}

    def load(self, isin, secid):
        secid, secid_type, domain = Isin2secid.get_secid(isin)
        if secid == '':
            print(
                f"isin {isin} not found in Morningstar for domain '{DOMAIN_DEFAULT}', skipping it... Try another domain with -d <domain>")
            return
        elif secid_type == "stock":
            print(f"isin {isin} is a stock, skipping it...")
            return
        self.secid = secid
        bearer_token, secid = self.get_bearer_token(secid, domain)
        print(f"Retrieving data for {secid_type} {isin} ({secid}) using domain '{domain}'...")
        headers = {'accept': '*/*', 'accept-encoding': 'gzip, deflate, br',
                   'accept-language': 'fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7',
                   'Authorization': f'Bearer {bearer_token}', }

        params = {'premiumNum': '10', 'freeNum': '10', 'languageId': 'de-DE', 'locale': 'en', 'clientId': 'MDC_intl',
                  'benchmarkId': 'category', 'version': '3.60.0', }

        self.grouping = dict()
        for taxonomy in taxonomies:
            self.grouping[taxonomy] = defaultdict(float)

        non_categories = ['avgMarketCap', 'portfolioDate', 'name', 'masterPortfolioId']
        json_not_found = False
        for grouping_name, taxonomy in taxonomies.items():
            params['component'] = taxonomy['component']
            url = taxonomy['url'] + secid + "/data"
            # use etf or fund endpoint
            url = url.replace("{type}", secid_type)
            resp = requests.get(url, params=params, headers=headers)
            if resp.status_code == 401:
                json_not_found = True
                print(f"  {grouping_name} for secid {secid} will be retrieved from x-ray...")
                continue
            try:
                response = resp.json()
                jsonpath = parse(taxonomy['jsonpath'])
                percent_field = taxonomy['percent']
                # single match of the jsonpath means the path contains the categories
                if len(jsonpath.find(response)) == 1:
                    value = jsonpath.find(response)[0].value
                    keys = [key for key in value if key not in non_categories]

                    if percent_field != "":
                        if value[keys[0]][percent_field] is not None:
                            percentages = [float(value[key][percent_field]) for key in keys]
                        else:
                            percentages = []
                    else:
                        if value[keys[0]] is not None:
                            percentages = [float(value[key]) for key in keys]
                        else:
                            percentages = []

                    if grouping_name == 'Asset-Type':
                        try:
                            long_equity = (float(value.get('assetAllocEquity', {}).get('longAllocation', 0)) + float(
                                value.get('AssetAllocNonUSEquity', {}).get('longAllocation', 0)) + float(
                                value.get('AssetAllocUSEquity', {}).get('longAllocation', 0))) / 100
                        except TypeError:
                            print(f"  No information on {grouping_name} for {secid}")
                else:
                    # every match is a category
                    value = jsonpath.find(response)
                    keys = [key.value[taxonomy['category']] for key in value]
                    if len(value) == 0 or value[0].value.get(taxonomy['percent'], "") == "":
                        print(f"  percentages not found for {grouping_name} for {secid}")
                    else:
                        percentages = [float(key.value[taxonomy['percent']]) for key in value]

                # Map names if there is a map
                if len(taxonomy.get('map', {})) != 0:
                    categories = [taxonomy['map'][key] for key in keys if key in taxonomy['map'].keys()]
                    unmapped = [key for key in keys if key not in taxonomy['map'].keys()]
                    if unmapped:
                        print(f"  Categories not mapped: {unmapped} for {secid}")
                else:
                    # capitalize first letter if not mapping
                    categories = [key[0].upper() + key[1:] for key in keys]

                if percentages:
                    self.calculate_grouping(categories, percentages, grouping_name, long_equity)

            except Exception:
                print(f"  Problem with {grouping_name} for secid {secid} in PortfolioSAL...")
                json_not_found = True

        if json_not_found:
            non_categories = ['Defensive', 'Cyclical', 'Sensitive', 'Greater Europe', 'Americas', 'Greater Asia', ]
            url = "https://lt.morningstar.com/j2uwuwirpv/xray/default.aspx?LanguageId=en-EN&PortfolioType=2&SecurityTokenList=" + secid + "]2]0]FOESP%24%24ALL_1340&values=100"
            resp = requests.get(url, headers=headers)
            soup = BeautifulSoup(resp.text, 'html.parser')
            for grouping_name, taxonomy in taxonomies.items():
                if grouping_name in self.grouping:
                    continue
                table = soup.select("table.ms_data")[taxonomy['table']]
                trs = table.select("tr")[1:]
                if grouping_name == 'Asset-Type':
                    long_equity = float(trs[0].select("td")[0].text.replace(",", ".")) / 100
                categories = []
                percentages = []
                for tr in trs:
                    if len(tr.select('th')) > 0:
                        header = tr.th
                    else:
                        header = tr.td
                    if tr.text != '' and header.text not in non_categories:
                        categories.append(header.text)
                        if len(tr.select("td")) > taxonomy['column']:
                            percentages.append(float(
                                '0' + tr.select("td")[taxonomy['column']].text.replace(",", ".").replace("-", "")))
                        else:
                            percentages.append(0.0)
                if len(taxonomy.get('map2', {})) != 0:
                    categories = [taxonomy['map2'][key] for key in categories]

                self.calculate_grouping(categories, percentages, grouping_name, long_equity)

    def group_by_key(self, key):
        return self.grouping[key]
