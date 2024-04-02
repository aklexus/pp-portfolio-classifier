import argparse
import json
import os
import re
import uuid
import xml.etree.ElementTree as ET
from collections import defaultdict
from itertools import cycle
from typing import NamedTuple
from xml.sax.saxutils import escape

import requests
import requests_cache
from bs4 import BeautifulSoup
from jinja2 import Environment, BaseLoader
from jsonpath_ng import parse

from utils.CONSTANTS import COLORS
from utils.taxonomies import taxonomies

requests_cache.install_cache(expire_after=60 * 60 * 24)  # cache downloaded files for a day
requests_cache.remove_expired_responses()


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
            url = f"https://www.morningstar.{DOMAIN}/en/util/SecuritySearch.ashx"
            payload = {'q': isin, 'preferedList': '', 'source': 'nav', 'moduleId': 6, 'ifIncludeAds': False,
                       'usrtType': 'v'}
            headers = {'accept': '*/*', 'accept-encoding': 'gzip, deflate, br',
                       'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.150 Safari/537.36', }
            resp = requests.post(url, data=payload, headers=headers)
            response = resp.content.decode('utf-8')
            if response:
                secid = re.search('\{"i":"([^"]+)"', response).group(1)
                secid_type = response.split("|")[2].lower()
                secid_type_domain = secid + "|" + secid_type + "|" + DOMAIN
                Isin2secid.mapping[isin] = secid_type_domain
            else:
                secid_type_domain = '||'
        else:
            secid_type_domain = Isin2secid.mapping[isin]
        return secid_type_domain.split("|")


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
                f"isin {isin} not found in Morningstar for domain '{DOMAIN}', skipping it... Try another domain with -d <domain>")
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


class PortfolioPerformanceCategory(NamedTuple):
    name: str
    color: str
    uuid: str


class PortfolioPerformanceFile:

    def __init__(self, filepath):
        self.filepath = filepath
        self.pp_tree = ET.parse(filepath)
        self.pp = self.pp_tree.getroot()
        self.securities = None

    def get_security(self, security_xpath):
        """return a security object """
        security = self.pp.findall(security_xpath)[0]
        if security is not None:
            isin = security.find('isin')
            if isin is not None:
                isin = isin.text
                secid = security.find('secid')
                if secid is not None:
                    secid = secid.text
                return Security(name=security.find('name').text, ISIN=isin, secid=secid,
                                UUID=security.find('uuid').text, )
            else:
                name = security.find('name').text
                print(f"security '{name}' does not have isin, skipping it...")
        return None

    def get_security_xpath_by_uuid(self, uuid):
        for idx, security in enumerate(self.pp.findall(".//securities/security")):
            sec_uuid = security.find('uuid').text
            if sec_uuid == uuid:
                return f"../../../../../../../../securities/security[{idx + 1}]"

    def add_taxonomy(self, kind):
        securities = self.get_securities()
        taxonomy_tpl = """
            <taxonomy>
                <id>{{ outer_uuid }}</id>
                <name>{{ kind }}</name>
                <root>
                    <id>{{ inner_uuid }}</id>
                    <name>{{ kind }}</name>
                    <color>#89afee</color>
                    <children>
                        {% for category in categories %}
                        <classification>
                            <id>{{ category["uuid"] }}</id>
                            <name>{{ category["name"] }}</name>
                            <color>{{ category["color"] }}</color>
                            <parent reference="../../.."/>
                            <children/>
                            <assignments>
                            {% for assignment in category["assignments"] %}
                                <assignment>
                                    <investmentVehicle class="security" reference="{{ assignment["security_xpath"] }}"/>
                                    <weight>{{ assignment["weight"] }}</weight>
                                    <rank>{{ assignment["rank"] }}</rank>
                                </assignment>
                             {% endfor %}
                            </assignments>
                            <weight>0</weight>
                            <rank>1</rank>
                        </classification>
                        {% endfor %}
                    </children>
                    <assignments/>
                    <weight>10000</weight>
                    <rank>0</rank>
                </root>
            </taxonomy>
            """

        unique_categories = defaultdict(list)

        rank = 1

        for security in securities:
            security_h = security.holdings
            security_assignments = security_h.group_by_key(kind)

            for category, weight in security_assignments.items():
                unique_categories[category].append(
                    {"security_xpath": self.get_security_xpath_by_uuid(security.UUID), "weight": round(weight * 100),
                     "rank": rank})
                rank += 1

        categories = []
        color = cycle(COLORS)
        for idx, (category, assignments) in enumerate(unique_categories.items()):
            cat_weight = 0
            for assignment in assignments:
                cat_weight += assignment['weight']

            categories.append(
                {"name": category, "uuid": str(uuid.uuid4()), "color": next(color), "assignments": assignments,
                 "weight": cat_weight})

        tax_tpl = Environment(loader=BaseLoader).from_string(taxonomy_tpl)
        taxonomy_xml = tax_tpl.render(outer_uuid=str(uuid.uuid4()), inner_uuid=str(uuid.uuid4()), kind=kind,
                                      categories=categories)
        self.pp.find('.//taxonomies').append(ET.fromstring(taxonomy_xml))

    def write_xml(self, output_file):
        with open(output_file, 'wb') as f:
            self.pp_tree.write(f, encoding="utf-8")

    def dump_xml(self):
        print(ET.tostring(self.pp, encoding="unicode"))

    def get_securities(self):
        if self.securities is None:
            self.securities = []
            sec_xpaths = []
            for transaction in self.pp.findall('.//portfolio-transaction'):
                for child in transaction:
                    if child.tag == "security":
                        sec_xpaths.append('.//' + child.attrib["reference"].split('/')[-1])

            for sec_xpath in list(set(sec_xpaths)):
                security = self.get_security(sec_xpath)
                if security is not None:
                    security_h = security.load_holdings()
                    if security_h.secid != '':
                        self.securities.append(security)
        return self.securities


def print_class(grouped_holding):
    for key, value in sorted(grouped_holding.items(), reverse=True):
        print(key, "\t\t{:.2f}%".format(value))
    print("-" * 30)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(  # usage="%(prog) <input_file> [<output_file>] [-d domain]",
        description='\r\n'.join(["reads a portfolio performance xml file and auto-classifies",
                                 "the securities in it by asset-type, stock-style, sector, holdings, region and country weights",
                                 "For each security, you need to have an ISIN"]))

    # Morningstar domain where your securities can be found
    # e.g. es for spain, de for germany, fr for france...
    # this is only used to find the corresponding secid from the ISIN
    parser.add_argument('-d', default='de', dest='domain', type=str,
                        help='Morningstar domain from which to retrieve the secid (default: es)')

    parser.add_argument('input_file', metavar='input_file', type=str, help='path to unencrypted pp.xml file')

    parser.add_argument('output_file', metavar='output_file', type=str, nargs='?',
                        help='path to auto-classified output file', default='pp_classified.xml')

    args = parser.parse_args()

    if "input_file" not in args:
        parser.print_help()
    else:
        DOMAIN = args.domain
        Isin2secid.load_cache()
        pp_file = PortfolioPerformanceFile(args.input_file)
        for taxonomy in taxonomies:
            pp_file.add_taxonomy(taxonomy)
        Isin2secid.save_cache()
        pp_file.write_xml(args.output_file)
