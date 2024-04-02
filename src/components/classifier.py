import uuid
from collections import defaultdict
from itertools import cycle
from typing import NamedTuple
from xml.etree import ElementTree as ET

from jinja2 import Environment, BaseLoader

from src.components.holdings import Security
from src.utils.CONSTANTS import COLORS


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
