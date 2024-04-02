import argparse
import logging
from os import path

import requests_cache

from src.components.classifier import PortfolioPerformanceFile
from src.components.isin2secid import Isin2secid
from src.utils.CONSTANTS import DOMAIN_DEFAULT
from src.utils.taxonomies import taxonomies

requests_cache.install_cache(expire_after=60 * 60 * 24)  # cache downloaded files for a day
requests_cache.remove_expired_responses()

if __name__ == '__main__':
    logging.basicConfig(filename=path.join('_tmp', 'app.log'), filemode='a+',
                        format='%(asctime)s-%(levelname)s-%(message)s', level=logging.INFO)
    logging.info('Starting the app')

    parser = argparse.ArgumentParser(  # usage="%(prog) <input_file> [<output_file>] [-d domain]",
        description='\r\n'.join(["reads a portfolio performance xml file and auto-classifies",
                                 "the securities in it by asset-type, stock-style, sector, holdings, region and country weights",
                                 "For each security, you need to have an ISIN"]))

    # Morningstar domain where your securities can be found
    # e.g. es for spain, de for germany, fr for france...
    # this is only used to find the corresponding secid from the ISIN
    parser.add_argument('-d', default=DOMAIN_DEFAULT, dest='domain', type=str,
                        help='Morningstar domain from which to retrieve the secid (default: es)')

    parser.add_argument('input_file', metavar='input_file', type=str, help='path to unencrypted pp.xml file')

    parser.add_argument('output_file', metavar='output_file', type=str, nargs='?',
                        help='path to auto-classified output file', default='pp_classified.xml')

    args = parser.parse_args()

    if "input_file" not in args:
        parser.print_help()
    else:
        domain = args.domain
        Isin2secid.load_cache()
        pp_file = PortfolioPerformanceFile(args.input_file, domain)
        for taxonomy in taxonomies:
            pp_file.add_taxonomy(taxonomy)
        Isin2secid.save_cache()
        # Write the enhanced portfolio
        output_path = args.output_file
        pp_file.write_xml(output_path)
