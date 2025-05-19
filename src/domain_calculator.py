import re
from urllib.parse import urlparse

import tldextract


def domain_calculator(url):
    if ":443" in url:
        match_obj = re.match(r".*(?=:)", url)
        url = match_obj[0]

    Extract = tldextract.extract(url)
    parse_result = urlparse(url)
    if "www." in parse_result.netloc and Extract.suffix not in parse_result.netloc:
        domain = parse_result.netloc
        suffix = Extract.suffix
        potential_domain = f"{domain}.{suffix}"
    elif "www." in parse_result.netloc and parse_result.netloc.endswith(Extract.suffix):
        domain = parse_result.netloc
        result = "".join(domain.rsplit(f".{Extract.suffix}", 1))
        suffix = Extract.suffix
        potential_domain = f"{result.replace('www.', '')}.{suffix}"
    elif "www." not in parse_result.netloc and parse_result.netloc != "":
        potential_domain = parse_result.netloc
    elif Extract.domain == "uk" and Extract.suffix == "com":
        potential_domain = f"{Extract.subdomain}.{Extract.domain}.{Extract.suffix}"
    else:
        domain = Extract.domain
        suffix = Extract.suffix
        potential_domain = f"{domain}.{suffix}"

    if potential_domain.startswith("www.www."):
        potential_domain = potential_domain.replace("www.www.", "www.")

    return potential_domain
