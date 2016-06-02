# -*- coding: utf-8 -*-
"""
SEO Demo
~~~~~~~~

A web-based demo of MonkeyLearn

:copyright: (c) 2015 Tryolabs.
"""
import aiohttp
import aiohttp_jinja2
import asyncio
import bs4
import contextlib
import jinja2
import json
import logging
import operator
import os

from aiohttp.web import Application
from xgoogle.search import GoogleSearch, SearchError


# Logging
logging.basicConfig()
log = logging.getLogger('aiohttp.web')


def rate_limit(requests_per_second):
    """
    Rate limit a function to the given number of requests per second.
    """
    min_interval = 1.0 / float(requests_per_second)
    loop = asyncio.get_event_loop()
    sem = asyncio.Semaphore()

    def decorate(func):
        last_time_called = [0.0]

        def rate_limited_fn(*args, **kargs):
            elapsed = loop.time() - last_time_called[0]
            left_to_wait = min_interval - elapsed

            with (yield from sem):
                if left_to_wait > 0:
                    yield from asyncio.sleep(left_to_wait)
                if not asyncio.iscoroutinefunction(func):
                    ret = yield from func(*args, **kargs)
                else:
                    ret = func(*args, **kargs)

            last_time_called[0] = loop.time()

            return ret

        return rate_limited_fn
    return decorate


class SEODemoException(Exception):
    pass


class DownloadMainSiteFail(SEODemoException):
    pass


class GoogleSearchError(SEODemoException):
    pass


class MonkeyLearnAPIError(SEODemoException):

    def __init__(self, status_code, response_body=''):
        super(MonkeyLearnAPIError, self).__init__()
        self.status_code = status_code
        self.response_body = response_body


# Classes
class Url(object):

    def __init__(self, url, text=''):
        self.url = url
        self.text = text

    def is_valid(self):
        """
        Trivial URL validation.
        """
        return self.url.startswith('http')

    @asyncio.coroutine
    def download(self, session=None):
        """
        Download the URL and return a new Url object with HTML.
        """
        if not self.is_valid():
            log.warning('Invalid URL, not downloading: {}'.format(self.url))
            return self

        log.debug('Downloading URL: {}'.format(self.url))

        response = yield from session.get(self.url)

        with contextlib.closing(response):
            try:
                html = yield from response.text()
                if response.status != 200:
                    log.warning(
                        'Attempt to GET {} returned HTTP code {}'.format(self.url, response.status)
                    )
                    return self
            except Exception as exc:
                log.warning('Error downloading: {}'.format(self.url))
                log.exception(exc)
                return self

        soup = bs4.BeautifulSoup(html, 'lxml')

        # Remove non-text tags
        for tag in ['script', 'noscript', 'style']:
            for elem in soup.findAll(tag):
                elem.extract()

        # Remove comments
        def is_comment(node):
            return isinstance(node, bs4.Comment)

        for comment in soup.findAll(text=is_comment):
            comment.extract()

        return Url(url=self.url, text=soup.getText())


class Keyword(object):
    """
    A keyword with an associated relevance.
    """

    def __init__(self, string, relevance):
        self.string = string
        self.relevance = relevance

    def compute_score(self, number_sites_found):
        """
        The score function.
        """
        return (self.relevance + (number_sites_found / 10)) / 2


class DisplayKeyword(Keyword):
    """
    A keyword with extra data.
    """

    @classmethod
    def from_keyword(self, keyword, sites_found, total_sites):
        """
        Create a DisplayKeyword from a Keyword.

        The ``sites_found`` argument is an array of URLs (strings) and
        ``total_sites`` is the total number of sites pulled by Google.
        """
        self = DisplayKeyword(keyword.string, keyword.relevance)
        self.score = keyword.compute_score(len(sites_found))
        self.sites_found = sites_found
        self.number_sites_found = len(sites_found)
        self.fraction_sites_found = len(sites_found) / total_sites
        return self

    def to_dict(self):
        """
        Return a dictionary of keyword values.
        """
        return {
            'string': self.string,
            'relevance': self.relevance,
            'score': self.score,
            'sites_found': [site.url for site in self.sites_found],
            'number_sites_found': self.number_sites_found,
            'fraction_sites_found': self.fraction_sites_found
        }


class Site(object):
    """
    A URL (string) and a collection of keywords.
    """

    def __init__(self, url, keywords):
        self.url = url
        self.keywords = keywords

    def average_relevance(self, string):
        """
        Compute the average relevance of a keyword in this site.
        """
        matching = [key for key in self.keywords if key.string == string]

        return sum([key.relevance for key in matching]) / len(matching)


# Functions
def search_google(term, domain):
    try:
        log.debug('Performing Google search for "{}"'.format(term))
        gs = GoogleSearch(term, tld=domain)
        gs.results_per_page = 10
        results = gs.get_results()
        log.debug('Got {} results'.format(len(results)))
        return [Url(res.url) for res in results[:10]]
    except SearchError as exc:
        log.exception(exc)
        return None


@asyncio.coroutine
@rate_limit(4)
def get_monkeylearn_json(app, data):
    ml_url = 'https://api.monkeylearn.com/v2/pipelines/{}/run/'.format(
        app['MONKEYLEARN_PIPELINE_ID']
    )
    log.debug('Calling MonkeyLearn API...')

    with app.get_aiohttp_client() as client:
        response = yield from client.post(
            ml_url,
            data=json.dumps(data),
            headers={
                'Authorization': 'Token {}'.format(app['MONKEYLEARN_API_KEY']),
                'Content-Type': 'application/json'
            }
        )
        with contextlib.closing(response):
            log.debug('Got MonkeyLearn response!')

            ctype = response.headers.get('Content-type', '').lower()

            if response.status != 200 or 'json' not in ctype:
                body = yield from response.text()
                raise MonkeyLearnAPIError(response.status, response_body=body)

            result = yield from response.json()

    return result


@asyncio.coroutine
def extract_keywords(app, urls):
    """
    Classify a list of URLs.
    """
    with app.get_aiohttp_client() as client:
        urls = yield from asyncio.gather(*[url.download(session=client) for url in urls])

    urls = [url for url in urls if url.text]

    log.debug('Fetched {} URLs'.format(len(urls)))
    data = {
        'html_list': [{'url': url.url, 'html': url.text} for url in urls]
    }

    result = yield from get_monkeylearn_json(app, data)
    if 'result' not in result:
        raise MonkeyLearnAPIError(200, response_body=json.dumps(result))

    output = []
    for site in result['result']['sites']:
        url = site['url']
        keywords = []
        for keyword in site['keywords']:
            string = keyword['keyword']
            relevance = float(keyword['relevance'])
            keywords.append(Keyword(string=string, relevance=relevance))
        output.append(Site(url=url, keywords=keywords))

    log.debug('Extracted keywords for {} sites.'.format(len(output)))
    return output


def sites_found(sites, string):
    "Return the sites where a keyword was found."
    def site_has_keyword(site):
        matches = [key for key in site.keywords if key.string == string]

        return len(matches) > 0

    return [site for site in sites if site_has_keyword(site)]


@asyncio.coroutine
def keyword_analysis(app, domain, search_term, search_tld):
    """
    Keyword analysis.
    """
    log.debug('Performing keyword analysis')

    def complete_domain(string):
        """If the string is a full URL, return it. Otherwise, prepend 'http://'."""
        if string.startswith('http'):
            return string
        return 'http://' + string

    def find_site(sites, url):
        matches = [site for site in sites if site.url == url]
        if matches:
            return matches[0]
        return None

    website_url = complete_domain(domain)
    search_results = search_google(search_term, search_tld)

    all_sites = yield from extract_keywords(app, [Url(url=website_url)] + search_results)

    main_site = find_site(all_sites, website_url)
    if not main_site:
        raise DownloadMainSiteFail('Could not download {}'.format(website_url))

    website_keywords = main_site.keywords
    search_sites = [site for site in all_sites if site.url != website_url]

    keywords = []
    for site in search_sites:
        keywords.extend(site.keywords)

    def find_unique_keywords():
        seen = {}
        unique = []
        for key in keywords:
            string = key.string
            if not (string in seen):
                seen[key] = string
                unique.append(key)
        return unique

    unique_keywords = find_unique_keywords()

    def display(keyword):
        found = sites_found(search_sites, keyword.string)
        return DisplayKeyword.from_keyword(
            keyword, sites_found=found, total_sites=len(search_sites)
        )

    return {
        'search_results': search_results,
        'total_sites': len(search_sites),
        'website_keywords': sorted(website_keywords, key=operator.attrgetter('relevance')),
        # Sort a DisplayKeywords by number of sites in which they were found
        'keywords': sorted(
            [display(key) for key in unique_keywords],
            key=lambda key: key.compute_score(key.number_sites_found)
        )
    }


# Views
@aiohttp_jinja2.template('index.html')
def index(request):
    return {'describe': True}


@aiohttp_jinja2.template('index.html')
def search(request):
    domain = request.GET['domain']
    search = request.GET['search']
    tld = request.GET['tld']

    if not domain:
        return {'error': 'You forgot the domain'}

    if not search:
        return {'error': 'You forgot the search term'}

    try:
        data = yield from keyword_analysis(request.app, domain, search, tld)
        results = {
            'search_results': [url.url for url in data['search_results']],
            'total_sites': data['total_sites'],
            'website_keywords': [key.to_dict() for key in data['keywords']],
            'keywords': [key.to_dict() for key in data['keywords']]
        }

        return {'domain': domain, 'search': search, 'tld': tld, 'results': results}
    except MonkeyLearnAPIError as exc:
        log.exception(exc)
        response = aiohttp_jinja2.render_template(
            'index.html', request, {
                'error':
                    'MonkeyLearn API Error, status code {}, body: "{}"'.format(
                        exc.status_code, exc.body
                    )
            }
        )
    except DownloadMainSiteFail as exc:
        log.exception(exc)
        response = aiohttp_jinja2.render_template('index.html', request, {'error': str(exc)})
    except Exception as exc:
        log.exception(exc)
        response = aiohttp_jinja2.render_template(
            'index.html', request, {'error': 'An internal error occurred.'}
        )

    response.set_status(500)
    return response


class SeoDemoApplication(Application):

    def __init__(self, *args, debug=False, **kwargs):
        debug = debug or os.environ.get('DEBUG', 'False') == 'True'
        super(SeoDemoApplication, self).__init__(*args, debug=debug, **kwargs)

        aiohttp_jinja2.setup(self, loader=jinja2.FileSystemLoader('templates'))
        self.load_config()
        self.load_routes()

    def load_config(self):
        self['MONKEYLEARN_API_KEY'] = os.environ['MONKEYLEARN_API_KEY']
        self['MONKEYLEARN_PIPELINE_ID'] = os.environ['MONKEYLEARN_PIPELINE_ID']

        # Optional proxy settings
        self['PROXY_HOST'] = os.environ.get('PROXY_HOST', '')
        self['PROXY_PORT'] = os.environ.get('PROXY_PORT', '')
        self['PROXY_USER'] = os.environ.get('PROXY_USER', '')
        self['PROXY_PASS'] = os.environ.get('PROXY_PASS', '')

        # List of Google sites by country
        self['COUNTRIES'] = [
            {'name': name, 'tld': tld}
            for name, tld in [
                ('Google.com US', 'com'),
                ('Google.co.uk UK', 'co.uk'),
                ('Google.ca Canada', 'ca'),
                ('Google.com.au Australia', 'com.au'),
            ]
        ]

    def load_routes(self):
        if self._debug:  # don't really want this in prod
            self.router.add_static('/assets', 'assets')
            log.setLevel(logging.DEBUG)
        else:
            log.setLevel(logging.INFO)

        self.router.add_route('GET', '/', index)
        self.router.add_route('GET', '/search', search)

    def get_aiohttp_client(self):
        connector = None
        if self['PROXY_HOST']:
            # Optional proxy service
            auth = aiohttp.BasicAuth(self['PROXY_USER'], self['PROXY_PASS'])
            proxy_url = 'http://{}:{}/'.format(
                self['PROXY_HOST'], self['PROXY_PORT']
            )
            connector = aiohttp.ProxyConnector(proxy=proxy_url, proxy_auth=auth)
            log.debug('Using proxy server {}'.format(self['PROXY_HOST']))

        return aiohttp.ClientSession(
            connector=connector,
            headers={
                'User-Agent':
                    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) '
                    'Chrome/47.0.2526.73 Safari/537.36'
            }
        )


app = SeoDemoApplication()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    handler = app.make_handler()
    f = loop.create_server(handler, '0.0.0.0', 5000)
    srv = loop.run_until_complete(f)
    print("Server started at http://0.0.0.0:5000")
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass
    finally:
        loop.run_until_complete(handler.finish_connections())
        srv.close()
        loop.run_until_complete(srv.wait_closed())
        loop.run_until_complete(app.finish())
    loop.close()
