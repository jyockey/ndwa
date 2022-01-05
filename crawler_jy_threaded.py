#!/usr/bin/env python3

import sys
import time
import math
import urllib
import urllib.error
import urllib.request
import optparse
from bs4 import BeautifulSoup
from html import escape
from threading import Thread
from time import sleep
from queue import Queue
from yarl import URL

__version__ = "0.2"

USAGE = "%prog [options] <url>"
VERSION = "%prog v" + __version__

AGENT = "%s/%s" % (__name__, __version__)


class Link(object):
    def __init__(self, src, dst, link_type):
        self.src = src
        self.dst = dst
        self.link_type = link_type

    def __hash__(self):
        return hash((self.src, self.dst, self.link_type))

    def __eq__(self, other):
        return (self.src == other.src
            and self.dst == other.dst
            and self.link_type == other.link_type)

    def __str__(self):
        return self.src + " -> " + self.dst


class Crawler(object):
    THREAD_POOL_SIZE = 10

    def crawl_url(root, depth_limit, confine=None, exclude=[],
                  filter_seen=True):
        crawler = Crawler(root, depth_limit, confine, exclude,
                          filter_seen)
        crawler.crawl()
        return crawler

    def __init__(self, root, depth_limit, confine=None, exclude=[],
                 filter_seen=True):
        # The seed URL at which to start the crawl
        self.root_url = URL(root)

        # Maximum graph depth to crawl (0 means only parse the root url)
        self.depth_limit = depth_limit

        # If not None, only crawl pages with a path that starts with this
        self.confine_prefix = confine

        # Don't crawl pages with a path that starts with any of these
        self.exclude_prefixes = exclude

        # A set of all link URLs seen on pages (even if they aren't crawled)
        self.urls_seen = set()

        # A set of all page URLs crawled
        self.visited_urls = set()

        # A set of all URLs saved to return to the caller
        self.saved_urls = set()

        # A set of all links
        self.links_remembered = set()

        self.num_links = 0
        self.num_followed = 0

        self.pre_visit_filters = [self._prefix_ok,
                                  self._exclude_ok,
                                  self._not_visited,
                                  self._same_host]

        if filter_seen:
            self.saved_url_filters = [self._prefix_ok,
                                    self._same_host]
        else:
            self.saved_url_filters = []

        self.active_threads = 0

    def _pre_visit_url_condense(self, url):
        return url.with_fragment(None)

    def _prefix_ok(self, url):
        return (self.confine_prefix is None
                or url.path.startswith(self.confine_prefix))

    def _exclude_ok(self, url):
        prefixes_ok = [not url.path.startswith(p) for p in self.exclude_prefixes]
        return all(prefixes_ok)

    def _not_visited(self, url):
        return (url not in self.visited_urls)

    def _same_host(self, url):
        return self.root_url.host == URL(url).host

    def crawl(self):
        q = Queue()
        q.put((self.root_url, 0))

        for i in range(Crawler.THREAD_POOL_SIZE):
            thread = Thread(target=self.crawl_from_queue, args=(q,))
            thread.setDaemon(True)
            thread.start()

        while not q.empty() or self.active_threads > 0:
            sleep(1)

    def crawl_from_queue(self, q):
        while True:
            this_url, depth = q.get()
            self.active_threads += 1
            print(q.qsize())

            do_not_follow = \
                [f for f in self.pre_visit_filters if not f(this_url)]

            if depth == 0 and [] != do_not_follow:
                print(sys.stderr, "Whoops! Starting URL %s rejected by the following filters:", do_not_follow)

            if [] == do_not_follow:
                try:
                    self.visited_urls.add(this_url)
                    self.num_followed += 1
                    links = LinkFetcher.fetch(str(this_url))
                    for link_url in [self._pre_visit_url_condense(URL(ol)) for ol in links]:
                        if link_url not in self.urls_seen:
                            if depth < self.depth_limit:
                                q.put((link_url, depth + 1))
                            self.urls_seen.add(link_url)

                        do_not_remember = [f for f in self.saved_url_filters if not f(link_url)]
                        if [] == do_not_remember:
                            self.num_links += 1
                            self.saved_urls.add(link_url)
                            link = Link(str(this_url), str(link_url), "href")
                            if link not in self.links_remembered:
                                self.links_remembered.add(link)
                except Exception as e:
                    print(sys.stderr, "ERROR: Can't process url '%s' (%s)" % (this_url, e))

            self.active_threads -= 1


class OpaqueDataException(Exception):
    def __init__(self, message, mimetype, url):
        Exception.__init__(self, message)
        self.mimetype = mimetype
        self.url = url


class LinkFetcher:
    def _addHeaders(request):
        request.add_header("User-Agent", AGENT)

    def _open(url):
        try:
            request = urllib.request.Request(url)
            handle = urllib.request.build_opener()
        except IOError:
            return None
        return (request, handle)

    def fetch(url):
        request, handle = LinkFetcher._open(url)
        LinkFetcher._addHeaders(request)
        out_urls = []
        out_url_set = set()
        if handle:
            try:
                data = handle.open(request)
                mime_type = data.info().get_content_type()
                url = data.geturl()
                if mime_type != "text/html":
                    raise OpaqueDataException("Not interested in files of type %s" % mime_type,
                                              mime_type, url)
                content = data.read().decode("utf-8", errors="replace")
                soup = BeautifulSoup(content, "html.parser")
                tags = soup('a')
            except urllib.error.HTTPError as error:
                if error.code == 404:
                    print(sys.stderr, "ERROR: %s -> %s" % (error, error.url))
                else:
                    print(sys.stderr, "ERROR: %s" % error)
                tags = []
            except urllib.error.URLError as error:
                print(sys.stderr, "ERROR: %s" % error)
                tags = []
            except OpaqueDataException as error:
                print(sys.stderr, "Skipping %s, has type %s" % (error.url, error.mimetype))
                tags = []

            for tag in tags:
                href = tag.get("href")
                if href is not None:
                    a_url = urllib.parse.urljoin(url, escape(href))
                    if a_url not in out_url_set:
                        out_url_set.add(a_url)
                        out_urls.append(a_url)
        return out_urls


def getLinks(url):
    for i, url in enumerate(LinkFetcher.fetch(url)):
        print("%d. %s" % (i, url))


def parse_options():
    parser = optparse.OptionParser(usage=USAGE, version=VERSION)

    parser.add_option("-l", "--links",
            action="store_true", default=False, dest="links",
            help="Get links for specified url only")

    parser.add_option("-d", "--depth",
            action="store", type="int", default=30, dest="depth_limit",
            help="Maximum depth to traverse")

    parser.add_option("-c", "--confine",
            action="store", type="string", dest="confine",
            help="Confine crawl to specified prefix")

    parser.add_option("-x", "--exclude", action="append", type="string",
                      dest="exclude", default=[], help="Exclude URLs by prefix")

    parser.add_option("-L", "--show-links", action="store_true", default=False,
                      dest="out_links", help="Output links found")

    parser.add_option("-u", "--show-urls", action="store_true", default=False,
                      dest="out_urls", help="Output URLs found")

    parser.add_option("-D", "--dot", action="store_true", default=False,
                      dest="out_dot", help="Output Graphviz dot file")

    opts, args = parser.parse_args()

    if len(args) < 1:
        parser.print_help(sys.stderr)
        raise SystemExit(1)

    if opts.out_links and opts.out_urls:
        parser.print_help(sys.stderr)
        parser.error("options -L and -u are mutually exclusive")

    return opts, args


class DotWriter:
    def __init__(self):
        self.node_alias = {}
        self.serial_id = 0

    def _safe_alias(self, node, node_strs):
        if node in self.node_alias:
            return self.node_alias[node]
        else:
            name = "N" + str(self.serial_id)
            self.serial_id += 1
            self.node_alias[node] = name
            node_strs.append("\t%s [label=\"%s\"];" % (name, node))
            return name

    def asDot(self, links):
        node_strs = []
        edge_strs = []
        for k in links:
            edge_strs.append("\t" + self._safe_alias(k.src, node_strs) + " -> "
                                  + self._safe_alias(k.dst, node_strs) + ";")

        dot = "digraph Crawl {\n\tedge [K=0.2, len=0.1];\n"
        dot += "\n".join(node_strs)
        dot += "\n".join(edge_strs)
        dot += "\n}"
        return dot


def main():
    opts, args = parse_options()

    url = args[0]

    if opts.links:
        getLinks(url)
        sys.exit(0)

    depth_limit = opts.depth_limit
    confine_prefix = opts.confine
    exclude = opts.exclude

    sTime = time.time()

    print(sys.stderr, "Crawling %s (Max Depth: %d)" % (url, depth_limit))
    crawler = Crawler.crawl_url(url, depth_limit, confine_prefix, exclude)

    if opts.out_urls:
        print("\n".join(crawler.urls_seen))

    if opts.out_links:
        print("\n".join([str(l) for l in crawler.links_remembered]))

    if opts.out_dot:
        d = DotWriter()
        print(d.asDot(crawler.links_remembered))

    eTime = time.time()
    tTime = eTime - sTime

    print(sys.stderr, "Found:    %d" % crawler.num_links)
    print(sys.stderr, "Followed: %d" % crawler.num_followed)
    print(sys.stderr, "Stats:    (%d/s after %0.2fs)" % (int(math.ceil(float(crawler.num_links) / tTime)), tTime))


if __name__ == "__main__":
    main()
