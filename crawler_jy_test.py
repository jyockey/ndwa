import urllib.request
from yarl import URL

from crawler_jy import Crawler

BASE_URL = 'http://X.X/'


def page_with_links(links):
    return "<html><body>%s</body></html>" \
        % '\n'.join(['<a href="%s">X</a>' % link for link in links])


def mock_page_response(html, content_type):
    class MockInfo:
        def get_content_type(self):
            return content_type

    class MockData:
        def info(self):
            return MockInfo()

        def read(self):
            return html.encode('utf-8')

        def geturl(self):
            return BASE_URL

    return MockData()


def patch_page_return(monkeypatch, html, content_type="text/html"):
    class MockOpener:
        def open(self, request):
            return mock_page_response(html, content_type)

    def mock_build_opener():
        return MockOpener()

    monkeypatch.setattr(urllib.request, 'build_opener', mock_build_opener)


def test_simple(monkeypatch):
    links = ["/a.html", "/b.html"]
    patch_page_return(monkeypatch, page_with_links(links))

    crawler = Crawler.crawl_url(BASE_URL, 0)
    assert crawler.saved_urls == {
        URL(BASE_URL + 'a.html'), URL(BASE_URL + 'b.html')}


def test_confine(monkeypatch):
    links = ["/foo/bar.html", "/bar/baz.html"]
    patch_page_return(monkeypatch, page_with_links(links))

    crawler = Crawler.crawl_url(BASE_URL + 'foo/foo.html', 0, confine='/foo')
    assert crawler.saved_urls == {URL(BASE_URL + 'foo/bar.html')}
