"""Tests for the hosted-catalog URL override (self-hosted catalog mirror)."""
from sky.catalog import common
from sky.skylet import constants


def test_default_urls_without_override(monkeypatch):
    monkeypatch.delenv('SKYPILOT_HOSTED_CATALOG_DIR_URL', raising=False)
    primary, fallback = common.hosted_catalog_base_urls()
    assert primary == constants.HOSTED_CATALOG_DIR_URL
    assert fallback == constants.HOSTED_CATALOG_DIR_URL_S3_MIRROR


def test_override_replaces_both_urls(monkeypatch):
    # With a self-hosted mirror there must be no fallback to upstream:
    # a transient mirror error would otherwise silently serve stale
    # upstream data, defeating the point of self-hosting.
    monkeypatch.setenv(
        'SKYPILOT_HOSTED_CATALOG_DIR_URL',
        'https://raw.githubusercontent.com/example/cat/master/catalogs/')
    primary, fallback = common.hosted_catalog_base_urls()
    assert primary == (
        'https://raw.githubusercontent.com/example/cat/master/catalogs')
    assert fallback == primary


def test_token_sent_only_with_override(monkeypatch):
    # A token must never be attached to requests to the default public
    # catalog hosts -- only to an explicitly configured private mirror.
    monkeypatch.delenv('SKYPILOT_HOSTED_CATALOG_DIR_URL', raising=False)
    monkeypatch.setenv('SKYPILOT_HOSTED_CATALOG_TOKEN', 'tok-123')
    assert 'Authorization' not in common.hosted_catalog_request_headers()

    monkeypatch.setenv('SKYPILOT_HOSTED_CATALOG_DIR_URL',
                       'https://raw.githubusercontent.com/example/cat/master')
    headers = common.hosted_catalog_request_headers()
    assert headers['Authorization'] == 'Bearer tok-123'


def test_no_token_no_auth_header(monkeypatch):
    monkeypatch.setenv('SKYPILOT_HOSTED_CATALOG_DIR_URL',
                       'https://raw.githubusercontent.com/example/cat/master')
    monkeypatch.delenv('SKYPILOT_HOSTED_CATALOG_TOKEN', raising=False)
    assert 'Authorization' not in common.hosted_catalog_request_headers()


class _FakeResponse:

    def __init__(self, text):
        self.text = text
        self.status_code = 200

    def raise_for_status(self):
        pass


def test_source_change_invalidates_fresh_cache(monkeypatch, tmp_path):
    """Switching catalog source must refetch immediately, not serve the old
    source's data until the pull interval expires."""
    monkeypatch.setattr(common, '_ABSOLUTE_VERSIONED_CATALOG_DIR',
                        str(tmp_path))
    monkeypatch.delenv('SKYPILOT_HOSTED_CATALOG_DIR_URL', raising=False)
    requested_urls = []

    def fake_get(url, headers=None, *, timeout):
        del headers
        assert timeout == common.DEFAULT_HTTP_TIMEOUT_SECONDS
        requested_urls.append(url)
        return _FakeResponse('InstanceType,Region\nx1,us-east-1\n')

    monkeypatch.setattr(common.requests, 'get', fake_get)

    # First read downloads from the default source and records it.
    common.read_catalog('do/test-vms.csv', pull_frequency_hours=7).columns  # pylint: disable=expression-not-assigned
    assert len(requested_urls) == 1
    assert requested_urls[0].startswith(constants.HOSTED_CATALOG_DIR_URL)

    # Second read within the pull interval: cache hit, no request.
    common.read_catalog('do/test-vms.csv', pull_frequency_hours=7).columns  # pylint: disable=expression-not-assigned
    assert len(requested_urls) == 1

    # Source switch: the still-fresh cache must be invalidated.
    monkeypatch.setenv('SKYPILOT_HOSTED_CATALOG_DIR_URL',
                       'https://mirror.example.com/catalogs')
    common.read_catalog('do/test-vms.csv', pull_frequency_hours=7).columns  # pylint: disable=expression-not-assigned
    assert len(requested_urls) == 2
    assert requested_urls[1].startswith('https://mirror.example.com/catalogs')


def test_pre_tracking_cache_refetched_under_override(monkeypatch, tmp_path):
    """A catalog cached before source tracking existed (no .source meta)
    must refetch once when a mirror override is configured — otherwise
    long-lived machines serve the old source's data until the TTL."""
    monkeypatch.setattr(common, '_ABSOLUTE_VERSIONED_CATALOG_DIR',
                        str(tmp_path))
    requested = []

    def fake_get(url, headers=None, *, timeout):
        del headers
        assert timeout == common.DEFAULT_HTTP_TIMEOUT_SECONDS
        requested.append(url)
        return _FakeResponse('InstanceType,Region\nx1,us-east-1\n')

    monkeypatch.setattr(common.requests, 'get', fake_get)
    # Simulate a pre-tracking cache: downloaded catalog (file + .md5 meta,
    # as pre-#82 downloads wrote) but no .source meta.
    import hashlib
    content = 'InstanceType,Region\n'
    (tmp_path / 'do').mkdir(parents=True)
    (tmp_path / 'do' / 'gap-vms.csv').write_text(content)
    (tmp_path / '.meta' / 'do').mkdir(parents=True)
    (tmp_path / '.meta' / 'do' / 'gap-vms.csv.md5').write_text(
        hashlib.md5(content.encode(), usedforsecurity=False).hexdigest())

    monkeypatch.delenv('SKYPILOT_HOSTED_CATALOG_DIR_URL', raising=False)
    common.read_catalog('do/gap-vms.csv', pull_frequency_hours=7).columns  # pylint: disable=expression-not-assigned
    assert not requested  # fresh by mtime, no override: cache honored

    monkeypatch.setenv('SKYPILOT_HOSTED_CATALOG_DIR_URL',
                       'https://mirror.example.com/catalogs')
    common.read_catalog('do/gap-vms.csv', pull_frequency_hours=7).columns  # pylint: disable=expression-not-assigned
    assert len(requested) == 1
    assert requested[0].startswith('https://mirror.example.com/catalogs')
