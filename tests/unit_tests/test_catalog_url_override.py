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
