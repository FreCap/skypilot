"""Logic tests for `aws.ingress_source_ranges`.

SkyPilot opens every port in `resources.ports` to the whole internet. For a
workload with no authentication of its own -- a SkyServe replica, whose bearer
token the load balancer strips before proxying -- that means anyone can reach
it. These tests pin the configurable source ranges AND, just as importantly,
that the default is unchanged.
"""
# pylint: disable=protected-access
from sky.provision.aws import config as aws_config
from sky.provision.aws import instance as aws_instance


def test_default_is_the_whole_internet():
    """The historical behaviour must be byte-identical when unconfigured."""
    assert aws_instance._ingress_source_ranges(None) == ['0.0.0.0/0']
    assert aws_instance._ingress_source_ranges({}) == ['0.0.0.0/0']
    assert (aws_instance._ingress_source_ranges({'ingress_source_ranges': None
                                                }) == ['0.0.0.0/0'])
    assert (aws_instance._ingress_source_ranges({'ingress_source_ranges': []
                                                }) == ['0.0.0.0/0'])


def test_configured_ranges_are_used_verbatim():
    assert aws_instance._ingress_source_ranges(
        {'ingress_source_ranges': ['52.54.64.159/32']}) == ['52.54.64.159/32']
    assert aws_instance._ingress_source_ranges({
        'ingress_source_ranges': ['10.0.0.0/8', '52.54.64.159/32']
    }) == ['10.0.0.0/8', '52.54.64.159/32']


def test_both_modules_agree_on_the_default():
    """A split default would silently reopen SSH while ports stay narrowed."""
    assert (aws_instance._DEFAULT_INGRESS_SOURCE_RANGE ==
            aws_config._DEFAULT_INGRESS_SOURCE_RANGE == '0.0.0.0/0')


def test_schema_accepts_and_rejects_appropriately():
    import jsonschema  # pylint: disable=import-outside-toplevel

    from sky.utils import schemas  # pylint: disable=import-outside-toplevel
    schema = schemas.get_config_schema()

    jsonschema.validate({'aws': {
        'ingress_source_ranges': ['52.54.64.159/32']
    }}, schema)
    # An empty list would silently mean "no rules", which is a footgun; require
    # at least one entry so the intent is explicit.
    for bad in ([], '52.54.64.159/32', [''], [None]):
        try:
            jsonschema.validate({'aws': {'ingress_source_ranges': bad}}, schema)
            raise AssertionError(f'schema accepted {bad!r}')
        except jsonschema.ValidationError:
            pass
