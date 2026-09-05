"""Component proof for paid-observer subprocess diagnostic transport.

Exercises the real IsolatedObserverSession process/framing boundary and the
production child response encoder. Only the child's external observation is
replaced; this does not claim provider or Serve system E2E coverage.
"""

import asyncio
import importlib.util
import json
import pathlib
import sys
import textwrap
import time

import pytest

pytestmark = pytest.mark.component

_SOURCE = (pathlib.Path(__file__).parents[1] / 'skyserve' / 'paid_capacity' /
           'qualify.py')
_SPEC = importlib.util.spec_from_file_location('paid_observer_diagnostics',
                                               _SOURCE)
assert _SPEC is not None and _SPEC.loader is not None
qualifier = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = qualifier
_SPEC.loader.exec_module(qualifier)


@pytest.mark.parametrize('malformed', [False, True])
def test_closed_diagnostic_survives_real_observer_process(tmp_path, malformed):
    child = tmp_path / 'observer_child.py'
    child.write_text(textwrap.dedent(f'''\
        import importlib.util
        import json
        import sys

        source = {_SOURCE.as_posix()!r}
        spec = importlib.util.spec_from_file_location('diagnostic_child', source)
        module = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = module
        spec.loader.exec_module(module)

        class Runtime:
            def execute(self, _kind, _arguments):
                module.validate_route_authority({{
                    'service_hash': 'private-incarnation',
                    'service_lifecycle_epoch': 1,
                    'route_generation': 1,
                    'route_fresh': False,
                    'route_service_hash': 'private-incarnation',
                    'route_lifecycle_epoch': 1,
                }})

        output = module._isolated_observer_protocol_stdout
        print(json.dumps({{
            'protocol_version': 1, 'ready': True, 'domain': 'postgres',
        }}), file=output, flush=True)
        for line in sys.stdin:
            response = module._isolated_observer_response(Runtime(),
                                                           json.loads(line))
            if {malformed!r}:
                response['diagnostic']['reason'] = 'signed-url-bearer-secret'
            module._write_isolated_observer_frame(output, response)
        '''),
                     encoding='utf-8')
    receipt = qualifier.Receipt(path=tmp_path / 'receipt.json',
                                service_name='paid-e2e',
                                profile=qualifier.PROFILES['scale'])

    async def exercise():
        session = qualifier.IsolatedObserverSession(
            qualifier.IsolatedObservationDomain.POSTGRES, child_program=child)
        try:
            with pytest.raises(qualifier.QualificationError) as failure:
                await session.request(
                    qualifier.IsolatedObservationKind.POSTGRES,
                    {'service_name': 'paid-e2e'},
                    time.monotonic() + 30)
            receipt.miss('scale', failure.value)
            if malformed:
                assert 'diagnostic is malformed' in str(failure.value)
        finally:
            await session.aclose()

    asyncio.run(exercise())
    sample = receipt._payload['samples'][0]  # pylint: disable=protected-access
    assert sample['observation_error_facet'] == ('unclassified'
                                                 if malformed else 'postgres')
    assert sample['observation_error_reason'] == ('incomplete_evidence' if
                                                  malformed else 'route_stale')
    assert 'private-incarnation' not in json.dumps(sample)
    assert 'signed-url-bearer-secret' not in json.dumps(sample)
    assert 'provider_running' not in sample
