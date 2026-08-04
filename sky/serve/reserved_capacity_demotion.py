"""Operator entrypoint for safely demoting reserved-fill to protocol v1."""

import argparse
from collections.abc import Sequence
import json
import os
import sys

from sky.serve import reserved_capacity_broker
from sky.serve import serve_state
from sky.skylet import constants as skylet_constants
from sky.utils.db import db_utils


def _build_parser() -> argparse.ArgumentParser:
    return argparse.ArgumentParser(
        description='Demote reserved-capacity-fill protocol v2 to v1 only '
        'after mechanically verifying the live Kubernetes writer rollout and '
        'atomically rebuilding every legacy claim projection.')


def run_cli(argv: Sequence[str] | None = None) -> tuple[int, str]:
    """Run demotion and return a shell status plus one JSON/text line."""
    try:
        _build_parser().parse_args(argv)
        if not os.environ.get(skylet_constants.ENV_VAR_DB_CONNECTION_URI):
            raise RuntimeError('Central PostgreSQL configuration is missing.')
        engine = serve_state.get_database_engine()
        if engine.dialect.name != db_utils.SQLAlchemyDialect.POSTGRESQL.value:
            raise RuntimeError('Demotion requires the central PostgreSQL '
                               'database.')
        changed = reserved_capacity_broker.demote_protocol_v1()
        state = serve_state.get_reserved_fill_protocol_state()
        if int(state['protocol_version']) != (
                reserved_capacity_broker.PROTOCOL_V1):
            raise RuntimeError('The protocol demotion transaction did not '
                               'install version 1.')
        return 0, json.dumps(
            {
                'changed': changed,
                'claim_generation': state['claim_generation'],
                'protocol_version': state['protocol_version'],
            },
            sort_keys=True,
            separators=(',', ':'))
    except (KeyError, TypeError, ValueError,
            reserved_capacity_broker.ProtocolV1DemotionError,
            reserved_capacity_broker.ProtocolV2ActivationError,
            RuntimeError) as error:
        return 1, f'Reserved-fill protocol-v1 demotion failed: {error}'


def main(argv: Sequence[str] | None = None) -> int:
    """Run the demotion CLI."""
    exit_code, output = run_cli(argv)
    print(output, file=sys.stderr if exit_code else sys.stdout)
    return exit_code


if __name__ == '__main__':
    sys.exit(main())
