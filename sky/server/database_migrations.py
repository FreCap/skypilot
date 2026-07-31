"""Entrypoint for the Helm-owned central database migration job."""

import os

from sky.skylet import constants


def initialize_central_databases() -> None:
    """Initializes every central Alembic schema under one ownership mode."""
    # Import after the caller selects the mode so lazy engine initialization
    # cannot observe another process role's deployment setting.
    # pylint: disable=import-outside-toplevel
    from sky import global_user_state
    from sky import skypilot_config
    from sky.jobs import state_storage
    from sky.physical_capacity import config as capacity_config
    from sky.serve import serve_state

    # pylint: enable=import-outside-toplevel
    capacity_configuration = capacity_config.load_config()
    # Global state must run first: explicit bootstrap proves the shared
    # effective PostgreSQL schema is empty before any companion schema creates
    # objects in it.
    global_engine = global_user_state.initialize_and_get_db()
    skypilot_config.initialize_and_get_db()
    serve_state.get_database_engine()
    state_storage.initialize_and_get_db()
    if os.environ.get('SKYPILOT_API_REQUEST_BACKEND') == 'postgres':
        # Keep local and compatibility SQLite installations independent from
        # this PostgreSQL-only central schema.
        # pylint: disable=import-outside-toplevel
        from sky.server.requests import postgres as request_postgres
        request_postgres.initialize_and_get_db()
    if global_engine.dialect.name == 'postgresql':
        capacity_config.validate_runtime_capability(capacity_configuration,
                                                    revision='001')
        # Capacity is initialized last. It shares the ordinary PostgreSQL
        # engine namespace and owns no DDL on SQLite.
        # pylint: disable=import-outside-toplevel
        from sky.physical_capacity import state as capacity_state
        capacity_state.initialize_and_get_db()
    elif capacity_configuration.mode != capacity_config.CapacityMode.DISABLED:
        raise RuntimeError('Physical capacity requires the central PostgreSQL '
                           'database; SQLite supports only disabled mode.')


def main() -> None:
    """Upgrades central schemas before replicas enter verify-only mode."""
    requested_mode = os.environ.get(constants.ENV_VAR_STATE_DB_MIGRATION_MODE,
                                    'upgrade')
    # Database engine selection consults this marker before honoring the
    # central PostgreSQL connection URI. Set it in the entrypoint as a
    # defense-in-depth guarantee even when the process is not chart-launched.
    os.environ[constants.ENV_VAR_IS_SKYPILOT_SERVER] = 'true'
    os.environ[constants.ENV_VAR_STATE_DB_MIGRATION_MODE] = (
        'bootstrap' if requested_mode == 'bootstrap' else 'upgrade')
    initialize_central_databases()


if __name__ == '__main__':
    main()
