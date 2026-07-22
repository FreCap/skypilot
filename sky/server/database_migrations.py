"""Entrypoint for the Helm-owned central database migration job."""

import os

from sky.skylet import constants


def main() -> None:
    """Upgrades central state before API replicas enter verify-only mode."""
    requested_mode = os.environ.get(constants.ENV_VAR_STATE_DB_MIGRATION_MODE,
                                    'upgrade')
    os.environ[constants.ENV_VAR_STATE_DB_MIGRATION_MODE] = (
        'bootstrap' if requested_mode == 'bootstrap' else 'upgrade')
    # Import after setting the mode so lazy engine initialization cannot observe
    # the API replica's verify-only deployment setting.
    from sky import global_user_state  # pylint: disable=import-outside-toplevel

    global_user_state.initialize_and_get_db()


if __name__ == '__main__':
    main()
