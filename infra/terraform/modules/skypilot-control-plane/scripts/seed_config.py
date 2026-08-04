"""Seed the SkyPilot api_server_config from IaC (DB mode).

In DB mode the chart forbids inline ``apiService.config`` and reads config ONLY from the postgres
``config_yaml`` table, so the module's ``inline_config`` cannot be rendered by helm. This Job
deep-merges the IaC config (mounted at ``/seed/config.yaml``) over the DB row — IaC keys win,
runtime-only keys survive. ``workspaces`` is REPLACED wholesale (not deep-merged) so removing a
workspace or clearing a flag actually propagates. Callers may explicitly opt in to pruning the
retired SkyServe controller topology keys; all other runtime-only keys survive.

Raw DB read-merge-write, NOT via sky's ``update_api_server_config`` (which silently loads an empty
CLIENT config unless ``IS_SKYPILOT_SERVER`` is set and would overwrite the whole row). ``FOR
UPDATE`` makes the read and write atomic against concurrent writers. ``config_yaml.value`` is YAML
TEXT (not jsonb), so the merge can't be a SQL ``jsonb_set``. The write is idempotent. Terraform
still restarts the API server after a new seed generation so an old process cannot re-persist a
pre-seed full-config snapshot.
"""

import os
import pathlib
import sys
import time

import yaml
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError, ProgrammingError

CONFIG_KEY = "api_server_config"
DESIRED_PATH = "/seed/config.yaml"
PRUNE_RETIRED_KEYS_ENV = "SKYPILOT_PRUNE_RETIRED_SERVE_CONTROLLER_KEYS"
_RETIRED_SERVE_CONTROLLER_KEYS = (
    "consolidation_mode",
    "external_load_balancer",
)


def deep_merge(base: dict, override: dict) -> dict:
    """Overlay ``override`` onto ``base`` — nested dicts merge recursively, scalars/lists replace."""
    result = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def prune_retired_serve_controller_keys(config: dict) -> dict:
    """Return ``config`` without retired external-LB topology keys.

    Copies only the mappings on the modified path so callers' input remains
    unchanged. Unexpected non-mapping parents fail before any mutation rather
    than silently discarding an invalid persisted config.
    """
    if "serve" not in config:
        return dict(config)

    serve = config["serve"]
    if not isinstance(serve, dict):
        raise ValueError(
            f"serve config is not a mapping ({type(serve).__name__}); "
            "refusing to prune retired controller keys"
        )
    if "controller" not in serve:
        return dict(config)

    controller = serve["controller"]
    if not isinstance(controller, dict):
        raise ValueError(
            "serve.controller config is not a mapping "
            f"({type(controller).__name__}); refusing to prune retired keys"
        )

    pruned_controller = dict(controller)
    for key in _RETIRED_SERVE_CONTROLLER_KEYS:
        pruned_controller.pop(key, None)

    pruned_serve = dict(serve)
    if pruned_controller:
        pruned_serve["controller"] = pruned_controller
    else:
        pruned_serve.pop("controller")

    pruned_config = dict(config)
    if pruned_serve:
        pruned_config["serve"] = pruned_serve
    else:
        pruned_config.pop("serve")
    return pruned_config


def seed(
    engine: Engine,
    desired: dict,
    *,
    prune_retired_controller_keys: bool = False,
) -> None:
    """Read-merge-write the IaC config in one locked transaction (no-op if already current)."""
    with engine.begin() as conn:
        row = conn.execute(
            text("select value from config_yaml where key = :k for update"),
            {"k": CONFIG_KEY},
        ).fetchone()

        existing = yaml.safe_load(row[0]) if row and row[0] else {}
        if existing is None:
            existing = {}
        if not isinstance(existing, dict):
            # Never overwrite an unexpected payload — fail loud instead of wiping the config.
            sys.exit(
                f"[seed] ABORT: api_server_config is not a mapping ({type(existing).__name__}); "
                "refusing to overwrite"
            )

        merge_base = existing
        if prune_retired_controller_keys:
            try:
                # Validate and prune the persisted path before IaC overrides can
                # replace a malformed mapping and hide it from the final check.
                merge_base = prune_retired_serve_controller_keys(existing)
            except ValueError as err:
                sys.exit(f"[seed] ABORT: {err}")

        merged = deep_merge(merge_base, desired)
        if "workspaces" in desired:
            merged["workspaces"] = desired["workspaces"]
        if prune_retired_controller_keys:
            try:
                merged = prune_retired_serve_controller_keys(merged)
            except ValueError as err:
                sys.exit(f"[seed] ABORT: {err}")
        if merged == existing:
            print("[seed] api_server_config already current; nothing to do", flush=True)
            return

        new_value = yaml.safe_dump(merged, default_flow_style=False, sort_keys=False)
        conn.execute(
            text(
                "insert into config_yaml (key, value) values (:k, :v) "
                "on conflict (key) do update set value = :v"
            ),
            {"k": CONFIG_KEY, "v": new_value},
        )
        print(
            f"[seed] seeded api_server_config; keys: {sorted(merged.keys())}",
            flush=True,
        )


def main() -> None:
    desired = yaml.safe_load(pathlib.Path(DESIRED_PATH).read_text()) or {}
    if not isinstance(desired, dict):
        sys.exit(
            f"[seed] ABORT: desired config is not a mapping ({type(desired).__name__})"
        )

    prune_retired_controller_keys = _read_bool_env(PRUNE_RETIRED_KEYS_ENV)
    engine = create_engine(os.environ["SKYPILOT_DB_CONNECTION_URI"])

    # config_yaml is created by api-server migrations on first boot; retry until reachable.
    last_err = None
    for attempt in range(60):
        try:
            seed(
                engine,
                desired,
                prune_retired_controller_keys=prune_retired_controller_keys,
            )
            return
        except (OperationalError, ProgrammingError) as err:
            last_err = err
            print(
                f"[seed] waiting for config_yaml (attempt {attempt}): {err}", flush=True
            )
            time.sleep(5)
    sys.exit(f"[seed] config_yaml never became readable: {last_err}")


def _read_bool_env(name: str) -> bool:
    value = os.environ.get(name, "false").lower()
    if value not in ("false", "true"):
        sys.exit(f"[seed] ABORT: {name} must be true or false, got {value!r}")
    return value == "true"


if __name__ == "__main__":
    main()
