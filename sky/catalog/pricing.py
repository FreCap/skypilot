"""Config-based pricing for virtual instance types."""

from typing import Any


def merge_pricing_dicts(base: dict[str, Any],
                        override: dict[str, Any]) -> dict[str, Any]:
    """Deep-merge *override* into *base*, returning a new dict.

    Top-level scalar keys (``cpu``, ``memory``) are replaced.
    The ``accelerators`` sub-dict is merged key-by-key so that
    unmentioned accelerators are preserved from *base*.
    """
    merged = dict(base)
    for key in ('cpu', 'memory'):
        if key in override:
            merged[key] = override[key]
    if 'accelerators' in override:
        merged_accels = dict(merged.get('accelerators', {}))
        merged_accels.update(override['accelerators'])
        merged['accelerators'] = merged_accels
    return merged


def resolve_pricing_config(
        *config_key_paths: tuple[str, ...]) -> dict[str, Any]:
    """Fetch pricing from config key paths and merge in priority order.

    Each path is a tuple of strings looked up via
    ``skypilot_config.get_nested``.  Later paths override earlier ones,
    with deep-merge semantics for the ``accelerators`` sub-dict.

    Example::

        resolve_pricing_config(
            ('kubernetes', 'pricing'),
            ('kubernetes', 'context_configs', ctx, 'pricing'),
        )
    """
    from sky import skypilot_config  # pylint: disable=import-outside-toplevel

    pricing: dict[str, Any] = {}
    for path in config_key_paths:
        level = skypilot_config.get_nested(path, default_value=None)
        if level is not None:
            pricing = merge_pricing_dicts(pricing, level)
    return pricing


def get_hourly_cost_from_pricing(
    pricing: dict[str, Any],
    cpus: float,
    memory: float,
    accelerator_name: str | None,
    accelerator_count: int | None,
) -> float:
    """Compute hourly cost from a pricing config dict.

    The pricing dict has the structure::

        {
            'cpu': <$/vCPU/hour>,
            'memory': <$/GB/hour>,
            'accelerators': {
                '<AcceleratorName>': <$/accelerator/hour>,
                ...
            },
        }

    The two tiers are mutually exclusive:

    - **Accelerator instances**: if the instance has an accelerator, the cost
      is ``accel_count * accel_rate``.  The ``cpu`` and ``memory`` rates are
      ignored because GPU/accelerator pricing is all-in per device.  If the
      accelerator is not listed in the config, the cost is ``$0.00``.
    - **CPU-only instances**: if there is no accelerator, the cost is
      ``cpus * cpu_rate + memory * mem_rate``.

    Missing keys default to 0.0.
    """
    if accelerator_name and accelerator_count:
        accels = pricing.get('accelerators', {})
        accel_rate = next((rate for name, rate in accels.items()
                           if name.lower() == accelerator_name.lower()), 0.0)
        return accelerator_count * accel_rate

    cpu_rate = pricing.get('cpu', 0.0)
    mem_rate = pricing.get('memory', 0.0)
    return cpus * cpu_rate + memory * mem_rate
