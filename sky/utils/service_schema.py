"""JSON schema construction for SkyServe service definitions."""


def get_service_schema():
    """Schema for top-level `service:` field (for SkyServe)."""
    # To avoid circular imports, only import when needed.
    # pylint: disable=import-outside-toplevel
    from sky.serve import load_balancing_policies
    from sky.serve import placement_policy
    return {
        '$schema': 'https://json-schema.org/draft/2020-12/schema',
        'type': 'object',
        'additionalProperties': False,
        'properties': {
            'readiness_probe': {
                'anyOf': [{
                    'type': 'string',
                }, {
                    'type': 'object',
                    'required': ['path'],
                    'additionalProperties': False,
                    'properties': {
                        'path': {
                            'type': 'string',
                        },
                        'initial_delay_seconds': {
                            'type': 'number',
                        },
                        'timeout_seconds': {
                            'type': 'number',
                        },
                        'endpoint_probe_interval_seconds': {
                            'type': 'number',
                        },
                        'consecutive_failure_threshold_timeout': {
                            'type': 'number',
                        },
                        'post_data': {
                            'anyOf': [{
                                'type': 'string',
                            }, {
                                'type': 'object',
                            }]
                        },
                        'headers': {
                            'type': 'object',
                            'additionalProperties': {
                                'type': 'string'
                            }
                        },
                    }
                }]
            },
            # Cap (seconds) on the in-flight-aware drain wait when a replica
            # is retired (autoscaler scale-down, incl. rolling-update
            # retirement of outdated replicas). Unset: wait for in-flight
            # requests to finish, capped at 120s. 0: no drain. The maximum
            # mirrors serve.constants.LB_OFF_READY_OCCUPANCY_RETENTION_SECONDS:
            # beyond it the LB may stop reporting a retiring replica's async
            # occupancy as 'unknown', which would silently end the drain early.
            'graceful_drain_seconds': {
                'type': 'integer',
                'minimum': 0,
                'maximum': 7200,
            },
            # Fast-ack jobs continue after their HTTP response. Declaring the
            # contract makes failed/never-run occupancy probes unknown from
            # the outset, so autoscaling and retirement fail closed.
            'graceful_drain_async_occupancy': {
                'type': 'boolean',
            },
            'load_balancer': {
                'type': 'object',
                'required': [],
                'additionalProperties': False,
                'properties': {
                    # DEPRECATED and IGNORED. Two synchronized one-replica LB
                    # slots behind the stable LoadBalancer Service (only the
                    # selected slot accepts new data-plane traffic) is the
                    # only supported topology for a service, and pools never
                    # get one. Still accepted so existing YAMLs keep parsing;
                    # the value is discarded with a warning.
                    'high_availability': {
                        'type': 'boolean',
                    },
                    'stream_timeout_seconds': {
                        'type': 'number',
                    },
                    # Replica responses with these statuses are re-routed
                    # to another replica like transport failures. Only
                    # sensible for idempotent services and "not now"
                    # statuses (503 while warming, 429 shedding).
                    'retriable_status_codes': {
                        'type': 'array',
                        'items': {
                            'type': 'integer',
                            'minimum': 100,
                            'maximum': 599,
                        },
                    },
                    # Attempts before the client sees the error (with
                    # failed-URL exclusion each attempt tries a distinct
                    # replica while any remain).
                    'max_retries': {
                        'type': 'integer',
                        'minimum': 1,
                    },
                    # First-retry backoff; exponential with jitter after.
                    'retry_initial_backoff_seconds': {
                        'type': 'number',
                        'exclusiveMinimum': 0,
                    },
                    # Effective queue size is min(max_size, max(min_size,
                    # ready_replicas * size_per_replica)).
                    'request_queue': {
                        'type': 'object',
                        'additionalProperties': False,
                        'properties': {
                            'min_size': {
                                'type': 'integer',
                                'minimum': 0,
                                'maximum': 10000,
                            },
                            'size_per_replica': {
                                'type': 'integer',
                                'minimum': 0,
                                'maximum': 10000,
                            },
                            'max_size': {
                                'type': 'integer',
                                'minimum': 1,
                                'maximum': 10000,
                            },
                            'max_concurrency_per_replica': {
                                'type': 'integer',
                                'minimum': 1,
                                'maximum': 128,
                            },
                            'max_concurrency': {
                                'type': 'integer',
                                'minimum': 1,
                                'maximum': 128,
                            },
                            'timeout_seconds': {
                                'type': 'number',
                                'exclusiveMinimum': 0,
                            },
                            'timeout_seconds_by_priority': {
                                'type': 'array',
                                'items': {
                                    'type': 'object',
                                    'required': [
                                        'min_priority', 'timeout_seconds'
                                    ],
                                    'additionalProperties': False,
                                    'properties': {
                                        'min_priority': {
                                            'type': 'integer',
                                            'minimum': 0,
                                            'maximum': 100,
                                        },
                                        'timeout_seconds': {
                                            'type': 'number',
                                            'exclusiveMinimum': 0,
                                        },
                                    },
                                },
                            },
                            'max_request_body_bytes': {
                                'type': 'integer',
                                'minimum': 1,
                                'maximum': 16777216,
                            },
                            'use_async_occupancy': {
                                'type': 'boolean',
                            },
                        },
                    },
                },
            },
            'pool': {
                'type': 'object',
                'required': [],
                'additionalProperties': False,
                'properties': {
                    'workers': {
                        'type': 'integer',
                        'minimum': 0,
                    },
                    'min_workers': {
                        'type': 'integer',
                        'minimum': 0,
                    },
                    'queue_length_threshold': {
                        'type': 'integer',
                        'minimum': 1,
                    },
                    'max_workers': {
                        'type': 'integer',
                        'minimum': 0,
                    },
                    'upscale_delay_seconds': {
                        'type': 'number',
                        'minimum': 0,
                    },
                    'downscale_delay_seconds': {
                        'type': 'number',
                        'minimum': 0,
                    },
                    # Pools count physical workers, so the logical per-GPU
                    # placer is intentionally excluded.
                    'spot_placer': {
                        'anyOf': [{
                            'type': 'null',
                        }, {
                            'type': 'string',
                            'case_insensitive_enum': [
                                placement_policy.SPOT_HEDGE_PLACER
                            ],
                        }],
                    },
                },
            },
            'replica_policy': {
                'type': 'object',
                'required': ['min_replicas'],
                'additionalProperties': False,
                'properties': {
                    'min_replicas': {
                        'type': 'integer',
                        'minimum': 0,
                    },
                    'min_replicas_by_accelerator': {
                        'type': 'object',
                        'minProperties': 1,
                        'maxProperties': 8,
                        'patternProperties': {
                            # Exact accelerator identifiers. In particular,
                            # A100 and A100-80GB are distinct keys.
                            '^[A-Za-z0-9-]+$': {
                                'type': 'integer',
                                'minimum': 0,
                            },
                        },
                        'additionalProperties': False,
                    },
                    'max_replicas': {
                        'type': 'integer',
                        'minimum': 0,
                    },
                    'num_overprovision': {
                        'type': 'integer',
                        'minimum': 0,
                    },
                    'target_qps_per_replica': {
                        'anyOf': [
                            {
                                'type': 'number',
                                'minimum': 0,
                            },
                            {
                                'type': 'object',
                                # An empty dict has no sizing signal, and a
                                # zero value gives that shape zero capacity:
                                # both feed divisions in the instance-aware
                                # autoscaler and load balancer.
                                'minProperties': 1,
                                'patternProperties': {
                                    # Accelerator types with optional count:
                                    # "H100:1", "A100", and hyphenated
                                    # variants like "A100-80GB:1" or
                                    # "H100-MEGA-80GB" (real catalog names
                                    # include hyphens).
                                    '^[A-Za-z0-9-]+(?::[0-9]+)?$': {
                                        'type': 'number',
                                        'exclusiveMinimum': 0,
                                    }
                                },
                                'additionalProperties': False,
                            }
                        ]
                    },
                    # Per-GPU outstanding-work target. Physical replicas have
                    # capacity knob * gpu_count; logical fleets divide demand
                    # by the knob and publish whole GPU-slot targets. Mutually
                    # exclusive with target_qps_per_replica; the exclusivity
                    # (plus logical integer and load-tracking policy
                    # requirements) is validated in SkyServiceSpec.__init__,
                    # not here — this schema has no cross-field constructs.
                    'target_concurrency_per_replica': {
                        'type': 'number',
                        'exclusiveMinimum': 0,
                    },
                    'target_utilization_percentage': {
                        'type': 'integer',
                        'minimum': 1,
                        'maximum': 100,
                    },
                    'expected_request_duration_seconds': {
                        'type': 'number',
                        'exclusiveMinimum': 0,
                    },
                    'initial_provision_lead_time_seconds': {
                        'anyOf': [{
                            'type': 'number',
                            'minimum': 0,
                        }, {
                            'type': 'string',
                            'enum': ['auto'],
                        }],
                    },
                    'adaptive_demand_estimation': {
                        'type': 'boolean',
                    },
                    'max_scale_up_rate_percentage': {
                        'type': 'integer',
                        'minimum': 1,
                        'maximum': 100,
                    },
                    'scale_up_rate_min_replicas': {
                        'type': 'integer',
                        'minimum': 1,
                    },
                    'scale_up_rate_period_seconds': {
                        'type': 'integer',
                        'minimum': 1,
                    },
                    'adaptive_scale_up': {
                        'type': 'object',
                        'additionalProperties': False,
                        'properties': {
                            'max_scale_up_rate_percentage': {
                                'type': 'integer',
                                'minimum': 1,
                                'maximum': 100,
                            },
                            'scale_up_rate_min_replicas': {
                                'type': 'integer',
                                'minimum': 1,
                            },
                            'pressure_observations': {
                                'type': 'integer',
                                'minimum': 1,
                            },
                            'hold_seconds': {
                                'type': 'number',
                                'exclusiveMinimum': 0,
                            },
                        },
                    },
                    'max_scale_down_rate_percentage': {
                        'type': 'integer',
                        'minimum': 1,
                        'maximum': 100,
                    },
                    # Opt-in: allow the autoscaler to scale up onto free
                    # reserved (zero-cost) capacity. Absent/False means no
                    # behavior change; orthogonal to the demand knobs, so no
                    # cross-field constraints here or in the spec.
                    # Bool form: plain enable. Object form: enable with
                    # tuning knobs (all-defaults object == plain True).
                    'reserved_capacity_fill': {
                        'oneOf': [
                            {
                                'type': 'boolean',
                            },
                            {
                                'type': 'object',
                                'additionalProperties': False,
                                'properties': {
                                    'floor_replicas': {
                                        'type': 'integer',
                                        'minimum': 0,
                                    },
                                    'weight': {
                                        'type': 'number',
                                        'exclusiveMinimum': 0,
                                        # Keep in sync with serve.constants
                                        # RESERVED_FILL_MAX_WEIGHT: larger
                                        # finite weights overflow the
                                        # broker's water-fill arithmetic.
                                        'maximum': 1e6,
                                    },
                                    # Make the whole fill entitlement depend
                                    # on demonstrated work. This defaults to
                                    # true: an idle claimant releases both its
                                    # weighted share and reserved floor in
                                    # bounded steps. False is the explicit
                                    # opt-out for a reservation that must stay
                                    # static without utilization.
                                    'utilization_gate': {
                                        'type': 'boolean',
                                    },
                                },
                            },
                        ],
                    },
                    # Default-on economic replacement for placer-backed
                    # services. False is the explicit opt-out; True or an
                    # object enables the conservative defaults.
                    'cost_rebalance': {
                        'oneOf': [
                            {
                                'type': 'null',
                            },
                            {
                                'type': 'boolean',
                            },
                            {
                                'type': 'object',
                                'additionalProperties': False,
                                'properties': {
                                    'min_savings_fraction': {
                                        'type': 'number',
                                        'exclusiveMinimum': 0,
                                        'maximum': 1,
                                    },
                                    'max_parallel_replacements': {
                                        'type': 'integer',
                                        'minimum': 1,
                                    },
                                    'stabilization_seconds': {
                                        'type': 'number',
                                        'minimum': 0,
                                    },
                                },
                            },
                        ],
                    },
                    'dynamic_ondemand_fallback': {
                        'type': 'boolean',
                    },
                    'base_ondemand_fallback_replicas': {
                        'type': 'integer',
                        'minimum': 0,
                    },
                    'spot_placer': {
                        'anyOf': [{
                            'type': 'null',
                        }, {
                            'type': 'string',
                            'case_insensitive_enum': list(
                                placement_policy.SUPPORTED_SPOT_PLACERS),
                        }],
                    },
                    'upscale_delay_seconds': {
                        'type': 'number',
                    },
                    'downscale_delay_seconds': {
                        'type': 'number',
                    },
                }
            },
            'ports': {
                'type': 'integer',
            },
            'replicas': {
                'type': 'integer',
            },
            'workers': {
                'type': 'integer',
            },
            'load_balancing_policy': {
                'type': 'string',
                'case_insensitive_enum': list(
                    load_balancing_policies.LB_POLICIES.keys())
            },
            'tls': {
                'type': 'object',
                'required': ['keyfile', 'certfile'],
                'additionalProperties': False,
                'properties': {
                    'keyfile': {
                        'type': 'string',
                    },
                    'certfile': {
                        'type': 'string',
                    },
                },
            },
        }
    }
