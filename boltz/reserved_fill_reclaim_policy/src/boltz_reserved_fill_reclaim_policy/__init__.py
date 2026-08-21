"""Boltz's code-owned SkyServe reserved-fill reclaim policy."""

# ``boltz/build-overlay.sh`` stamps this package version to the immutable
# overlay release.  It identifies the built artifact, not the policy contract.
__version__ = '0.0.0'

# This revision advances only when the executable reclaim-policy contract
# changes.  Fleet and provider data have their own domain-separated digests.
# Keeping this independent from an unrelated SkyPilot release lets an unchanged
# policy continue heartbeating claims while the canonical activation command
# rotates the writer-cohort receipt.
POLICY_REVISION = '1.1.1425'
