# Reproductions

Standalone, cluster-free reproductions for open issues. These are **not** part
of the test suite and are not expected to pass on `improvements`: their job is
to demonstrate a defect and to give whoever picks it up a harness that already
encodes the scenarios.

Run one with the repo on the path:

```bash
PYTHONPATH=. python tests/reproductions/repro_1301_preempted_card_repricing.py
```

| File | Issue |
|---|---|
| `repro_1301_preempted_card_repricing.py` | #1301, free-tier card targets leak into the paid tier |
| `test_1301_preempted_card_repricing.py` | #1301, the scenarios a fix must satisfy |

For `#1301`, the three defect cases are `xfail(strict=True)`: green while the
bug exists, hard-failing the moment a fix lands so that change must promote
them into ordinary regression tests. The guard classes already pass and **must
keep passing**: they are what proves a fix has not traded the cost defect for
dropped serving capacity during a rolling update.
