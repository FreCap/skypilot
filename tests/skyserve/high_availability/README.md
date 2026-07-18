# SkyServe external load-balancer HA qualification

These tests qualify the two-slot, per-service external load balancer. They do
not combine services, replicate request bodies, or create GPU replicas.

## Presubmit and packed local scale lab

Run the deterministic gates first:

```bash
PYTHONPATH=. python -m pytest -q \
  tests/unit_tests/test_serve_lb_ha_observability.py \
  tests/unit_tests/test_serve_lb_ha_scale.py \
  tests/unit_tests/test_serve_lb_ha_cluster_qualification.py
```

Then exercise 10 services, two logical LB slots per service, and 500 backend
URLs per service with at most 100 CPU-only origins:

```bash
PYTHONPATH=. python tests/load_tests/skyserve_lb_ha_scale.py \
  --services 10 \
  --backends-per-service 500 \
  --emulator-origins 100 \
  --emulator-workers 10 \
  --output /tmp/skyserve-lb-ha-scale-10x500.json
```

The packed lab measures full payload size and serialization, complete probe
sampling, connection creation, and synchronized and jittered work for every
service pair. It runs one pair at a time because localhost has one source-IP
and ephemeral-port pool. It cannot clear aggregate 20-Pod connection churn,
conntrack, Kubernetes API, PostgreSQL, or EndpointSlice risks. The cluster run
below is authoritative for those concerns. On Darwin, large runs automatically
wait 31 seconds between synchronized and jittered scenarios because the default
16,384-port range retains closed connections for two 15-second MSL intervals.

## Existing-cluster qualification

The cluster driver is read-only except for the requested fault. It expects an
already deployed HA test fleet and never creates or scales GPU replicas. Its
target file contains the public data-plane endpoint for each service:

```json
{
  "services": [
    {
      "name": "service-a",
      "url": "https://service-a.example/health",
      "method": "GET",
      "expected_status": 200,
      "headers_env": {"Authorization": "SERVICE_A_AUTH"}
    }
  ]
}
```

Header values are resolved from environment variables and are never written
to the artifact. Use a dedicated qualification namespace: the driver rejects
extra HA services so a one-service baseline cannot accidentally measure an
already loaded 10-service namespace. Start with only one HA service deployed
to fix the Kubernetes client-latency baseline:

```bash
PYTHONPATH=. python tests/skyserve/high_availability/qualify_cluster.py \
  --targets /tmp/ha-one-service.json \
  --output /tmp/ha-one-service-baseline.json \
  --namespace skypilot \
  --expected-services 1 \
  --expected-backends 500 \
  --duration-seconds 120 \
  --mode observe
```

Then deploy the other nine services from the same release, or use a separate
10-service qualification namespace on the same cluster, and run the
steady-state case:

```bash
PYTHONPATH=. python tests/skyserve/high_availability/qualify_cluster.py \
  --targets /tmp/ha-ten-services.json \
  --output /tmp/ha-ten-services-observe.json \
  --namespace skypilot \
  --expected-services 10 \
  --expected-backends 500 \
  --single-service-baseline /tmp/ha-one-service-baseline.json \
  --duration-seconds 120 \
  --mode observe
```

For an active-Pod-loss trial, the driver deletes the selected Pod for every
service after the fault delay. It requires recovery within 15 seconds and
records deterministic role outcomes:

```bash
PYTHONPATH=. python tests/skyserve/high_availability/qualify_cluster.py \
  --targets /tmp/ha-ten-services.json \
  --output /tmp/ha-ten-services-active-loss.json \
  --namespace skypilot \
  --expected-services 10 \
  --expected-backends 500 \
  --single-service-baseline /tmp/ha-one-service-baseline.json \
  --duration-seconds 120 \
  --mode active-loss
```

For a planned handoff, append the exact update command after `--`. The command
must target the fleet represented by the target file:

```bash
PYTHONPATH=. python tests/skyserve/high_availability/qualify_cluster.py \
  --targets /tmp/ha-one-service.json \
  --output /tmp/ha-one-service-planned.json \
  --namespace skypilot \
  --expected-services 1 \
  --expected-backends 500 \
  --duration-seconds 180 \
  --mode planned -- \
  sky serve update service-a service.yaml
```

The executable gates cover traffic continuity, EndpointSlice Ready
continuity, role outcome classification, warm recovery, LB RSS, complete
backend samples, and client-observed Kubernetes 429, 5xx, timeout, and p99
scaling. A full qualification also requires time-aligned API-server and
PostgreSQL CPU, memory, connection, I/O, and event-loop evidence showing at
least 50% headroom. Distinct-backend-IP production telemetry or a temporary
500-IP service is still required to clear conntrack and `TIME_WAIT` risk.
