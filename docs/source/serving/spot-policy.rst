.. _spot_policy:

Using Spot Instances for Serving
================================

SkyServe supports serving models on a mixture of spot and on-demand replicas with two options: :code:`base_ondemand_fallback_replicas` and :code:`dynamic_ondemand_fallback`. Currently, SkyServe relies on the user side to retry in the event of spot instance preemptions.

Base on-demand fallback
-----------------------

:code:`base_ondemand_fallback_replicas` sets the number of on-demand replicas to keep running at all times. This is useful for ensuring service availability and making sure that there is always some capacity available, even if spot replicas are not available. :code:`use_spot` should be set to :code:`true` to enable spot replicas.

.. code-block:: yaml

    service:
      readiness_probe: /health
      replica_policy:
        min_replicas: 2
        max_replicas: 3
        target_qps_per_replica: 1
        # Ensures that one of the replicas is run on on-demand instances
        base_ondemand_fallback_replicas: 1

    resources:
      ports: 8081
      cpus: 2+
      use_spot: true

    workdir: examples/serve/http_server

    run: python3 server.py


.. tip::

    Kubernetes instances are considered on-demand instances. You can use the :code:`base_ondemand_fallback_replicas` option to have some replicas run on Kubernetes, while others run on cloud spot instances.

Dynamic on-demand fallback
--------------------------

SkyServe supports dynamically fallback to on-demand replicas when spot replicas are not available.
This is enabled by setting :code:`dynamic_ondemand_fallback` to be :code:`true`.
This is useful for ensuring the required capacity of replicas in the case of spot instance interruptions.
When spot replicas are available, SkyServe will automatically switch back to using spot replicas to maximize cost savings.

.. code-block:: yaml

    service:
      readiness_probe: /health
      replica_policy:
        min_replicas: 2
        max_replicas: 3
        target_qps_per_replica: 1
        # Allows replicas to be run on on-demand instances if spot instances are not available
        dynamic_ondemand_fallback: true

    resources:
      ports: 8081
      cpus: 2+
      use_spot: true

    workdir: examples/serve/http_server

    run: python3 server.py


.. tip::

    SkyServe supports specifying both :code:`base_ondemand_fallback_replicas` and :code:`dynamic_ondemand_fallback`. Specifying both will set a base number of on-demand replicas and dynamically fallback to on-demand replicas when spot replicas are not available.

Cost-aware multi-GPU placement
------------------------------

For a heterogeneous GPU ``resources.any_of`` fleet with per-GPU concurrency,
``dynamic_fallback_per_gpu`` is the primary placement policy. It fills the
lowest-cost active machine shape by
hourly price divided by accelerator count. SkyPilot keeps selecting that shape
until a launch failure temporarily benches its exact location, then falls
through to the next-cheapest active candidate. Benched locations become
eligible for a bounded probe after the retry window, so recovered cheap
capacity is filled again without pinning retries to an unavailable location.
SkyPilot expands each paid spot entry across the whole-GPU machine widths
supported by its provider catalog, so the service does not need to duplicate
instance-shape lists. Configured cloud and region constraints still apply.

.. code-block:: yaml

    service:
      graceful_drain_async_occupancy: true
      replica_policy:
        min_replicas: 1
        max_replicas: 100
        target_concurrency_per_replica: 1
        target_utilization_percentage: 90
        expected_request_duration_seconds: 30
        max_scale_up_rate_percentage: 20
        scale_up_rate_min_replicas: 10
        scale_up_rate_period_seconds: 60
        max_scale_down_rate_percentage: 50
        spot_placer: dynamic_fallback_per_gpu

    resources:
      any_of:
        - infra: aws/us-east-1
          accelerators: L4
          use_spot: true
        - infra: gcp/us-central1
          accelerators: L4
          use_spot: true

``dynamic_fallback`` continues to compare raw hourly machine prices and count
physical backends; pools use that physical-worker contract. Use the per-GPU
policy when each configured GPU contributes one equivalent serving
slot. The policy automatically makes ``min_replicas``, ``max_replicas``, and
the autoscaler target count logical GPU slots, while the selected physical
backend shape stays internal to SkyServe. A positive integer
``target_concurrency_per_replica`` controls how many simultaneous requests map
to each slot; it does not change the occupancy-gated execution concurrency.
``target_utilization_percentage`` reserves request-slot headroom. When
``expected_request_duration_seconds`` is set, recently rejected requests are
converted from the load balancer's retained population into concurrent work.
The scale-rate fields bound target changes to timed waves. Multiple accelerator
models can be supplied as separate ``any_of`` entries; supported widths are
discovered independently for each model.
Non-spot entries and cluster-backed clouds such as Kubernetes remain at their
explicitly configured count. SkyPilot does not inspect those live cluster APIs
to discover additional shapes.

.. note::

    Controllers that predate ``dynamic_fallback_per_gpu`` do not recognize the
    policy. Before updating an existing service, make sure the SkyPilot API
    server and service controllers run a release that supports it.

Cost-aware replacement
----------------------

With a multi-location ``dynamic_fallback`` policy, cost rebalancing is enabled
by default. It replaces an existing replica when a capacity-equivalent
location remains at least the configured fraction cheaper. SkyServe launches
and health-checks the replacement first. It then removes the incumbent from
load-balancer routing and terminates it only after the load balancer proves
that no request is still in flight. Normal demand autoscaling remains bounded
by ``max_replicas``; ``max_parallel_replacements`` permits only temporary
paired overlap. Set ``cost_rebalance: false`` to opt out.

.. code-block:: yaml

    service:
      replica_policy:
        min_replicas: 1
        max_replicas: 500
        target_concurrency_per_replica: 1
        spot_placer: dynamic_fallback
        cost_rebalance:
          min_savings_fraction: 0.3
          max_parallel_replacements: 8
          stabilization_seconds: 300

The savings comparison uses hourly cost per configured serving-capacity unit,
not raw per-GPU price. ``stabilization_seconds`` requires the candidate to stay
eligible continuously before a replacement starts. With
``reserved_capacity_fill``, generic rebalancing remains paid-to-paid;
broker-granted fill remains the only path that launches zero-cost capacity.

Example
-------

The following example demonstrates how to use spot replicas with SkyServe with dynamic fallback. The example is a simple HTTP server that listens on port 8081 with :code:`dynamic_ondemand_fallback: true`. To run:

.. code-block:: console

    $ sky serve up examples/serve/spot_policy/dynamic_on_demand_fallback.yaml -n http-server

When the service is up, we can check the status of the service and the replicas using the following command. Initially, we will see:

.. code-block:: console

    $ sky serve status http-server

    Services
    NAME         VERSION  UPTIME  STATUS      REPLICAS  ENDPOINT
    http-server  1        1m 17s  NO_REPLICA  0/4       54.227.229.217:30001

    Service Replicas
    SERVICE_NAME  ID  VERSION  ENDPOINT  LAUNCHED    INFRA                RESOURCES                                      STATUS         
    http-server   1   1        -         1 min ago   GCP (us-east1)       1x[spot](cpus=2, mem=8, n2-standard-2, ...)   PROVISIONING  
    http-server   2   1        -         1 min ago   GCP (us-central1)    1x[spot](cpus=2, mem=8, n2-standard-2, ...)   PROVISIONING  
    http-server   3   1        -         1 mins ago  GCP (us-east1)       1x(cpus=2, mem=8, n2-standard-2, ...)         PROVISIONING  
    http-server   4   1        -         1 min ago   GCP (us-central1)    1x(cpus=2, mem=8, n2-standard-2, ...)         PROVISIONING  

When the required number of spot replicas are not available, SkyServe will provision on-demand replicas to meet the target number of replicas. For example, when the target number is 2 and no spot replicas are ready, SkyServe will provision 2 on-demand replicas to meet the target number of replicas.

.. code-block:: console

    $ sky serve status http-server

    Services
    NAME         VERSION  UPTIME  STATUS  REPLICAS  ENDPOINT
    http-server  1        1m 17s  READY   2/4       54.227.229.217:30001

    Service Replicas
    SERVICE_NAME  ID  VERSION  ENDPOINT                   LAUNCHED    INFRA                RESOURCES                                      STATUS         
    http-server   1   1        http://34.23.22.160:8081   3 min ago   GCP (us-east1)       1x[spot](cpus=2, mem=8, n2-standard-2, ...)   READY          
    http-server   2   1        http://34.68.226.193:8081  3 min ago   GCP (us-central1)    1x[spot](cpus=2, mem=8, n2-standard-2, ...)   READY          
    http-server   3   1        -                          3 mins ago  GCP (us-east1)       1x(cpus=2, mem=8, n2-standard-2, ...)         SHUTTING_DOWN  
    http-server   4   1        -                          3 min ago   GCP (us-central1)    1x(cpus=2, mem=8, n2-standard-2, ...)         SHUTTING_DOWN  

When the spot replicas are ready, SkyServe will automatically scale down on-demand replicas to maximize cost savings.

.. code-block:: console

    $ sky serve status http-server

    Services
    NAME         VERSION  UPTIME  STATUS  REPLICAS  ENDPOINT
    http-server  1        3m 59s  READY   2/2       54.227.229.217:30001

    Service Replicas
    SERVICE_NAME  ID  VERSION  ENDPOINT                   LAUNCHED    INFRA                RESOURCES                                      STATUS         
    http-server   1   1        http://34.23.22.160:8081   4 mins ago  GCP (us-east1)       1x[spot](cpus=2, mem=8, n2-standard-2, ...)   READY          
    http-server   2   1        http://34.68.226.193:8081  4 mins ago  GCP (us-central1)    1x[spot](cpus=2, mem=8, n2-standard-2, ...)   READY          

In the event of spot instance interruptions (e.g. replica 1), SkyServe will automatically fallback to on-demand replicas (e.g. launch one on-demand replica) to meet the required capacity of replicas. SkyServe will continue trying to provision one spot replica in the event where spot availability is back. Note that SkyServe will try different regions and clouds to maximize the chance of successfully provisioning spot instances.

.. code-block:: console

    $ sky serve status http-server

    Services
    NAME         VERSION  UPTIME  STATUS  REPLICAS  ENDPOINT
    http-server  1        7m 2s   READY   1/3       54.227.229.217:30001

    Service Replicas
    SERVICE_NAME  ID  VERSION  ENDPOINT                   LAUNCHED     INFRA                RESOURCES                                      STATUS         
    http-server   2   1        http://34.68.226.193:8081  7 mins ago   GCP (us-central1)    1x[spot](cpus=2, mem=8, n2-standard-2, ...)   READY         
    http-server   5   1        -                          13 secs ago  GCP (us-central1)    1x[spot](cpus=2, mem=8, n2-standard-2, ...)   PROVISIONING  
    http-server   6   1        -                          13 secs ago  GCP (us-central1)    1x(cpus=2, mem=8, n2-standard-2, ...)         PROVISIONING  

Eventually, when the spot availability is back, SkyServe will automatically scale down on-demand replicas.

.. code-block:: console

    $ sky serve status http-server

    Services
    NAME         VERSION  UPTIME  STATUS  REPLICAS  ENDPOINT
    http-server  1        10m 5s  READY   2/3       54.227.229.217:30001

    Service Replicas
    SERVICE_NAME  ID  VERSION  ENDPOINT                   LAUNCHED     INFRA                RESOURCES                                      STATUS         
    http-server   2   1        http://34.68.226.193:8081  10 mins ago  GCP (us-central1)    1x[spot](cpus=2, mem=8, n2-standard-2, ...)   READY          
    http-server   5   1        http://34.121.49.94:8081   1 min ago    GCP (us-central1)    1x[spot](cpus=2, mem=8, n2-standard-2, ...)   READY          
