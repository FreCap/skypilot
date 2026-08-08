.. _api-server-metrics-setup:

Monitoring SkyPilot API Server Metrics
======================================

SkyPilot API Server can export Prometheus-compatible metrics and
optionally deploy a *one-click* Prometheus + Grafana stack so that you get
a fully functional monitoring solution out of the box.

.. tip::

   Metrics are **disabled by default**.  All the
   knobs described below can be set via ``helm upgrade`` during the initial
   installation or a later upgrade.


.. image:: ../../../images/metrics/api-srv-metrics.jpg
    :alt: Grafana dashboard
    :align: center
    :width: 80%

Quickstart: enable the full metrics stack
-----------------------------------------

If you do not already have Prometheus or Grafana running, the quickest way to get started is to let the SkyPilot Helm
chart deploy everything for you with a single command:

.. code-block:: bash

    helm upgrade --install skypilot skypilot/skypilot-nightly --devel \
      --namespace skypilot \
      --create-namespace \
      --reuse-values \
      --set apiService.metrics.enabled=true \
      --set prometheus.enabled=true \
      --set grafana.enabled=true

.. dropdown:: Turn off GPU metrics scraping

   The above command also configures Prometheus to scrape the SkyPilot API server's ``/gpu-metrics`` endpoint. To disable scraping of ``/gpu-metrics``, append ``--set prometheus.extraScrapeConfigs=""`` to the Helm command:

   .. code-block:: bash

       helm upgrade --install skypilot skypilot/skypilot-nightly --devel \
         --namespace skypilot \
         --create-namespace \
         --reuse-values \
         --set apiService.metrics.enabled=true \
         --set prometheus.enabled=true \
         --set prometheus.extraScrapeConfigs="" \
         --set grafana.enabled=true

You can access Grafana at the ``/grafana`` endpoint:

.. code-block:: bash

   # Fetch the endpoint URL
   HOST=$(kubectl get svc ${RELEASE_NAME}-ingress-nginx-controller --namespace $NAMESPACE -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
   echo http://$HOST/grafana

Metrics exposed
---------------

The ``/metrics`` endpoint exposes Prometheus-format metrics covering:

* API server health — request rate, latency, queue wait time, per-worker memory.
* Cluster inventory by workspace, user, status, cloud, and kind
  (``cluster`` / ``managed_job`` / ``controller``) — counts and GPU
  occupancy by accelerator model. Filter ``kind="cluster"`` to avoid
  overlap with managed-job clusters; sum across kinds for total
  resource usage.
* Managed jobs by workspace, user, status, and cloud (all statuses
  including terminal; use ``delta(...)`` over a window for per-period
  success/failure rate).

In guarded HA mode, API, executor, and controller pods expose independent
pod-local endpoints. This is necessary because executor workers and controller
children write to the multiprocess registry in their own pod. Discover every
annotated role pod; the API Service selects only API pods and cannot expose
executor- or controller-local series.
Shared-state collectors (including burn rate, workspace usage, managed jobs,
and plugin custom collectors) remain on API/all targets so aggregating all role
targets does not duplicate those series. Plugins emit executor/controller-local
telemetry with multiprocess-aware Prometheus metric types.

You can also :ref:`setup GPU metric collection <api-server-gpu-metrics-setup>`
to directly export GPU memory, utilization and power consumption from
each compute cluster.

Forward metrics to an OpenTelemetry-based backend
-------------------------------------------------

If your observability stack is built on OpenTelemetry (Datadog,
Honeycomb, GCP Cloud Monitoring, Tempo + Mimir, etc.) rather than
vanilla Prometheus, deploy an `OpenTelemetry Collector
<https://opentelemetry.io/docs/collector/>`__ as a bridge: its
`Prometheus receiver
<https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/receiver/prometheusreceiver>`__
scrapes SkyPilot's endpoints and an OTLP exporter forwards downstream.

The only SkyPilot-specific part is the scrape config. For a standalone server,
point it at the API Service (``<release>-api-service.<namespace>.svc`` on the
metrics port, 9090 by default). In guarded HA mode, configure Kubernetes pod
discovery for the chart's scrape annotations so the API, executor, and
controller registries are all collected. The federated ``/gpu-metrics`` route
remains on the API Service and needs a ``scrape_timeout`` larger than the API
server's per-context federation budget (20 s):

.. code-block:: yaml

   receivers:
     prometheus:
       config:
         scrape_configs:
           - job_name: skypilot-roles
             kubernetes_sd_configs:
               - role: pod
                 namespaces:
                   names: ['<namespace>']
             relabel_configs:
               - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_scrape]
                 action: keep
                 regex: 'true'
               - source_labels: [__meta_kubernetes_pod_label_app]
                 action: keep
                 regex: '<release>-(api|executor|controller)'
               - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_path]
                 action: replace
                 target_label: __metrics_path__
               - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_port,
                                 __meta_kubernetes_pod_ip]
                 action: replace
                 regex: '(\d+);(.+)'
                 replacement: '$2:$1'
                 target_label: __address__
           - job_name: skypilot-gpu
             metrics_path: /gpu-metrics
             scrape_timeout: 25s   # must exceed the 20s per-context budget
             static_configs:
               - targets: ['<release>-api-service.<namespace>.svc:9090']

Configure the processors, OTLP exporter, pipelines, and Collector
deployment mode per the `Collector configuration docs
<https://opentelemetry.io/docs/collector/configuration/>`__ — those are
generic OpenTelemetry concerns.

Using existing Prometheus / Grafana
-----------------------------------

The Helm chart introduces **three new top-level blocks** to provide flexibility in how you set up Prometheus and Grafana:

* ``apiService.metrics.enabled`` – enables the ``/metrics`` HTTP endpoint on API-server role pods.
* ``prometheus.enabled`` – deploys a Prometheus instance configured to discover the enabled role endpoints.
* ``grafana.enabled`` – deploys Grafana with a pre-baked dashboard to display the SkyPilot API server metrics from prometheus.

All three default to ``false`` so you can mix & match:

* **Fully managed Prometheus + Grafana** – set ``apiService.metrics.enabled: true``, ``prometheus.enabled: true``, and ``grafana.enabled: true``. The chart will deploy a fully managed Prometheus + Grafana stack.
* **External Prometheus / Grafana** – set *only* ``apiService.metrics.enabled: true``. API-server role pods expose ``/metrics`` and are annotated with ``prometheus.io/scrape: true`` for automatic Prometheus discovery.
* **External Grafana, internal Prometheus** – enable ``prometheus`` but disable ``grafana``. Point your existing Grafana at the Prometheus service created by the chart.
