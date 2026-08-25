.. _sky-api-server-upgrade:

Upgrades and High Availability
==============================

This page covers how to keep a remote SkyPilot API server resilient and up to date:

* :ref:`api-server-ha` — back the API server with an external PostgreSQL database for production deployments
* :ref:`sky-api-server-helm-upgrade` — upgrade a Helm-deployed API server gracefully
* :ref:`sky-api-server-vm-upgrade` — upgrade an API server deployed on a VM

.. _api-server-ha:

High availability
-----------------

The guarded high availability mode runs independent API, request executor, and
controller Deployments. PostgreSQL owns durable request delivery and controller
fencing, while a shared ReadWriteMany volume stores request artifacts and logs.
Any API replica can therefore accept a request or its follow-up operations.

High availability requires:

* an external, highly available PostgreSQL database;
* a PostgreSQL connection Secret that exists before the Helm installation;
* a ReadWriteMany storage class;
* at least two API, executor, and controller replicas; and
* ``RollingUpdate`` with the chart-managed disruption and drain safeguards.

Create the PostgreSQL Secret before installing the release:

.. code-block:: bash

    kubectl create secret generic skypilot-db-connection-uri \
      --namespace $NAMESPACE \
      --from-literal connection_string=postgresql://<username>:<password>@<host>:<port>/<database>

Create ``ha-values.yaml``:

.. code-block:: yaml

    apiService:
      highAvailability:
        enabled: true
      replicas: 2
      upgradeStrategy: RollingUpdate
      dbConnectionSecretName: skypilot-db-connection-uri

    requestStore:
      backend: postgres

    executorService:
      replicas: 2

    controllerService:
      replicas: 2

    storage:
      enabled: true
      accessMode: ReadWriteMany
      storageClassName: <rwx-storage-class>

The API Deployment retains the guarded ``maxSurge: 1`` and
``maxUnavailable: 0`` availability contract. The controller and executor
Deployments default to ``maxSurge: 0`` and ``maxUnavailable: 1`` so each can
replace one of its two or more replicas without requiring room for a temporary
third Pod. Their rollout bounds can be overridden independently under
``controllerService.rollingUpdate`` and ``executorService.rollingUpdate`` when
rollout preflight proves the required surge capacity.

Install or upgrade with those values:

.. code-block:: bash

    helm upgrade --install $RELEASE_NAME skypilot/skypilot-nightly --devel \
      --namespace $NAMESPACE \
      --create-namespace \
      --values ha-values.yaml \
      --wait

The migration image runs as a blocking Helm pre-install and pre-upgrade hook.
New pods are not created if the migration fails, so the previous Deployments
continue serving. Each role has a PodDisruptionBudget, spreads replicas across
nodes by default, and publishes an unready drain marker before termination.
Ingress cookie affinity is disabled in this mode because request state is
replica-independent.

Verify that every role and disruption budget is healthy:

.. code-block:: bash

    kubectl rollout status deployment/$RELEASE_NAME-api-server -n $NAMESPACE
    kubectl rollout status deployment/$RELEASE_NAME-executor -n $NAMESPACE
    kubectl rollout status deployment/$RELEASE_NAME-controller -n $NAMESPACE
    kubectl get pdb -n $NAMESPACE \
      $RELEASE_NAME-api $RELEASE_NAME-executor $RELEASE_NAME-controller

``apiService.highAvailability.readinessDrainSeconds`` defaults to 20 seconds.
The chart rejects values shorter than the configured readiness failure
detection window and rejects termination grace periods that leave less than
ten seconds after the drain interval.

Repository operators can run the destructive test-cluster conformance script
while upgrading from ``IMAGE_A`` to ``IMAGE_B``. It maintains raw and
authenticated in-cluster traffic, deletes role pods, rolls back, and upgrades
again:

.. code-block:: bash

    export SKYPILOT_HA_CONTEXT=<test-context>
    export SKYPILOT_HA_NAMESPACE=<isolated-test-namespace>
    export SKYPILOT_HA_RELEASE=<isolated-test-release>
    export SKYPILOT_HA_IMAGE_B=<target-image-or-digest>
    export SKYPILOT_HA_TOKEN_SECRET=<existing-token-secret>
    export SKYPILOT_HA_CONFIRM="$SKYPILOT_HA_CONTEXT/$SKYPILOT_HA_NAMESPACE/$SKYPILOT_HA_RELEASE"
    tests/kubernetes/ha_conformance.sh

Run this only against an isolated test release. The confirmation value is an
intentional guard because the script deletes every original API, executor, and
controller pod.

.. _api-server-persistence-db:

Back the API server with a persistent database
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The API server can optionally be configured with a PostgreSQL database to persist state. It can be an externally managed database.

If a persistent DB is not specified, the API server uses a Kubernetes persistent volume to persist state.

.. note::

  Database configuration must be set in the Helm deployment.

Configure PostgreSQL during the first Helm deployment using one of the two
options below. Guarded high availability must use option 2 because its
pre-install migration hook runs before chart-managed Secrets are created.

**Option 1: Set the DB connection URI in helm values**

Set :ref:`apiService.dbConnectionString <helm-values-apiService-dbConnectionString>` to ``postgresql://<username>:<password>@<host>:<port>/<database>`` in the helm values:

.. code-block:: bash

    # --reuse-values keeps the Helm chart values set in the previous step
    helm upgrade --install $RELEASE_NAME skypilot/skypilot-nightly --devel \
      --namespace $NAMESPACE \
      --reuse-values \
      --set apiService.dbConnectionString=postgresql://<username>:<password>@<host>:<port>/<database>

**Option 2: Set the DB connection URI via Kubernetes secret**

(available on nightly version 20250626 and later)

Create a Kubernetes secret that contains the DB connection URI:

.. code-block:: bash

    kubectl create secret generic skypilot-db-connection-uri \
      --namespace $NAMESPACE \
      --from-literal connection_string=postgresql://<username>:<password>@<host>:<port>/<database>

When installing or upgrading the Helm chart, set
``apiService.dbConnectionSecretName`` to the Secret name:

.. code-block:: bash

    helm upgrade --install $RELEASE_NAME skypilot/skypilot-nightly --devel \
      --namespace $NAMESPACE \
      --reuse-values \
      --set apiService.dbConnectionSecretName=skypilot-db-connection-uri

You can also directly set this value in the ``values.yaml`` file, e.g.:

.. code-block:: yaml

    apiService:
      dbConnectionSecretName: skypilot-db-connection-uri

.. note::

    Once :ref:`apiService.dbConnectionString <helm-values-apiService-dbConnectionString>` or :ref:`apiService.dbConnectionSecretName <helm-values-apiService-dbConnectionSecretName>` is specified, no other SkyPilot configuration can be specified in the helm chart. That is, :ref:`apiService.config <helm-values-apiService-config>` must be ``null``. To set any other SkyPilot configuration, see :ref:`sky-api-server-config`.

.. _sky-api-server-helm-upgrade:

Upgrade API server deployed with Helm
-------------------------------------

With :ref:`Helm deployement <sky-api-server-deploy>`, it is possible to :ref:`upgrade the SkyPilot API server gracefully<sky-api-server-graceful-upgrade>` without causing client-side error with the steps below.

Step 1: Prepare an upgrade
~~~~~~~~~~~~~~~~~~~~~~~~~~

1. Find the version to use in SkyPilot `nightly build <https://pypi.org/project/skypilot-nightly/#history>`_.
2. Update SkyPilot helm repository to the latest version:

.. code-block:: bash

    helm repo update skypilot

3. Prepare versioning environment variables.  ``NAMESPACE`` and ``RELEASE_NAME`` should be set to the currently installed namespace and release:

.. code-block:: bash

    NAMESPACE=skypilot # TODO: change to your installed namespace
    RELEASE_NAME=skypilot # TODO: change to your installed release name
    VERSION=1.0.0-dev20250410 # TODO: change to the version you want to upgrade to
    IMAGE_REPO=berkeleyskypilot/skypilot-nightly

Step 2: Upgrade the API server and clients
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Upgrade the clients:

.. code-block:: bash

    pip install -U skypilot-nightly==${VERSION}

Upgrade the API server:

.. code-block:: bash

    # --reuse-values is critical to keep the values set in the previous installation steps.
    helm upgrade -n $NAMESPACE $RELEASE_NAME skypilot/skypilot-nightly --devel --reuse-values \
      --set apiService.image=${IMAGE_REPO}:${VERSION}

When the API server is being upgraded, the SkyPilot CLI and Python SDK will automatically retry requests until the new version of the API server is started. So the upgrade process is graceful if the new version of the API server does not break :ref:`API compatbility<sky-api-server-api-compatibility>`. For more details, refer to :ref:`sky-api-server-graceful-upgrade`.

Optionally, you can watch the upgrade progress with:

.. code-block:: console

    $ kubectl get pod --namespace $NAMESPACE -l app=${RELEASE_NAME}-api --watch
    NAME                                       READY   STATUS            RESTARTS   AGE
    skypilot-demo-api-server-cf4896bdf-62c96   0/1     Init:0/2          0          7s
    skypilot-demo-api-server-cf4896bdf-62c96   0/1     Init:1/2          0          24s
    skypilot-demo-api-server-cf4896bdf-62c96   0/1     PodInitializing   0          26s
    skypilot-demo-api-server-cf4896bdf-62c96   0/1     Running           0          27s
    skypilot-demo-api-server-cf4896bdf-62c96   1/1     Running           0          50s

The upgraded API server is ready to serve requests after the pod becomes running and the ``READY`` column shows ``1/1``.

.. note::

    ``apiService.config`` will be IGNORED during an upgrade. To update your SkyPilot config, see :ref:`here <sky-api-server-config>`.


Step 3: Verify the upgrade
~~~~~~~~~~~~~~~~~~~~~~~~~~

Verify the API server is able to serve requests and the version is consistent with the version you upgraded to:

.. code-block:: console

    $ sky api info
    Using SkyPilot API server: <ENDPOINT>
    ├── Status: healthy, commit: 022a5c3ffe258f365764b03cb20fac70934f5a60, version: 1.0.0.dev20250410
    └── User: aclice (abcd1234)

If possible, you can also trigger your pipelines that depend on the API server to verify there is no compatibility issue after the upgrade.

.. _sky-api-server-vm-upgrade:

Upgrade the API server deployed on VM
-------------------------------------

.. note::

    VM deployment does not offer graceful upgrade. We recommend the Helm deployment :ref:`sky-api-server-deploy` in production environments. The following is a workaround for upgrading SkyPilot API server in VM deployments.

Suppose the cluster name of the API server is ``api-server`` (which is used in the :ref:`sky-api-server-cloud-deploy` guide), you can upgrade the API server with the following steps:

1. Get the version to upgrade to from SkyPilot `nightly build <https://pypi.org/project/skypilot-nightly/#history>`_.

2. Switch to the original API server endpoint used to launch the cloud VM for API server. It is usually locally started when you ran ``sky launch -c api-server skypilot-api-server.yaml`` in :ref:`sky-api-server-cloud-deploy` guide:

.. code-block:: bash

    # Replace http://localhost:46580 with the real API server endpoint if you were not using the local API server to launch the API server VM instance.
    sky api login -e http://localhost:46580

3. Check the API server VM instance is ``UP``:

.. code-block:: console

    $ sky status api-server
    Clusters
    NAME        LAUNCHED     RESOURCES                                                                  STATUS  AUTOSTOP  COMMAND
    api-server  41 mins ago  1x AWS(c6i.2xlarge, image_id={'us-east-1': 'docker:berkeleyskypilot/sk...  UP      -         sky exec api-server pip i...

4. Upgrade the clients:

.. code-block:: bash

    pip install -U skypilot-nightly==${VERSION}

.. note::

    After upgrading the clients, they should not be used until the API server is upgraded to the new version.

5. Upgrade the SkyPilot on the VM and restart the API server:

.. note::

    Upgrading and restarting the API server will interrupt all pending and running requests.

.. code-block:: bash

    sky exec api-server "pip install -U skypilot-nightly[all] && sky api stop && sky api start --deploy"
    # Alternatively, you can also upgrade to a specific version with:
    sky exec api-server "pip install -U skypilot-nightly[all]==${VERSION} && sky api stop && sky api start --deploy"

6. Switch back to the remote API server:

.. code-block:: bash

    ENDPOINT=$(sky status --endpoint api-server)
    sky api login -e $ENDPOINT

7. Verify the API server is running and the version is consistent with the version you upgraded to:

.. code-block:: console

    $ sky api info
    Using SkyPilot API server: <ENDPOINT>
    ├── Status: healthy, commit: 022a5c3ffe258f365764b03cb20fac70934f5a60, version: 1.0.0.dev20250410
    └── User: aclice (abcd1234)

.. _sky-api-server-graceful-upgrade:

Graceful upgrade
----------------

A server can be gracefully upgraded when the following conditions are met:

* :ref:`Helm deployment<sky-api-server-deploy>` is used;
* Versions before and after upgrade are :ref:`compatible<sky-api-server-api-compatibility>`;

Behavior when the API server is being upgraded:

* For critical ongoing requests (e.g., launching a cluster), it waits for them to finish with a timeout.
* For non-critical ongoing requests (e.g., log tailing), it cancels them and returns an error to ask the client to retry.
* For new requests, it returns an error to ask the client to retry. New requests will be served when the new version of the API server is ready.

To further reduce the waiting time during upgrade, you can use :ref:`rolling update for the API server<sky-api-server-upgrade-strategy>`.

SkyPilot Python SDK and CLI will automatically retry until the new version of API server starts, and ongoing requests (e.g., log tailing) will automatically resume:

.. image:: https://i.imgur.com/jUjXu0J.gif
  :alt: GIF for graceful upgrade
  :align: center

To ensure that all the regular critical requests can complete within the timeout, you can adjust the timeout by setting :ref:`apiService.terminationGracePeriodSeconds <helm-values-apiService-terminationGracePeriodSeconds>` in helm values based on your workload, e.g.:

.. code-block:: bash

    helm upgrade -n $NAMESPACE $RELEASE_NAME skypilot/skypilot-nightly --devel --reuse-values \
      --set apiService.terminationGracePeriodSeconds=300

.. _sky-api-server-upgrade-strategy:

Upgrade strategy
----------------

By default, the API server is upgraded with the ``Recreate`` strategy, which introduces waiting time for new requests during upgrade. To eliminate the waiting time, you can upgrade the API server with the ``RollingUpdate`` strategy.

.. note::

    ``RollingUpdate`` remains experimental for a compatibility deployment that
    does not enable ``apiService.highAvailability.enabled``. The guarded high
    availability configuration above uses durable request ownership and is the
    supported multi-replica rolling-upgrade path.

.. warning::

    **Compatibility RollingUpdate and local file mounts:** Local ``file_mounts``
    and ``workdir`` can be lost when an unguarded compatibility pod is replaced.
    Guarded high availability requires ``storage.enabled=false`` and rejects
    local uploads which would make one Pod's filesystem authoritative. For a
    compatibility deployment:

    - Prefer :ref:`cloud buckets <sky-storage>`, :ref:`volumes <volumes-on-kubernetes>`, or :ref:`git <sync-code-and-project-files-git>` instead of local paths; or set :ref:`jobs.bucket <config-yaml-jobs-bucket>` to redirect local file uploads to a cloud bucket.
    - If local persistent state is required, use the ``Recreate`` strategy and
      qualify that compatibility profile independently. It is not the guarded
      high-availability path.

    This does not apply if you are using a :ref:`remote jobs controller <jobs-controller-remote>`.

For a claim provisioned outside the chart, set
:ref:`storage.existingClaim <helm-values-storage-existingClaim>` and keep
``storage.enabled=true``. The claim must exist in the release namespace and be
populated and qualified before the upgrade. This option belongs only to a
non-guarded compatibility profile. Guarded high availability rejects every
PVC-backed state path and upgrades with PostgreSQL authority plus disposable,
bounded Pod-local materializations.

The following table compares the two upgrade strategies:

.. list-table:: Upgrade Strategy Comparison
   :widths: 25 35 40
   :header-rows: 1

   * - Aspect
     - ``Recreate``
     - ``RollingUpdate``
   * - **Availability**
     - Brief downtime during upgrade
     - Zero downtime
   * - **Request Handling**
     - New requests wait until upgrade completes
     - New requests served continuously by available replicas
   * - **Database Requirements**
     - Can use local storage (SQLite)
     - Must use external persistent database
   * - **Resource Usage During Upgrade**
     - Terminates old API server pod, then starts new one
     - Starts new API server pod, then terminates old one
   * - **Use Cases**
     - Development environments, simple setups
     - Production environments requiring high availability

For guarded high availability, use the complete configuration in
:ref:`api-server-ha`. For the experimental compatibility ``RollingUpdate``
path, you need to:

* :ref:`Back the API server with a persistent database <api-server-persistence-db>`;
* Disable local peristence by setting :ref:`storage.enabled <helm-values-storage-enabled>` to ``false``;
* Set :ref:`apiService.upgradeStrategy <helm-values-apiService-upgradeStrategy>` to ``RollingUpdate``;
* Keep the ingress enabled (:ref:`ingress.enabled <helm-values-ingress-enabled>` is ``true`` by default) or :ref:`configure your ingress to improve the availability during upgrade <sky-api-server-rolling-update-ingress>`;

Here's an example of deploying the API server with the ``RollingUpdate`` strategy:

.. code-block:: bash

    helm upgrade --install -n $NAMESPACE $RELEASE_NAME skypilot/skypilot-nightly --devel --reuse-values \
      --set apiService.upgradeStrategy=RollingUpdate \
      --set storage.enabled=false \
      --set apiService.dbConnectionSecretName=my-db-secret

.. _sky-api-server-rolling-update-ingress:

Ingress config
--------------

The SkyPilot Helm chart configures retry behavior during rolling upgrades.
Guarded high availability deliberately disables session affinity because any
replica can serve uploads, lookups, cancellation, and logs. If you manage the
Ingress outside the chart, do not add cookie affinity or upstream-hash
annotations for a guarded HA release.

The following compatibility-only example retains session affinity for an
unguarded rolling deployment:

.. dropdown:: Example ingress based on nginx-ingress-controller

    .. code-block:: yaml

        apiVersion: networking.k8s.io/v1
        kind: Ingress
        metadata:
          name: your-ingress-name
          annotations:
            # Compatibility mode only. Do not use affinity in guarded HA mode.
            nginx.ingress.kubernetes.io/affinity: "cookie"
            nginx.ingress.kubernetes.io/session-cookie-name: "SKYPILOT_ROUTEID"
            nginx.ingress.kubernetes.io/affinity-mode: "persistent"
            nginx.ingress.kubernetes.io/session-cookie-change-on-failure: "true"

.. _sky-api-server-api-compatibility:

API compatibility
-----------------

Starting from ``0.10.0``, SkyPilot guarantees API compatibility between adjacent minor versions, which makes graceful upgrades across minor versions possible. 

For example, assuming ``0.11.0`` is released, the following table shows one possible upgrade sequence that can upgrade the API server and clients from ``0.10.0`` to ``0.11.0`` without breaking API compatibility:

.. list-table:: Upgrade across minor versions
   :widths: 25 25 10 35
   :header-rows: 1

   * - ``Client``
     - ``Server``
     - ``Compatible``
     - ``Notes``
   * - ``0.10.0``
     - ``0.10.0``
     - ``Yes``
     - Initial state
   * - ``0.10.0``
     - ``0.11.0``
     - ``Yes``
     - Upgrade the API server first
   * - ``0.11.0``
     - ``0.11.0``
     - ``Yes``
     - Gradually upgrade all clients

When the client and server are running on different minor versions, SkyPilot CLI will print an upgrade hint as a reminder to upgrade the client:

.. code-block:: console

    $ sky status
    The SkyPilot API server is running in version X, which is newer than your client version Y. The compatibility for your current version might be dropped in the next server upgrade.
    Consider upgrading your client with:
    pip install -U skypilot==X.X.X

For a nightly build, its API compatibility is equivalent to its previous minor version, e.g., all nightly builds after ``0.10.0`` and before ``0.11.0`` have the same API compatibility guarantee as ``0.10.0``.
