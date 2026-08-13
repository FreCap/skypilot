.. _yaml-spec:

SkyPilot YAML
=============

SkyPilot provides an intuitive YAML interface to specify clusters, jobs, or services (resource requirements, setup commands, run commands, file mounts, storage mounts, and so on).

**All fields in the YAML specification are optional.** When unspecified, its
default value is used. You can specify only the fields that are relevant to
your task.

YAMLs can be used with the :ref:`CLI <cli>`, or the programmatic API (e.g., :meth:`sky.Task.from_yaml`).


Syntax
------

Below is the configuration syntax and some example values.  See details under each field.

.. parsed-literal::

  :ref:`name <yaml-spec-name>`: my-task

  :ref:`workdir <yaml-spec-workdir>`: ~/my-task-code

  :ref:`num_nodes <yaml-spec-num-nodes>`: 4

  :ref:`resources <yaml-spec-resources>`:
    # Infra to use. Click to see schema and example values.
    :ref:`infra <yaml-spec-resources-infra>`: aws

    # Hardware.
    :ref:`accelerators <yaml-spec-resources-accelerators>`: H100:8
    :ref:`accelerator_args <yaml-spec-resources-accelerator-args>`:
      runtime_version: tpu-vm-base
    :ref:`cpus <yaml-spec-resources-cpus>`: 4+
    :ref:`memory <yaml-spec-resources-memory>`: 32+
    :ref:`instance_type <yaml-spec-resources-instance-type>`: p3.8xlarge
    :ref:`use_spot <yaml-spec-resources-use-spot>`: false
    :ref:`disk_size <yaml-spec-resources-disk-size>`: 256
    :ref:`ephemeral_storage <yaml-spec-resources-ephemeral-storage>`: 50
    :ref:`disk_tier <yaml-spec-resources-disk-tier>`: medium
    :ref:`network_tier <yaml-spec-resources-network-tier>`: best
    :ref:`max_hourly_cost <yaml-spec-resources-max-hourly-cost>`: 10.0

    # Config.
    :ref:`container_image <yaml-spec-resources-container-image>`: ghcr.io/my-org/model@sha256:<64-hex-digest>
    :ref:`image_id <yaml-spec-resources-image-id>`: ami-0868a20f5a3bf9702
    :ref:`ports <yaml-spec-resources-ports>`: 8081
    :ref:`labels <yaml-spec-resources-labels>`:
      my-label: my-value
    :ref:`autostop <yaml-spec-resources-autostop>`:
      idle_minutes: 10
      wait_for: none

    :ref:`any_of <yaml-spec-resources-any-of>`:
      - infra: aws/us-west-2
        accelerators: H100
      - infra: gcp/us-central1
        accelerators: H100

    :ref:`ordered <yaml-spec-resources-ordered>`:
      - infra: aws/us-east-1
      - infra: aws/us-west-2

    :ref:`job_recovery <yaml-spec-resources-job-recovery>`: none

  :ref:`envs <yaml-spec-envs>`:
    MY_BUCKET: skypilot-temp-gcs-test
    MY_LOCAL_PATH: tmp-workdir
    MODEL_SIZE: 13b

  :ref:`secrets <yaml-spec-secrets>`:
    MY_HF_TOKEN: my-secret-value
    WANDB_API_KEY: my-secret-value-2

  :ref:`api_server_access <yaml-spec-api-server-access>`: true

  :ref:`volumes <yaml-spec-new-volumes>`:
    /mnt/data: volume-name
    /mnt/cache:
      size: 100Gi

  :ref:`file_mounts <yaml-spec-file-mounts>`:
    # Sync a local directory to a remote directory
    /remote/path: /local/path
    # Mount a S3 bucket to a remote directory
    /checkpoints:
      source: s3://existing-bucket
      mode: MOUNT
    # Mount with VFS caching and a pre-tuned workload type
    /data:
      source: s3://my-model-data
      mode: MOUNT_CACHED
      type: DATASET_RO
    /datasets-s3: s3://my-awesome-dataset

  :ref:`setup <yaml-spec-setup>`: |
    echo "Begin setup."
    pip install -r requirements.txt
    echo "Setup complete."

  :ref:`run <yaml-spec-run>`: |
    echo "Begin run."
    python train.py
    echo Env var MODEL_SIZE has value: ${MODEL_SIZE}

  :ref:`config <yaml-spec-config>`:
    kubernetes:
      provision_timeout: 600

Fields
----------

.. _yaml-spec-name:

``name``
~~~~~~~~

Task name (optional), used for display purposes.

.. code-block:: yaml

  name: my-task

.. _yaml-spec-workdir:

``workdir``
~~~~~~~~~~~

``workdir`` can be a local working directory or a git repository (optional). It is synced or cloned to ``~/sky_workdir`` on the remote cluster each time ``sky launch`` or ``sky exec`` is run with the YAML file.

**Local Directory**:

If ``workdir`` is a local path, the entire directory is synced to the remote cluster. To exclude files from syncing, see :ref:`exclude-uploading-files`.

If a relative path is used, it's evaluated relative to the location from which ``sky`` is called.

**Git Repository**:

If ``workdir`` is a git repository, the ``url`` field is required and can be in one of the following formats:

* HTTPS: ``https://github.com/skypilot-org/skypilot.git``
* SSH: ``ssh://git@github.com/skypilot-org/skypilot.git``
* SCP: ``git@github.com:skypilot-org/skypilot.git``

The ``ref`` field specifies the git reference to checkout, which can be:

* A branch name (e.g., ``main``, ``develop``)
* A tag name (e.g., ``v1.0.0``)
* A commit hash (e.g., ``abc123def456``)

**Authentication for Private Repositories**:

*For HTTPS URLs*: Set the ``GIT_TOKEN`` environment variable. SkyPilot will automatically use this token for authentication.

*For SSH/SCP URLs*: SkyPilot will attempt to authenticate using SSH keys in the following order:

1. SSH key specified by the ``GIT_SSH_KEY_PATH`` environment variable
2. SSH key configured in ``~/.ssh/config`` for the git host
3. Default SSH key at ``~/.ssh/id_rsa``
4. Default SSH key at ``~/.ssh/id_ed25519`` (if ``~/.ssh/id_rsa`` does not exist)

Commands in ``setup`` and ``run`` will be executed under ``~/sky_workdir``.

.. code-block:: yaml

  workdir: ~/my-task-code

OR

.. code-block:: yaml

  workdir: ../my-project  # Relative path

OR

.. code-block:: yaml

  workdir:
    url: https://github.com/skypilot-org/skypilot.git
    ref: main

.. _yaml-spec-num-nodes:

``num_nodes``
~~~~~~~~~~~~~

Number of nodes (optional; defaults to 1) to launch including the head node.

A task can set this to a smaller value than the size of a cluster.

.. code-block:: yaml

  num_nodes: 4


.. _yaml-spec-resources:

``resources``
~~~~~~~~~~~~~

Per-node resource requirements (optional).

.. code-block:: yaml

  resources:
    infra: aws
    instance_type: p3.8xlarge


.. _yaml-spec-resources-infra:

``resources.infra``
~~~~~~~~~~~~~~~~~~~


Infrastructure to use (optional).

Schema: ``<cloud>/<region>/<zone>`` (region
and zone are optional), or ``k8s/<context-name>`` (context-name is optional).
Wildcards are supported in any component.

Example values: ``aws``, ``aws/us-east-1``, ``aws/us-east-1/us-east-1a``,
``aws/*/us-east-1a``, ``k8s``, ``k8s/my-cluster-context``.

.. code-block:: yaml

  resources:
    infra: aws  # Use any available AWS region/zone.


.. code-block:: yaml

  resources:
    infra: k8s  # Use any available Kubernetes context.

You can also specify a specific region, zone, or Kubernetes context.

.. code-block:: yaml

  resources:
    infra: aws/us-east-1


.. code-block:: yaml

  resources:
    infra: aws/us-east-1/us-east-1a


.. code-block:: yaml

  resources:
    infra: k8s/my-h100-cluster-context


.. _yaml-spec-resources-autostop:

``resources.autostop``
~~~~~~~~~~~~~~~~~~~~~~

Autostop configuration (optional).

Controls whether and when to automatically stop or tear down the cluster after it becomes idle. See :ref:`auto-stop` for more details.

Format:

- ``true``: Use default idle minutes (5)
- ``false``: Disable autostop
- ``<num>``: Stop after this many idle minutes
- ``<num><unit>``: Stop after this much time
- Object with configuration:

  - ``idle_minutes``: Number of idle minutes before stopping
  - ``down``: If true, tear down the cluster instead of stopping it
  - ``wait_for``: Determines the condition for resetting the idleness timer.
    Options:

    - ``jobs_and_ssh`` (default): Wait for in‑progress jobs and SSH connections to finish
    - ``jobs``: Only wait for in‑progress jobs
    - ``none``: Wait for nothing; autostop right after ``idle_minutes``

To run a script before autostop, see :ref:`Lifecycle hooks <lifecycle-hooks>`
(under ``config.hooks`` with ``events: [stop]`` for autostop, or
``events: [down]`` for autodown — ``autostop: {down: true}``).

``<unit>`` can be one of:
- ``m``: minutes (default if not specified)
- ``h``: hours
- ``d``: days
- ``w``: weeks


Example:

.. code-block:: yaml

  resources:
    autostop: true  # Stop after default idle minutes (5)

OR

.. code-block:: yaml

  resources:
    autostop: 10  # Stop after 10 minutes

OR

.. code-block:: yaml

  resources:
    autostop: 10h  # Stop after 10 hours

OR

.. code-block:: yaml

  resources:
    autostop:
      idle_minutes: 10
      down: true  # Use autodown instead of autostop

OR

.. code-block:: yaml

  resources:
    autostop:
      idle_minutes: 10
      wait_for: none  # Stop after 10 minutes, regardless of running jobs or SSH connections


.. _yaml-spec-resources-accelerators:

``resources.accelerators``
~~~~~~~~~~~~~~~~~~~~~~~~~~

Accelerator name and count per node (optional).

Use ``sky gpus list`` to view available accelerator configurations.

The following three ways are valid for specifying accelerators for a cluster:

- To specify a single type of accelerator:

  Format: ``<name>:<count>`` (or simply ``<name>``, short for a count of 1).

  Example: ``H100:4``

- To specify an ordered list of accelerators (try the accelerators in the specified order):

  Format: ``[<name>:<count>, ...]``

  Example: ``['L4:1', 'H100:1', 'A100:1']``

- To specify an unordered set of accelerators (optimize all specified accelerators together, and try accelerator with lowest cost first):

  Format: ``{<name>:<count>, ...}``

  Example: ``{'L4:1', 'H100:1', 'A100:1'}``

.. code-block:: yaml

  resources:
    accelerators: V100:8

OR

.. code-block:: yaml

  resources:
    accelerators:
      - A100:1
      - V100:1

OR

.. code-block:: yaml

  resources:
    accelerators: {A100:1, V100:1}


.. _yaml-spec-resources-accelerator-args:

``resources.accelerator_args``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Additional accelerator metadata (optional); only used for TPU node and TPU VM.

Example usage:

- To request a TPU VM:

  .. code-block:: yaml

    resources:
      accelerator_args:
        tpu_vm: true  # optional, default: True

- To request a TPU node:

  .. code-block:: yaml

    resources:
      accelerator_args:
        tpu_name: mytpu
        tpu_vm: false

By default, the value for ``runtime_version`` is decided based on which is requested and should work for either case. If passing in an incompatible version, GCP will throw an error during provisioning.

Example:

.. code-block:: yaml

  resources:
    accelerator_args:
      # Default is "tpu-vm-base" for TPU VM and "2.12.0" for TPU node.
      runtime_version: tpu-vm-base
      # tpu_name: mytpu
      # tpu_vm: false  # True to use TPU VM (the default); False to use TPU node.



.. _yaml-spec-resources-cpus:

``resources.cpus``
~~~~~~~~~~~~~~~~~~

Number of vCPUs per node (optional).

Format:

- ``<count>``: exactly ``<count>`` vCPUs
- ``<count>+``: at least ``<count>`` vCPUs

Example: ``4+`` means first try to find an instance type with >= 4 vCPUs. If not found, use the next cheapest instance with more than 4 vCPUs.

.. code-block:: yaml

  resources:
    cpus: 4+

OR

.. code-block:: yaml

  resources:
    cpus: 16


.. _yaml-spec-resources-memory:

``resources.memory``
~~~~~~~~~~~~~~~~~~~~

Memory specification per node (optional).

Format:

-  ``<num>``: exactly ``<num>`` GB
-  ``<num>+``: at least ``<num>`` GB
-  ``<num><unit>``: memory with unit (e.g., ``1024MB``, ``64GB``)

Units supported (case-insensitive):
- KB (kilobytes, 2^10 bytes)
- MB (megabytes, 2^20 bytes)
- GB (gigabytes, 2^30 bytes) (default if not specified)
- TB (terabytes, 2^40 bytes)
- PB (petabytes, 2^50 bytes)

Example: ``32+`` means first try to find an instance type with >= 32 GiB. If not found, use the next cheapest instance with more than 32 GiB.

.. code-block:: yaml

  resources:
    memory: 32+

OR

.. code-block:: yaml

  resources:
    memory: 64GB

.. _yaml-spec-resources-instance-type:

``resources.instance_type``
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Instance type to use (optional).

If ``accelerators`` is specified, the corresponding instance type is automatically inferred.

.. code-block:: yaml

  resources:
    instance_type: p3.8xlarge


.. _yaml-spec-resources-use-spot:

``resources.use_spot``
~~~~~~~~~~~~~~~~~~~~~~

Whether the cluster should use spot instances (optional).

If unspecified, defaults to ``false`` (on-demand instances).

.. code-block:: yaml

  resources:
    use_spot: true


.. _yaml-spec-resources-disk-size:

``resources.disk_size``
~~~~~~~~~~~~~~~~~~~~~~~

Integer disk size in GB to allocate for OS (mounted at ``/``) OR specify units.

Increase this if you have a large working directory or tasks that write out large outputs.

Units supported (case-insensitive):

- KB (kilobytes, 2^10 bytes)
- MB (megabytes, 2^20 bytes)
- GB (gigabytes, 2^30 bytes)
- TB (terabytes, 2^40 bytes)
- PB (petabytes, 2^50 bytes)

.. warning::

   The disk size will be rounded down (floored) to the nearest gigabyte. For example, ``1500MB`` or ``2000MB`` will be rounded to ``1GB``.

.. code-block:: yaml

  resources:
    disk_size: 256

OR

.. code-block:: yaml

  resources:
    disk_size: 256GB



.. _yaml-spec-resources-ephemeral-storage:

``resources.ephemeral_storage``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ephemeral storage to request for Kubernetes pods, specified as an integer in GB or as a string with units (e.g., ``50GB``).

This sets the ``resources.requests.ephemeral-storage`` field in the Kubernetes pod spec.
When :ref:`set_pod_resource_limits <config-yaml-kubernetes-set-pod-resource-limits>` is configured in the SkyPilot config, it also sets
``resources.limits.ephemeral-storage`` using the multiplier defined there.

This field is **only effective on Kubernetes**. It is ignored on other clouds.

Increase this if your tasks download large datasets or produce significant temporary files that
could exhaust the node's ephemeral storage and trigger pod evictions.

Units supported (case-insensitive):

- KB (kilobytes, 2^10 bytes)
- MB (megabytes, 2^20 bytes)
- GB (gigabytes, 2^30 bytes)
- TB (terabytes, 2^40 bytes)
- PB (petabytes, 2^50 bytes)

.. warning::

   The ephemeral storage size will be rounded down (floored) to the nearest gigabyte. For example, ``1500MB`` or ``2000MB`` will be rounded to ``1GB``.

.. code-block:: yaml

  resources:
    infra: kubernetes
    ephemeral_storage: 50

OR

.. code-block:: yaml

  resources:
    infra: kubernetes
    ephemeral_storage: 50GB



.. _yaml-spec-resources-disk-tier:

``resources.disk_tier``
~~~~~~~~~~~~~~~~~~~~~~~
Disk tier to use for OS (optional).

Could be one of ``'low'``, ``'medium'``, ``'high'``, ``'ultra'`` or ``'best'`` (default: ``'medium'``).

If ``'best'`` is specified, use the best disk tier enabled.

Rough performance estimate:

- low: 1000 IOPS; read 90 MB/s; write 90 MB/s
- medium: 3000 IOPS; read 220 MB/s; write 220 MB/s
- high: 6000 IOPS; read 400 MB/s; write 400 MB/s
- ultra: 60000 IOPS;  read 4000 MB/s; write 3000 MB/s

Measured by ``examples/perf/storage_rawperf.yaml``

.. code-block:: yaml

  resources:
    disk_tier: medium

OR

.. code-block:: yaml

  resources:
    disk_tier: best


.. _yaml-spec-resources-network-tier:

``resources.network_tier``
~~~~~~~~~~~~~~~~~~~~~~~~~~
Network tier to use (optional).

Could be one of ``'standard'`` or ``'best'`` (default: ``'standard'``).

If ``'best'`` is specified, use the best network tier available on the specified infra. This currently supports:

**VM-based:**

- ``infra: aws``: Enable Elastic Fabric Adapter (EFA) for high-performance inter-node communication on EFA-supported instance types (e.g., p4d, p5, p5e, p5en, p6-b200, p6-b300, etc.).
- ``infra: gcp``: Enable GPUDirect-TCPX/TCPXO/RDMA for high-performance node-to-node GPU communication on supported instance types (A3 High, A3 Edge, A3 Mega, A3 Ultra, A4).
- ``infra: nebius``: Enable InfiniBand for high-performance GPU communication across Nebius VMs. Currently only supported for H100:8 and H200:8 nodes.

**Kubernetes-based:**

- ``infra: k8s/my-eks-or-hyperpod-cluster``: Enable EFA for high-performance inter-node communication across pods on AWS EKS/HyperPod clusters.
- ``infra: k8s/my-gke-cluster``: Enable GPUDirect-TCPX/TCPXO/RDMA for high-performance GPU communication across pods on Google Kubernetes Engine (GKE).
- ``infra: k8s/my-coreweave-cluster``: Enable InfiniBand for high-performance GPU communication across pods on CoreWeave CKS clusters.
- ``infra: k8s/my-nebius-cluster``: Enable InfiniBand for high-performance GPU communication across pods on Nebius managed Kubernetes.
- ``infra: k8s/my-together-cluster``: Enable InfiniBand for high-performance GPU communication across pods on Together AI Kubernetes clusters.
- ``infra: k8s/my-oke-cluster``: Enable RoCEv2 for high-performance GPU communication across pods on Oracle OKE clusters with bare-metal GPU shapes (BM.GPU.*.8) provisioned via dedicated RDMA capacity pools.

**Slurm-based:**

- ``infra: slurm``: On AWS HyperPod Slurm clusters with EFA-enabled instances (p4d, p5, etc.), EFA is available by default. No `network_tier` setting is needed.

.. code-block:: yaml

  resources:
    network_tier: best


.. _yaml-spec-resources-max-hourly-cost:

``resources.max_hourly_cost``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Maximum hourly cost in USD for instances (optional).

If specified, only instances with an hourly price at or below this limit will be considered during resource optimization. This is useful for setting a budget cap on the per-instance cost.

When ``use_spot`` is true, the limit is applied against spot prices; otherwise, it is applied against on-demand prices.

Must be a positive value.

.. code-block:: yaml

  resources:
    accelerators: A100
    max_hourly_cost: 10.0

.. code-block:: yaml

  # Combined with spot instances: filters by spot price
  resources:
    use_spot: true
    max_hourly_cost: 5.0


.. _yaml-spec-resources-ports:

``resources.ports``
~~~~~~~~~~~~~~~~~~~

Ports to expose (optional).

All ports specified here will be exposed to the public Internet. Under the hood, a firewall rule / inbound rule is automatically added to allow inbound traffic to these ports.

Applies to all VMs of a cluster created with this field set.

Currently only TCP protocol is supported.

Ports Lifecycle:

A cluster's ports will be updated whenever ``sky launch`` is executed. When launching an existing cluster, any new ports specified will be opened for the cluster, and the firewall rules for old ports will never be removed until the cluster is terminated.

Could be an integer, a range, or a list of integers and ranges:

- To specify a single port: ``8081``
- To specify a port range: ``10052-10100``
- To specify multiple ports / port ranges:

.. code-block:: yaml

  resources:
  ports:
    - 8080
    - 10022-10040

OR

.. code-block:: yaml

  resources:
    ports: 8081

OR

.. code-block:: yaml

  resources:
    ports: 10052-10100

OR

.. code-block:: yaml

  resources:
    ports:
      - 8080
      - 10022-10040


.. _yaml-spec-resources-container-image:

``resources.container_image``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

OCI container image to use as the task runtime (optional, advanced).

Use the scalar form for a digest-pinned OCI source reference:

.. code-block:: yaml

  resources:
    container_image: ghcr.io/my-org/model@sha256:<64-hex-digest>

Use the object form to bind a human-readable release or select a registry
distribution profile:

.. code-block:: yaml

  resources:
    container_image:
      ref: ghcr.io/my-org/model@sha256:<64-hex-digest>
      release: model-production-2026-07-18
      distribution: production

The supported fields are:

- ``ref``: an OCI image reference pinned by a SHA-256 digest. To start from a
  mutable tag, first run ``sky image publish <tag> --release <name>`` and use
  the returned release or artifact identity in the workload.
- ``release``: a workspace-scoped, human-readable immutable alias. Combine it
  with ``ref`` to prove that an existing publication resolves to the same
  digest, or use it alone after the image has been published.
- ``artifact_id``: the SkyPilot-generated UUID for an exact catalog artifact.
  It cannot be combined with ``ref`` or ``release``.
- ``distribution``: an administrator-configured registry profile. Use
  ``direct`` to bypass managed distribution in a ``managed_preferred``
  workspace. ``direct`` is rejected in a ``managed_required`` workspace.

If ``distribution`` is omitted, SkyPilot uses the workspace default profile,
then the API server default profile. If no profile is configured, a ``ref``
keeps the direct-pull behavior. ``release`` and ``artifact_id`` require a
managed profile because they do not identify a physical registry reference on
their own.

For a managed workload, all ``any_of`` or ``ordered`` resource candidates must
resolve to the same immutable artifact. One node pull is shared by all GPUs on
a multi-GPU VM; starting one model process per GPU remains the task's
responsibility.

The legacy ``image_id: docker:<image>`` syntax remains supported with its
existing direct-pull and heterogeneous-candidate behavior, but is deprecated.
It does not opt a task into managed distribution. See
:ref:`container_registries <config-yaml-container-registries>` for the API
server and workspace configuration.


.. _yaml-spec-resources-image-id:

``resources.image_id``
~~~~~~~~~~~~~~~~~~~~~~
Custom image id (optional, advanced).

The image id used to boot the instances. Only supported for AWS, GCP, OCI, IBM, Verda and Nebius. IBM and Verda only support non-docker images.

If not specified, SkyPilot will use the default debian-based image suitable for machine learning tasks.

**Docker support**

You can specify a Docker image by setting ``image_id`` to
``docker:<image name>`` for Azure, AWS, GCP, and RunPod. This form is
deprecated for container runtimes; use
:ref:`resources.container_image <yaml-spec-resources-container-image>` for new
workloads. Existing workloads retain their direct-pull behavior. For example,

.. code-block:: yaml

  resources:
    image_id: docker:ubuntu:latest

Currently, only debian and ubuntu images are supported.

If you want to use a docker image in a private registry, you can specify your username, password, and registry server as task environment variable. For details, please refer to the ``envs`` section below.

**AWS**

To find AWS AMI ids: https://leaherb.com/how-to-find-an-aws-marketplace-ami-image-id

You can also change the default OS version by choosing from the following image tags provided by SkyPilot:

.. code-block:: yaml

  resources:
    image_id: skypilot:gpu-ubuntu-2004
    image_id: skypilot:k80-ubuntu-2004
    image_id: skypilot:gpu-ubuntu-1804
    image_id: skypilot:k80-ubuntu-1804

It is also possible to specify a per-region image id (failover will only go through the regions specified as keys; useful when you have the custom images in multiple regions):

.. code-block:: yaml

  resources:
    image_id:
      us-east-1: ami-0729d913a335efca7
      us-west-2: ami-050814f384259894c

**GCP**

To find GCP images: https://cloud.google.com/compute/docs/images

.. code-block:: yaml

  resources:
    image_id: projects/deeplearning-platform-release/global/images/common-cpu-v20230615-debian-11-py310

Or machine image: https://cloud.google.com/compute/docs/machine-images

.. code-block:: yaml

  resources:
    image_id: projects/my-project/global/machineImages/my-machine-image

**Azure**

To find Azure images: https://docs.microsoft.com/en-us/azure/virtual-machines/linux/cli-ps-findimage

.. code-block:: yaml

  resources:
    image_id: microsoft-dsvm:ubuntu-2004:2004:21.11.04

**OCI**

To find OCI images: https://docs.oracle.com/en-us/iaas/images

You can choose the image with OS version from the following image tags provided by SkyPilot:

.. code-block:: yaml

  resources:
    image_id: skypilot:gpu-ubuntu-2204
    image_id: skypilot:gpu-ubuntu-2004
    image_id: skypilot:gpu-oraclelinux9
    image_id: skypilot:gpu-oraclelinux8
    image_id: skypilot:cpu-ubuntu-2204
    image_id: skypilot:cpu-ubuntu-2004
    image_id: skypilot:cpu-oraclelinux9
    image_id: skypilot:cpu-oraclelinux8

It is also possible to specify your custom image's OCID with OS type, for example:

.. code-block:: yaml

  resources:
    image_id: ocid1.image.oc1.us-sanjose-1.aaaaaaaaywwfvy67wwe7f24juvjwhyjn3u7g7s3wzkhduxcbewzaeki2nt5q:oraclelinux
    image_id: ocid1.image.oc1.us-sanjose-1.aaaaaaaa5tnuiqevhoyfnaa5pqeiwjv6w5vf6w4q2hpj3atyvu3yd6rhlhyq:ubuntu

**IBM**

Create a private VPC image and paste its ID in the following format:

.. code-block:: yaml

  resources:
    image_id: <unique_image_id>

To create an image manually:
https://cloud.ibm.com/docs/vpc?topic=vpc-creating-and-using-an-image-from-volume.

To use an official VPC image creation tool:
https://www.ibm.com/cloud/blog/use-ibm-packer-plugin-to-create-custom-images-on-ibm-cloud-vpc-infrastructure

To use a more limited but easier to manage tool:
https://github.com/IBM/vpc-img-inst

.. code-block:: yaml

  resources:
    image_id: ami-0868a20f5a3bf9702  # IBM example
    # image_id: projects/deeplearning-platform-release/global/images/common-cpu-v20230615-debian-11-py310  # GCP example
    # image_id: docker:pytorch/pytorch:1.13.1-cuda11.6-cudnn8-runtime # Docker example

OR

.. code-block:: yaml

  resources:
    image_id:
      us-east-1: ami-123
      us-west-2: ami-456

**Nebius**

The ``image_id`` parameter supports specifying an image by ID, or by image family.

.. code-block:: yaml

  resources:
    # Specify an image by ID
    image_id: computeimage-e00d6q343kqz6ayd63
    # Or use the latest image from a family
    image_id: ubuntu24.04-cuda13.0

**RunPod**

RunPod natively supports Docker images. You can specify any Docker image:

.. code-block:: yaml

  resources:
    image_id: docker:ubuntu:22.04
    # Or use a specific registry
    image_id: docker:nvcr.io/nvidia/pytorch:24.10-py3

For multi-region deployments, you can specify different images per region:

.. code-block:: yaml

  resources:
    image_id:
      US: docker:us-registry.io/myapp:latest
      CA: docker:ca-registry.io/myapp:latest
      CZ: docker:eu-registry.io/myapp:latest


.. _yaml-spec-resources-labels:

``resources.labels``
~~~~~~~~~~~~~~~~~~~~
Labels to apply to the instances (optional).

If specified, these labels will be applied to the VMs or pods created by SkyPilot.

These are useful for assigning metadata that may be used by external tools.

Implementation differs by cloud provider:

- AWS: Labels are mapped to instance tags
- GCP: Labels are mapped to instance labels
- Kubernetes: Labels are mapped to pod labels
- Other: Labels are not supported and will be ignored

Note: Labels are applied only on the first launch of the cluster. They are not updated on subsequent launches.

Example:

.. code-block:: yaml

  resources:
    labels:
      project: my-project
      department: research


.. _yaml-spec-resources-any-of:

``resources.any_of``
~~~~~~~~~~~~~~~~~~~~
Candidate resources (optional).

If specified, SkyPilot will only use these candidate resources to launch the cluster.

The fields specified outside of ``any_of`` will be used as the default values for all candidate resources, and any duplicate fields specified inside ``any_of`` will override the default values.

``any_of`` means that SkyPilot will try to find a resource that matches any of the candidate resources, i.e. the failover order will be decided by the optimizer.

Example:

.. code-block:: yaml

  resources:
    accelerators: H100
    any_of:
      - infra: aws/us-west-2
      - infra: gcp/us-central1

.. _yaml-spec-resources-ordered:

``resources.ordered``
~~~~~~~~~~~~~~~~~~~~~~
Ordered candidate resources (optional).

If specified, SkyPilot will failover through the candidate resources with the specified order.

The fields specified outside of ``ordered`` will be used as the default values for all candidate resources, and any duplicate fields specified inside ``ordered`` will override the default values.

``ordered`` means that SkyPilot will failover through the candidate resources with the specified order.

Example:

.. code-block:: yaml

  resources:
    ordered:
      - infra: aws/us-east-1
      - infra: aws/us-west-2

.. _yaml-spec-resources-job-recovery:

``resources.job_recovery``
~~~~~~~~~~~~~~~~~~~~~~~~~~
The recovery strategy for managed jobs (optional).

We can specify the strategy for which region to recover the job to when it fails. Possible values are ``FAILOVER`` and ``EAGER_NEXT_REGION``.

If ``FAILOVER`` is specified, the job will be restarted in the same region if the node fails, and go to the next region if no available resources are found in the same region.

If ``EAGER_NEXT_REGION`` is specified, the job will go to the next region directly if the node fails. This is useful for spot instances, as in practice, preemptions in a region usually indicate a shortage of resources in that region.

Default: ``EAGER_NEXT_REGION``

Example:

.. code-block:: yaml

  resources:
    job_recovery:
      strategy: FAILOVER

OR

.. code-block:: yaml

  resources:
    job_recovery:
      strategy: EAGER_NEXT_REGION

We can also specify the maximum number of times to restart the job on user code errors (non-zero exit codes).

.. code-block:: yaml

  resources:
    job_recovery:
      max_restarts_on_errors: 3

We can also specify the exit codes that should always trigger recovery, regardless of the :code:`max_restarts_on_errors` limit. This is useful when certain exit codes indicate transient errors that should always be retried (e.g., NCCL timeouts, specific GPU driver issues).

We can specify multiple exit codes:

.. code-block:: yaml

  resources:
    job_recovery:
      # Always recover on these exit codes
      recover_on_exit_codes: [33, 34]

Or a single exit code:

.. code-block:: yaml

  resources:
    job_recovery:
      # Always recover on these exit codes
      recover_on_exit_codes: 33

Available fields:

- :code:`strategy`: The recovery strategy to use (:code:`FAILOVER` or :code:`EAGER_NEXT_REGION`)
- :code:`max_restarts_on_errors`: Maximum number of times to restart the job on user code errors (non-zero exit codes)
- :code:`recover_on_exit_codes`: Exit code(s) (0-255) that should always trigger recovery. Can be a single integer (e.g., :code:`33`) or a list (e.g., :code:`[33, 34]`). Restarts triggered by these exit codes do not count towards the :code:`max_restarts_on_errors` limit. Useful for specific transient errors like NCCL timeouts.


.. _yaml-spec-envs:

``envs``
~~~~~~~~

Environment variables (optional).

These values can be accessed in the ``file_mounts``, ``setup``, and ``run`` sections below.

Values set here can be overridden by a CLI flag: ``sky launch/exec --env ENV=val`` (if ``ENV`` is present).


Example of using envs:

.. code-block:: yaml

  envs:
    MY_BUCKET: skypilot-data
    MODEL_SIZE: 13b
    MY_LOCAL_PATH: tmp-workdir

.. dropdown:: Docker login authentication with environment variables

  For costumized non-root docker image in RunPod, you need to set ``SKYPILOT_RUNPOD_DOCKER_USERNAME`` to specify the login username for the docker image. See :ref:`docker-containers-as-runtime-environments` for more.

  If you want to use a docker image as runtime environment in a private registry, you can specify your username, password, and registry server as task environment variable.  For example:

  .. code-block:: yaml

    envs:
      SKYPILOT_DOCKER_USERNAME: <username>
      SKYPILOT_DOCKER_PASSWORD: <password>
      SKYPILOT_DOCKER_SERVER: <registry server>

  SkyPilot will execute ``docker login --username <username> --password <password> <registry server>`` before pulling the docker image. For ``docker login``, see https://docs.docker.com/engine/reference/commandline/login/

  You could also specify any of them through the CLI flag if you don't want to store them in your yaml file or if you want to generate them for constantly changing password. For example:

  .. code-block:: yaml

    sky launch --env SKYPILOT_DOCKER_PASSWORD=$(aws ecr get-login-password --region us-east-1).

  For more information about docker support in SkyPilot, please refer to :ref:`Using private docker registries <docker-containers-private-registries>`.

  You can also use :ref:`secrets <yaml-spec-secrets>` to set the authentication above.

.. _yaml-spec-secrets:

``secrets``
~~~~~~~~~~~

Secrets (optional).

Secrets are similar to :ref:`envs <yaml-spec-envs>` above but can only be used in the ``setup`` and ``run``, and will be redacted in the entrypoint/YAML in the dashboard.

Values set here can be overridden by a CLI flag: ``sky launch/exec --secret SECRET=val`` (if ``SECRET`` is present).

Example:

.. code-block:: yaml

  secrets:
    HF_TOKEN: my-huggingface-token
    WANDB_API_KEY: my-wandb-api-key

.. _yaml-spec-api-server-access:

``api_server_access``
~~~~~~~~~~~~~~~~~~~~~

Whether to inject API server credentials into the task's environment so that it can call ``sky`` CLI/SDK to launch nested SkyPilot operations. Defaults to ``true``. Set to ``false`` to disable.

When enabled and the API server supports it, SkyPilot automatically injects credentials. No setup is required for most users.

.. code-block:: yaml

  # Opt out of API server access injection
  api_server_access: false

See :ref:`Nested SkyPilot from managed jobs <nested-skypilot-managed-jobs>` for details.

.. _yaml-spec-new-volumes:

``volumes``
~~~~~~~~~~~

SkyPilot supports managing persistent and ephemeral volumes for tasks or jobs on Kubernetes clusters. Refer to :ref:`volumes on Kubernetes <volumes-on-kubernetes>` for more details.

Example:

.. code-block:: yaml

  volumes:
    # Persistent volume
    /mnt/data: volume-name
    # Ephemeral volume
    /mnt/cache:
      size: 100Gi


.. _yaml-spec-file-mounts:

``file_mounts``
~~~~~~~~~~~~~~~

File mounts configuration.

Example:

.. code-block:: yaml

  file_mounts:
    # Uses rsync to sync local files/directories to all nodes of the cluster.
    #
    # If a relative path is used, it's evaluated relative to the location from
    # which `sky` is called.
    #
    # If symlinks are present, they are copied as symlinks, and their targets
    # must also be synced using file_mounts to ensure correctness.
    /remote/dir1/file: /local/dir1/file
    /remote/dir2: /local/dir2

    # Create a S3 bucket named sky-dataset, uploads the contents of
    # /local/path/datasets to the bucket, and marks the bucket as persistent
    # (it will not be deleted after the completion of this task).
    # Symlinks and their contents are NOT copied.
    #
    # Mounts the bucket at /datasets-storage on every node of the cluster.
    /datasets-storage:
      name: sky-dataset  # Name of storage, optional when source is bucket URI
      source: /local/path/datasets  # Source path, can be local or bucket URI. Optional, do not specify to create an empty bucket.
      store: s3  # Could be either 's3', 'gcs', 'azure', 'r2', 'vastdata', 'oci', or 'ibm'; default: None. Optional.
      persistent: True  # Defaults to True; can be set to false to delete bucket after cluster is downed. Optional.
      mode: MOUNT  # MOUNT or COPY or MOUNT_CACHED. Defaults to MOUNT. Optional.

    # Mount with VFS caching and a pre-tuned workload type for model checkpoints.
    /checkpoints:
      source: s3://my-checkpoint-bucket
      mode: MOUNT_CACHED
      type: MODEL_CHECKPOINT_RW  # Pre-tuned workload type. Optional.

    # Mount a bucket as read-only to prevent accidental writes.
    /readonly-data:
      source: s3://my-dataset-bucket
      mode: MOUNT
      config:
        mount:
          read_only: true

    # Copies a cloud object store URI to the cluster. Can be private buckets.
    /datasets-s3: s3://my-awesome-dataset

    # Demoing env var usage.
    /checkpoint/${MODEL_SIZE}: ~/${MY_LOCAL_PATH}
    /mydir:
      name: ${MY_BUCKET}  # Name of the bucket.
      mode: MOUNT

OR

.. code-block:: yaml

  file_mounts:
    /remote/config: ./local_config  # Local to remote
    /remote/output: s3://my-bucket/outputs  # Cloud storage
    /remote/models:
      name: my-models-bucket
      source: ~/local_models
      store: gcs
      mode: MOUNT
    /remote/data:
      source: gs://my-data-bucket
      mode: MOUNT_CACHED
      type: DATASET_RO

The ``type`` field specifies a pre-tuned workload type for ``MOUNT_CACHED`` mode.
Available types: ``MODEL_CHECKPOINT_RO``, ``MODEL_CHECKPOINT_RW``, ``DATASET_RO``, ``DATASET_RW``.
See :ref:`mount_cached_workload_types` for details on workload types and ``config.mount_cached`` parameters.

The ``config.mount`` section supports parameters for ``MOUNT`` mode.
Setting ``read_only: true`` mounts the bucket as read-only, preventing accidental writes.
See :ref:`storage-yaml-reference` for all available parameters.

.. _yaml-spec-setup:

``setup``
~~~~~~~~~

Setup script (optional) to execute on every ``sky launch``.

This is executed before the ``run`` commands.

Example:

To specify a single command:

.. code-block:: yaml

  setup: pip install -r requirements.txt

The ``|`` separator indicates a multiline string.

.. code-block:: yaml

  setup: |
    echo "Begin setup."
    pip install -r requirements.txt
    echo "Setup complete."

OR

.. code-block:: yaml

  setup: |
    conda create -n myenv python=3.9 -y
    conda activate myenv
    pip install torch torchvision

.. _yaml-spec-run:

``run``
~~~~~~~

Main program (optional, but recommended) to run on every node of the cluster.

Example:

.. code-block:: yaml

  run: |
    echo "Beginning task."
    python train.py

    # Demoing env var usage.
    echo Env var MODEL_SIZE has value: ${MODEL_SIZE}

OR

.. code-block:: yaml

  run: |
    conda activate myenv
    python my_script.py --data-dir /remote/data --output-dir /remote/output


.. _yaml-spec-config:
.. _task-yaml-experimental:

``config``
~~~~~~~~~~

:ref:`Advanced configuration options <config-client-job-task-yaml>` to apply to the task.

Example:

.. code-block:: yaml

  config:
    docker:
      run_options: ...
    kubernetes:
      pod_config: ...
      provision_timeout: ...
    gcp:
      managed_instance_group: ...
    nvidia_gpus:
      disable_ecc: ...
    hooks:
      - run: |
          cd my-code-base
          git add . && git commit -m "Auto-commit" && git push
        events: [stop, preemption, down]  # optional; defaults to all three
        timeout: 300                      # optional; default 3600s

The ``hooks`` field lists scripts to run on the cluster on lifecycle events
(``stop``, ``preemption``, ``down``). See :ref:`Lifecycle hooks
<lifecycle-hooks>` for the full reference.

.. _service-yaml-spec:

SkyServe Service
================

To define a YAML for use for :ref:`services <sky-serve>`, use previously mentioned fields to describe each replica, then add a service section to describe the entire service.

If neither ``replicas`` nor ``replica_policy`` is specified, SkyServe defaults
to zero replicas.

Syntax

.. parsed-literal::

  service:
    :ref:`readiness_probe <yaml-spec-service-readiness-probe>`:
      :ref:`path <yaml-spec-service-readiness-probe-path>`: /v1/models
      :ref:`post_data <yaml-spec-service-readiness-probe-post-data>`: {'model_name': 'model'}
      :ref:`initial_delay_seconds <yaml-spec-service-readiness-probe-initial-delay-seconds>`: 1200
      :ref:`timeout_seconds <yaml-spec-service-readiness-probe-timeout-seconds>`: 15
      :ref:`endpoint_probe_interval_seconds <yaml-spec-service-readiness-probe-endpoint-probe-interval-seconds>`: 10
      :ref:`consecutive_failure_threshold_timeout <yaml-spec-service-readiness-probe-consecutive-failure-threshold-timeout>`: 180

    :ref:`load_balancer <yaml-spec-service-load-balancer>`:
      :ref:`stream_timeout_seconds <yaml-spec-service-load-balancer-stream-timeout-seconds>`: 120

    :ref:`readiness_probe <yaml-spec-service-readiness-probe>`: /v1/models

    :ref:`replica_policy <yaml-spec-service-replica-policy>`:
      :ref:`min_replicas <yaml-spec-service-replica-policy-min-replicas>`: 1
      :ref:`max_replicas <yaml-spec-service-replica-policy-max-replicas>`: 3
      :ref:`target_qps_per_replica <yaml-spec-service-replica-policy-target-qps-per-replica>`: 5
      :ref:`target_concurrency_per_replica <yaml-spec-service-replica-policy-target-concurrency-per-replica>`: 1
      :ref:`upscale_delay_seconds <yaml-spec-service-replica-policy-upscale-delay-seconds>`: 300
      :ref:`downscale_delay_seconds <yaml-spec-service-replica-policy-downscale-delay-seconds>`: 1200

    :ref:`replicas <yaml-spec-service-replicas>`: 2

  resources:
    :ref:`ports <yaml-spec-service-resources-ports>`: 8080


Fields
----------

.. _yaml-spec-service-readiness-probe:

``service.readiness_probe``
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Readiness probe configuration (required).

Used by SkyServe to check if your service replicas are ready for accepting traffic.

If the readiness probe returns a 200, SkyServe will start routing traffic to that replica.

Can be defined as a path string (for GET requests with defaults) or a detailed dictionary.

.. code-block:: yaml

  service:
    readiness_probe: /v1/models

OR

.. code-block:: yaml

  service:
    readiness_probe:
      path: /v1/models
      post_data: '{"model_name": "my_model"}'
      initial_delay_seconds: 600
      timeout_seconds: 10
      endpoint_probe_interval_seconds: 10
      consecutive_failure_threshold_timeout: 180

    load_balancer:
      stream_timeout_seconds: 120


.. _yaml-spec-service-readiness-probe-path:

``service.readiness_probe.path``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Endpoint path for readiness checks (required).

Path to probe. SkyServe sends periodic requests to this path after the initial delay.

.. code-block:: yaml

  service:
    readiness_probe:
      path: /v1/models


.. _yaml-spec-service-readiness-probe-post-data:

``service.readiness_probe.post_data``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

POST request payload (optional).

If this is specified, the readiness probe will use POST instead of GET, and the post data will be sent as the request body.

.. code-block:: yaml

  service:
    readiness_probe:
      path: /v1/models
      post_data: '{"model_name": "my_model"}'

.. _yaml-spec-service-readiness-probe-initial-delay-seconds:

``service.readiness_probe.initial_delay_seconds``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Grace period before initiating health checks (default: 1200).

Initial delay in seconds. Any readiness probe failures during this period will be ignored.

This is highly related to your service, so it is recommended to set this value based on your service's startup time.


.. code-block:: yaml

  service:
    readiness_probe:
      initial_delay_seconds: 600

.. _yaml-spec-service-readiness-probe-timeout-seconds:

``service.readiness_probe.timeout_seconds``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Maximum wait time per probe request (default: 15).

The Timeout in seconds for a readiness probe request.

If the readiness probe takes longer than this time to respond, the probe will be considered as failed.

This is useful when your service is slow to respond to readiness probe requests.

Note, having a too high timeout will delay the detection of a real failure of your service replica.

.. code-block:: yaml

    service:
      readiness_probe:
        timeout_seconds: 10


.. _yaml-spec-service-readiness-probe-endpoint-probe-interval-seconds:

``service.readiness_probe.endpoint_probe_interval_seconds``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Time between readiness probe attempts (default: 10).

SkyServe probes each replica endpoint at this interval to update readiness and
detect unhealthy replicas.

.. code-block:: yaml

    service:
      readiness_probe:
        endpoint_probe_interval_seconds: 5


.. _yaml-spec-service-readiness-probe-consecutive-failure-threshold-timeout:

``service.readiness_probe.consecutive_failure_threshold_timeout``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Maximum consecutive probe failure window before tearing down a ready replica.

If omitted, SkyServe keeps the existing defaults: ``10`` seconds for pools and
``180`` seconds for regular services.

.. code-block:: yaml

    service:
      readiness_probe:
        consecutive_failure_threshold_timeout: 30


.. _yaml-spec-service-load-balancer:

``service.load_balancer``
~~~~~~~~~~~~~~~~~~~~~~~~~

Load balancer configuration (optional).

Controls request proxy behavior for the SkyServe load balancer.

.. code-block:: yaml

    service:
      load_balancer:
        stream_timeout_seconds: 300
        request_queue:
          min_size: 10
          size_per_replica: 3
          max_size: 1000
          max_concurrency_per_replica: 1
          max_concurrency: 32
          timeout_seconds: 120
          max_request_body_bytes: 1048576
          use_async_occupancy: false


.. _yaml-spec-service-load-balancer-stream-timeout-seconds:

``service.load_balancer.stream_timeout_seconds``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Maximum time the load balancer waits for a proxied response stream (default:
120).

This controls the timeout for requests forwarded by the SkyServe load balancer
to a ready replica.

.. code-block:: yaml

    service:
      load_balancer:
        stream_timeout_seconds: 300


.. _yaml-spec-service-load-balancer-request-queue:

``service.load_balancer.request_queue``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Enables a bounded request queue in the load balancer. By default, the effective
queue size is
``min(max_size, max(min_size, ready_replicas * size_per_replica))``. When
``use_async_occupancy`` is true, ``ready_replicas`` in this formula is replaced
by the sum of fresh, probed predict-concurrency slots, with each replica capped
by ``max_concurrency_per_replica``. An empty object enables the defaults shown
below. Requests beyond the effective queue size receive HTTP 503, requests that
wait longer than
``timeout_seconds`` receive HTTP 503, and bodies larger than
``max_request_body_bytes`` receive HTTP 413.

``max_size`` and ``max_request_body_bytes`` are hard memory-safety bounds.
``max_concurrency_per_replica`` controls how many requests may be dispatched
per ready replica before new arrivals wait in the queue, while
``max_concurrency`` is the absolute load-balancer-wide dispatch ceiling.
When ``use_async_occupancy`` is true, dispatch concurrency is also clamped by
the fresh free-slot total reported by occupancy-capable replicas. Unknown
occupancy contributes neither queue capacity nor free capacity, and occupancy
probe updates wake waiting requests. Each dispatched request reserves one
reported slot until a new, non-racing occupancy probe reconciles it. Other
reported slots on the same replica remain available, which allows one SkyServe
replica backed by a multi-GPU instance to accept work for each free model worker
even when requests return an asynchronous acknowledgement.
``max_concurrency_per_replica`` remains a safety ceiling; set it at least as
high as the largest replica's reported predict concurrency to use every slot.
When ``use_async_occupancy`` is true and this field is omitted, it defaults to
``max_concurrency`` so probed multi-worker replicas work without a duplicated
machine-width setting. Set it explicitly only to impose a stricter per-replica
ceiling.
If ``min_size`` is 0, arrivals fail immediately while every occupancy probe is
unknown, including the interval before the first successful probe after a load
balancer restart. Set a positive ``min_size`` to let arrivals wait through that
interval.

Inference requests may set ``X-SkyServe-Priority`` to an integer from 0 to 100.
Higher-priority queued requests are dispatched first, with first-in-first-out
ordering among requests at the same priority. Scheduling is non-preemptive: a
request that has already been dispatched continues running. Missing headers
default to priority 0. Malformed, duplicate, or out-of-range priority headers
receive HTTP 400. The load balancer consumes this header and does not forward it
to replicas. Priority is validated even when ``request_queue`` is disabled, but
without a queue it does not otherwise delay dispatch. Strict priority can starve
lower-priority requests under sustained higher-priority load, and a full queue
rejects new arrivals rather than evicting an existing lower-priority request.

The queue accepts at most 3,000 waiting requests, 128 concurrent requests,
and 16 MiB per request body. The product of ``max_concurrency`` and
``max_request_body_bytes`` must stay within a 128 MiB active-request buffering
budget. Bodies cached before admission share a separate 128 MiB runtime budget
based on their actual sizes. Requests that would exceed that waiting-body
budget receive HTTP 503 with ``Retry-After``. Together, the two budgets leave
headroom under the external load balancer's default 512 MiB limit while still
allowing large queues of small payloads.

.. code-block:: yaml

    service:
      load_balancer:
        request_queue:
          min_size: 10
          size_per_replica: 3
          max_size: 1000
          max_concurrency_per_replica: 1
          max_concurrency: 32
          timeout_seconds: 120
          max_request_body_bytes: 1048576
          use_async_occupancy: false


.. _yaml-spec-service-replica-policy:

``service.replica_policy``
~~~~~~~~~~~~~~~~~~~~~~~~~~

Optional autoscaling configuration for service replicas. If both
``replica_policy`` and ``replicas`` are omitted, SkyServe uses a fixed zero
replica count until an update supplies a positive count.

Describes how SkyServe autoscales your service based on the QPS (queries per second) of your service.

.. code-block:: yaml

    service:
      replica_policy:
        min_replicas: 1
        max_replicas: 5
        target_qps_per_replica: 10

For async multi-GPU concurrency services, ``dynamic_fallback_per_gpu`` is the
primary spot placer. It automatically changes these public replica counts to
logical GPU slots. ``dynamic_fallback`` and all other placement strategies keep
the historical meaning of one replica per physical SkyServe backend. Job pools
that use cost-aware placement therefore use ``dynamic_fallback``; pools may
also omit ``spot_placer``. This unit is an internal consequence of the placer,
not a separate user setting.

The per-GPU placer currently supports the local async router's
one-job-per-whole-GPU execution contract. It requires a positive integer
``target_concurrency_per_replica``,
``graceful_drain_async_occupancy: true``, and a whole-GPU accelerator shape.
Values above one retain waiting work as headroom without increasing execution
concurrency. SkyServe may provision a multi-GPU backend that contributes
several replicas.
Because a backend is indivisible, ready capacity can exceed ``max_replicas`` by
the width of the final backend without causing scaling churn. This mode
currently requires rolling updates and does not yet support blue-green updates
or multi-GPU ``reserved_capacity_fill`` shapes.

.. code-block:: yaml

  service:
    graceful_drain_async_occupancy: true
    replica_policy:
      min_replicas: 1
      max_replicas: 1000
      target_concurrency_per_replica: 2
      spot_placer: dynamic_fallback_per_gpu

.. _yaml-spec-service-replica-policy-min-replicas:

``service.replica_policy.min_replicas``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Minimum number of active replicas (required). With
``dynamic_fallback_per_gpu``, this is the minimum number of logical GPU slots,
not physical backends.

Service never scales below this count.

.. code-block:: yaml

  service:
    replica_policy:
      min_replicas: 1


.. _yaml-spec-service-replica-policy-max-replicas:

``service.replica_policy.max_replicas``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Maximum requested replicas (optional). With ``dynamic_fallback_per_gpu``, this
clamps the demand target in logical GPU slots; an indivisible multi-GPU backend
may create stable materialized capacity above it.

If not specified, SkyServe will use a fixed number of replicas (the same as min_replicas) and ignore any QPS threshold specified below.

.. code-block:: yaml

  service:
    replica_policy:
      max_replicas: 3


.. _yaml-spec-service-replica-policy-target-qps-per-replica:

``service.replica_policy.target_qps_per_replica``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Target queries per second per replica (optional).

SkyServe will scale your service so that, ultimately, each replica manages approximately ``target_qps_per_replica`` queries per second.

**Autoscaling will only be enabled if this value is specified.**

.. code-block:: yaml

  service:
    replica_policy:
      target_qps_per_replica: 5


.. _yaml-spec-service-replica-policy-target-concurrency-per-replica:

``service.replica_policy.target_concurrency_per_replica``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Target simultaneous work per GPU (optional). Mutually exclusive with
``target_qps_per_replica``.

Outstanding work includes in-flight, queued, and recently rejected requests.
For ordinary physical-backend services, SkyServe packs that work onto each
replica's configured target multiplied by its GPU count. With
``dynamic_fallback_per_gpu``, the value must be a positive integer and SkyServe
publishes a logical GPU target before packing those slots into physical
backends. Running and queued work remain current-state signals. The optional
duration and utilization fields below control rejected-pressure conversion and
headroom without changing model execution concurrency.

.. code-block:: yaml

  service:
    replica_policy:
      target_concurrency_per_replica: 2


Logical concurrency tuning fields
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The following optional fields require ``dynamic_fallback_per_gpu`` and
``target_concurrency_per_replica``:

* ``target_utilization_percentage`` is an integer from 1 to 100 (default 100).
* ``expected_request_duration_seconds`` is a positive number. It converts the
  retained rejected-request population and stale arrival rate into concurrent
  work. When absent, each retained rejection contributes one work unit.
* ``max_scale_up_rate_percentage``, ``scale_up_rate_min_replicas``, and
  ``scale_up_rate_period_seconds`` must be set together. Each upward wave adds
  at most the larger of the minimum replica count or the configured percentage
  of committed logical capacity, then waits for the configured period.
* ``max_scale_down_rate_percentage`` is an integer from 1 to 100 (default 50).
  After the downscale delay, each wave removes at most that fraction of
  committed logical capacity and requires a new full delay before another
  wave.

.. code-block:: yaml

  service:
    replica_policy:
      target_concurrency_per_replica: 1
      target_utilization_percentage: 90
      expected_request_duration_seconds: 30
      max_scale_up_rate_percentage: 20
      scale_up_rate_min_replicas: 10
      scale_up_rate_period_seconds: 60
      max_scale_down_rate_percentage: 50


``service.replica_policy.cost_rebalance``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Cost-aware replacement policy for services with ``spot_placer``. It is enabled
by default; set it to ``false`` to opt out. ``true`` uses the defaults, while
an object customizes them.

When a capacity-equivalent active location stays at least
``min_savings_fraction`` cheaper for ``stabilization_seconds``, SkyServe
launches a replacement before gracefully draining the expensive incumbent.
``max_parallel_replacements`` bounds temporary replacement overlap and does not
change the demand autoscaler's ``max_replicas`` limit.

The replacement admission and stabilization state survive controller and API
server restarts. Only structured provider capacity or quota failures suppress
a candidate location; other launch errors do not classify it as unavailable.

.. code-block:: yaml

  service:
    replica_policy:
      cost_rebalance:
        min_savings_fraction: 0.3
        max_parallel_replacements: 8
        stabilization_seconds: 300


.. _yaml-spec-service-replica-policy-upscale-delay-seconds:

``service.replica_policy.upscale_delay_seconds``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Stabilization period before adding replicas (default: 300).

Upscale delay in seconds. To avoid aggressive autoscaling, SkyServe will only upscale your service if the QPS of your service is higher than the target QPS for a period of time.

.. code-block:: yaml

  service:
    replica_policy:
      upscale_delay_seconds: 300


.. _yaml-spec-service-replica-policy-downscale-delay-seconds:

``service.replica_policy.downscale_delay_seconds``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Cooldown period before removing replicas (default: 1200).

Downscale delay in seconds. To avoid aggressive autoscaling, SkyServe will only downscale your service if the QPS of your service is lower than the target QPS for a period of time.

.. code-block:: yaml

  service:
    replica_policy:
      downscale_delay_seconds: 1200


.. _yaml-spec-service-replicas:

``service.replicas``
~~~~~~~~~~~~~~~~~~~~

Fixed replica count alternative to autoscaling.

Simplified version of replica policy that uses a fixed number of replicas.

.. code-block:: yaml

  service:
    replicas: 2


.. _yaml-spec-service-resources-ports:

``resources.ports``
~~~~~~~~~~~~~~~~~~~

Required exposed port for service traffic.

Port to run your service on each replica.

.. code-block:: yaml

  resources:
    ports: 8080

.. _pool-yaml-spec:

Job Pools
=========

To define a YAML for use with :ref:`job pools <pool>`, use previously mentioned fields to describe each worker, then add a pool section to configure the pool's scaling behavior.

Syntax

.. parsed-literal::

  pool:
    :ref:`workers <yaml-spec-pool-workers>`: 3
    :ref:`spot_placer <yaml-spec-pool-spot-placer>`: dynamic_fallback

  pool:
    :ref:`min_workers <yaml-spec-pool-min-workers>`: 1
    :ref:`max_workers <yaml-spec-pool-max-workers>`: 10
    :ref:`queue_length_threshold <yaml-spec-pool-queue-length-threshold>`: 5
    :ref:`upscale_delay_seconds <yaml-spec-pool-upscale-delay-seconds>`: 300
    :ref:`downscale_delay_seconds <yaml-spec-pool-downscale-delay-seconds>`: 1200


Fields
----------

.. _yaml-spec-pool-workers:

``pool.workers``
~~~~~~~~~~~~~~~~

Number of workers in the pool.

If ``min_workers`` and ``max_workers`` are not specified, the pool maintains a fixed number of workers with no autoscaling. If autoscaling is enabled (``min_workers``/``max_workers`` are set), this serves as the initial number of workers.

.. code-block:: yaml

  pool:
    workers: 3


.. _yaml-spec-pool-spot-placer:

``pool.spot_placer``
~~~~~~~~~~~~~~~~~~~~

Optional cost-aware placement policy for independently launched workers.
The supported value is ``dynamic_fallback``. Configure Spot and on-demand
candidates with ``resources.any_of``. SkyPilot prefers lower-cost active
locations, temporarily benches an exact location after a capacity or quota
failure, and falls through to another candidate for later worker launches.

.. code-block:: yaml

  pool:
    workers: 3
    spot_placer: dynamic_fallback

  resources:
    any_of:
      - infra: aws/us-east-1
        instance_type: r6a.xlarge
        use_spot: true
      - infra: aws/us-east-1
        instance_type: r6a.xlarge
        use_spot: false

This is per-worker placement, not a Spot gang. ``resources.ordered`` and
``dynamic_fallback_per_gpu`` are not supported for pools.


.. _yaml-spec-pool-min-workers:

``pool.min_workers``
~~~~~~~~~~~~~~~~~~~~

Minimum number of workers when autoscaling is enabled (required with ``max_workers``).

The pool never scales below this count. Setting to ``0`` enables **scale-to-zero**: the pool terminates all workers when idle, and provisions workers automatically when new jobs are submitted.

.. code-block:: yaml

  pool:
    min_workers: 1
    max_workers: 10


.. _yaml-spec-pool-max-workers:

``pool.max_workers``
~~~~~~~~~~~~~~~~~~~~

Maximum number of workers when autoscaling is enabled (required with ``min_workers``).

The pool never scales above this count. Must be greater than or equal to ``min_workers``.

.. code-block:: yaml

  pool:
    min_workers: 1
    max_workers: 10


.. _yaml-spec-pool-queue-length-threshold:

``pool.queue_length_threshold``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Number of pending jobs that triggers upscaling (default: 1).

When the number of pending jobs exceeds this threshold, the pool scales up. Requires ``max_workers`` to be set.

.. code-block:: yaml

  pool:
    min_workers: 1
    max_workers: 10
    queue_length_threshold: 5


.. _yaml-spec-pool-upscale-delay-seconds:

``pool.upscale_delay_seconds``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Delay in seconds between upscaling decisions (default: 300).

Controls how frequently the pool evaluates whether to add workers.

.. code-block:: yaml

  pool:
    min_workers: 1
    max_workers: 10
    upscale_delay_seconds: 60


.. _yaml-spec-pool-downscale-delay-seconds:

``pool.downscale_delay_seconds``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Delay in seconds between downscaling decisions (default: 1200).

Controls how frequently the pool evaluates whether to remove workers.

.. code-block:: yaml

  pool:
    min_workers: 1
    max_workers: 10
    downscale_delay_seconds: 600
