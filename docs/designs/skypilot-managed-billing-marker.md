# SkyPilot-managed billing marker

- **Status:** Implemented; deployment gates open
- **Last updated:** 2026-07-24

## Problem

AWS and GCP resources created by SkyPilot do not have one invariant marker that
cost tooling can use to identify SkyPilot-managed spend. Cluster-name and user
labels cover many virtual machines, but they do not consistently reach attached
storage, images, buckets, or secondary resources created by a launch.

## Goals and non-goals

Goals:

- Put `skypilot-managed=true` on the billing and attribution resources listed
  in the public contract for SkyPilot's AWS and GCP compute, image, and
  object-storage paths.
- Make the marker invariant across clusters, managed jobs, SkyServe replicas,
  controllers, and internal container-image canaries.
- Tag resources atomically at creation when the provider API supports it.
- Preserve every existing user tag and label except an attempted override of
  the reserved marker.

Non-goals:

- Retroactively backfill resources that already exist. A stopped instance gets
  the marker when SkyPilot resumes and retags it, but this is not a fleet-wide
  migration.
- Introduce provider-organization tag keys. GCP Resource Manager tags require
  pre-created organization or project resources and are a separate policy.
- Tag user-provided resources that SkyPilot merely attaches or reuses.
- Add permissions solely to tag non-billable shared IAM resources.

## Public contract

`skypilot-managed` is a reserved system tag/label with the string value `true`.
SkyPilot writes it after merging global labels, resource labels, and raw
provider configuration. A user-supplied value for this key therefore cannot
override the marker.

The initial resource coverage is:

| Provider | Resource | Behavior |
|---|---|---|
| AWS | EC2 instance | Tagged on launch and on stopped-node resume |
| AWS | New EBS volume, ENI, spot request | Tagged atomically by `RunInstances` |
| AWS | Auto-created security group | Tagged at creation; BYO groups untouched |
| AWS | AMI and its new snapshots | Tagged on create/copy |
| AWS | SkyPilot-created S3 bucket | Tagged immediately after creation |
| AWS | Container-image canary resources | Tagged atomically by `RunInstances` |
| GCP | Compute VM, TPU VM, MIG-created VM | Labeled at creation and on resume |
| GCP | Newly initialized persistent disk in SkyPilot's launch config | Labeled in `initializeParams`; attached existing disks untouched |
| GCP | Legacy TPU node | Labeled by the create command |
| GCP | Compute image | Labeled at creation |
| GCP | SkyPilot-created GCS bucket | Labeled at creation |

Some provider control-plane or networking objects do not support legacy
tags/labels, are not separately billable, or both. This includes GCP instance
templates and MIG manager objects, ephemeral IPs, VPCs, subnets, firewall
rules, and service accounts. Labels on MIG instance-template properties still
propagate to the VMs and initialized disks that carry the spend.

When a user supplies a GCP source instance template without explicit disk
configuration, SkyPilot cannot safely rewrite that external template's disk
initialization. The generated VM receives the marker, but its template-defined
disk may require a billing-policy label on the source template itself.

Non-billable shared IAM resources and the currently unused AWS placement-group
helper are outside the resource matrix. If either becomes a metered active
provisioning path, its marker coverage must be decided before activation.

## Architecture and invariants

The marker name and value live in the shared provision constants module.
Provider boundaries enforce the marker; templates alone are not authoritative
because user configuration can override template fields.

AWS constructs one `TagSpecifications` entry per resource type. User entries
merge by resource type and key so the request never contains duplicate
specifications. Existing precedence remains unchanged for all other keys.

GCP adds the marker to the labels passed through each instance handler. Compute
and MIG handlers also merge it into every disk `initializeParams.labels`
mapping. A disk with `source` and no initialization block is user-provided and
is not changed.

Creation must fail normally if the caller lacks tag-on-create permission. A
successful unmarked resource would violate the billing invariant. S3 does not
support bucket tags in `CreateBucket`; SkyPilot applies the tags immediately
after creation and attempts to delete the still-empty bucket if tagging fails.

## Alternatives

- Relying on `skypilot-cluster-name` was rejected because storage, images,
  buckets, and internal resources are not all cluster-scoped.
- Making the marker configurable was rejected because billing queries need a
  stable fleet-wide predicate.
- Applying only post-creation tags was rejected where atomic provider APIs are
  available, since a crash between creation and tagging would leak unmarked
  spend.
- GCP Resource Manager tags were rejected for this change because they require
  external tag-key setup and organization-level permissions.

## Implementation phases

1. Add the shared reserved marker.
2. Enforce it in AWS compute, security-group, image, S3, and canary paths.
3. Enforce it in GCP compute, disk, TPU, image, and GCS paths.
4. Update minimal AWS and GCP permissions and focused unit tests.
5. Activate the AWS cost-allocation tag and validate provider billing exports
   operationally.

## Deployment and rollback

The code change requires no database migration. It does require a permission
migration before deployment:

- AWS roles must allow `ec2:CreateTags` for instances, volumes, network
  interfaces, spot requests, security groups, and (when clone-disk is enabled)
  images and snapshots.
- GCP roles must allow `compute.disks.setLabels`; clone-disk users must also
  allow `compute.images.setLabels`.

These IAM updates are a pre-upgrade gate because the new provider requests
enforce tags at creation and intentionally fail rather than create unmarked
resources. After the gate, mixed application versions are safe: resources
created by an older process may lack the marker, while resources created by the
new version carry it. Rollback stops marking new resources but does not remove
tags or labels already written.

AWS administrators must activate `skypilot-managed` as a user-defined cost
allocation tag before Cost Explorer can group new spend by it. GCP billing
exports can expose resource labels; the exact reporting surface depends on the
organization's export configuration.

## Verification

- Unit-test AWS tag-specification merging, reserved-key precedence, all
  launch-created resource types, stopped-node resume, security groups, images,
  S3, and canaries.
- Unit-test GCP VM/TPU label precedence, initialized-disk labeling, images,
  legacy TPU nodes, GCS, and required permission lists.
- Run the focused AWS, GCP, storage, cloud-image, and container-image tests.
- Run `format.sh` on every changed Python file.
- Manually launch one AWS and one GCP cluster and verify the marker on the VM
  and its boot disk. Create one managed bucket and image per cloud and verify
  the marker in each provider console or CLI.
- After billing-export latency, group costs by the marker and compare the AWS
  EC2 compute subtotal with SkyPilot's pay-as-you-go-equivalent estimate for
  the same complete UTC interval.

### Evidence (2026-07-24)

- The combined focused AWS, GCP, cloud-image, object-storage, security-group,
  resume, tag-merge, and container-canary unit suite passed.
- The additional GCP permission-list, image error-path, and S3 rollback tests
  passed.
- YAPF and isort completed for the changed Python files.
- Mypy passed across 745 source files; changed source modules received a
  Pylint score of 10.00/10.
- `git diff --check` passed.
- Adversarial review of this exact design and implementation found no remaining
  blocker or high-severity issue after the IAM rollout, S3 rollback, and image
  error-handling corrections.
- For 2026-07-01 through 2026-07-22 UTC, the remote workspace's
  `Boltz-Multi-Tenant-Workloads` AWS account reported $63,441.95 of EC2 compute
  cost ($34,147.11 on-demand and $29,294.83 Spot). SkyPilot estimated
  $73,397.03 for the same AWS interval ($34,725.57 on-demand and $38,671.46
  Spot), 15.69% above AWS. Existing cost-allocation tags were inactive, so this
  is an account-scoped baseline rather than a tag-filtered historical result.

## Open gates

- Update deployed AWS and GCP roles with the new tag/label permissions before
  rolling out the application.
- An administrator must activate the AWS user-defined cost-allocation tag.
- Historical resource backfill, if desired, needs an explicit inventory and
  ownership-safe migration outside this implementation.
