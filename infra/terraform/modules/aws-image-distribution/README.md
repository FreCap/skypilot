# AWS managed image distribution

This region-scoped module creates the complete fixed ECR shard ring for a set of
SkyPilot workspaces, retained qualification repository generations, separate
copy and lifecycle target roles, repository policies, and a Terraform-managed
permissions boundary. It never creates repositories or copies content at
deployment time.

Qualification generation `0` always remains at the legacy fixed repository
path. To replace a damaged qualification repository without destroying it, add
a nonzero value to `qualification_repository_generations` and select it with
`active_qualification_repository_generation`. The selected generation must
always be the highest retained value, so a quarantined repository cannot be
selected again. Retained repositories remain protected Terraform resources and
stay visible in the generation maps. Copy and lifecycle boundaries and inline
policies include only the active qualification generation, while the
compatibility URL and qualification role fingerprint identify that same active
generation. The handoff fingerprints both its rendered allow policy and its
complete `tags_all` ownership set, including provider default tags, so live
qualification fails closed on policy or custody drift. Every inactive retained
repository also receives a
wildcard-principal explicit deny for image pull, content inspection, upload,
publication, and digest deletion. The deny excludes repository, policy, and tag
metadata and management actions so Terraform can continue to refresh and
reconcile the retained resource. Drained workers therefore have neither
identity nor repository-policy authority over an older retained or tombstoned
generation.

Call the module once per profile target and AWS region with an aliased provider,
then aggregate
`qualified_shards_by_workspace`, `role_fingerprints`, and `quota_facts` into one
qualification manifest per workspace. The companion example performs that
aggregation and produces a ConfigMap-ready JSON map.

Every repository has immutable tags, `force_delete = false`, and Terraform
destroy protection. The lifecycle role can inspect every managed shard
repository and only the active qualification generation, so it can prove exact
canonical absence without retaining provider authority over an older
qualification generation. Delete authority is omitted from canonical
repositories in the inline policy, permissions boundary, and repository policy.
Runtime principals receive repository-side pull grants only; they still need
identity permission for `ecr:GetAuthorizationToken` and pull APIs.

By default the module reads the account's applied ECR images-per-repository
quota through Service Quotas and rejects a shard ceiling that would consume the
reserved headroom. `applied_images_per_repository_quota` is an explicit override
for organizations that deny quota reads during Terraform planning. The runtime
copy worker independently re-reads live quota and repository state before a
PENDING shard can become READY, so Terraform output is desired state rather than
qualification evidence.

Copy and lifecycle target roles have separate inline policies and separate
Terraform-managed permissions boundaries. The copy role can inspect and publish
only the declared repositories and read quota facts. The lifecycle role can
inspect canonical and regional content plus the active qualification generation,
but can delete only eligible regional content and that active qualification
generation. Managed v0 always creates and owns both target roles, their trust
policies, their inline policies, and their permissions boundaries. Operators
adopting deterministic role names that already exist must import the exact
roles, and any same-named boundary policies, into this module's Terraform state
and let Terraform converge them. There is no external-role escape hatch because
an unattached policy or boundary would make the qualification fingerprints
misleading.

Each rendered, minified target-role trust policy must fit
`applied_role_trust_policy_quota`. The variable defaults to AWS's 2,048
character account quota and accepts an integer up to AWS's 8,192 character
maximum. Set it above the default only after that quota increase is applied in
the registry account.
The module also rejects a rendered customer-managed permissions boundary above
AWS's fixed 6,144-character limit and an inline role policy above the
10,240-character per-role limit before AWS receives it.
When `worker_assume_role_external_id` is set, Terraform enforces the AWS STS
contract before rendering either trust policy: 2-1,224 characters containing
only letters, digits, and `_+=,.@:/-`.

Run the mocked custody-boundary and stateful generation tests with Terraform 1.7
or newer. The separate harness proves that planning removal of an applied
retained generation fails specifically on `lifecycle.prevent_destroy`:

```bash
terraform test -test-directory=terraform-tests
bash terraform-tests/verify_prevent_destroy.sh
```
