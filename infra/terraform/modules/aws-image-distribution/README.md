# AWS managed image distribution

This region-scoped module creates the complete fixed ECR shard ring for a set of
SkyPilot workspaces, one qualification repository, separate copy and lifecycle
target roles, repository policies, and a Terraform-managed permissions boundary.
It never creates repositories or copies content at deployment time.

Call the module once per profile target and AWS region with an aliased provider,
then aggregate
`qualified_shards_by_workspace`, `role_fingerprints`, and `quota_facts` into one
qualification manifest per workspace. The companion example performs that
aggregation and produces a ConfigMap-ready JSON map.

Every repository has immutable tags, `force_delete = false`, and Terraform
destroy protection. Regional lifecycle authority is omitted from canonical
repositories. Runtime principals receive repository-side pull grants only; they
still need identity permission for `ecr:GetAuthorizationToken` and pull APIs.

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
inspect and delete only eligible regional content. Keep these roles separate
when supplying existing role ARNs as well.
