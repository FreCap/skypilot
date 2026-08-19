# EKS private API ingress node exposure

## Context

PR #1159 added `cluster_api_ingress_cidrs` to the reusable EKS spoke pool
module. It creates a TCP/443 ingress rule on the security group returned as
`clusterSecurityGroupId` by EKS. Its code and documentation described the rule
as private API endpoint access without disclosing the managed-node attachment.

That boundary is not true for the default managed-node topology. AWS documents
that EKS automatically associates the cluster security group with both the
control-plane network interfaces and the network interfaces of managed node
groups. The rule therefore grants each configured source CIDR TCP/443 access to
both attachment classes whenever managed nodes retain the default cluster
security group.

## Behavior contract

- An empty `cluster_api_ingress_cidrs` list creates no rule and requires no
  acknowledgement.
- A nonempty list continues to target only TCP/443 on the EKS-managed cluster
  security group.
- A nonempty list fails during planning unless
  `allow_cluster_security_group_node_ingress` is explicitly true.
- The opt-in acknowledges that the source CIDRs can reach TCP/443 listeners on
  managed nodes that carry the cluster security group.
- Sources must be valid IPv4 CIDRs, may not use a `/0` prefix, and must remain
  unique after AWS canonicalization.

### Scope of the canonical-uniqueness check

This check is a diagnostics improvement, not a security correction, and the
audit deliberately records that distinction.

The merged module deduplicated raw strings, so `["10.0.0.0/8", "10.1.2.3/8"]`
passed variable validation. It did not, however, reach apply as a duplicate
AWS rule: the AWS provider independently rejects a non-canonical `cidr_blocks`
entry during planning with

```
"10.1.2.3/8" is not a valid IPv4 CIDR block; did you mean "10.0.0.0/8"?
```

Because the provider requires canonical IPv4 form, and canonical IPv4 form is
unique, no input can satisfy both the merged variable validation and the
provider and still collapse into one rule. The merged behavior was therefore
already fail-closed for this case.

What the correction changes is *where* and *how clearly* the input is refused:
a module-level validation message naming the offending variable, instead of a
provider-level message attached to a generated resource attribute. It rejects
no configuration that the merged module would have successfully applied.
- The correction adds no AWS reads, writes, resources, retries, or per-node
  operations. It adds one constant-time resource precondition.

## Alternatives

### Continue silently

Rejected. Updating prose alone would leave an unreviewed network scope
expansion active for callers that reasonably relied on the original boundary.

### Require a control-plane-only security group

Deferred. A customer-specified security group attached at EKS cluster creation
is not automatically attached to managed node groups, but existing clusters do
not necessarily have one. Requiring such a group would turn this bounded audit
correction into a cluster networking migration.

### Explicitly accept the shared cluster security group

Selected. The module retains its existing API ingress mechanism but fails
closed until the caller acknowledges the actual attachment scope.

## Rollout

1. Existing callers with an empty CIDR list require no change.
2. Callers with a nonempty list must inspect whether managed nodes carry the
   cluster security group.
3. If the shared TCP/443 exposure is intended, set
   `allow_cluster_security_group_node_ingress = true` and review the plan.
4. If the exposure is not intended, leave the opt-in false and move endpoint
   access to a dedicated control-plane security-group design before applying.

The precondition blocks planning instead of deleting or replacing an existing
rule automatically. This preserves an explicit operator decision for deployed
state.

## Test plan

- Prove the regression test fails on PR #1159 because a nonempty CIDR list
  plans without acknowledgement.
- Prove the corrected module rejects that plan at the resource precondition.
- Prove an acknowledged commercial-partition plan still targets the exact EKS
  cluster security group and preserves the reviewed CIDRs.
- Prove the default creates no rule without requiring acknowledgement.
- Prove textually distinct CIDRs that AWS canonicalizes to the same range fail
  module input validation, and record that the merged module already failed the
  same input at the provider rather than applying a duplicate rule.
- Prove invalid, IPv6, canonical `/0`, and noncanonical `/0` inputs remain
  fail-closed.
- Run the complete module Terraform test suite, recursive Terraform formatting,
  and repository diff checks.
- Confirm the `infra/terraform/**` path activates the dedicated Terraform Infra
  Tests workflow and that it passes on the exact follow-up head.

## Primary evidence

AWS EKS security group requirements:
https://docs.aws.amazon.com/eks/latest/userguide/sec-group-reqs.html

AWS EC2 ingress CIDR canonicalization:
https://docs.aws.amazon.com/AWSEC2/latest/APIReference/API_AuthorizeSecurityGroupIngress.html
