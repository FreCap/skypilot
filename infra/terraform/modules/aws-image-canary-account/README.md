# AWS image canary account bootstrap

Apply this module exactly once in each AWS compute account before enabling
Spot-backed managed-image canaries. It creates the account-global
`AWSServiceRoleForEC2Spot` role required when Spot is requested through the AWS
API. Every regional `aws-image-canary-target` module shares this one role.

If the account already has the role, import it instead of attempting a second
creation:

```bash
terraform import \
  module.image_canary_account.aws_iam_service_linked_role.ec2_spot \
  arn:<partition>:iam::<account-id>:role/aws-service-role/spot.amazonaws.com/AWSServiceRoleForEC2Spot
```

Pass `spot_service_linked_role_arn` to each target module. This creates an
explicit dependency when both modules are in one Terraform state. For a target
in another state, apply this account bootstrap first and pass or validate the
same canonical ARN.

The role is account-global, so do not repeat this module per region. Regional
customer-managed KMS grants remain in `aws-image-canary-target`, beside the AMI
and the key.

The resource has `prevent_destroy` because every regional target state and any
other Spot workload in the account share it. Decommission regional targets and
KMS grants first. Removing the account-global role requires a separate,
deliberate lifecycle change after confirming that no Spot request or instance
still depends on it.
