mock_provider "aws" {
  override_during = plan

  mock_data "aws_caller_identity" {
    defaults = {
      account_id = "123456789012"
      arn        = "arn:aws-us-gov:iam::123456789012:role/terraform-test"
      id         = "123456789012"
      user_id    = "terraform-test"
    }
  }

  mock_data "aws_partition" {
    defaults = {
      dns_suffix         = "amazonaws.com"
      id                 = "aws-us-gov"
      partition          = "aws-us-gov"
      reverse_dns_prefix = "gov.amazonaws"
    }
  }

  mock_data "aws_region" {
    defaults = {
      description = "AWS GovCloud (US-West)"
      endpoint    = "ec2.us-gov-west-1.amazonaws.com"
      id          = "us-gov-west-1"
      name        = "us-gov-west-1"
      region      = "us-gov-west-1"
    }
  }

  mock_data "aws_eks_cluster" {
    defaults = {
      arn      = "arn:aws-us-gov:eks:us-gov-west-1:123456789012:cluster/platform-eks"
      endpoint = "https://example.eks.amazonaws.com"
      id       = "platform-eks"
      certificate_authority = [{
        data = "dGVzdC1jYQ=="
      }]
      identity = [{
        oidc = [{
          issuer = "https://oidc.eks.us-gov-west-1.amazonaws.com/id/example"
        }]
      }]
    }
  }

  mock_data "aws_iam_policy_document" {
    defaults = {
      id            = "terraform-test-policy"
      json          = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
      minified_json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
    }
  }
}

mock_provider "kubernetes" {
  override_during = plan
}

mock_provider "helm" {
  override_during = plan
}

mock_provider "time" {
  override_during = plan
}

variables {
  aws_region                = "us-gov-west-1"
  aws_account_id            = "123456789012"
  host_cluster_name         = "platform-eks"
  chart_version             = "1.1.0"
  operations_helper_image   = "registry.example/skypilot-ops:1.1.0"
  db_connection_secret_name = "skypilot-postgres"
  oauth_enabled             = false
  catalog_mirror = {
    url                      = "https://catalog.example"
    token_secretsmanager_key = "skypilot/catalog-token"
  }
  eso_secrets_reader_role_name = "external-secrets"
}

run "govcloud_secret_arns_use_active_partition" {
  command = plan

  assert {
    condition = (
      jsondecode(aws_iam_role_policy.eso_read_oauth_secret[0].policy).Statement[0].Resource[0] ==
      "arn:aws-us-gov:secretsmanager:us-gov-west-1:123456789012:secret:skypilot/catalog-token-*"
    )
    error_message = "Secrets Manager grants must derive the GovCloud partition."
  }

  assert {
    condition = (
      jsondecode(aws_iam_role.api_server.assume_role_policy).Statement[0].Principal.Service ==
      "pods.eks.amazonaws.com"
    )
    error_message = "The EKS Pod Identity service principal must remain valid in GovCloud."
  }
}

run "china_secret_arns_use_active_partition" {
  command = plan

  providers = {
    aws        = aws.china
    kubernetes = kubernetes
    helm       = helm
    time       = time
  }

  variables {
    aws_region = "cn-north-1"
  }

  assert {
    condition = (
      jsondecode(aws_iam_role_policy.eso_read_oauth_secret[0].policy).Statement[0].Resource[0] ==
      "arn:aws-cn:secretsmanager:cn-north-1:123456789012:secret:skypilot/catalog-token-*"
    )
    error_message = "Secrets Manager grants must derive the China partition."
  }

  assert {
    condition = (
      jsondecode(aws_iam_role.api_server.assume_role_policy).Statement[0].Principal.Service ==
      "pods.eks.amazonaws.com"
    )
    error_message = "The EKS Pod Identity service principal must remain valid in China."
  }
}

mock_provider "aws" {
  alias           = "china"
  override_during = plan

  mock_data "aws_caller_identity" {
    defaults = {
      account_id = "123456789012"
      arn        = "arn:aws-cn:iam::123456789012:role/terraform-test"
      id         = "123456789012"
      user_id    = "terraform-test"
    }
  }

  mock_data "aws_partition" {
    defaults = {
      dns_suffix         = "amazonaws.com.cn"
      id                 = "aws-cn"
      partition          = "aws-cn"
      reverse_dns_prefix = "cn.com.amazonaws"
    }
  }

  mock_data "aws_region" {
    defaults = {
      description = "China (Beijing)"
      endpoint    = "ec2.cn-north-1.amazonaws.com.cn"
      id          = "cn-north-1"
      name        = "cn-north-1"
      region      = "cn-north-1"
    }
  }

  mock_data "aws_eks_cluster" {
    defaults = {
      arn      = "arn:aws-cn:eks:cn-north-1:123456789012:cluster/platform-eks"
      endpoint = "https://example.eks.amazonaws.com.cn"
      id       = "platform-eks"
      certificate_authority = [{
        data = "dGVzdC1jYQ=="
      }]
      identity = [{
        oidc = [{
          issuer = "https://oidc.eks.cn-north-1.amazonaws.com.cn/id/example"
        }]
      }]
    }
  }

  mock_data "aws_iam_policy_document" {
    defaults = {
      id            = "terraform-test-policy"
      json          = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
      minified_json = "{\"Version\":\"2012-10-17\",\"Statement\":[]}"
    }
  }
}
