mock_provider "kubernetes" {
  override_during = plan
}

variables {
  subjects = [{
    kind = "Group"
    name = "skypilot:pool"
  }]
}

run "rejects_an_invalid_rbac_name" {
  command = plan

  variables {
    name = "Invalid_Name"
  }

  expect_failures = [var.name]
}

run "rejects_an_invalid_namespace" {
  command = plan

  variables {
    namespace = "Invalid_Namespace"
  }

  expect_failures = [var.namespace]
}

run "rejects_an_invalid_service_account_name" {
  command = plan

  variables {
    service_account_name = "Invalid_Service_Account"
  }

  expect_failures = [var.service_account_name]
}

run "rejects_an_empty_subject_list" {
  command = plan

  variables {
    subjects = []
  }

  expect_failures = [var.subjects]
}

run "rejects_a_service_account_subject_without_a_namespace" {
  command = plan

  variables {
    subjects = [{
      kind = "ServiceAccount"
      name = "another-service-account"
    }]
  }

  expect_failures = [var.subjects]
}

run "rejects_duplicate_subjects" {
  command = plan

  variables {
    subjects = [
      {
        kind = "User"
        name = "platform@example.com"
      },
      {
        kind = "User"
        name = "platform@example.com"
      },
    ]
  }

  expect_failures = [var.subjects]
}

run "rejects_the_wrong_rbac_api_group" {
  command = plan

  variables {
    subjects = [{
      kind      = "Group"
      name      = "skypilot:pool"
      api_group = "example.invalid"
    }]
  }

  expect_failures = [var.subjects]
}

run "rejects_a_blank_local_queue_name" {
  command = plan

  variables {
    kueue = {
      local_queue_name   = ""
      cluster_queue_name = "inference"
    }
  }

  expect_failures = [var.kueue]
}

run "rejects_an_invalid_local_queue_name" {
  command = plan

  variables {
    kueue = {
      local_queue_name   = "Invalid_Queue"
      cluster_queue_name = "inference"
    }
  }

  expect_failures = [var.kueue]
}

run "accepts_a_63_character_cluster_queue_dns_label" {
  command = plan

  variables {
    kueue = {
      local_queue_name   = "default"
      cluster_queue_name = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    }
  }

  assert {
    condition     = output.kueue.cluster_queue_name == "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    error_message = "A 63-character DNS-1123 label must remain a valid ClusterQueue name."
  }
}

run "rejects_a_dotted_cluster_queue_name" {
  command = plan

  variables {
    kueue = {
      local_queue_name   = "default"
      cluster_queue_name = "inference.reserved"
    }
  }

  expect_failures = [var.kueue]
}

run "rejects_an_uppercase_cluster_queue_name" {
  command = plan

  variables {
    kueue = {
      local_queue_name   = "default"
      cluster_queue_name = "Inference"
    }
  }

  expect_failures = [var.kueue]
}

run "rejects_an_underscore_cluster_queue_name" {
  command = plan

  variables {
    kueue = {
      local_queue_name   = "default"
      cluster_queue_name = "inference_reserved"
    }
  }

  expect_failures = [var.kueue]
}

run "rejects_an_overlength_cluster_queue_name" {
  command = plan

  variables {
    kueue = {
      local_queue_name   = "default"
      cluster_queue_name = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    }
  }

  expect_failures = [var.kueue]
}

run "rejects_a_trailing_hyphen_cluster_queue_name" {
  command = plan

  variables {
    kueue = {
      local_queue_name   = "default"
      cluster_queue_name = "inference-"
    }
  }

  expect_failures = [var.kueue]
}
