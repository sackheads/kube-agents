locals {
  base_apis = [
    "container.googleapis.com",
    "iam.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "monitoring.googleapis.com",
    "logging.googleapis.com",
  ]
  chat_apis = var.enable_google_chat ? [
    "pubsub.googleapis.com",
    "chat.googleapis.com",
    "gsuiteaddons.googleapis.com",
  ] : []
  minter_apis = var.enable_github_minter ? ["cloudkms.googleapis.com"] : []

  required_apis = toset(concat(local.base_apis, local.chat_apis, local.minter_apis))

  # Only non-empty credential keys end up in the Secret, so an unset optional
  # provider key does not create an empty entry.
  credentials = {
    for key, value in {
      API_SERVER_KEY    = var.api_server_key
      ANTHROPIC_API_KEY = var.anthropic_api_key
      GEMINI_API_KEY    = var.gemini_api_key
      OPENAI_API_KEY    = var.openai_api_key
    } : key => value if value != ""
  }
}

resource "google_project_service" "required" {
  for_each = local.required_apis

  project            = var.project_id
  service            = each.value
  disable_on_destroy = false
}

module "gke_cluster" {
  source = "../../modules/gke-cluster"

  project_id          = var.project_id
  cluster_name        = var.cluster_name
  location            = var.location
  deletion_protection = var.deletion_protection
  release_channel     = var.release_channel

  depends_on = [google_project_service.required]
}

locals {
  # Defaults to the cluster this composition creates, so an unconfigured install
  # gets per-cluster credentials rather than one identity that reads the whole
  # project. Named from `var.*` rather than from `module.gke_cluster.*` on
  # purpose: the pool's service accounts would otherwise depend on the cluster
  # existing, and Terraform would refuse to plan the IAM until after the cluster
  # was created. The two values are the same by construction -- the gke_cluster
  # module is given exactly these.
  scoped_clusters = var.scoped_clusters != null ? var.scoped_clusters : [{
    project_id   = var.project_id
    location     = var.location
    cluster_name = var.cluster_name
  }]

  # Indexed by the same key the module uses, so the emails coming back out of
  # the module can be rejoined with the tuple that produced them without
  # re-deriving anything.
  scoped_pool_entries = {
    for cluster in local.scoped_clusters :
    "projects/${cluster.project_id}/locations/${cluster.location}/clusters/${cluster.cluster_name}" => cluster
  }
}

module "kube_agents_iam" {
  source = "../../modules/kube-agents-iam"

  project_id      = var.project_id
  namespace       = var.namespace
  project_roles   = var.project_roles
  scoped_clusters = local.scoped_clusters

  depends_on = [google_project_service.required]
}

module "chat_pubsub" {
  source = "../../modules/chat-pubsub"
  count  = var.enable_google_chat ? 1 : 0

  project_id                  = var.project_id
  agent_service_account_email = module.kube_agents_iam.service_account_email

  depends_on = [google_project_service.required]
}

module "github_minter" {
  source = "../../modules/github-minter"
  count  = var.enable_github_minter ? 1 : 0

  project_id = var.project_id
  location   = var.location
  namespace  = var.namespace

  depends_on = [google_project_service.required]
}

resource "helm_release" "kube_agents" {
  name             = "kube-agents"
  chart            = "${path.module}/../../../charts/kube-agents"
  namespace        = var.namespace
  create_namespace = true

  values = [yamlencode({
    operator = {
      image = {
        tag = var.image_tag
      }
    }
    litellm = {
      modelProvider    = var.model_provider
      modelDefaultName = var.model_default_name
    }
    platformAgent = {
      harness = {
        clusterName = module.gke_cluster.cluster_name
        location    = module.gke_cluster.cluster_location
        projectId   = var.project_id
      }
      deployment = {
        image = {
          tag = var.image_tag
        }
      }
      security = {
        # With annotations set, the OPERATOR creates and manages the KSA (see
        # the chart README's ServiceAccount-ownership section); this one wires
        # Workload Identity to the GSA the kube-agents-iam module created.
        serviceAccountAnnotations = {
          "iam.gke.io/gcp-service-account" = module.kube_agents_iam.service_account_email
        }
        # The mapping the credential broker selects from. It has to reach the
        # cluster as data rather than being recomputed there: the broker refuses
        # a scope it has no entry for, so a second implementation of the naming
        # rule would turn a mismatch into a refusal at request time instead of a
        # diff at plan time.
        scopedServiceAccounts = [
          for key in sort(keys(module.kube_agents_iam.scoped_service_accounts)) : {
            projectId           = local.scoped_pool_entries[key].project_id
            location            = local.scoped_pool_entries[key].location
            clusterName         = local.scoped_pool_entries[key].cluster_name
            serviceAccountEmail = module.kube_agents_iam.scoped_service_accounts[key]
          }
        ]
      }
      credentials = {
        create = true
        data   = local.credentials
      }
      integration = merge(
        var.enable_google_chat ? {
          googleChat = {
            enabled          = true
            topicName        = module.chat_pubsub[0].topic_name
            subscriptionName = module.chat_pubsub[0].subscription_name
            allowedUsers     = var.google_chat_allowed_users
          }
        } : {},
        var.github_repo != "" ? {
          github = { gitRepo = var.github_repo }
        } : {}
      )
    }
  })]

  depends_on = [module.gke_cluster]
}
