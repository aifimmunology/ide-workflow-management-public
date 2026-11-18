package env

import (
	"fmt"
	"strings"

	"github.com/aifimmunology/shared-utility-library/utils"
)

func ArtifactRegistry() string {
	return utils.EnvOrDefault("ARTIFACT_REGISTRY", fmt.Sprintf("us-west1-docker.pkg.dev/%s", Project()))
}

func AuthClientId() string {
	return utils.EnvOrDefault("AUTH_CLIENT_ID", "")
}

func BadStateCronExpression() string {
	return utils.EnvOrDefault("BAD_STATE_CRON_EXPRESSION", "*/30 * * * *") // every 30 minutes
}

func BadStateFixedCronExpression() string {
	return utils.EnvOrDefault("BAD_STATE_FIXED_CRON_EXPRESSION", "*/5 * * * *") // every 5 minutes
}

func BillingQuotaCronExpression() string {
	return utils.EnvOrDefault("BILLING_QUOTA_CRON_EXPRESSION", "0 8 * * *") // every day at 8am
}

func CompletedWorkflowsTopic() string {
	return utils.EnvOrDefault("COMPLETED_WORKFLOWS_TOPIC", "")
}

func ComputeZone() string {
	return utils.EnvOrDefault("COMPUTE_GCP_ZONE", "")
}

func Env() string {
	return utils.EnvOrDefault("ENVIRONMENT", "")
}

func IDEManagementDebug() bool {
	return !strings.EqualFold(utils.EnvOrDefault("IDE_MANAGEMENT_DEBUG", "false"), "false")
}

func IdeProcEndPoint() string {
	return utils.EnvOrDefault("IDE_PROC_ENDPOINT", "")
}

func IdeServiceAccount() string {
	return utils.EnvOrDefault("IDE_SERVICE_ACCOUNT", "")
}

func K8sNamespace() string {
	return utils.EnvOrDefault("KUBERNETES_NAMESPACE", "default")
}

func KubernetesPodPendingTimeout() int {
	return utils.EnvOrDefaultInt("KUBERNETES_POD_PENDING_TIMEOUT", 360) //6 hours
}

func KubernetesPodTimeout() int {
	return utils.EnvOrDefaultInt("KUBERNETES_POD_TIMEOUT", 15) // 15 minutes
}

func MaxPubSubPullMessages() int32 {
	return int32(utils.EnvOrDefaultInt("MAX_PUBSUB_PULL_MESSAGES", 5000))
}

func MonthlyQuotaCronExpression() string {
	return utils.EnvOrDefault("MONTHLY_QUOTA_CRON_EXPRESSION", "0 0 1 * *") // first day of every month at midnight
}

func PackageVulnerabilitiesCronExpression() string {
	return utils.EnvOrDefault("PACKAGE_VULNERABILITIES_CRON_EXPRESSION", "0 7 * * 1") // every Monday at 7am
}

func PermanentDeleteIdesCronExpression() string {
	return utils.EnvOrDefault("PERMANENT_DELETE_IDES_CRON_EXPRESSION", "0 9 * * *") // every day at 9am
}

func PodTimeout() string {
	return utils.EnvOrDefault("POD_TIMEOUT", "1200") //in seconds
}

func Port() string {
	return utils.EnvOrDefault("PORT", "")
}

func Project() string {
	return utils.EnvOrDefault("GCP_PROJECT", "")
}

// region is just zone without the final -a, e.g. us-west1
func Region() string {
	regionTokens := strings.Split(Zone(), "-")
	return fmt.Sprintf("%s-%s", regionTokens[0], regionTokens[1])
}

func Stage() string {
	return strings.Split(Project(), "-")[0]
}

func StaleIDEsCronExpression() string {
	return utils.EnvOrDefault("STALE_IDES_CRON_EXPRESSION", "0 8 * * *") // every day at 8am
}

func StaleVolumesCronExpression() string {
	return utils.EnvOrDefault("STALE_VOLUMES_CRON_EXPRESSION", "30 7 * * *") // every day at 7:30am
}

func SupportEmail() string {
	return utils.EnvOrDefault("SUPPORT_EMAIL", "")
}

func TempUserDiskCronExpression() string {
	return utils.EnvOrDefault("TEMP_USER_DISK_CRON_EXPRESSION", "0 7 * * *") // every day at 7am
}

func TimeoutCronExpression() string {
	return utils.EnvOrDefault("TIMEOUT_CRON_EXPRESSION", "* * * * *") // every minute
}

func UserDiskSize() string {
	return utils.EnvOrDefault("DEFAULT_USER_DISK_SIZE", "108Gi")
}

func VulnerabilitiesSubscription() string {
	return utils.EnvOrDefault("VULNERABILITIES_SUBSCRIPTION", "")
}

func Zone() string {
	return utils.EnvOrDefault("GCP_ZONE", "")
}
