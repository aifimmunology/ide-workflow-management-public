package handler

import (
	"cmp"
	"context"
	"fmt"
	"net/http"
	"os"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"github.com/aifimmunology/hise-data-contracts/clients/emailclient"
	"github.com/aifimmunology/hise-data-contracts/clients/tracerclient"
	"github.com/aifimmunology/hise-data-contracts/contracts"
	emailmodel "github.com/aifimmunology/hise-data-contracts/contracts/email"
	"github.com/aifimmunology/hise-data-contracts/contracts/pubsubmessages"
	"github.com/aifimmunology/hise-data-contracts/contracts/tracer"
	"github.com/aifimmunology/hise-data-contracts/core"
	"github.com/aifimmunology/ide-workflow-management/dependencycontainer"
	"github.com/aifimmunology/ide-workflow-management/env"
	"github.com/aifimmunology/shared-utility-library/argo"
	"github.com/aifimmunology/shared-utility-library/datastructures"
	"github.com/aifimmunology/shared-utility-library/kubernetesclient"
	"github.com/aifimmunology/shared-utility-library/log"
	"github.com/aifimmunology/shared-utility-library/types"
	"github.com/aifimmunology/shared-utility-library/utils"

	"github.com/argoproj/argo-workflows/v3/pkg/apis/workflow/v1alpha1"
	"github.com/google/uuid"
	"github.com/samber/lo"
	"k8s.io/apimachinery/pkg/api/resource"
)

const (
	argoDebug                       bool   = false
	kubernetesNodeInstanceTypeLabel string = "node.kubernetes.io/instance-type"
	logSizeName                     string = "log_size"
	logSizeVal                      string = "4000"
	previousAction                  string = "prevAction"
	previousStatusKey               string = "prevStatus"
	pvcSuffix                       string = "pvcSuffix"
	wfNotFoundErr                   string = "argo workflow not found"
	workflowNameNotFound            string = "name not found"
)

var (
	clusterLimitRegex *regexp.Regexp = regexp.MustCompile(
		`\d+\s+max cluster\s+[\w-]+\s+limit reached`,
	)
	eTagActions datastructures.Set[string] = datastructures.SliceToSet(
		[]string{
			tracerclient.ActionChangeJupyterTimeout,
			tracerclient.ActionCreate,
			tracerclient.ActionEditMachineConfig,
			tracerclient.ActionProvisionHttpRoute,
			tracerclient.ActionSetIapPolicy,
			tracerclient.ActionStart,
			tracerclient.ActionStop,
		},
	)
	nodesNotAvailableRegex *regexp.Regexp             = regexp.MustCompile(`0/\d+\s+nodes\s+are\s+available`)
	pvcRegex               *regexp.Regexp             = regexp.MustCompile(`persistentvolumeclaim\s+(.+?)\s+not found`)
	sseActions             datastructures.Set[string] = datastructures.SliceToSet(
		[]string{
			tracerclient.ActionCreate,
			tracerclient.ActionDelete,
			tracerclient.ActionProvisionHttpRoute,
			tracerclient.ActionSetIapPolicy,
			tracerclient.ActionStart,
			tracerclient.ActionStop,
		},
	)
	statusLock sync.Mutex
)

type argoWorkflowBuilder struct {
	params     argo.ArgoParams
	workingDir string
	file       string
}

func (argoWfB *argoWorkflowBuilder) AddParam(key string, val string) {
	argoWfB.params.Add(key, val)
}

func (argoWfB *argoWorkflowBuilder) File() string {
	return argoWfB.file
}

func (argoWfB *argoWorkflowBuilder) Params() argo.ArgoParams {
	argoWfB.params.Add(logSizeName, logSizeVal)
	if argoDebug {
		argoWfB.params.Add(argo.CleanupFlag, "false")
	}
	return argoWfB.params
}

func (argoWfB *argoWorkflowBuilder) SaveFlow(id string, wf *v1alpha1.Workflow) error {
	return nil
}

func (argoWfB *argoWorkflowBuilder) SetFile(file string) {
	argoWfB.file = file
}

func (argoWfB *argoWorkflowBuilder) SetOutputFlagTrue() {
	argoWfB.params.Add(argo.OutputFlag, "true")
}

func (argoWfB *argoWorkflowBuilder) SetWorkingDirectory(workingDir string) {
	argoWfB.workingDir = workingDir
}

func (argoWfB *argoWorkflowBuilder) WorkingDirectory() string {
	return argoWfB.workingDir
}

func ackMsgAndCleanUpWorkflow(m *pubsub.Message, wf *v1alpha1.Workflow) {
	m.Ack()
	if wf == nil {
		return
	}

	if err := cleanupWorkflow(wf); err != nil {
		log.Main.Error(err)
	}
}

func appendToWorkflowHistory(
	ide *tracer.IDEInstance,
	wf *v1alpha1.Workflow,
	argoNodeStatuses []*v1alpha1.NodeStatus) (*tracer.IDEInstance, error) {
	recentIde, err := dependencycontainer.TracerDataServiceClient.GetIdeInstance(ide.ID)
	if err != nil {
		return nil, err
	}

	if recentIde.WorkflowHistory == nil {
		recentIde.WorkflowHistory = make(map[string][]tracer.IDEArgoStep)
	}

	actionName := getActionName(wf.Name, recentIde)
	existingSteps := recentIde.WorkflowHistory[actionName]

	stepIndex := make(map[string]int, len(existingSteps))
	for i, step := range existingSteps {
		stepIndex[step.Name] = i
	}

	for _, argoNodeStatus := range argoNodeStatuses {
		stepName, _, _ := strings.Cut(argoNodeStatus.DisplayName, "(")

		latest := tracer.IDEArgoStep{
			Name:      stepName,
			Status:    string(argoNodeStatus.Phase),
			StartTime: argoNodeStatus.StartedAt.Time,
			EndTime:   argoNodeStatus.FinishedAt.Time,
		}

		if i, ok := stepIndex[stepName]; ok {
			existingSteps[i] = latest
		} else {
			stepIndex[stepName] = len(existingSteps)
			existingSteps = append(existingSteps, latest)
		}
	}

	recentIde.WorkflowHistory[actionName] = existingSteps
	return recentIde, nil
}

func HandleArgoWorkflow(
	wfBuilder *argoWorkflowBuilder,
	ide *tracer.IDEInstance,
	user *core.UserInfo,
	failureStatus, serviceAccount string,
) (*v1alpha1.Workflow, error) {
	wf, err := argo.GetArgoWorkflow(wfBuilder)
	if err != nil {
		logGetArgoError(wfBuilder, err, ide, failureStatus)
		return nil, err
	}
	if serviceAccount != "" {
		wf.Spec.ServiceAccountName = serviceAccount
	}

	var flowError error
	newwf, flowError := argo.SubmitArgoWorkflow(wf)
	if flowError != nil {
		logSubmitArgoError(wfBuilder, flowError, ide, failureStatus)
		return nil, flowError
	}
	return newwf, nil
}

func InitWorkFlowBuilder(argoYAMLFileName string) *argoWorkflowBuilder {
	rootDirectory, _ := os.Getwd()
	rootDirectory = rootDirectory + "/flows"
	wfBuilder := argoWorkflowBuilder{}
	wfBuilder.SetWorkingDirectory(rootDirectory)
	wfBuilder.SetFile(argoYAMLFileName)
	return &wfBuilder
}

func createRunIamPolicyWfBuilder(ide *tracer.IDEInstance, message *pubsubmessages.IDEFlowRequest) (*argoWorkflowBuilder, error) {
	serviceAcctEmail, err := formatServiceAccountEmail(ide.AuditInfo.AddedUser)
	if err != nil {
		return nil, err
	}

	wfBuilder := InitWorkFlowBuilder("set-iam-policy-workflow.yaml")
	wfBuilder.params.Add("ideManSA", env.IdeServiceAccount())
	wfBuilder.params.Add("serviceAccountEmail", serviceAcctEmail)
	wfBuilder.params.Add("userEmail", strings.ToLower(ide.AuditInfo.AddedUser))
	wfBuilder.params.Add("projectId", env.Project())
	wfBuilder.params.Add("ideUrl", fmt.Sprintf("https://%s/%s/lab", env.IdeProcEndPoint(), ide.PodName))

	if eTagActions.Contains(message.Action) {
		wfBuilder.params.Add(argo.CleanupFlag, "false")
	}

	if ide.DashApp.True() {
		wfBuilder.params.Add("podName", fmt.Sprintf("%s-dashapp-ingress", ide.PodName))
	} else {
		wfBuilder.params.Add("podName", fmt.Sprintf("%s-ingress", ide.PodName))
	}

	return wfBuilder, nil
}

func RunDescribeK8sPodArgo(
	argoWf *v1alpha1.Workflow,
	message *pubsubmessages.IDEFlowRequest,
	ide *tracer.IDEInstance,
	failureStatus string,
) error {
	if argoWf != nil {
		defer cleanupWorkflow(argoWf)
	}

	wfBuilder := InitWorkFlowBuilder("describe-pod.yaml")
	wfBuilder.params.Add(
		"pod_name",
		fmt.Sprintf("%s-0", ide.PodName),
	)
	wfBuilder.params.Add(argo.OutputFlag, "true")
	_, outputParams, err := handleArgoFromWfBuilder(wfBuilder, message, ide, failureStatus, "")
	if err != nil {
		return err
	}

	var errorDetails string
	podEvents, exists := outputParams["pod_events"]
	if exists {
		errorDetails, failureStatus = parsePodEventsAndStatus(podEvents)
	} else {
		errorDetails = fmt.Sprintf(
			"Current status is %s. Kubernetes pod not found, no additional logs available.",
			failureStatus,
		)
	}

	if _, serr := sendErrorLogsEmail(ide, errorDetails, failureStatus); serr != nil {
		log.Main.Error(fmt.Sprintf("error sending error logs email about ide %s: %s", ide.ID.String(), serr.Error()))
	}

	setIdeStatus(ide, failureStatus, errorDetails)
	return dependencycontainer.TracerDataServiceClient.UpdateIdeInstance(ide)
}

func RunSetIamPolicyArgo(ide *tracer.IDEInstance, message *pubsubmessages.IDEFlowRequest) (*v1alpha1.Workflow, error) {
	if wfBuilder, err := createRunIamPolicyWfBuilder(ide, message); err != nil {
		return nil, err
	} else if wf, _, err := handleArgoFromWfBuilder(wfBuilder, message, ide, "iap-policy-failure", env.IdeServiceAccount()); err != nil {
		return nil, err
	} else {
		setIdeInstanceActionHistory(ide, tracerclient.ActionSetIapPolicy, wf)
		if uerr := dependencycontainer.TracerDataServiceClient.UpdateIdeInstance(ide); uerr != nil {
			log.Main.Error(uerr)
		}
		return wf, nil
	}
}

func SubmitSetIamPolicyArgo(ctx context.Context, ide *tracer.IDEInstance, message *pubsubmessages.IDEFlowRequest, failureStatus string) (*v1alpha1.Workflow, error) {
	recentIde, err := dependencycontainer.TracerDataServiceClient.GetIdeInstance((ide.ID))
	if err != nil {
		return nil, err
	}
	if suffix, has := message.AdditionalPayload[pvcSuffix]; has {
		recentIde.VolumeHistory = append(recentIde.VolumeHistory, tracer.IDEVolumeHistory{PvcSuffix: suffix.(string)})
	}
	if err := setIdeResolvedMachineInfo(ctx, recentIde); err != nil {
		log.Main.Error(err)
	}
	if err := dependencycontainer.TracerDataServiceClient.UpdateIdeInstanceWithETag(recentIde); err != nil {
		return nil, err
	}

	if wfBuilder, err := createRunIamPolicyWfBuilder(recentIde, message); err != nil {
		return nil, err
	} else if wf, err := submitArgoWorkflow(wfBuilder, env.IdeServiceAccount()); err != nil {
		updateIdeFailureStatus(recentIde, failureStatus, message.Action)
		return nil, err
	} else {
		return wf, nil
	}
}

func TriggerStopIde(ideId uuid.UUID, quotaReached ...bool) error {
	_, err := dependencycontainer.IdeManagementServiceClient.StopIDE(ideId, quotaReached...)
	return err
}

func UpdateNotificationStatus(ide *tracer.IDEInstance, notificationType, status string) error {
	statusLock.Lock()
	defer statusLock.Unlock()

	recentIde, err := dependencycontainer.TracerDataServiceClient.GetIdeInstance(ide.ID)
	if err != nil {
		log.Main.Error(fmt.Sprintf("error getting IDE instance %s: %s", ide.ID.String(), err.Error()))
		return err
	}

	recentIde.NotificationStatus = tracer.NotificationStatus{
		NotificationType: notificationType,
		PodStatus:        status,
		LastNotified:     time.Now(),
	}

	if err := dependencycontainer.TracerDataServiceClient.UpdateIdeInstance(recentIde); err != nil {
		log.Main.Error(fmt.Errorf("could not update ide instance %s: %s", ide.PodName, err.Error()))
	}
	return nil
}

func cleanupWorkflow(argoWorkflow *v1alpha1.Workflow) error {
	if argoDebug {
		log.Main.Info(
			"ARGO WORKFLOWS NOT BEING CLEANED UP. NO CLEANUP FOR YOU",
			log.D{"workflow": argoWorkflow.Name, "didit": false},
		)
		return nil
	}

	if runner, arerr := argo.NewArgoWorkflowRunner(); arerr != nil {
		//don't return error, just log it. We don't want to stop the remaining ide flow
		log.Main.Error(fmt.Sprintf("error creating argo workflow runner: %s", arerr.Error()))
		return arerr
	} else {
		return runner.Cleanup(argoWorkflow)
	}
}

func findAndMonitorArgoWorkflowAndUpdateIde(
	wf *v1alpha1.Workflow,
	ide *tracer.IDEInstance,
) (map[string]string, error) {
	runner, err := argo.GetArgoWorkflowRunner()
	if err != nil {
		return nil, err
	}

	monitor, err := runner.FindByNameAndMonitor(wf.GetName())
	if err != nil {
		return nil, err
	}

	for {
		select {
		case wf = <-monitor.WorkflowUpdateChan:
			if err = updateIdeWorkflowHistory(ide, wf); err != nil {
				log.Main.Error(err, log.D{"context": "Update IDE Workflow History", "workflow": wf.Name})
			}
		case finalStatus := <-monitor.WorkflowPhaseChannel:
			wf = monitor.Workflow
			if err = updateIdeWorkflowHistory(ide, wf); err != nil {
				log.Main.Error(err, log.D{"context": "Update IDE Workflow History", "workflow": wf.Name})
			}

			output, cleanup := argo.GetArgoOutputParams(monitor)
			if cleanup {
				if err = cleanupWorkflow(wf); err != nil {
					log.Main.Error(err, log.D{"context": "Cleanup of Argo Job", "workflow": wf.Name})
				}
			}

			if finalStatus != v1alpha1.WorkflowSucceeded {
				return nil, fmt.Errorf("workflow watch for %s ended with status %v", wf.Name, finalStatus)
			}
			return output, nil
		}
	}
}

func formatServiceAccountEmail(userEmail string) (string, error) {
	// change from first.last@example.org
	// to first-last-sa@example-project.iam.gserviceaccount.com
	splitEmail := strings.Split(userEmail, "@")
	if len(splitEmail) != 2 {
		return "", fmt.Errorf("invalid email format: %s", userEmail)
	}

	username := strings.ReplaceAll(splitEmail[0], ".", "-")
	return fmt.Sprintf("%s-sa@%s.iam.gserviceaccount.com", username, env.Project()), nil
}

func getAccountName(acctGuid uuid.UUID) (string, error) {
	acct, err := dependencycontainer.AccountDataServiceClient.GetAccountByGuid(acctGuid)
	if err != nil {
		return "", fmt.Errorf("error getting account %s: %s", acctGuid, err.Error())
	}
	return acct.Name, nil
}

func getActionName(workflowName string, ide *tracer.IDEInstance) string {
	switch workflowName = strings.ToLower(workflowName); {
	case strings.Contains(workflowName, "ide-create"),
		strings.Contains(workflowName, "ide-health-route"):
		return "create"
	case strings.Contains(workflowName, "ide-scale-up"):
		return "start"
	case strings.Contains(workflowName, "ide-scale-down-"):
		return "stop"
	case strings.Contains(workflowName, "ide-teardown-"):
		return "delete"
	case strings.Contains(workflowName, "set-iam-policy"):
		return getSetIamActionType(workflowName, ide)
	default:
		return workflowName
	}
}

func getSetIamActionType(workflowName string, ide *tracer.IDEInstance) string {
	if ide.Status == tracerclient.StatusCreating {
		return "create"
	}
	return "start"
}

func getIdeFromFlowRequest(message *pubsubmessages.IDEFlowRequest) (*tracer.IDEInstance, error) {
	return dependencycontainer.TracerDataServiceClient.GetIdeInstance(message.IDEInstanceId)
}

func getPreviousStatus(message *pubsubmessages.IDEFlowRequest) (string, error) {
	var prevStatus string
	if ps, has := message.AdditionalPayload[previousStatusKey]; has {
		if prevStatus, has = ps.(string); !has || prevStatus == "" {
			return "", fmt.Errorf(
				`expected message.AdditionalPayload["prevStatus"] to be a non-empty string, but got "%v" (type %T)`,
				ps,
				ps,
			)
		}
	} else {
		return "", fmt.Errorf(`message.AdditionalPayload["prevStatus"] not found`)
	}
	return prevStatus, nil
}

func handleArgoFindAndMonitor(
	message *pubsubmessages.IDEFlowRequest,
	wf *v1alpha1.Workflow,
	ide *tracer.IDEInstance,
) (map[string]string, error) {
	time.Sleep(2 * time.Second)
	if sseActions.Contains(message.Action) {
		return findAndMonitorArgoWorkflowAndUpdateIde(wf, ide)
	}

	return argo.FindAndMonitorArgoWorkflowByName(wf.Name)
}

func handleArgoFromRequest(
	flowRequest *pubsubmessages.IDEFlowRequest,
	failureStatus string,
) (*v1alpha1.Workflow, *tracer.IDEInstance, error) {
	if ide, err := getIdeFromFlowRequest(flowRequest); err != nil {
		return nil, nil, err
	} else if wf, err := utils.ConvertJsonToPointer[v1alpha1.Workflow](flowRequest.Workflow); err != nil {
		return nil, ide, err
	} else if _, err = handleArgoFindAndMonitor(flowRequest, wf, ide); err != nil {
		log.Main.Error(fmt.Sprintf("error during find and monitor argo workflow %s: %s", wf.GetName(), err.Error()))
		if !isEtagError(err) {
			updateIdeFailureStatus(ide, failureStatus, flowRequest.Action)
		}
		return nil, ide, err
	} else {
		return wf, ide, nil
	}
}

func handleArgoFromWfBuilder(
	workflowBuilder *argoWorkflowBuilder,
	message *pubsubmessages.IDEFlowRequest,
	ideInstance *tracer.IDEInstance,
	failureStatus, serviceAccount string,
) (*v1alpha1.Workflow, map[string]string, error) {
	if submittedFlow, err := submitArgoWorkflow(workflowBuilder, serviceAccount); err != nil {
		updateIdeFailureStatus(ideInstance, failureStatus, message.Action)
		return nil, nil, err
	} else if outParams, err := handleArgoFindAndMonitor(message, submittedFlow, ideInstance); err != nil {
		log.Main.Error(
			fmt.Sprintf("error during find and monitor argo workflow %s: %s", submittedFlow.Name, err.Error()),
		)
		updateIdeFailureStatus(ideInstance, failureStatus, message.Action)
		return nil, outParams, err
	} else {
		return submittedFlow, outParams, nil
	}
}

func handleETagError(err error, msg *pubsub.Message, wf *v1alpha1.Workflow) error {
	log.Main.Error(err)
	if isEtagError(err) {
		msg.Nack()
		return nil
	} else {
		msg.Ack()
		if wf != nil {
			if cerr := cleanupWorkflow(wf); cerr != nil {
				log.Main.Error(cerr)
			}
		}
		return err
	}
}

func isEtagError(err error) bool {
	if err != nil {
		e, is := err.(types.ServiceResponseError)
		return is && e.StatusCode() == http.StatusPreconditionFailed
	}
	log.Main.Error("error is nil when checking for etag errors")
	return false
}

func logGetArgoError(wfBuilder *argoWorkflowBuilder, err error, ide *tracer.IDEInstance, status string) {
	if ide != nil {
		if err != nil {
			setIdeStatus(ide, status, err.Error())
		} else {
			setIdeStatus(ide, status, fmt.Sprintf("error during argo workflow %s", wfBuilder.File()))
		}
	}
}

func logSubmitArgoError(wfBuilder *argoWorkflowBuilder, flowError error, ide *tracer.IDEInstance, status string) {
	if ide != nil {
		setIdeStatus(ide, status, flowError.Error())
	}
	log.Main.Error(fmt.Sprintf("error during submit argo workflow %s: %s", wfBuilder.File(), flowError.Error()))
}

func parsePodEventsAndStatus(podEvents string) (string, string) {
	podEvents = strings.ToLower(podEvents)
	if podEvents == "" {
		return "Kubernetes pod events not found. No details available.", tracerclient.StatusTemporarilyUnavailable
	} else if strings.Contains(podEvents, "gce quota exceeded") {
		return "The Google Cloud CPU/GPU quota has been exceeded. Contact management about the possibility of increasing it.", tracerclient.StatusGoogleQuotaExceeded
	} else if strings.Contains(podEvents, "imagepullbackoff") || strings.Contains(podEvents, "errimagepull") {
		return "The specified Docker image could not be pulled. Please verify the image exists and your account has access to it.", tracerclient.StatusDockerImagePullError
	}

	pvcMatches := pvcRegex.FindStringSubmatch(podEvents)
	if len(pvcMatches) > 1 {
		return fmt.Sprintf(
			"PVC %s not found, IDE creation process was likely interrupted.",
			pvcMatches[1],
		), tracerclient.StatusPvcNotFound
	}

	if clusterLimitRegex.MatchString(podEvents) {
		return "The cluster has reached its Google resource limit, none are available currently. " + "Contact DevOps about increasing the maximum resources available.", tracerclient.StatusClusterResourceLimitReached
	}

	if nodesNotAvailableRegex.MatchString(podEvents) {
		return podEvents, tracerclient.StatusNodesNotAvailable
	}
	return podEvents, tracerclient.StatusTemporarilyUnavailable
}

func revertToOriginalStatus(message *pubsubmessages.IDEFlowRequest, wf *v1alpha1.Workflow) (*tracer.IDEInstance, error) {
	ide, err := setPreviousIdeStatusFromFlowRequest(message)
	if err != nil {
		return nil, err
	}

	if eTagActions.Contains(message.Action) {
		setIdeInstanceActionHistory(ide, message.Action, wf)
		return ide, dependencycontainer.TracerDataServiceClient.UpdateIdeInstanceWithETag(ide)
	}
	return ide, dependencycontainer.TracerDataServiceClient.UpdateIdeInstance(ide)
}

func sendErrorLogsEmail(ide *tracer.IDEInstance, logs string, failureStatus string) (string, error) {
	accountVal := ide.AccountGUID.String()
	if acctName, err := getAccountName(ide.AccountGUID); err != nil {
		log.Main.Error(err.Error())
	} else {
		accountVal = acctName
	}

	return emailclient.SendEmailCustomRequest(
		&emailmodel.EmailRequest{
			ToAddress:        ide.AuditInfo.AddedUser,
			CCAddress:        env.SupportEmail(),
			NotificationType: emailmodel.NotificationIdeErrorV2,
			DynamicTemplateData: map[string]any{
				"ide_name":      ide.FriendlyName,
				"pod_name":      ide.PodName,
				"acct_name":     accountVal,
				"ide_status":    failureStatus,
				"error_details": logs,
			},
		},
	)
}

func setIdeInstanceActionHistory(ideInstance *tracer.IDEInstance, action string, wf *v1alpha1.Workflow) {
	resourceSpec := make(map[string]any)
	if ideInstance.ResourceSpec != nil {
		resourceSpec = ideInstance.ResourceSpec
	}

	deploySpec := make(map[string]any)
	if wf != nil {
		deploySpec[ideInstance.PodName] = *wf
	}

	ideInstance.ActionHistory = append(
		ideInstance.ActionHistory,
		tracer.IDEActionHistory{
			Action:       action,
			ResourceSpec: resourceSpec,
			DeploySpec:   deploySpec,
			AuditInfo: contracts.Audit{
				Added:          time.Now(),
				AddedUser:      "ide-workflow-management",
				LastModified:   time.Now(),
				LastModifiedBy: "ide-workflow-management",
			},
		},
	)
}

func setIdeStatus(ideInstance *tracer.IDEInstance, status string, exceptionDetail ...string) {
	if status == tracerclient.StatusDeleted {
		ideInstance.IsDeleted = contracts.ToSBool(true)
	}

	if ideInstance.Status == tracerclient.StatusStoppedQuotaReached && status == tracerclient.StatusStopped {
		// don't overwrite status if it's already stopped-quota-reached, b/c cron sets this before triggering stop
		// and StatusStoppedQuotaReached makes sure users can't start the IDE again
		return
	}

	if len(exceptionDetail) > 0 {
		if ideInstance.ExceptionDetails == nil {
			ideInstance.ExceptionDetails = make(map[string]any)
		}
		ideInstance.ExceptionDetails[tracerclient.StatusTemporarilyUnavailable] = exceptionDetail[0]
	}

	ideInstance.Status = status
	ideInstance.StatusHistory = append(
		ideInstance.StatusHistory,
		tracer.IDEStatusHistory{
			Status:    status,
			Detail:    utils.FirstOrDefault(exceptionDetail), // if not a failure status, this will be empty
			UpdatedAt: time.Now(),
			UpdatedBy: "IDE Management Workflow Service",
		},
	)
}

func setIdeResolvedMachineInfo(ctx context.Context, ideInstance *tracer.IDEInstance) error {
	kubecli, err := kubernetesclient.NewForCurrentCluster(ctx)
	if err != nil {
		return err
	}

	pod, err := kubecli.GetPod(ctx, kubernetesclient.K8sNamespace(), ideInstance.PodName+"-0")
	if err != nil {
		return err
	}

	node, err := kubecli.GetNode(ctx, pod.Spec.NodeName)
	if err != nil {
		return err
	}

	machineType := node.Labels[kubernetesNodeInstanceTypeLabel]
	parts := strings.Split(machineType, "-")
	if len(parts) != 3 {
		return fmt.Errorf(
			`failed to parse machine type "%s" from node label %s on node %s"`,
			machineType,
			kubernetesNodeInstanceTypeLabel,
			node.Name,
		)
	}

	ideInstance.ResolvedMachineInfo = tracer.ResolvedMachineInfo{
		CPUCount:    parts[2],
		GPUCount:    node.Status.Capacity.Name("nvidia.com/gpu", resource.DecimalSI).String(),
		MachineType: machineType,
		Memory:      utils.ConvertStorageSize(node.Status.Allocatable.Memory().String(), "Gi"),
		Name:        parts[0],
		Type:        parts[1],
	}
	return nil
}

func submitArgoWorkflow(workflowBuilder *argoWorkflowBuilder, serviceAccount string) (*v1alpha1.Workflow, error) {
	wf, err := argo.GetArgoWorkflow(workflowBuilder)
	if err != nil {
		return nil, err
	}

	if serviceAccount != "" {
		wf.Spec.ServiceAccountName = serviceAccount
	}

	return argo.SubmitArgoWorkflow(wf)
}

func setPreviousIdeStatusFromFlowRequest(msg *pubsubmessages.IDEFlowRequest) (*tracer.IDEInstance, error) {
	if ide, err := getIdeFromFlowRequest(msg); err != nil {
		return nil, err
	} else if prevStatus, perr := getPreviousStatus(msg); perr != nil {
		return nil, perr
	} else {
		setIdeStatus(ide, prevStatus)
		return ide, nil
	}
}

func updateActionHistoryWithEtag(msg *pubsubmessages.IDEFlowRequest) error {
	if ide, err := dependencycontainer.TracerDataServiceClient.GetIdeInstance(msg.IDEInstanceId); err != nil {
		return err
	} else {
		action := tracer.IDEActionHistory{
			Action: msg.Action,
			AuditInfo: contracts.Audit{
				AddedUser:    ide.AuditInfo.AddedUser,
				Added:        time.Now(),
				LastModified: time.Now(),
			},
		}
		return dependencycontainer.TracerDataServiceClient.UpdateIdeActionHistoryWithEtag(ide, action)
	}
}

func updateIdeFailureStatus(ide *tracer.IDEInstance, failureStatus, action string) {
	recentIde, err := dependencycontainer.TracerDataServiceClient.GetIdeInstance(ide.ID)
	if err != nil {
		log.Main.Error(err)
		return
	}
	recentIde.Status = failureStatus

	if eTagActions.Contains(action) {
		if err := dependencycontainer.TracerDataServiceClient.UpdateIdeInstanceWithETag(recentIde); err != nil {
			log.Main.Error(fmt.Sprintf("error updating IDE %v: %s", recentIde.ID, err.Error()))
		}
	} else if err := dependencycontainer.TracerDataServiceClient.UpdateIdeInstance(ide); err != nil {
		log.Main.Error(fmt.Sprintf("error updating IDE %v: %s", recentIde.ID, err.Error()))
	}
}

func updateIdeWorkflowHistory(ide *tracer.IDEInstance, wf *v1alpha1.Workflow) error {
	entryTemplate, err := utils.FirstBy(
		utils.MapToPointers(wf.Spec.Templates),
		func(template *v1alpha1.Template) bool { return template.Name == wf.Spec.Entrypoint },
	)

	if err != nil {
		return fmt.Errorf(`failed to find entry point template "%s" in workflow "%s"`, wf.Spec.Entrypoint, wf.Name)
	}

	templateOrder := make(map[string]int)
	for i := range entryTemplate.Steps {
		var parallelStep *v1alpha1.ParallelSteps = &entryTemplate.Steps[i]
		for j := range parallelStep.Steps {
			templateOrder[parallelStep.Steps[j].Template] = i
		}
	}

	argoNodeStatuses := utils.Filter(
		utils.MapToPointers(lo.Values(wf.Status.Nodes)),
		func(nodeStatus *v1alpha1.NodeStatus) bool { return nodeStatus.Type == v1alpha1.NodeTypePod },
	)

	slices.SortStableFunc(argoNodeStatuses, func(a, b *v1alpha1.NodeStatus) int {
		if c := cmp.Compare(templateOrder[a.TemplateName], templateOrder[b.TemplateName]); c != 0 {
			return c
		}

		ast, bst := a.StartedAt.Time, b.StartedAt.Time
		if az, bz := ast.IsZero(), bst.IsZero(); az && bz {
			return 0
		} else if az {
			return 1
		} else if bz {
			return -1
		}
		return ast.Compare(bst)
	})

	recentIde, err := appendToWorkflowHistory(ide, wf, argoNodeStatuses)
	if err != nil {
		return err
	}

	return dependencycontainer.TracerDataServiceClient.UpdateIdeInstanceWithETag(recentIde)
}
