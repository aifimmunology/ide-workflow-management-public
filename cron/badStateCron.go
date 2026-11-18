package cron

import (
	"fmt"
	"slices"
	"strings"

	"github.com/aifimmunology/hise-data-contracts/clients/tracerclient"
	"github.com/aifimmunology/hise-data-contracts/contracts/pubsubmessages"
	"github.com/aifimmunology/ide-workflow-management/dependencycontainer"
	"github.com/aifimmunology/ide-workflow-management/env"
	"github.com/aifimmunology/ide-workflow-management/handler"
	"github.com/aifimmunology/shared-utility-library/argo"
	"github.com/aifimmunology/shared-utility-library/log"

	"github.com/google/uuid"
)

const PodStatusCompleted = "Completed"

var badStates = []string{
	tracerclient.StatusClusterResourceLimitReached,
	tracerclient.StatusDockerImagePullError,
	tracerclient.StatusGoogleResourceStockout,
	tracerclient.StatusGoogleQuotaExceeded,
	tracerclient.StatusInstallSDKFailure,
	tracerclient.StatusJupyterTimeoutFailure,
	tracerclient.StatusNodesNotAvailable,
	tracerclient.StatusPackProvisioningFailure,
	tracerclient.StatusPodInBadState,
	tracerclient.StatusPvcNotFound,
	tracerclient.StatusRevertUserDiskFailure,
	tracerclient.StatusScalingFailure,
	tracerclient.StatusScalingPVCStorageFailure,
	tracerclient.StatusStartingFailure,
	tracerclient.StatusStartupScriptFailure,
	tracerclient.StatusStoppingFailure,
	tracerclient.StatusStuckPending,
	tracerclient.StatusTemporarilyUnavailable,
}

type BadStateCronJob struct{}

func (BadStateCronJob) Expression() string {
	return env.BadStateCronExpression()
}

func (BadStateCronJob) Name() string {
	return "BadStateCronJob"
}

func (cj BadStateCronJob) Run() {
	runCronFunc(cj, RunBadStateCronJob)
}

func RunBadStateCronJob() (bool, error) {
	if badPods, err := runBadStateCronArgo(); err != nil {
		return false, err
	} else if badPods != "" {
		ideList := createIdesInBadStateMap(badPods)

		for ideId := range ideList {
			log.Main.Info(fmt.Sprintf("Bad State Cron: processing IDE %s", ideId))
			if ideGuid, err := uuid.Parse(ideId); err != nil {
				log.Main.Error(err)
			} else if ide, err := dependencycontainer.TracerDataServiceClient.GetIdeInstance(ideGuid); err != nil {
				log.Main.Error(err)
			} else if err = handler.RunDescribeK8sPodArgo(nil, &pubsubmessages.IDEFlowRequest{}, ide, tracerclient.StatusTemporarilyUnavailable); err != nil {
				log.Main.Error(fmt.Errorf("could not run describe pod argo for ide %s: %s", ideId, err.Error()))
			}
		}
		return true, nil
	}
	return false, nil
}

func badStateAlreadyCaught(state string) bool {
	return slices.Contains(badStates, state)
}

func createIdesInBadStateMap(pods string) map[string]string {
	ideMap := make(map[string]string)

	podInfo := strings.Split(pods, ";")
	for _, pod := range podInfo {
		if pod != "" {
			splitInfo := strings.Split(pod, ",")
			if len(splitInfo) == 2 {
				podName := splitInfo[0]
				ide, err := dependencycontainer.TracerDataServiceClient.GetIdeInstanceByPodName(podName)
				if err != nil {
					log.Main.Error(fmt.Errorf("could not get ide instance by pod name %s: %s", podName, err.Error()))
					continue
				}

				if badStateAlreadyCaught(ide.Status) {
					continue
				}

				podState := splitInfo[1]
				if podState != PodStatusCompleted { // this state is caused by idle timeouts, handled by a different cron
					ideMap[ide.ID.String()] = podState
				}
			}
		}
	}
	return ideMap // map[ideId] = podState
}

func runBadStateCronArgo() (string, error) {
	wfBuilder := handler.InitWorkFlowBuilder("cron-bad-state.yaml")
	wfBuilder.SetOutputFlagTrue()

	if wf, err := handler.HandleArgoWorkflow(
		wfBuilder,
		nil,
		nil,
		"bad-state-cron-failure",
		env.IdeServiceAccount(),
	); err != nil {
		return "", err
	} else if outParams, err := argo.FindAndMonitorArgoWorkflow(wf); err != nil {
		return "", err
	} else {
		if pods, exists := outParams["bad-pods"]; exists {
			return pods, nil
		} else {
			return "", fmt.Errorf("output params not returned from bad state cron argo workflow")
		}
	}
}
