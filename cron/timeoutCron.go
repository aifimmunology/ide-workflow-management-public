package cron

import (
	"fmt"
	"strings"

	"github.com/aifimmunology/hise-data-contracts/clients/emailclient"
	"github.com/aifimmunology/hise-data-contracts/clients/tracerclient"
	emailmodel "github.com/aifimmunology/hise-data-contracts/contracts/email"
	"github.com/aifimmunology/hise-data-contracts/contracts/tracer"
	"github.com/aifimmunology/ide-workflow-management/dependencycontainer"
	"github.com/aifimmunology/ide-workflow-management/env"
	"github.com/aifimmunology/ide-workflow-management/handler"
	"github.com/aifimmunology/shared-utility-library/argo"
	"github.com/aifimmunology/shared-utility-library/log"
)

type TimeoutCronJob struct{}

func (TimeoutCronJob) Expression() string {
	return env.TimeoutCronExpression()
}

func (TimeoutCronJob) Name() string {
	return "TimeoutCronJob"
}

func (cj TimeoutCronJob) Run() {
	runCronFunc(cj, RunTimeoutCronJob)
}

func RunTimeoutCronJob() (bool, error) {
	if podList, err := runCronArgo("cron-timeout.yaml", "timeout-cron-failure", "pods"); err != nil {
		return false, err
	} else if len(podList) > 0 {
		timeoutCronTasks(podList)
		return true, nil
	}
	return false, nil
}

func convertSecondsToTimeStr(seconds int) string {
	hours := seconds / 3600
	minutes := (seconds % 3600) / 60
	if hours == 0 {
		return fmt.Sprintf("%d minutes", minutes)
	} else {
		return fmt.Sprintf("%d hours %d minutes", hours, minutes)
	}
}

func cronTimeoutEmail(ide *tracer.IDEInstance) error {
	if err := handler.UpdateNotificationStatus(
		ide,
		emailmodel.NotificationIdleIdeStopped,
		tracerclient.StatusStopped,
	); err != nil {
		return err
	}

	acct, err := dependencycontainer.AccountDataServiceClient.GetAccountByGuid(ide.AccountGUID)
	if err != nil {
		return err
	}

	dynamicData := map[string]any{
		"ide_name":  ide.FriendlyName,
		"acct_name": acct.Name,
		"time":      convertSecondsToTimeStr(ide.CullIdleTimeout),
	}

	if _, err := emailclient.SendEmail(
		ide.AuditInfo.AddedUser,
		emailmodel.NotificationIdleIdeStopped,
		nil,
		nil,
		dynamicData,
	); err != nil {
		return err
	}
	return nil
}

func runCronArgo(argoYaml, failureStatus, outputParam string) (string, error) {
	wfBuilder := handler.InitWorkFlowBuilder(argoYaml)
	wfBuilder.SetOutputFlagTrue()

	if wf, err := handler.HandleArgoWorkflow(wfBuilder, nil, nil, failureStatus, env.IdeServiceAccount()); err != nil {
		return "", err
	} else if outParams, err := argo.FindAndMonitorArgoWorkflow(wf); err != nil {
		return "", err
	} else {
		if pods, exists := outParams[outputParam]; exists {
			return pods, nil
		} else {
			return "", fmt.Errorf("output params not returned from %s argo workflow", argoYaml)
		}
	}
}

func timeoutCronTasks(podList string) {
	podList = strings.TrimSuffix(podList, ";")
	pods := strings.SplitSeq(podList, ";")
	for podName := range pods {
		ide, err := dependencycontainer.TracerDataServiceClient.GetIdeInstanceByPodName(podName)
		if err != nil {
			log.Main.Error(
				fmt.Errorf("ide timeout cron: could not get ide instance by pod name %s: %s", podName, err.Error()),
			)
			continue
		}

		if ide.Status == tracerclient.StatusRunning {
			if err := handler.TriggerStopIde(ide.ID); err != nil {
				log.Main.Error(
					fmt.Errorf("ide timeout cron: could not stop ide instance %s: %s", ide.ID.String(), err.Error()),
				)
				continue
			} else if err := cronTimeoutEmail(ide); err != nil {
				log.Main.Error(
					fmt.Errorf(
						"ide timeout cron: could not send timeout email for ide instance %s: %s",
						ide.ID.String(),
						err.Error(),
					),
				)
			}
		}
	}
}
