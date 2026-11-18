package cron

import (
	"fmt"
	"strings"

	"github.com/aifimmunology/hise-data-contracts/clients"
	"github.com/aifimmunology/hise-data-contracts/clients/emailclient"
	"github.com/aifimmunology/hise-data-contracts/clients/tracerclient"
	emailmodel "github.com/aifimmunology/hise-data-contracts/contracts/email"
	"github.com/aifimmunology/hise-data-contracts/contracts/pubsubmessages"
	"github.com/aifimmunology/hise-data-contracts/contracts/tracer"
	"github.com/aifimmunology/ide-workflow-management/dependencycontainer"
	"github.com/aifimmunology/ide-workflow-management/env"
	"github.com/aifimmunology/ide-workflow-management/handler"
	"github.com/aifimmunology/shared-utility-library/argo"
	"github.com/aifimmunology/shared-utility-library/log"
)

type BadStateFixedCronJob struct{}

func (BadStateFixedCronJob) Expression() string {
	return env.BadStateFixedCronExpression()
}

func (BadStateFixedCronJob) Name() string {
	return "BadStateFixedCronJob"
}

func (cj BadStateFixedCronJob) Run() {
	runCronFunc(cj, badStateFixedCronTasks)
}

func badStateFixedCronTasks() (bool, error) {
	log.Main.Info("Starting IDE Bad State Fixed Cron Job")

	states := make([]map[string]any, len(badStates))
	for i, state := range badStates {
		states[i] = map[string]any{"status": state}
	}

	filter := map[string]any{"$or": states}
	ides, err := dependencycontainer.TracerDataServiceClient.GetIdeInstanceByFilter(clients.Query{Filter: filter})
	if err != nil {
		return false, err
	}

	// no need to run this cron job
	if len(ides) == 0 {
		return false, nil
	}

	podList := formatBadStateFixedArgoParam(ides)

	if fixedPods, err := runBadStateFixedCronArgo(podList); err != nil {
		return false, err
	} else if fixedPods == "" {
		return false, nil
	} else {
		fixedPods = strings.TrimSuffix(fixedPods, ",")
		podsArray := strings.SplitSeq(fixedPods, ",")
		for pod := range podsArray {
			// don't exit if one of the ides errors out, still handle the rest
			if ide, err := dependencycontainer.TracerDataServiceClient.GetIdeInstanceByPodName(pod); err != nil {
				log.Main.Error(fmt.Errorf("could not get ide instance by pod name %s: %s", pod, err.Error()))
			} else if _, err := handler.RunSetIamPolicyArgo(ide, &pubsubmessages.IDEFlowRequest{}); err != nil {
				log.Main.Error(
					fmt.Errorf("error running set iam policy argo for ide %s: %s", ide.ID.String(), err.Error()),
				)
			} else if err := dependencycontainer.TracerDataServiceClient.UpdateIdeInstanceStatus(ide, tracer.IDEStatusHistory{Status: tracerclient.StatusRunning, Detail: "Updated by cron job"}); err != nil {
				log.Main.Error(fmt.Errorf("could not update ide instance status to running: %s", err.Error()))
			} else if err := badStateFixedEmail(ide); err != nil {
				log.Main.Error(err)
			}
		}
		return true, nil
	}
}

func badStateFixedEmail(ide *tracer.IDEInstance) error {
	if err := handler.UpdateNotificationStatus(
		ide,
		emailmodel.NotificationIdeBadStateFixed,
		tracerclient.StatusRunning,
	); err != nil {
		return err
	}

	acct, err := dependencycontainer.AccountDataServiceClient.GetAccountByGuid(ide.AccountGUID)
	if err != nil {
		return err
	}

	dynamicData := map[string]any{"ide_name": ide.FriendlyName, "acct_name": acct.Name}

	req := emailmodel.EmailRequest{
		ToAddress:           ide.AuditInfo.AddedUser,
		CCAddress:           env.SupportEmail(),
		NotificationType:    emailmodel.NotificationIdeBadStateFixed,
		DynamicTemplateData: dynamicData,
	}

	if _, err := emailclient.SendEmailCustomRequest(&req); err != nil {
		log.Main.Error(fmt.Sprintf("Failed to send email notification: %s", err.Error()))
		return err
	}
	return nil
}

func formatBadStateFixedArgoParam(ides []tracer.IDEInstance) string {
	// make a string that looks like "pod1, pod2, pod3"
	var podListParam strings.Builder
	for i := range ides {
		fmt.Fprintf(&podListParam, "%s,", ides[i].PodName)
	}
	return strings.TrimSuffix(podListParam.String(), ",")
}

func runBadStateFixedCronArgo(podList string) (string, error) {
	wfBuilder := handler.InitWorkFlowBuilder("cron-bad-state-fixed.yaml")
	wfBuilder.SetOutputFlagTrue()
	wfBuilder.AddParam("podList", podList)

	if wf, err := handler.HandleArgoWorkflow(
		wfBuilder,
		nil,
		nil,
		"bad-state-fixed-cron-failure",
		env.IdeServiceAccount(),
	); err != nil {
		return "", err
	} else if outParams, err := argo.FindAndMonitorArgoWorkflow(wf); err != nil {
		return "", err
	} else {
		if pods, exists := outParams["running_pods"]; exists {
			return pods, nil
		} else {
			return "", fmt.Errorf("output params not returned from bad state fixed cron argo workflow")
		}
	}
}
