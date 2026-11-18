package cron

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/aifimmunology/hise-data-contracts/clients/emailclient"
	"github.com/aifimmunology/hise-data-contracts/clients/tracerclient"
	emailmodel "github.com/aifimmunology/hise-data-contracts/contracts/email"
	"github.com/aifimmunology/hise-data-contracts/contracts/tracer"
	"github.com/aifimmunology/hise-data-contracts/core"
	"github.com/aifimmunology/ide-workflow-management/dependencycontainer"
	"github.com/aifimmunology/ide-workflow-management/env"
	"github.com/aifimmunology/ide-workflow-management/handler"
	"github.com/aifimmunology/shared-utility-library/argo"
	"github.com/aifimmunology/shared-utility-library/log"
)

const TimeRemainingKey = "TempUserDiskTimeRemaining"

type TempUserDiskCronJob struct{}

func (TempUserDiskCronJob) Expression() string {
	return env.TempUserDiskCronExpression()
}

func (TempUserDiskCronJob) Name() string {
	return "TempUserDiskCronJob"
}

func (cj TempUserDiskCronJob) Run() {
	runCronFunc(cj, RunTempUserDiskCronJob)
}

func RunTempUserDiskCronJob() (bool, error) {
	expandedDisks, err := runTempUserDiskArgo()
	if err != nil {
		log.Main.Error(fmt.Errorf("Temp User Disk Cron: error during argo: %s", err.Error()))
		return false, err
	}

	if expandedDisks == "" {
		log.Main.Info("Temp User Disk Cron: no expanded user disks found, skipping.")
		return false, nil
	}

	log.Main.Info(fmt.Sprintf("Temp User Disk Cron: found expanded user disks: %s", expandedDisks))
	return handleDisksApproachingOrReadyForRevert(expandedDisks), nil
}

func approachingTimeout(dateExpanded time.Time, timeoutDays int) (bool, int) {
	for i := 1; i <= 5; i++ {
		if timeoutDays > i && happenedNumDaysAgo(dateExpanded, timeoutDays-i) {
			return true, i
		}
	}
	return false, -1
}

func convertDaysInterface(days any) (int, error) {
	switch val := days.(type) {
	case int:
		return val, nil
	case float64:
		return int(val), nil
	case string:
		return strconv.Atoi(val)
	default:
		return 0, fmt.Errorf("unsupported days val type %T", days)
	}
}

func diskApproachingTimeoutCronTasks(ide *tracer.IDEInstance, dateExpanded time.Time, timeoutDays int) bool {
	approachingTimeout, daysRemaining := approachingTimeout(dateExpanded, timeoutDays)

	if approachingTimeout {
		log.Main.Info(
			fmt.Sprintf("Temp User Disk Cron: ide %s is approaching timeout, sending warning email", ide.ID.String()),
		)

		if err := updateTempDiskDaysLeft(ide, daysRemaining); err != nil {
			log.Main.Error(
				fmt.Errorf(
					"Temp User Disk Cron: error updating ide %s with days remaining: %s",
					ide.ID.String(),
					err.Error(),
				),
			)
		}

		err := sendUserDiskRevertWarningEmail(ide, timeoutDays, dateExpanded, ide.MachineConfig.Disk.RequestSize)
		if err != nil {
			log.Main.Error(
				fmt.Errorf(
					"Temp User Disk Cron: error sending warning email for ide %s: %s",
					ide.ID.String(),
					err.Error(),
				),
			)
			return false
		}
		return true
	}

	return false
}

func diskRevertCronTasks(ide *tracer.IDEInstance, dateExpanded time.Time, timeoutDays, index int) bool {
	if happenedNumDaysAgo(dateExpanded, timeoutDays) {
		log.Main.Info(
			fmt.Sprintf(
				"Temp User Disk Cron: ide %d passed timeout of %s, triggering disk size revert",
				timeoutDays,
				ide.ID.String(),
			),
		)

		ide.ActionHistory[index].Action = "temp-scale-reverted"
		ide.ActionHistory[index].AuditInfo.LastModified = time.Now().UTC()
		delete(ide.ExecutionLog, TimeRemainingKey)
		if err := dependencycontainer.TracerDataServiceClient.UpdateIdeInstance(ide); err != nil {
			log.Main.Error(fmt.Errorf("Temp User Disk Cron: error updating ide %s: %s", ide.ID.String(), err.Error()))
		}

		if rerr := triggerRevertUserDisk(ide); rerr != nil {
			log.Main.Error(
				fmt.Errorf(
					"Temp User Disk Cron: error triggering revert for ide %s: %s",
					ide.ID.String(),
					rerr.Error(),
				),
			)
			return false
		} else if err := sendUserDiskRevertedEmail(ide, ide.MachineConfig.Disk.RequestSize, timeoutDays); err != nil {
			log.Main.Error(
				fmt.Errorf(
					"Temp User Disk Cron: error sending user disk reverted email for ide %s: %s",
					ide.ID.String(),
					err.Error(),
				),
			)
			return false
		} else {
			return true
		}
	}

	return false
}

func handleDisksApproachingOrReadyForRevert(disks string) bool {
	notificationsSent := 0

	diskList := strings.Split(disks, ";")
	for _, disk := range diskList {
		podName := strings.TrimSuffix(disk, "-userdisk")
		ide, err := dependencycontainer.TracerDataServiceClient.GetIdeInstanceByPodName(podName)
		if err != nil {
			log.Main.Error(
				fmt.Errorf("Temp User Disk Cron: error getting ide by pod name %s: %s", podName, err.Error()),
			)
			continue
		}

		log.Main.Info(fmt.Sprintf("Temp User Disk Cron: checking action history of ide %s", ide.ID.String()))

		// check for the temp scale entry and handle related tasks based on its info
		for index, history := range ide.ActionHistory {
			if history.Action == tracerclient.ActionTempScalePVCStorage {
				if days, exists := history.ResourceSpec[tracerclient.TempAllocationDaysKey]; exists {
					daysAllocated, cerr := convertDaysInterface(days)
					if cerr != nil {
						log.Main.Error(fmt.Errorf("Temp User Disk Cron: %s", cerr.Error()))
						continue
					}

					if diskRevertCronTasks(ide, history.AuditInfo.Added, daysAllocated, index) {
						notificationsSent++
						continue
					}

					if diskApproachingTimeoutCronTasks(ide, history.AuditInfo.Added, daysAllocated) {
						notificationsSent++
					}
				}
			}
		}
	}

	return notificationsSent > 0
}

func runTempUserDiskArgo() (string, error) {
	wfBuilder := handler.InitWorkFlowBuilder("cron-temp-user-disk.yaml")
	wfBuilder.AddParam("defaultSizeGb", strings.TrimSuffix(env.UserDiskSize(), "Gi"))
	wfBuilder.SetOutputFlagTrue()

	if wf, err := handler.HandleArgoWorkflow(
		wfBuilder,
		nil,
		nil,
		"cron-temp-user-disk-failure",
		env.IdeServiceAccount(),
	); err != nil {
		return "", err
	} else if outParams, err := argo.FindAndMonitorArgoWorkflow(wf); err != nil {
		return "", err
	} else {
		if disks, exists := outParams["user_disks"]; exists {
			return disks, nil
		} else {
			return "", fmt.Errorf("user_disks output param not found")
		}
	}
}

func sendUserDiskRevertWarningEmail(
	ide *tracer.IDEInstance,
	allocatedDays int,
	dateExpanded time.Time,
	curSize string,
) error {
	acct, err := dependencycontainer.AccountDataServiceClient.GetAccountByGuid(ide.AccountGUID)
	if err != nil {
		return err
	}

	revertDate := dateExpanded.Add(time.Duration(allocatedDays) * 24 * time.Hour)

	dynamicData := map[string]any{
		"curSize":       curSize,
		"ideName":       ide.FriendlyName,
		"account":       acct.Name,
		"allocatedDays": allocatedDays,
		"originalSize":  env.UserDiskSize(),
		"revertDate":    revertDate.Format("January 2, 2006"),
		"time":          "11pm",
	}

	if _, err := emailclient.SendEmail(
		ide.AuditInfo.AddedUser,
		emailmodel.NotificationTempDiskExpansionTimeoutApproaching,
		nil,
		nil,
		dynamicData,
	); err != nil {
		return err
	}
	return nil
}

func sendUserDiskRevertedEmail(ide *tracer.IDEInstance, curDiskSize string, allocatedDays int) error {
	acct, err := dependencycontainer.AccountDataServiceClient.GetAccountByGuid(ide.AccountGUID)
	if err != nil {
		return err
	}

	dynamicData := map[string]any{
		"curSize":       curDiskSize,
		"ideName":       ide.FriendlyName,
		"account":       acct.Name,
		"allocatedDays": allocatedDays,
		"originalSize":  env.UserDiskSize(),
	}

	if _, err := emailclient.SendEmail(
		ide.AuditInfo.AddedUser,
		emailmodel.NotificationTempDiskSizeReverted,
		nil,
		nil,
		dynamicData,
	); err != nil {
		return err
	}
	return nil
}

func triggerRevertUserDisk(ide *tracer.IDEInstance) error {
	req := core.HISERequest{Path: fmt.Sprintf("pvc/revert/userdisk/%s", ide.ID.String()), Method: http.MethodPost}
	return core.ServiceClientRequest(dependencycontainer.IdeManagementServiceClient, &req)
}

func updateTempDiskDaysLeft(ide *tracer.IDEInstance, daysRemaining int) error {
	if ide.ExecutionLog == nil {
		ide.ExecutionLog = make(map[string]string)
	}
	ide.ExecutionLog[TimeRemainingKey] = fmt.Sprintf("%d days", daysRemaining)

	return dependencycontainer.TracerDataServiceClient.UpdateIdeInstance(ide)
}
