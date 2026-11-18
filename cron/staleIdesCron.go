package cron

import (
	"fmt"
	"time"

	"github.com/aifimmunology/hise-data-contracts/clients"
	"github.com/aifimmunology/hise-data-contracts/clients/emailclient"
	"github.com/aifimmunology/hise-data-contracts/clients/tracerclient"
	"github.com/aifimmunology/hise-data-contracts/contracts"
	emailmodel "github.com/aifimmunology/hise-data-contracts/contracts/email"
	"github.com/aifimmunology/hise-data-contracts/contracts/tracer"
	"github.com/aifimmunology/ide-workflow-management/dependencycontainer"
	"github.com/aifimmunology/ide-workflow-management/env"
	"github.com/aifimmunology/ide-workflow-management/handler"
	"github.com/aifimmunology/shared-utility-library/log"
)

const (
	StaleIdeDeletion = "StaleIdeDeletion"
	StaleIdeWarning  = "StaleIdeWarning"
)

type StaleIDEsCronJob struct{}

func (StaleIDEsCronJob) Expression() string {
	return env.StaleIDEsCronExpression()
}

func (StaleIDEsCronJob) Name() string {
	return "StaleIDEsCronJob"
}

func (cj StaleIDEsCronJob) Run() {
	runCronFunc(cj, RunStaleIdesCronJob)
}

func RunStaleIdesCronJob() (bool, error) {
	almostStaleIdes, staleIdes, err := findStaleIDEs()
	if err != nil {
		log.Main.Error(fmt.Errorf("StaleIDEsCronJob: error finding IDEs: %s", err.Error()))
		return false, err
	}

	sendStaleIdeWarningEmail(almostStaleIdes)
	softDeleteStaleIdes(staleIdes)
	return true, nil
}

func findStaleIDEs() ([]*tracer.IDEInstance, []*tracer.IDEInstance, error) {
	filter := map[string]any{"status": tracerclient.StatusStopped}

	ides, err := dependencycontainer.TracerDataServiceClient.GetIdeInstanceByFilter(clients.Query{Filter: filter})
	if err != nil {
		return nil, nil, err
	}

	almostStaleIdes := make([]*tracer.IDEInstance, 0, len(ides))
	staleIdes := make([]*tracer.IDEInstance, 0, len(ides))

	for i := range ides {
		ide := &ides[i]
		if len(ide.ActionHistory) > 0 {
			lastAction := ide.ActionHistory[len(ide.ActionHistory)-1]

			if lastAction.Action == tracerclient.ActionStop {
				if lastAction.AuditInfo.LastModified.IsZero() {
					log.Main.Info(
						fmt.Sprintf(
							"StaleIDEsCronJob: IDE %s has no lastModified time for stop action, skipping",
							ide.ID.String(),
						),
					)
					continue // skip edge cases where time was not set
				}
				if lastAction.AuditInfo.LastModified.Before(time.Now().AddDate(0, 0, -90)) {
					staleIdes = append(staleIdes, ide)
				} else if isAlmostStale(lastAction.AuditInfo.LastModified) {
					almostStaleIdes = append(almostStaleIdes, ide)
				}
			}
		}
	}

	log.Main.Info(
		fmt.Sprintf(
			"StaleIDEsCronJob: found %d almost stale IDEs and %d stale IDEs",
			len(almostStaleIdes),
			len(staleIdes),
		),
	)

	return almostStaleIdes, staleIdes, nil
}

func happenedNumDaysAgo(curTime time.Time, numDaysAgo int) bool {
	numDays := time.Now().AddDate(0, 0, -numDaysAgo)
	return curTime.Year() == numDays.Year() && curTime.Month() == numDays.Month() && curTime.Day() == numDays.Day()
}

func isAlmostStale(lastModified time.Time) bool {
	return happenedNumDaysAgo(lastModified, 30) || happenedNumDaysAgo(lastModified, 60) ||
		happenedNumDaysAgo(lastModified, 76) ||
		happenedNumDaysAgo(lastModified, 83) ||
		happenedNumDaysAgo(lastModified, 89)
}

func sendStaleIdeDeletionEmail(ide *tracer.IDEInstance) error {
	acct, err := dependencycontainer.AccountDataServiceClient.GetAccountByGuid(ide.AccountGUID)
	if err != nil {
		return err
	}

	dynamicData := map[string]any{"ide_name": ide.FriendlyName, "acct_name": acct.Name}
	if _, err := emailclient.SendEmail(
		ide.AuditInfo.AddedUser,
		emailmodel.NotificationStaleIdeDeletion,
		nil,
		nil,
		dynamicData,
	); err != nil {
		return err
	}
	return nil
}

func sendStaleIdeWarningEmail(almostStaleIdes []*tracer.IDEInstance) {
	if len(almostStaleIdes) == 0 {
		return
	}

	for _, ide := range almostStaleIdes {
		acct, err := dependencycontainer.AccountDataServiceClient.GetAccountByGuid(ide.AccountGUID)
		if err != nil {
			log.Main.Error(
				fmt.Sprintf("StaleIDEsCronJob: error getting account for IDE %s: %s", ide.ID.String(), err.Error()),
			)
			continue
		}

		lastAction := ide.ActionHistory[len(ide.ActionHistory)-1]
		timePassed := time.Since(lastAction.AuditInfo.LastModified)
		daysPassed := int(timePassed.Hours() / 24)

		if _, err := emailclient.SendEmail(
			ide.AuditInfo.AddedUser,
			emailmodel.NotificationStaleIdeDeletionWarning,
			nil,
			nil,
			map[string]any{"ide_name": ide.FriendlyName, "acct_name": acct.Name, "num_days": daysPassed},
		); err != nil {
			log.Main.Error(
				fmt.Sprintf(
					"StaleIDEsCronJob: error sending warning email for IDE %s: %s",
					ide.ID.String(),
					err.Error(),
				),
			)
		} else if err := handler.UpdateNotificationStatus(ide, StaleIdeWarning, ide.Status); err != nil {
			log.Main.Error(
				fmt.Sprintf(
					"StaleIDEsCronJob: error updating status for almost stale IDE %s: %s",
					ide.ID.String(),
					err.Error(),
				),
			)
		} else {
			log.Main.Info(
				fmt.Sprintf(
					"StaleIDEsCronJob: notified %s almost stale IDE %s",
					ide.AuditInfo.AddedUser,
					ide.ID.String(),
				),
			)
		}
	}
}

func softDeleteStaleIdes(staleIdes []*tracer.IDEInstance) {
	if len(staleIdes) == 0 {
		return
	}

	for _, ide := range staleIdes {
		log.Main.Info(fmt.Sprintf("StaleIDEsCronJob: deleting stale IDE %s", ide.ID.String()))
		if err := triggerSoftDeleteIde(ide); err != nil {
			log.Main.Error(
				fmt.Sprintf("StaleIDEsCronJob: error soft deleting stale IDE %s: %s", ide.ID.String(), err.Error()),
			)
		} else if err := sendStaleIdeDeletionEmail(ide); err != nil {
			log.Main.Error(
				fmt.Sprintf(
					"StaleIDEsCronJob: error sending stale IDE deletion email for %s: %s",
					ide.ID.String(),
					err.Error(),
				),
			)
		} else if err := handler.UpdateNotificationStatus(ide, StaleIdeDeletion, tracerclient.StatusDeleted); err != nil {
			log.Main.Error(
				fmt.Sprintf(
					"StaleIDEsCronJob: error updating status for stale IDE %s: %s",
					ide.ID.String(),
					err.Error(),
				),
			)
		}
	}
}

func triggerSoftDeleteIde(ide *tracer.IDEInstance) error {
	ide.IsDeleted = contracts.ToSBool(true)

	newStatus := tracer.IDEStatusHistory{
		Status:    tracerclient.StatusSoftDeleted,
		Detail:    "Soft deleted by StaleIDEsCronJob",
		UpdatedAt: time.Now(),
		UpdatedBy: "ide-workflow-management",
	}

	return dependencycontainer.TracerDataServiceClient.UpdateIdeInstanceStatus(ide, newStatus)
}
