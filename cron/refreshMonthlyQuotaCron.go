package cron

import (
	"fmt"

	"github.com/aifimmunology/hise-data-contracts/clients"
	"github.com/aifimmunology/hise-data-contracts/clients/tracerclient"
	"github.com/aifimmunology/ide-workflow-management/dependencycontainer"
	"github.com/aifimmunology/ide-workflow-management/env"
	"github.com/aifimmunology/shared-utility-library/log"
)

type MonthlyQuotaCronJob struct{}

func (MonthlyQuotaCronJob) Expression() string {
	return env.MonthlyQuotaCronExpression()
}

func (MonthlyQuotaCronJob) Name() string {
	return "MonthlyQuotaCronJob"
}

func (cj MonthlyQuotaCronJob) Run() {
	runCronFunc(cj, MonthlyQuotaCronTasks)
}

func MonthlyQuotaCronTasks() (bool, error) {
	ides, err := dependencycontainer.TracerDataServiceClient.GetIdeInstanceByFilter(
		clients.Query{Filter: map[string]any{"status": tracerclient.StatusStoppedQuotaReached}},
	)

	if err != nil {
		return false, err
	}

	// no need to run this cron job
	if len(ides) == 0 {
		return false, nil
	}

	for i := range ides {
		ide := &ides[i]
		ide.Status = tracerclient.StatusStopped
		if err := dependencycontainer.TracerDataServiceClient.UpdateIdeInstance(ide); err != nil {
			log.Main.Error(
				fmt.Errorf("IDE Monthly Quota Cron: failed to update status of %s: %s", ide.ID.String(), err.Error()),
			)
		} else {
			log.Main.Info(
				fmt.Sprintf(
					"IDE Monthly Quota Cron: updated status of %s to %s",
					ide.ID.String(),
					tracerclient.StatusStopped,
				),
			)
		}
	}
	return true, nil
}
