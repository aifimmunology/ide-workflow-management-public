package cron

import (
	"fmt"
	"net/http"
	"time"

	"github.com/aifimmunology/hise-data-contracts/clients"
	"github.com/aifimmunology/hise-data-contracts/clients/tracerclient"
	"github.com/aifimmunology/hise-data-contracts/contracts/tracer"
	"github.com/aifimmunology/hise-data-contracts/core"
	"github.com/aifimmunology/ide-workflow-management/dependencycontainer"
	"github.com/aifimmunology/ide-workflow-management/env"
	"github.com/aifimmunology/shared-utility-library/log"
)

type PermanentDeleteIdesCron struct{}

func (PermanentDeleteIdesCron) Expression() string {
	return env.PermanentDeleteIdesCronExpression()
}

func (PermanentDeleteIdesCron) Name() string {
	return "PermanentDeleteIDEsCronJob"
}

func (cj PermanentDeleteIdesCron) Run() {
	runCronFunc(cj, RunPermanentDeleteIdesCron)
}

func RunPermanentDeleteIdesCron() (bool, error) {
	ides, err := dependencycontainer.TracerDataServiceClient.GetIdeInstanceByFilter(
		clients.Query{Filter: map[string]any{"status": tracerclient.StatusSoftDeleted}},
	)
	if err != nil {
		return false, err
	} else if len(ides) == 0 {
		log.Main.Info("PermanentDeleteIdesCron: No soft deleted IDEs found for permanent deletion")
		return false, nil
	}

	log.Main.Info(
		fmt.Sprintf("PermanentDeleteIdesCron: found %d soft deleted IDEs ready for permanent deletion", len(ides)),
	)

	// if has been soft deleted for more than 30 days, trigger permanent delete
	handleSoftDeletedIdes(ides)
	return true, nil
}

func deleteIfOlderThan30Days(ide *tracer.IDEInstance, lastStatusEntry tracer.IDEStatusHistory) {
	if lastStatusEntry.UpdatedAt.Before(time.Now().AddDate(0, 0, -30)) {
		if err := triggerDeleteIde(ide); err != nil {
			log.Main.Error(
				fmt.Errorf(
					"PermanentDeleteIdesCron: failed to delete IDE %s in account %s: %s",
					ide.ID.String(),
					ide.AccountGUID.String(),
					err.Error(),
				),
			)
		} else {
			log.Main.Info(
				fmt.Sprintf(
					"PermanentDeleteIdesCron: triggered permanent deletion of IDE %s in account %s",
					ide.ID.String(),
					ide.AccountGUID.String(),
				),
			)
		}
	}
}

func handleSoftDeletedIdes(ides []tracer.IDEInstance) {
	for i := range ides {
		ide := &ides[i]
		if len(ide.StatusHistory) == 0 {
			continue
		}

		lastStatusEntry := ide.StatusHistory[len(ide.StatusHistory)-1]
		if lastStatusEntry.Status == tracerclient.StatusSoftDeleted {
			deleteIfOlderThan30Days(ide, lastStatusEntry)
		}
	}
}

func triggerDeleteIde(ide *tracer.IDEInstance) error {
	return core.ServiceClientRequest(
		dependencycontainer.IdeManagementServiceClient,
		&core.HISERequest{Path: fmt.Sprintf("instances/%s", ide.ID.String()), Method: http.MethodDelete},
	)
}
