package cron

import (
	"context"
	"fmt"
	"time"

	"github.com/aifimmunology/hise-data-contracts/contracts/ledger"
	"github.com/aifimmunology/ide-workflow-management/dependencycontainer"
	"github.com/aifimmunology/shared-utility-library/cron"
	"github.com/aifimmunology/shared-utility-library/log"
)

func cronContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func runCronFunc(cronJob cron.CronJob, run func() (bool, error)) {
	log.Main.Info(fmt.Sprintf("Starting cron %s", cronJob.Name()))
	ran, err := run()
	if err != nil {
		log.Main.Error(fmt.Errorf("Cron Job %s Failed: %s", cronJob.Name(), err.Error()))
	}

	if ran {
		if err = dependencycontainer.LedgerDataServiceClient.CreateCron(
			&ledger.Cron{CronType: cronJob.Name(), Timestamp: time.Now()},
		); err != nil {
			log.Main.Error(err)
		}
	}
}
