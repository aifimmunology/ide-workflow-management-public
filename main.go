package main

import (
	"context"
	"fmt"
	"time"

	ideCron "github.com/aifimmunology/ide-workflow-management/cron"
	"github.com/aifimmunology/ide-workflow-management/dependencycontainer"
	"github.com/aifimmunology/ide-workflow-management/pubsub"
	"github.com/aifimmunology/shared-utility-library/cron"
	googledeps "github.com/aifimmunology/shared-utility-library/dependencycontainer"
	"github.com/aifimmunology/shared-utility-library/log"
	spubsub "github.com/aifimmunology/shared-utility-library/pubsub"
	"github.com/aifimmunology/shared-utility-library/signal"
)

func cronJobs(ctx context.Context) []cron.CronJob {
	return []cron.CronJob{
		ideCron.BadStateCronJob{},
		ideCron.BadStateFixedCronJob{},
		ideCron.MonthlyQuotaCronJob{},
		ideCron.PackageVulnerabilitiesCron{Ctx: ctx},
		ideCron.PermanentDeleteIdesCron{},
		ideCron.StaleIDEsCronJob{},
		ideCron.StaleVolumesCronJob{Ctx: ctx},
		ideCron.TempUserDiskCronJob{},
		ideCron.TimeoutCronJob{},
	}
}

func main() {
	start := time.Now()
	ctx := context.Background()

	googledeps.InitializeServiceDependencies()
	dependencycontainer.InitDependencies(ctx)
	defer dependencycontainer.Close()
	defer googledeps.Close()

	for _, cronJob := range cronJobs(ctx) {
		cron.RegisterCronJob(cronJob)
	}

	cron.Init()

	signal.Register("pubsub", pubsub.PubsubCallback)
	signal.Register("processors", func() {
		bench := time.Now()
		log.Main.Info(
			"IDE Workflow Management handler done processing, allowing exit",
			log.D{"elapsed": fmt.Sprintf("%.2f", time.Since(bench).Seconds())},
		)
	})

	listenerErrs := make(chan error)
	spubsub.InitPubsubListeners(
		listenerErrs,
		pubsub.CompletedWorkflowListener,
		pubsub.CondaPackBuildListener,
		pubsub.IdeQuotaReachedListener,
		pubsub.QueueListener,
		pubsub.TrainingImageArtifactsInitiatedListener,
	)

	ticker := time.NewTicker(time.Minute).C
	for signal.Run() {
		select {
		case err := <-listenerErrs:
			if err != nil {
				panic(fmt.Errorf("creating listeners failed: %s", err.Error()))
			}
		case <-ticker:
			log.Main.Info(
				"IDE Workflow Management service heartbeat",
				log.D{
					"uptime":          fmt.Sprintf("%.2f", time.Since(start).Seconds()),
					"messages length": fmt.Sprintf("%d", pubsub.QueueLength()),
				},
			)
		default:
		}
	}

	log.Main.Info(
		"IDE Workflow Management exiting",
		log.D{"runtime": fmt.Sprintf("%.2f", time.Since(start).Seconds())},
	)
}
