package pubsub

import (
	"context"
	"time"

	"github.com/aifimmunology/hise-data-contracts/contracts"
	"github.com/aifimmunology/ide-workflow-management/env"
	"github.com/aifimmunology/ide-workflow-management/process"
	"github.com/aifimmunology/shared-utility-library/log"
	spubsub "github.com/aifimmunology/shared-utility-library/pubsub"

	"cloud.google.com/go/pubsub/v2"
)

const (
	defaultListener string = "ide-workflow-management"
	maxBackOff      int    = 5 * 60
)

var (
	CompletedWorkflowListener spubsub.PubsubListener = spubsub.PubsubListener{
		Executor:     messageExecutor,
		ListenerName: defaultListener,
		Topic:        env.CompletedWorkflowsTopic(),
	}
	CondaPackBuildListener spubsub.PubsubListener = spubsub.PubsubListener{
		Executor:     messageExecutor,
		ListenerName: defaultListener,
		Topic:        contracts.PubsubCondaPackBuildTopic,
	}
	IdeQuotaReachedListener spubsub.PubsubListener = spubsub.PubsubListener{
		Executor:     messageExecutor,
		ListenerName: defaultListener,
		Topic:        contracts.PubsubIdeQuotaReachedTopic,
	}
	QueueListener spubsub.PubsubListener = spubsub.PubsubListener{
		Executor:     messageExecutor,
		ListenerName: defaultListener,
		Topic:        contracts.PubsubIdeFlowRequestTopic,
	}
	TrainingImageArtifactsInitiatedListener spubsub.PubsubListener = spubsub.PubsubListener{
		Executor:     messageExecutor,
		ListenerName: defaultListener,
		Topic:        contracts.PubsubTrainingImageArtifactsInitiatedTopic,
	}

	backoffNum         int  = 1
	processNewMessages bool = true
)

func PubsubCallback() {
	log.Main.Info("SIGTERM callback received, stopping message processing.")
	processNewMessages = false
}

func messageExecutor(c context.Context, m *pubsub.Message) {
	if !processNewMessages {
		log.Main.Info("SIGTERM present. Automatically nacking message.", log.D{"id": m.ID})
		time.Sleep(time.Duration(backoffNum) * time.Second)
		if backoffNum < maxBackOff {
			backoffNum *= 2
		}
		m.Nack()
		return
	}

	go func() {
		if err := writeWorkerLog(m); err != nil {
			//we are currently dealing with this file. Do nothing
			log.Main.Info(
				"message is already being processed, ignoring",
				log.D{"message": m.ID, "errorMessage": err.Error()},
			)
			return
		}

		defer deleteWorkerLog(m.ID)
		if err := process.ProcessMessage(c, m); err != nil {
			m.Nack()
		} else {
			log.Main.Info("Acknowledging message", log.D{"message": m.ID})
			m.Ack()
		}
	}()
}
