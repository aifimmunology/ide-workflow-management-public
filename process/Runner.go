package process

import (
	"context"
	"fmt"
	"strings"

	"github.com/aifimmunology/ide-workflow-management/handler"
	"github.com/aifimmunology/shared-utility-library/log"

	"cloud.google.com/go/pubsub/v2"
)

var processors = []Processor{
	&handler.CondaPackCreateFailureProcessor{},
	&handler.CreateDashRouteProcessor{},
	&handler.EditIdeMachineConfigProcessor{},
	&handler.HarvestArtifactsProcessor{},
	&handler.HealthRouteProcessor{},
	&handler.IDECreateProcessor{},
	&handler.IDEDeleteProcessor{},
	&handler.IDEPVCInstallProcessor{},
	&handler.IDEStartProcessor{},
	&handler.IDEStopProcessor{},
	&handler.InstallSDKProcessor{},
	&handler.JupyterTimeoutProcessor{},
	&handler.QuotaReachedProcessor{},
	&handler.RevertUserDiskSizeProcessor{},
	&handler.ScalePVCStorageProcessor{},
	&handler.SetIapPolicyProcessor{},
	&handler.UploadFilesFailureProcessor{},
}

// A Processor is an interface that "processes" files in a from the topic messages
// Claim method determines whether processor can accept the associated pubusb message looking at pipelineType
// Process method performs the actual pipeline orchestration
type Processor interface {
	Claim(*pubsub.Message) bool
	Initialize() error
	IsInThrottleLimit() bool
	Name() string
	Process(context.Context, *pubsub.Message) error
}

func ProcessMessage(ctx context.Context, m *pubsub.Message) error {
	for _, processor := range processors {
		if processor.Claim(m) {
			log.Main.Info("Invoking Processor", log.D{"Processor": processor.Name()})
			if !processor.IsInThrottleLimit() {
				return fmt.Errorf("Nack")
			} else if err := processor.Process(ctx, m); err == nil {
				log.Main.Info("Processor processing completed", log.D{"Processor": processor.Name()})
			} else if strings.EqualFold(err.Error(), "Nack") {
				return err
			} else {
				log.Main.Info(
					"Processor processing completed",
					log.D{"Processor": processor.Name(), "error": err.Error()},
				)
			}
		}
	}
	return nil
}
