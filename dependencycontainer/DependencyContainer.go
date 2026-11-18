package dependencycontainer

import (
	"context"
	"sync"
	"time"

	"github.com/aifimmunology/hise-data-contracts/clients/accountmanagement"
	"github.com/aifimmunology/hise-data-contracts/clients/billing"
	"github.com/aifimmunology/hise-data-contracts/clients/ideclient"
	"github.com/aifimmunology/hise-data-contracts/clients/ledgerclient"
	"github.com/aifimmunology/hise-data-contracts/clients/tracerclient"
	"github.com/aifimmunology/hise-data-contracts/clients/workflow"
)

var (
	AccountDataServiceClient   *accountmanagement.AccountDataServiceClient
	BillingServiceClient       *billing.BillingServiceClient
	IdeManagementServiceClient *ideclient.IDEManagementServiceClient
	initDependenciesOnce       sync.Once
	LedgerDataServiceClient    *ledgerclient.LedgerDataServiceClient
	stopRefresh                chan bool = make(chan bool, 1)
	TracerDataServiceClient    *tracerclient.TracerDataServiceClient
	WorkflowDataServiceClient  *workflow.WorkflowDataServiceClient
)

// Close stops the otherwise inifinitely-running refresh goroutine
func Close() {
	stopRefresh <- true
	close(stopRefresh)
}

func InitDependencies(ctx context.Context) {
	initDependenciesOnce.Do(func() {
		initDependencies(ctx)
		go refreshServiceAccountTokens()
	})
}

func InitUnitTestDependencies(ctx context.Context) {
	initDependenciesOnce.Do(func() {
		accountmanagement.InitMockAMDSClient()
		accountmanagement.InitMockAMDSServer()
		ledgerclient.InitMockLDSClient()
		tracerclient.InitMockTDSServer()
		workflow.InitMockWorkflowClient()
		ideclient.InitMockIDEManagementServer()
		initDependencies(ctx)
	})
}

func initDependencies(ctx context.Context) {
	AccountDataServiceClient = accountmanagement.NewAccountDataServiceClient(ctx)
	AccountDataServiceClient.AsServiceAccount()

	BillingServiceClient = billing.NewBillingServiceClient(ctx)
	BillingServiceClient.AsServiceAccount()

	IdeManagementServiceClient = ideclient.NewIDEManagementServiceClient(ctx)
	IdeManagementServiceClient.AsServiceAccount()

	LedgerDataServiceClient = ledgerclient.NewLedgerDataServiceClient(ctx)
	LedgerDataServiceClient.AsServiceAccount()

	TracerDataServiceClient = tracerclient.NewTracerDataServiceClient(ctx)
	TracerDataServiceClient.AsServiceAccount()

	WorkflowDataServiceClient = workflow.NewWorkflowDataServiceClient()
	WorkflowDataServiceClient.AsServiceAccount()
}

func refreshServiceAccountTokens() {
	ticker := time.NewTicker(time.Second * 3000).C
	for {
		select {
		case <-ticker:
			AccountDataServiceClient.RefreshServiceToken()
			BillingServiceClient.RefreshServiceToken()
			IdeManagementServiceClient.RefreshServiceToken()
			LedgerDataServiceClient.RefreshServiceToken()
			TracerDataServiceClient.RefreshServiceToken()
			WorkflowDataServiceClient.RefreshServiceToken()
		case stop, open := <-stopRefresh:
			if stop || !open {
				return
			}
		}
	}
}
