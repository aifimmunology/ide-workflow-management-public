package handler

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/aifimmunology/hise-data-contracts/clients"
	"github.com/aifimmunology/hise-data-contracts/clients/tracerclient"
	"github.com/aifimmunology/hise-data-contracts/contracts/pubsubmessages"
	"github.com/aifimmunology/ide-workflow-management/dependencycontainer"
	"github.com/aifimmunology/shared-utility-library/log"

	"cloud.google.com/go/pubsub/v2"
	"github.com/google/uuid"
)

type QuotaReachedProcessor struct{ baseProcessor }

func (*QuotaReachedProcessor) Claim(m *pubsub.Message) bool {
	var qm pubsubmessages.QuotaReachedMessage
	if err := json.Unmarshal(m.Data, &qm); err != nil {
		log.Main.Error(err)
		return false
	}
	return qm.Action == tracerclient.ActionQuotaReached
}

func (*QuotaReachedProcessor) Name() string {
	return "QuotaReachedProcessor"
}

func (*QuotaReachedProcessor) Process(ctx context.Context, m *pubsub.Message) error {
	var qm pubsubmessages.QuotaReachedMessage
	var ideIds []string
	if err := json.Unmarshal(m.Data, &qm); err != nil {
		return err
	} else if reachedQuota, err := dependencycontainer.BillingServiceClient.GetReachedQuotaByUser(qm.UserGuid, qm.AccountGuid.String()); err != nil {
		return err
	} else if !reachedQuota {
		log.Main.Info(
			fmt.Sprintf(
				"user %s in account %v has not reached its quota; skipping stopping IDE instances",
				qm.UserEmail,
				qm.AccountGuid,
			),
		)
		return nil
	} else if ideIds, err = stopUsersIdesInAccountForQuotaReached(qm.UserEmail, qm.AccountGuid); err != nil {
		return err
	}

	log.Main.Info(
		"successfully stopped all IDEs that hit 100% quota",
		log.D{"account": qm.AccountGuid, "ideInstanceIds": ideIds, "user": qm.UserEmail},
	)
	return nil
}

func stopUsersIdesInAccountForQuotaReached(userEmail string, accountGuid uuid.UUID) ([]string, error) {
	// get all running AND stopped ides for user in account this time
	filter := map[string]any{
		"$or": []map[string]any{
			{"status": tracerclient.StatusRunning},
			{"status": tracerclient.StatusStopped},
		},
		"auditInfo.addedUser": userEmail,
		"accountGuid":         accountGuid,
	}

	ides, err := dependencycontainer.TracerDataServiceClient.GetIdeInstanceByFilter(clients.Query{Filter: filter})
	if err != nil {
		return nil, err
	}

	ids := make([]string, 0, len(ides))
	for _, ide := range ides {
		if err = TriggerStopIde(ide.ID, true); err != nil {
			log.Main.Error(fmt.Errorf("could not stop ide instance %s: %s", ide.ID.String(), err.Error()))
		} else {
			ids = append(ids, ide.GetId())
		}
	}
	return ids, nil
}
