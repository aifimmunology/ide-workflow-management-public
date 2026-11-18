package handler

import (
	"context"
	"fmt"
	"strings"

	"github.com/aifimmunology/hise-data-contracts/clients/emailclient"
	"github.com/aifimmunology/hise-data-contracts/clients/tracerclient"
	"github.com/aifimmunology/hise-data-contracts/contracts"
	emailmodel "github.com/aifimmunology/hise-data-contracts/contracts/email"
	"github.com/aifimmunology/hise-data-contracts/contracts/tracer"
	"github.com/aifimmunology/ide-workflow-management/dependencycontainer"
	"github.com/aifimmunology/shared-utility-library/log"

	"cloud.google.com/go/pubsub/v2"
)

type CreateDashRouteProcessor struct{ baseProcessor }

func (p *CreateDashRouteProcessor) Claim(m *pubsub.Message) bool {
	return claimFlowAction(m, tracerclient.ActionCreateDashRoute)
}

func (p *CreateDashRouteProcessor) Name() string {
	return "CreateDashRouteProcessor"
}

func (p *CreateDashRouteProcessor) Process(ctx context.Context, m *pubsub.Message) error {
	defer m.Ack()
	message, err := decodeFlowRequest(m)
	if err != nil {
		return err
	}

	_, ide, err := handleArgoFromRequest(message, "create-dash-route-failure")
	if err != nil {
		if !strings.Contains(err.Error(), wfNotFoundErr) {
			if _, err := sendDashRouteEmail(ide, true); err != nil {
				log.Main.Error(
					fmt.Errorf("error sending dash route notification for ide %s: %s", ide.ID.String(), err.Error()),
				)
			}
		}
		return err
	}

	ide.DashApp = contracts.ToSBool(true)
	if err := dependencycontainer.TracerDataServiceClient.UpdateIdeInstance(ide); err != nil {
		log.Main.Error(fmt.Errorf("could not update ide %s: %s", ide.ID.String(), err.Error()))
	}

	_, err = sendDashRouteEmail(ide, false)
	return err
}

func sendDashRouteEmail(ide *tracer.IDEInstance, failed bool) (string, error) {
	accountVal := ide.AccountGUID.String()
	if acctName, err := getAccountName(ide.AccountGUID); err != nil {
		log.Main.Error(err.Error())
	} else {
		accountVal = acctName
	}

	status := "is now available to use."
	subjectStatus := "Ready"
	if failed {
		status = "failed to create, please contact support to debug."
		subjectStatus = "Failed"
	}

	data := map[string]any{
		"account":       accountVal,
		"ideName":       ide.FriendlyName,
		"status":        status,
		"subjectStatus": subjectStatus,
	}

	req := emailmodel.EmailRequest{
		ToAddress:           ide.AuditInfo.AddedUser,
		NotificationType:    emailmodel.NotificationDashAppRoute,
		DynamicTemplateData: data,
	}
	return emailclient.SendEmailCustomRequest(&req)
}
