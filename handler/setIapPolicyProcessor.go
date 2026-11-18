package handler

import (
	"context"
	"fmt"

	"github.com/aifimmunology/hise-data-contracts/clients/emailclient"
	"github.com/aifimmunology/hise-data-contracts/clients/tracerclient"
	emailmodel "github.com/aifimmunology/hise-data-contracts/contracts/email"
	"github.com/aifimmunology/hise-data-contracts/contracts/pubsubmessages"
	"github.com/aifimmunology/hise-data-contracts/contracts/tracer"
	"github.com/aifimmunology/shared-utility-library/log"

	"cloud.google.com/go/pubsub/v2"
)

type SetIapPolicyProcessor struct{ baseProcessor }

func (p *SetIapPolicyProcessor) Claim(m *pubsub.Message) bool {
	return claimFlowAction(m, tracerclient.ActionSetIapPolicy)
}

func (p *SetIapPolicyProcessor) Name() string {
	return "SetIapPolicyProcessor"
}

func (p *SetIapPolicyProcessor) Process(ctx context.Context, m *pubsub.Message) error {
	if msg, err := decodeFlowRequest(m); err != nil {
		m.Ack()
		return err
	} else if ok, prevAction, failureStatus := needsIdeUpdatesAndEmail(msg); ok {
		if err := updateActionHistoryWithEtag(msg); err != nil {
			return handleETagError(err, m, nil)
		} else if wf, ide, err := handleArgoFromRequest(msg, failureStatus); err != nil {
			return handleETagError(err, m, wf)
		} else {
			ackMsgAndCleanUpWorkflow(m, wf)

			if prevAction == tracerclient.ActionCreate {
				if err = sendIdeCreationCompleteEmail(ide); err != nil {
					log.Main.Error(err)
				}
			}
			return nil
		}
	} else { // this is the iap policy endpoint, used by CE team to retry setting it to an already running IDE
		m.Ack()
		if ide, err := getIdeFromFlowRequest(msg); err != nil {
			return err
		} else if _, err := RunSetIamPolicyArgo(ide, msg); err != nil {
			return err
		}
		return nil
	}
}

func needsIdeUpdatesAndEmail(msg *pubsubmessages.IDEFlowRequest) (bool, string, string) {
	if prevAction, ok := msg.AdditionalPayload[previousAction].(string); !ok {
		return false, "", ""
	} else if prevAction == tracerclient.ActionStart {
		return true, prevAction, tracerclient.StatusStartingFailure
	} else if prevAction == tracerclient.ActionCreate {
		return true, prevAction, tracerclient.StatusCreationFailure
	}
	return false, "", ""
}

func sendIdeCreationCompleteEmail(ide *tracer.IDEInstance) error {
	if _, err := emailclient.SendEmailCustomRequest(
		&emailmodel.EmailRequest{
			ToAddress:           ide.AuditInfo.AddedUser,
			NotificationType:    emailmodel.NotificationIdeCreationComplete,
			DynamicTemplateData: map[string]any{"ide_name": ide.PodName},
		},
	); err != nil {
		return fmt.Errorf("failed to send email notification: %s", err.Error())
	}
	return nil
}
