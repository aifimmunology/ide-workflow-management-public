package cron

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"unicode"

	"github.com/aifimmunology/hise-data-contracts/clients/emailclient"
	emailmodel "github.com/aifimmunology/hise-data-contracts/contracts/email"
	"github.com/aifimmunology/hise-data-contracts/contracts/tracer"
	"github.com/aifimmunology/ide-workflow-management/dependencycontainer"
	"github.com/aifimmunology/ide-workflow-management/env"
	sharedutils "github.com/aifimmunology/shared-utility-library/dependencycontainer"
	"github.com/aifimmunology/shared-utility-library/log"
)

type Findings struct {
	Package          string  `json:"package" bson:"package"`
	InstalledVersion string  `json:"installed_version" bson:"installed_version"`
	Severity         string  `json:"severity" bson:"severity"`
	FixedVersion     string  `json:"fixed_version" bson:"fixed_version"`
	Risk             float64 `json:"risk" bson:"risk"`
}

type PackageVulnerabilitiesCron struct {
	Ctx context.Context
}

type Vulnerabilities struct {
	Findings []Findings `json:"findings" bson:"findings"`
}
type VulnerabilitiesMessage struct {
	EnvName         string          `json:"env_name" bson:"env_name"`
	EnvType         string          `json:"env_type" bson:"env_type"`
	IdeName         string          `json:"ide_name" bson:"ide_name"`
	Owner           string          `json:"owner" bson:"owner"`
	Vulnerabilities Vulnerabilities `json:"vulnerabilities" bson:"vulnerabilities"`
}

func (PackageVulnerabilitiesCron) Expression() string {
	return env.PackageVulnerabilitiesCronExpression()
}

func (PackageVulnerabilitiesCron) Name() string {
	return "PackageVulnerabilitiesCronJob"
}

func (c PackageVulnerabilitiesCron) Run() {
	runCronFunc(c, func() (bool, error) { return RunPackageVulnerabilitiesCron(cronContext(c.Ctx)) })
}

func RunPackageVulnerabilitiesCron(ctx context.Context) (bool, error) {
	PubSubClient := sharedutils.GooglePubSubClient(env.Project(), ctx)
	if PubSubClient == nil {
		return false, fmt.Errorf("Vulnerabilities Cron: failed to initialize Google PubSub client")
	}

	pullResp, err := PubSubClient.Pull(ctx, env.VulnerabilitiesSubscription(), env.MaxPubSubPullMessages())
	if err != nil {
		return false, fmt.Errorf("Vulnerabilities Cron: failed to pull pubsub messages: %s", err.Error())
	}

	if pullResp == nil || len(pullResp.ReceivedMessages) == 0 {
		log.Main.Info("Vulnerabilities Cron: no messages in pubsub subscription, processing complete.")
		return true, nil
	}

	ackIds := []string{}

	for _, received := range pullResp.ReceivedMessages {
		ackIds = append(ackIds, received.AckId)

		var msg VulnerabilitiesMessage
		if err := json.Unmarshal(received.Message.Data, &msg); err != nil {
			log.Main.Error(fmt.Errorf("Vulnerabilities Cron: failed to unmarshal message: %s", err.Error()))
			continue
		}

		ide, err := dependencycontainer.TracerDataServiceClient.GetIdeInstanceByPodName(msg.IdeName)
		if err != nil {
			log.Main.Error(fmt.Errorf("Vulnerabilities Cron: failed to get ide by pod name %s: %s",
				msg.IdeName, err.Error()))
			continue
		}

		if verr := sendVulnerabilitiesEmail(msg, ide); verr != nil {
			log.Main.Error(fmt.Errorf("Vulnerabilities Cron: failed to send email: %s", verr.Error()))
		}
	}

	if aerr := PubSubClient.Acknowledge(ctx, env.VulnerabilitiesSubscription(), ackIds); aerr != nil {
		return false, fmt.Errorf("Vulnerabilities Cron: failed to acknowledge messages: %s", aerr.Error())
	}
	log.Main.Info(fmt.Sprintf("Vulnerabilities Cron: acknowledged %d messages, processing complete.", len(ackIds)))

	return true, nil
}

func formatUserName(email string) string {
	split := strings.Split(email, ".")
	if len(split) > 0 {
		firstName := split[0]
		if len(firstName) > 0 {
			runes := []rune(firstName)
			runes[0] = unicode.ToUpper(runes[0])
			return string(runes)
		}
	}
	return ""
}

func formatVulnerabilitiesEmailPayload(msg VulnerabilitiesMessage, ide *tracer.IDEInstance) (bool, map[string]any, error) {
	account, err := dependencycontainer.AccountDataServiceClient.GetAccountByGuid(ide.AccountGUID)
	if err != nil {
		return false,
			nil,
			fmt.Errorf("Vulnerabilities Cron: failed to get account %s: %s", ide.AccountGUID.String(), err.Error())
	}

	newFindings := []Findings{}
	oldFindings := []Findings{}
	packageNames := []string{}

	for _, finding := range msg.Vulnerabilities.Findings {
		if finding.Risk >= 1 {
			packageNames = append(packageNames, finding.Package)
			if finding.FixedVersion == "" {
				finding.FixedVersion = "No fix available"
			}

			if slices.Contains(ide.VulnerablePackages, finding.Package) {
				oldFindings = append(oldFindings, finding)
			} else {
				newFindings = append(newFindings, finding)
			}
		}
	}

	if len(packageNames) == 0 {
		log.Main.Info(fmt.Sprintf("Vulnerabilities Cron: no vulnerabilities found in ide: %s, skipping email.", msg.IdeName))
		return false, nil, nil
	} else if len(newFindings) == 0 {
		log.Main.Info(fmt.Sprintf("Vulnerabilities Cron: no new vulnerabilities found in ide: %s, skipping email.", msg.IdeName))
		return false, nil, nil
	}

	if err = updateIdeVulnerablePackages(ide, packageNames); err != nil {
		return false, nil, err
	}

	emailPayload := map[string]any{
		"user":            formatUserName(ide.AuditInfo.AddedUser),
		"ide_name":        ide.FriendlyName,
		"acct_name":       account.Name,
		"env_name":        msg.EnvName,
		"env_type":        msg.EnvType,
		"vulnerabilities": newFindings,
	}

	if len(oldFindings) > 0 {
		emailPayload["old_vulnerabilities"] = oldFindings
	}

	return true, emailPayload, nil
}

func sendVulnerabilitiesEmail(msg VulnerabilitiesMessage, ide *tracer.IDEInstance) error {
	notify, emailPayload, err := formatVulnerabilitiesEmailPayload(msg, ide)
	if err != nil {
		return err
	} else if !notify {
		return nil
	}

	req := emailmodel.EmailRequest{
		ToAddress:           ide.AuditInfo.AddedUser,
		NotificationType:    emailmodel.NotificationIdePackageVulnerabilities,
		DynamicTemplateData: emailPayload,
	}

	if _, err := emailclient.SendEmailCustomRequest(&req); err != nil {
		return err
	}

	log.Main.Info(fmt.Sprintf("Vulnerabilities Cron: sent email to %s for ide: %s",
		ide.AuditInfo.AddedUser, msg.IdeName))
	return nil
}

func updateIdeVulnerablePackages(ide *tracer.IDEInstance, newVulnerablePackages []string) error {
	recentIde, err := dependencycontainer.TracerDataServiceClient.GetIdeInstance(ide.ID)
	if err != nil {
		return fmt.Errorf("Vulnerabilities Cron: %s", err.Error())
	}

	recentIde.VulnerablePackages = newVulnerablePackages
	return dependencycontainer.TracerDataServiceClient.UpdateIdeInstance(recentIde)
}
