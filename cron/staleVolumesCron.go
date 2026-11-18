package cron

import (
	"context"
	"fmt"
	"slices"

	"github.com/aifimmunology/hise-data-contracts/clients"
	"github.com/aifimmunology/hise-data-contracts/contracts"
	"github.com/aifimmunology/hise-data-contracts/contracts/tracer"
	"github.com/aifimmunology/ide-workflow-management/dependencycontainer"
	"github.com/aifimmunology/ide-workflow-management/env"
	"github.com/aifimmunology/shared-utility-library/datastructures"
	"github.com/aifimmunology/shared-utility-library/kubernetesclient"
	"github.com/aifimmunology/shared-utility-library/log"
	"github.com/aifimmunology/shared-utility-library/types"
	"github.com/aifimmunology/shared-utility-library/utils"

	snap "github.com/kubernetes-csi/external-snapshotter/client/v7/apis/volumesnapshot/v1"
	"github.com/samber/lo"
	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	meta "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const maxVolumeHistorySize int = 2

var volumeNameSubstrings = []string{"-conda-envdisk", "-sdk-envdisk", "-userdisk", "-rootdisk"}

type StaleVolumesCronJob struct {
	Ctx context.Context
}

func (StaleVolumesCronJob) Expression() string {
	return env.StaleVolumesCronExpression()
}

func (StaleVolumesCronJob) Name() string {
	return "StaleVolumesCronJob"
}

func (cj StaleVolumesCronJob) Run() {
	runCronFunc(cj, func() (bool, error) { return deleteStaleVolumes(cronContext(cj.Ctx)) })
}

func cleanupNVolumeHistories(
	ctx context.Context,
	kubecli kubernetesclient.Client,
	ns string,
	ide *tracer.IDEInstance,
	n int,
	pvcSet, snapshotSet datastructures.Set[string],
) ([]tracer.IDEVolumeHistory, []error) {
	remainingHistories := make([]tracer.IDEVolumeHistory, 0, len(ide.VolumeHistory))
	errs := make([]error, 0, n*2*len(volumeNameSubstrings))
	for i, volHist := range ide.VolumeHistory {
		if i >= n {
			remainingHistories = append(remainingHistories, volHist)
			continue
		}

		log.Main.Info(
			fmt.Sprintf("deleting PVC and Snapshot History #%d for IDE %s", i+1, ide.PodName),
			log.D{"pvcSuffix": volHist.PvcSuffix, "snapshotSuffix": volHist.VolumeSnapshotSuffix},
		)

		pvcs, snapshots := getPvcAndSnapshotNames(ide, &volHist)
		pvcDeleteErrs := deleteKubernetesEntities(ctx, ns, pvcs, pvcSet, kubecli.DeletePVC)
		snapshotDeleteErrs := deleteKubernetesEntities(ctx, ns, snapshots, snapshotSet, kubecli.DeleteVolumeSnapshot)
		if len(pvcDeleteErrs) > 0 || len(snapshotDeleteErrs) > 0 {
			errs = slices.Concat(errs, pvcDeleteErrs, snapshotDeleteErrs)
			remainingHistories = append(remainingHistories, volHist)
		}
	}
	return remainingHistories, errs
}

func deleteKubernetesEntities(
	ctx context.Context,
	ns string,
	names []string,
	set datastructures.Set[string],
	delete func(context.Context, string, string, ...meta.DeleteOptions) error,
) []error {
	errs := make([]error, 0, len(names))
	for _, name := range names {
		if set.Contains(name) {
			if err := delete(ctx, ns, name); err != nil && !errors.IsNotFound(err) {
				errs = append(errs, err)
			}
		}
	}
	return errs
}

func deleteStaleVolumes(ctx context.Context) (bool, error) {
	kubecli, err := kubernetesclient.NewForCurrentCluster(ctx)
	if err != nil {
		return false, err
	}

	ns := kubernetesclient.K8sNamespace()
	pvcs, err := kubecli.ListPVCs(ctx, ns)
	if err != nil {
		return false, err
	}

	pvcSet := datastructures.SliceToSet(
		utils.Map(utils.MapToPointers(pvcs), func(pvc *core.PersistentVolumeClaim) string {
			return pvc.Name
		}),
	)

	snapshots, err := kubecli.ListVolumeSnapshots(ctx, ns)
	if err != nil {
		return false, err
	}

	snapshotSet := datastructures.SliceToSet(
		utils.Map(utils.MapToPointers(snapshots), func(snapshot *snap.VolumeSnapshot) string {
			return snapshot.Name
		}),
	)

	ides, err := dependencycontainer.TracerDataServiceClient.GetIdeInstanceByFilter(
		clients.Query{
			Filter: map[string]any{
				"$or": []any{
					map[string]any{"isDeleted": map[string]any{"$ne": contracts.ToSBool(true).String()}},
					map[string]any{
						"isDeleted":                contracts.ToSBool(true).String(),
						"strandedVolumesCleanedUp": map[string]any{"$ne": contracts.ToSBool(true).String()},
					},
				},
			},
		},
	)

	if err != nil {
		return false, err
	}

	errs := make([]error, 0)
	for i := range ides {
		ide := &ides[i]

		// Older deleted instance; let's make sure we've really cleaned up so we can mark it as such
		if ide.IsDeleted.True() && len(ide.VolumeHistory) == 0 {
			ide.VolumeHistory = []tracer.IDEVolumeHistory{{}}
		}

		volumesToCleanup := len(ide.VolumeHistory) - lo.Ternary(ide.IsDeleted.True(), 0, maxVolumeHistorySize)
		if volumesToCleanup <= 0 {
			continue
		}

		volHist, es := cleanupNVolumeHistories(ctx, kubecli, ns, ide, volumesToCleanup, pvcSet, snapshotSet)
		errs = append(errs, es...)

		// sparse update; add to errs to indicate that work was done
		errs = append(
			errs,
			dependencycontainer.TracerDataServiceClient.UpdateIdeInstance(
				&tracer.IDEInstance{
					ID:                       ide.ID,
					StrandedVolumesCleanedUp: contracts.ToSBool(ide.IsDeleted.True() && len(volHist) == 0),
					VolumeHistory:            volHist,
				},
			),
		)
	}
	return len(errs) > 0, types.GetAggregateError(errs...)
}

func getPvcAndSnapshotNames(ide *tracer.IDEInstance, volumeHistory *tracer.IDEVolumeHistory) ([]string, []string) {
	n := len(volumeNameSubstrings)
	pvcs, snapshots := make([]string, 0, n), make([]string, 0, n)
	for _, volumeSubstr := range volumeNameSubstrings {
		pvcs = append(pvcs, ide.PodName+volumeSubstr+volumeHistory.PvcSuffix)
		snapshots = append(snapshots, ide.PodName+volumeSubstr+"-snapshot"+volumeHistory.VolumeSnapshotSuffix)
	}
	return pvcs, snapshots
}
