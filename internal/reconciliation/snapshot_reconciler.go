package reconciliation

import (
	"context"
	"fmt"
	"time"

	"github.com/openchami/fabrica/pkg/events"
	"github.com/openchami/fabrica/pkg/reconcile"

	// Use the full module path
	"github.com/user/inventory-api/internal/storage"
	"github.com/user/inventory-api/pkg/resources/discoverysnapshot"
)

// SnapshotReconciler reconciles a DiscoverySnapshot resource
type SnapshotReconciler struct {
	reconcile.BaseReconciler
	client *storage.StorageClient // Correct type from your storage.go
	logger reconcile.Logger       // Correct type for the reconciler
}

// NewSnapshotReconciler creates a new reconciler
func NewSnapshotReconciler(eb events.EventBus, client *storage.StorageClient, logger reconcile.Logger) *SnapshotReconciler {
	return &SnapshotReconciler{
		BaseReconciler: reconcile.BaseReconciler{
			EventBus: eb,
			Logger:   logger,
		},
		client: client,
		logger: logger,
	}
}

// GetResourceKind returns the resource kind this reconciler handles
func (r *SnapshotReconciler) GetResourceKind() string {
	return "DiscoverySnapshot"
}

// Reconcile is the core logic. The controller passes the resource object.
func (r *SnapshotReconciler) Reconcile(ctx context.Context, resource interface{}) (reconcile.Result, error) {
	r.logger.Infof("RECONCILER: Received resource, attempting cast. Actual type: %T", resource)
	// 1. Cast the resource
	snapshot, ok := resource.(*discoverysnapshot.DiscoverySnapshot)
	if !ok {
		return reconcile.Result{}, fmt.Errorf("received resource is not a DiscoverySnapshot")
	}

	// Only process if phase is not "Completed"
	if snapshot.Status.Phase == "Completed" {
		return reconcile.Result{}, nil // Already done, no requeue.
	}

	r.logger.Infof("RECONCILER: Received request for DiscoverySnapshot %s", snapshot.GetName())

	// 2. Set phase to "Processing"
	snapshot.Status.Phase = "Processing"
	snapshot.Status.Message = "Reconciler has started processing the snapshot."
	snapshot.Status.Ready = false
	// Use the client's Update method to save status
	if err := r.client.Update(ctx, snapshot); err != nil {
		return reconcile.Result{}, fmt.Errorf("failed to update snapshot status to Processing: %w", err)
	}

	// 3. --- STUB: Simulate work ---
	r.logger.Infof("RECONCILER: 'Parsing' raw data for %s: %s", snapshot.GetName(), string(snapshot.Spec.RawData))
	time.Sleep(2 * time.Second) // Simulate processing time
	r.logger.Infof("RECONCILER: Processing complete for %s", snapshot.GetName())
	// --- End Stub ---

	// 4. Set phase to "Completed"
	snapshot.Status.Phase = "Completed"
	snapshot.Status.Message = "Snapshot processed successfully."
	snapshot.Status.Ready = true // Mark as ready
	if err := r.client.Update(ctx, snapshot); err != nil {
		return reconcile.Result{}, fmt.Errorf("failed to update snapshot status to Completed: %w", err)
	}

	r.logger.Infof("RECONCILER: Successfully reconciled %s", snapshot.GetName())

	// We are done, no need to requeue
	return reconcile.Result{}, nil
}