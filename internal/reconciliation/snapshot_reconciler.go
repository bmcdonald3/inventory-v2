package reconciliation

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/openchami/fabrica/pkg/events"
	"github.com/openchami/fabrica/pkg/reconcile"
	"github.com/openchami/fabrica/pkg/storage"

	// Import your snapshot resource definition
	"github.com/user/inventory-api/pkg/resources/discoverysnapshot"
)

// SnapshotReconciler reconciles a DiscoverySnapshot resource
type SnapshotReconciler struct {
	reconcile.BaseReconciler
	store  storage.Storage
	logger *log.Logger
}

// NewSnapshotReconciler creates a new reconciler
func NewSnapshotReconciler(eb events.EventBus, store storage.Storage, logger *log.Logger) *SnapshotReconciler {
	return &SnapshotReconciler{
		BaseReconciler: reconcile.BaseReconciler{
			EventBus: eb,
			Logger:   logger,
		},
		store:  store,
		logger: logger,
	}
}

// GetResourceKind returns the resource kind this reconciler handles
func (r *SnapshotReconciler) GetResourceKind() string {
	return "DiscoverySnapshot"
}

// Reconcile is the core logic. It's triggered when a DiscoverySnapshot is created or updated.
func (r *SnapshotReconciler) Reconcile(ctx context.Context, req reconcile.ReconcileRequest) (reconcile.Result, error) {
	r.logger.Printf("RECONCILER: Received request for DiscoverySnapshot %s", req.ResourceUID)

	// 1. Load the resource
	resource, err := r.store.Get(ctx, "DiscoverySnapshot", req.ResourceUID)
	if err != nil {
		if storage.IsNotFound(err) {
			r.logger.Printf("RECONCILER: Snapshot %s already deleted. Ignoring.", req.ResourceUID)
			return reconcile.Result{}, nil
		}
		return reconcile.Result{}, fmt.Errorf("failed to get snapshot: %w", err)
	}

	// Cast to our specific type
	snapshot, ok := resource.(*discoverysnapshot.DiscoverySnapshot)
	if !ok {
		return reconcile.Result{}, fmt.Errorf("received resource is not a DiscoverySnapshot")
	}

	// 2. Set phase to "Processing"
	snapshot.Status.Phase = "Processing"
	snapshot.Status.Message = "Reconciler has started processing the snapshot."
	snapshot.Status.Ready = false
	if err := r.store.UpdateStatus(ctx, "DiscoverySnapshot", snapshot.GetUID(), snapshot); err != nil {
		return reconcile.Result{}, fmt.Errorf("failed to update snapshot status to Processing: %w", err)
	}

	// 3. --- STUB: Simulate work ---
	// In Step 3, we will parse snapshot.Spec.RawData here.
	r.logger.Printf("RECONCILER: 'Parsing' raw data for %s: %s", snapshot.GetName(), string(snapshot.Spec.RawData))
	time.Sleep(2 * time.Second) // Simulate processing time
	r.logger.Printf("RECONCILER: Processing complete for %s", snapshot.GetName())
	// --- End Stub ---

	// 4. Set phase to "Completed"
	snapshot.Status.Phase = "Completed"
	snapshot.Status.Message = "Snapshot processed successfully."
	snapshot.Status.Ready = true // Mark as ready
	if err := r.store.UpdateStatus(ctx, "DiscoverySnapshot", snapshot.GetUID(), snapshot); err != nil {
		return reconcile.Result{}, fmt.Errorf("failed to update snapshot status to Completed: %w", err)
	}

	r.logger.Printf("RECONCILER: Successfully reconciled %s", snapshot.GetName())

	// We are done, no need to requeue
	return reconcile.Result{}, nil
}