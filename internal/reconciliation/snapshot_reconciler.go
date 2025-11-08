package reconciliation

import (
	"context"
	"encoding/json" // <<< ADD THIS IMPORT
	"fmt"
	"time"

	"github.com/openchami/fabrica/pkg/events"
	"github.com/openchami/fabrica/pkg/reconcile"

	"github.com/user/inventory-api/internal/storage"
	"github.com/user/inventory-api/pkg/resources/discoverysnapshot"
)

// SnapshotReconciler reconciles a DiscoverySnapshot resource
type SnapshotReconciler struct {
	reconcile.BaseReconciler
	client *storage.StorageClient
	logger reconcile.Logger
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

// Reconcile is the core logic.
func (r *SnapshotReconciler) Reconcile(ctx context.Context, resource interface{}) (reconcile.Result, error) {
	// 1. UNMARSHAL THE RESOURCE
	// The controller passes json.RawMessage, so we unmarshal it.
	raw, ok := resource.(json.RawMessage)
	if !ok {
		return reconcile.Result{}, fmt.Errorf("received resource is not json.RawMessage, but %T", resource)
	}

	var snapshot discoverysnapshot.DiscoverySnapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return reconcile.Result{}, fmt.Errorf("failed to unmarshal snapshot: %w", err)
	}

	// 2. Check if already processed
	if snapshot.Status.Phase == "Completed" {
		return reconcile.Result{}, nil // Already done, no requeue.
	}

	r.logger.Infof("RECONCILER: Received request for DiscoverySnapshot %s", snapshot.GetName())

	// 3. Set phase to "Processing"
	snapshot.Status.Phase = "Processing"
	snapshot.Status.Message = "Reconciler has started processing the snapshot."
	snapshot.Status.Ready = false
	// Use the *client* to update the status (which saves the resource)
	if err := r.client.Update(ctx, &snapshot); err != nil {
		return reconcile.Result{}, fmt.Errorf("failed to update snapshot status to Processing: %w", err)
	}

	// 4. --- STUB: Simulate work ---
	r.logger.Infof("RECONCILER: 'Parsing' raw data for %s: %s", snapshot.GetName(), string(snapshot.Spec.RawData))
	time.Sleep(2 * time.Second) // Simulate processing time
	r.logger.Infof("RECONCILER: Processing complete for %s", snapshot.GetName())
	// --- End Stub ---

	// 5. Set phase to "Completed"
	snapshot.Status.Phase = "Completed"
	snapshot.Status.Message = "Snapshot processed successfully."
	snapshot.Status.Ready = true
	if err := r.client.Update(ctx, &snapshot); err != nil {
		return reconcile.Result{}, fmt.Errorf("failed to update snapshot status to Completed: %w", err)
	}

	r.logger.Infof("RECONCILER: Successfully reconciled %s", snapshot.GetName())

	return reconcile.Result{}, nil
}