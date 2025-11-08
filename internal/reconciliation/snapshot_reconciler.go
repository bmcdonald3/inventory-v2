package reconciliation

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/openchami/fabrica/pkg/events"
	"github.com/openchami/fabrica/pkg/reconcile"
	
	// We alias the import to 'fabResource' to avoid shadowing
	fabResource "github.com/openchami/fabrica/pkg/resource"

	"github.com/user/inventory-api/internal/storage"
	"github.com/user/inventory-api/pkg/resources/device"
	"github.com/user/inventory-api/pkg/resources/discoverysnapshot"
)

// SnapshotReconciler (This struct is unchanged)
type SnapshotReconciler struct {
	reconcile.BaseReconciler
	client *storage.StorageClient
	logger reconcile.Logger
}

// NewSnapshotReconciler (This function is unchanged)
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

// GetResourceKind (This function is unchanged)
func (r *SnapshotReconciler) GetResourceKind() string {
	return "DiscoverySnapshot"
}

// Reconcile is the core logic.
func (r *SnapshotReconciler) Reconcile(ctx context.Context, resource interface{}) (reconcile.Result, error) {
	// 1. UNMARSHAL THE SNAPSHOT
	// (This logic is unchanged)
	raw, ok := resource.(json.RawMessage)
	if !ok {
		return reconcile.Result{}, fmt.Errorf("received resource is not json.RawMessage, but %T", resource)
	}
	var snapshot discoverysnapshot.DiscoverySnapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return reconcile.Result{}, fmt.Errorf("failed to unmarshal snapshot: %w", err)
	}
	if snapshot.Status.Phase == "Completed" {
		return reconcile.Result{}, nil
	}
	r.logger.Infof("RECONCILER: Received request for DiscoverySnapshot %s", snapshot.GetName())

	// 2. Set phase to "Processing"
	// (This logic is unchanged)
	snapshot.Status.Phase = "Processing"
	snapshot.Status.Message = "Reconciler has started processing the snapshot."
	snapshot.Status.Ready = false
	if err := r.client.Update(ctx, &snapshot); err != nil {
		return reconcile.Result{}, fmt.Errorf("failed to update snapshot status to Processing: %w", err)
	}

	// 3. --- START PAYLOAD PROCESSING ---
	// (This logic is unchanged)
	deviceMap, err := r.buildDeviceMap(ctx)
	if err != nil {
		return reconcile.Result{}, fmt.Errorf("failed to build device map: %w", err)
	}
	r.logger.Infof("RECONCILER: Loaded %d existing devices into map", len(deviceMap))

	var payloadSpecs []device.DeviceSpec
	if err := json.Unmarshal(snapshot.Spec.RawData, &payloadSpecs); err != nil {
		// (Error handling is unchanged)
		snapshot.Status.Phase = "Error"
		snapshot.Status.Message = "Failed to parse rawData: " + err.Error()
		if updateErr := r.client.Update(ctx, &snapshot); updateErr != nil {
			return reconcile.Result{}, updateErr
		}
		return reconcile.Result{}, fmt.Errorf("failed to parse snapshot rawData: %w", err)
	}

	processedCount := 0
	for _, spec := range payloadSpecs {
		if spec.SerialNumber == "" {
			r.logger.Errorf("RECONCILER: Skipping device with no serial number")
			continue
		}

		existingDevice, found := deviceMap[spec.SerialNumber]
		if !found {
			// --- CREATE NEW DEVICE (THIS BLOCK IS FIXED) ---
			r.logger.Infof("RECONCILER: Creating new device: %s", spec.SerialNumber)
			
			newDevice := &device.Device{
				Resource: fabResource.Resource{
					APIVersion:    "v1",
					Kind:          "Device",
					SchemaVersion: "v1",
				},
				Spec: spec,
			}

			// --- THIS IS THE FIX ---
			// Manually initialize metadata based on your resource.go
			uid, err := fabResource.GenerateUIDForResource("Device")
			if err != nil {
				r.logger.Errorf("RECONCILER: Failed to generate UID for device: %v", err)
				continue
			}
			now := time.Now()
			newDevice.Metadata.UID = uid
			newDevice.Metadata.Name = spec.SerialNumber // Use serial as name
			newDevice.Metadata.CreatedAt = now
			newDevice.Metadata.UpdatedAt = now
			// --- END FIX ---

			if err := r.client.Create(ctx, newDevice); err != nil {
				r.logger.Errorf("RECONCILER: Failed to create device %s: %v", spec.SerialNumber, err)
				continue
			}

		} else {
			// --- UPDATE EXISTING DEVICE ---
			// (This logic is unchanged)
			r.logger.Infof("RECONCILER: Updating existing device: %s (UID: %s)", spec.SerialNumber, existingDevice.GetUID())
			
			existingDevice.Spec = spec
			existingDevice.Metadata.UpdatedAt = time.Now()

			if err := r.client.Update(ctx, existingDevice); err != nil {
				r.logger.Errorf("RECONCILER: Failed to update device %s: %v", spec.SerialNumber, err)
				continue
			}
		}
		processedCount++
	}
	// --- END PAYLOAD PROCESSING ---

	// 4. Set phase to "Completed"
	// (This logic is unchanged)
	snapshot.Status.Phase = "Completed"
	snapshot.Status.Message = fmt.Sprintf("Snapshot processed successfully. %d devices created/updated.", processedCount)
	snapshot.Status.Ready = true
	if err := r.client.Update(ctx, &snapshot); err != nil {
		return reconcile.Result{}, fmt.Errorf("failed to update snapshot status to Completed: %w", err)
	}

	r.logger.Infof("RECONCILER: Successfully reconciled %s", snapshot.GetName())

	return reconcile.Result{}, nil
}

// buildDeviceMap (This function is unchanged)
func (r *SnapshotReconciler) buildDeviceMap(ctx context.Context) (map[string]*device.Device, error) {
	deviceList, err := r.client.List(ctx, "Device")
	if err != nil {
		return nil, err
	}

	deviceMap := make(map[string]*device.Device)
	for _, item := range deviceList {
		dev, ok := item.(*device.Device)
		if !ok {
			r.logger.Errorf("RECONCILER: Found non-device item in storage, skipping.")
			continue
		}
		if dev.Spec.SerialNumber != "" {
			deviceMap[dev.Spec.SerialNumber] = dev
		}
	}
	return deviceMap, nil
}