package reconciliation

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/openchami/fabrica/pkg/events"
	"github.com/openchami/fabrica/pkg/reconcile"
	fabResource "github.com/openchami/fabrica/pkg/resource"

	"github.com/user/inventory-api/internal/storage"
	"github.com/user/inventory-api/pkg/resources/device"
	"github.com/user/inventory-api/pkg/resources/discoverysnapshot"
)

// (SnapshotReconciler struct, NewSnapshotReconciler, GetResourceKind are unchanged)
// ...
type SnapshotReconciler struct {
	reconcile.BaseReconciler
	client *storage.StorageClient
	logger reconcile.Logger
}
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
func (r *SnapshotReconciler) GetResourceKind() string {
	return "DiscoverySnapshot"
}

// --- RECONCILE FUNCTION IS UPDATED ---
func (r *SnapshotReconciler) Reconcile(ctx context.Context, resource interface{}) (reconcile.Result, error) {
	// 1. UNMARSHAL THE SNAPSHOT
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
	snapshot.Status.Phase = "Processing"
	snapshot.Status.Message = "Reconciler has started processing the snapshot."
	snapshot.Status.Ready = false
	if err := r.client.Update(ctx, &snapshot); err != nil {
		return reconcile.Result{}, fmt.Errorf("failed to update snapshot status to Processing: %w", err)
	}

	// 3. --- START PAYLOAD PROCESSING (TWO-PASS LOGIC) ---

	// 3a. Unmarshal the payload
	var payloadSpecs []device.DeviceSpec
	if err := json.Unmarshal(snapshot.Spec.RawData, &payloadSpecs); err != nil {
		return r.failSnapshot(ctx, &snapshot, "Failed to parse rawData", err)
	}

	// 3b. Load all existing devices from storage
	deviceMapBySerial, err := r.buildDeviceMapBySerial(ctx)
	if err != nil {
		return r.failSnapshot(ctx, &snapshot, "Failed to build device map", err)
	}
	r.logger.Infof("RECONCILER: Loaded %d existing devices into map", len(deviceMapBySerial))

	// This map will hold all devices *from this snapshot* (new and updated)
	// We need it for the second pass
	snapshotDeviceMap := make(map[string]*device.Device)

	// --- PASS 1: CREATE AND UPDATE DEVICES ---
	// We loop through the payload, create new devices, and update existing ones.
	// We also populate our snapshotDeviceMap.

	processedCount := 0
	for _, spec := range payloadSpecs {
		if spec.SerialNumber == "" {
			r.logger.Errorf("RECONCILER: Skipping device with no serial number")
			continue
		}

		existingDevice, found := deviceMapBySerial[spec.SerialNumber]
		if !found {
			// --- CREATE NEW DEVICE ---
			r.logger.Infof("RECONCILER (Pass 1): Creating new device: %s", spec.SerialNumber)
			newDevice, err := r.createNewDevice(ctx, spec)
			if err != nil {
				r.logger.Errorf("RECONCILER (Pass 1): Failed to create device %s: %v", spec.SerialNumber, err)
				continue
			}
			snapshotDeviceMap[newDevice.Spec.SerialNumber] = newDevice
			deviceMapBySerial[newDevice.Spec.SerialNumber] = newDevice // Add to global map

		} else {
			// --- UPDATE EXISTING DEVICE ---
			r.logger.Infof("RECONCILER (Pass 1): Updating existing device: %s (UID: %s)", spec.SerialNumber, existingDevice.GetUID())

			// Preserve the ParentID from the database, in case the snapshot doesn't have it
			// This is important for the 2-pass linking
			spec.ParentID = existingDevice.Spec.ParentID
			existingDevice.Spec = spec // Update the spec
			existingDevice.Metadata.UpdatedAt = time.Now()

			if err := r.client.Update(ctx, existingDevice); err != nil {
				r.logger.Errorf("RECONCILER (Pass 1): Failed to update device %s: %v", spec.SerialNumber, err)
				continue
			}
			snapshotDeviceMap[existingDevice.Spec.SerialNumber] = existingDevice
		}
		processedCount++
	}

	// --- PASS 2: LINK PARENT IDs ---
	// Now we loop through the devices *we just processed* and link them.
	// We use the *full* deviceMapBySerial so we can link to parents
	// that might have existed before this snapshot.

	r.logger.Infof("RECONCILER (Pass 2): Linking parent relationships...")
	linksUpdated := 0
	for _, dev := range snapshotDeviceMap {
		parentSerial := dev.Spec.ParentSerialNumber
		if parentSerial == "" {
			continue // This device has no parent
		}

		parentDevice, found := deviceMapBySerial[parentSerial]
		if !found {
			r.logger.Errorf("RECONCILER (Pass 2): Parent device with serial %s not found for child %s", parentSerial, dev.Spec.SerialNumber)
			continue
		}

		// If the ParentID is already correct, skip update
		if dev.Spec.ParentID == parentDevice.GetUID() {
			continue
		}

		// Link the child to the parent
		r.logger.Infof("RECONCILER (Pass 2): Linking %s (UID: %s) to parent %s (UID: %s)",
			dev.Spec.SerialNumber, dev.GetUID(), parentDevice.Spec.SerialNumber, parentDevice.GetUID())

		dev.Spec.ParentID = parentDevice.GetUID()
		dev.Metadata.UpdatedAt = time.Now()

		if err := r.client.Update(ctx, dev); err != nil {
			r.logger.Errorf("RECONCILER (Pass 2): Failed to update parent link for %s: %v", dev.Spec.SerialNumber, err)
		} else {
			linksUpdated++
		}
	}
	// --- END PAYLOAD PROCESSING ---

	// 4. Set phase to "Completed"
	snapshot.Status.Phase = "Completed"
	snapshot.Status.Message = fmt.Sprintf("Snapshot processed. %d devices created/updated. %d parent links updated.", processedCount, linksUpdated)
	snapshot.Status.Ready = true
	if err := r.client.Update(ctx, &snapshot); err != nil {
		return reconcile.Result{}, fmt.Errorf("failed to update snapshot status to Completed: %w", err)
	}

	r.logger.Infof("RECONCILER: Successfully reconciled %s", snapshot.GetName())

	return reconcile.Result{}, nil
}

// createNewDevice is a helper to build and save a new device
func (r *SnapshotReconciler) createNewDevice(ctx context.Context, spec device.DeviceSpec) (*device.Device, error) {
	newDevice := &device.Device{
		Resource: fabResource.Resource{
			APIVersion:    "v1",
			Kind:          "Device",
			SchemaVersion: "v1",
		},
		Spec: spec,
	}

	uid, err := fabResource.GenerateUIDForResource("Device")
	if err != nil {
		return nil, fmt.Errorf("failed to generate UID for device: %w", err)
	}
	now := time.Now()
	newDevice.Metadata.UID = uid
	newDevice.Metadata.Name = spec.SerialNumber // Use serial as name
	newDevice.Metadata.CreatedAt = now
	newDevice.Metadata.UpdatedAt = now

	if err := r.client.Create(ctx, newDevice); err != nil {
		return nil, fmt.Errorf("failed to create device %s: %w", spec.SerialNumber, err)
	}

	return newDevice, nil
}

// buildDeviceMapBySerial fetches all devices and creates a map of [SerialNumber] -> *Device
func (r *SnapshotReconciler) buildDeviceMapBySerial(ctx context.Context) (map[string]*device.Device, error) {
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

// failSnapshot is a helper to update the snapshot's status to Error
func (r *SnapshotReconciler) failSnapshot(ctx context.Context, snapshot *discoverysnapshot.DiscoverySnapshot, message string, err error) (reconcile.Result, error) {
	snapshot.Status.Phase = "Error"
	snapshot.Status.Message = fmt.Sprintf("%s: %v", message, err)
	if updateErr := r.client.Update(ctx, snapshot); updateErr != nil {
		return reconcile.Result{}, updateErr
	}
	// Return the original error to trigger a retry
	return reconcile.Result{}, err
}