package main

import (
	"context"
	"log"

	"github.com/openchami/fabrica/pkg/events"
	"github.com/openchami/fabrica/pkg/reconcile"
	"github.com/openchami/fabrica/pkg/server" // This import should be correct

	// Use the full module path
	"github.com/user/inventory-api/internal/reconciliation"
	"github.com/user/inventory-api/internal/storage" // Import your storage package

	// Blank imports to register resources
	_ "github.com/user/inventory-api/pkg/resources/device"
	_ "github.com/user/inventory-api/pkg/resources/discoverysnapshot"
)

func main() {
	// --- 1. Setup Logger ---
	// Use the reconciler's default logger, as it satisfies the interface
	logger := reconcile.NewDefaultLogger()

	// --- 2. Setup Storage (Using your storage.go) ---
	// Initialize the file backend as defined in your storage.go
	if err := storage.InitFileBackend("./data"); err != nil {
		logger.Fatalf("Failed to initialize storage backend: %v", err)
	}
	logger.Infof("Storage backend initialized.")

	// --- 3. Setup Event Bus ---
	eventBus := events.NewInMemoryEventBus(1000, 10)
	eventBus.Start()
	defer eventBus.Close()
	logger.Infof("Event bus started.")

	// --- 4. Setup Reconciliation Controller ---
	// Create the StorageClient adapter for the reconciler
	apiStorageClient := storage.NewStorageClient()

	// Create the controller, passing the client adapter
	controller := reconcile.NewController(eventBus, apiStorageClient)

	// Create and register your reconciler
	snapshotReconciler := reconciliation.NewSnapshotReconciler(eventBus, apiStorageClient, logger)
	if err := controller.RegisterReconciler(snapshotReconciler); err != nil {
		logger.Fatalf("Failed to register reconciler: %v", err)
	}

	// Start the controller in the background
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		if err := controller.Start(ctx); err != nil {
			logger.Errorf("Reconciliation controller error: %v", err)
		}
	}()
	logger.Infof("Reconciliation controller started.")

	// --- 5. Setup and Run API Server ---
	// Create the Fabrica app.
	// We pass the *raw backend* to the server (for its handlers)
	// and the *typed client* to the controller (for reconciliation).
	app, err := server.NewApp(
		server.WithStorage(storage.Backend),
		server.WithEventBus(eventBus),
	)
	if err != nil {
		log.Fatalf("Failed to create server app: %v", err)
	}

	logger.Infof("Starting API server on :8080...")
	if err := app.Listen(":8080"); err != nil {
		log.Fatalf("Server error: %v", err)
	}
}