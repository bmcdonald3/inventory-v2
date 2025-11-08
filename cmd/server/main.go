package main

import (
	"context"
	"log"
	"os"

	"github.com/openchami/fabrica/pkg/events"
	"github.com/openchami/fabrica/pkg/reconcile"
	"github.com/openchami/fabrica/pkg/storage"

	// Import the generated server and your new reconciler
	"github.com/user/inventory-api/internal/reconciliation"
	"github.com/user/inventory-api/internal/server"

	// This blank import ensures your resource code is registered
	_ "github.com/user/inventory-api/pkg/resources/device"
	_ "github.com/user/inventory-api/pkg/resources/discoverysnapshot"
)

func main() {
	// --- 1. Standard Storage Setup ---
	// Use the default in-memory storage for now.
	store := storage.NewInMemoryStorage()
	logger := log.New(os.Stdout, "[inventory-api] ", log.LstdFlags)

	// --- 2. NEW: Event Bus Setup ---
	// Create the event bus that links the API to the reconciler
	eventBus := events.NewInMemoryEventBus(1000, 10)
	eventBus.Start()
	defer eventBus.Close()

	// --- 3. NEW: Controller Setup ---
	// Create the main reconciliation controller
	controller := reconcile.NewController(eventBus, store)

	// Create and register your new SnapshotReconciler
	snapshotReconciler := reconciliation.NewSnapshotReconciler(eventBus, store, logger)
	if err := controller.RegisterReconciler(snapshotReconciler); err != nil {
		logger.Fatalf("Failed to register reconciler: %v", err)
	}

	// Start the controller in the background
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		if err := controller.Start(ctx); err != nil {
			logger.Printf("Reconciliation controller error: %v", err)
		}
	}()
	logger.Println("Reconciliation controller started.")

	// --- 4. Standard App Server Setup ---
	// Create the Fabrica app, passing in the *same* storage and event bus
	app, err := server.NewApp(server.WithStorage(store), server.WithEventBus(eventBus))
	if err != nil {
		logger.Fatalf("Failed to create server app: %v", err)
	}

	// --- 5. Run the Server ---
	logger.Println("Starting API server on :8080...")
	if err := app.Listen(":8080"); err != nil {
		logger.Fatalf("Server error: %v", err)
	}
}