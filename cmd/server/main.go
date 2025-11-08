package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	// --- ADDED: Fabrica and Reconciliation imports ---
	"github.com/openchami/fabrica/pkg/events"
	"github.com/openchami/fabrica/pkg/reconcile"
	fabrica_storage "github.com/openchami/fabrica/pkg/storage"
	"encoding/json"
	"github.com/user/inventory-api/pkg/resources/discoverysnapshot"

	// --- Your existing storage and NEW reconciler import ---
	internal_storage "github.com/user/inventory-api/internal/storage"
	"github.com/user/inventory-api/internal/reconciliation"
	
	// --- Blank imports to register resources ---
	_ "github.com/user/inventory-api/pkg/resources/device"
	_ "github.com/user/inventory-api/pkg/resources/discoverysnapshot"
)

// --- Global variables for handlers ---
var (
	globalStorage  fabrica_storage.StorageBackend
	globalEventBus events.EventBus // <<< ADDED
)

// SetStorageBackend sets the global storage backend
func SetStorageBackend(s fabrica_storage.StorageBackend) {
	globalStorage = s
}

// SetEventBus sets the global event bus
func SetEventBus(eb events.EventBus) { // <<< ADDED
	globalEventBus = eb
}
// --- End global variables ---

// Config holds all configuration for the service
type Config struct {
	// (Your Config struct remains unchanged)
	Port         int    `mapstructure:"port"`
	Host         string `mapstructure:"host"`
	ReadTimeout  int    `mapstructure:"read_timeout"`
	WriteTimeout int    `mapstructure:"write_timeout"`
	IdleTimeout  int    `mapstructure:"idle_timeout"`
	DataDir      string `mapstructure:"data_dir"`
	Debug        bool   `mapstructure:"debug"`
}

// DefaultConfig returns the default configuration
func DefaultConfig() *Config {
	// (This function remains unchanged)
	return &Config{
		Port:         8080,
		Host:         "0.0.0.0",
		ReadTimeout:  15,
		WriteTimeout: 15,
		IdleTimeout:  60,
		DataDir:      "./data",
		Debug:        false,
	}
}

var (
	cfgFile string
	config  *Config
)

func main() {
	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

var rootCmd = &cobra.Command{
	Use:   "inventory-api",
	Short: "",
	Long:  `inventory-api - A Fabrica-generated OpenCHAMI service`,
	RunE:  runServer,
}

var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "Start the inventory-api server",
	Long:  `Start the inventory-api HTTP server with the configured options`,
	RunE:  runServer,
}

func init() {
	// (Your init() function remains unchanged)
	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.inventory-api.yaml)")
	rootCmd.PersistentFlags().Bool("debug", false, "Enable debug logging")
	serveCmd.Flags().IntP("port", "p", 8080, "Port to listen on")
	serveCmd.Flags().String("host", "0.0.0.0", "Host to bind to")
	serveCmd.Flags().Int("read-timeout", 15, "Read timeout in seconds")
	serveCmd.Flags().Int("write-timeout", 15, "Write timeout in seconds")
	serveCmd.Flags().Int("idle-timeout", 60, "Idle timeout in seconds")
	serveCmd.Flags().String("data-dir", "./data", "Directory for file storage")
	viper.BindPFlags(serveCmd.Flags())
	viper.BindPFlags(rootCmd.PersistentFlags())
	rootCmd.AddCommand(serveCmd)
	rootCmd.AddCommand(versionCmd)
}

func initConfig() {
	// (Your initConfig() function remains unchanged)
	config = DefaultConfig()
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		home, err := os.UserHomeDir()
		cobra.CheckErr(err)
		viper.AddConfigPath(home)
		viper.AddConfigPath(".")
		viper.SetConfigType("yaml")
		viper.SetConfigName(".inventory-api")
	}
	viper.SetEnvPrefix("INVENTORY-API")
	viper.AutomaticEnv()
	if err := viper.ReadInConfig(); err == nil {
		log.Printf("Using config file: %s", viper.ConfigFileUsed())
	}
	if err := viper.Unmarshal(config); err != nil {
		log.Fatalf("Unable to decode into config struct: %v", err)
	}
	if config.Debug {
		log.SetFlags(log.LstdFlags | log.Lshortfile)
		log.Println("Debug logging enabled")
	}
}

func runServer(cmd *cobra.Command, args []string) error {
	log.Printf("Starting inventory-api server...")
	
	// Create a logger for the reconciliation system
	reconLogger := reconcile.NewDefaultLogger() // <<< ADDED

	// --- 1. Initialize Storage Backend ---
	if err := internal_storage.InitFileBackend(config.DataDir); err != nil {
		return fmt.Errorf("failed to initialize file storage: %w", err)
	}
	storageBackend := internal_storage.Backend
	if storageBackend == nil {
		return fmt.Errorf("storage backend is nil after initialization")
	}
	SetStorageBackend(storageBackend) // This sets globalStorage
	log.Printf("File storage initialized in %s", config.DataDir)

	eventConf := &events.EventConfig{
		Enabled:                true,
		LifecycleEventsEnabled: true,
		ConditionEventsEnabled: true,
		EventTypePrefix:        "io.fabrica", // Default prefix
		Source:                 "inventory-api",
	}
	events.SetEventConfig(eventConf)
	log.Println("Event system configured and enabled.")

	// --- 2. Initialize Event Bus --- (ADDED BACK)
	eventBus := events.NewInMemoryEventBus(1000, 10)
	eventBus.Start()
	defer eventBus.Close()
	SetEventBus(eventBus) // Set the global for handlers
	log.Println("Event bus started.")

	// --- 4. Register Reconcilers --- (ADDED BACK)
	// The reconciler needs the *typed client* from your storage.go
	apiStorageClient := internal_storage.NewStorageClient()
	controller := reconcile.NewController(eventBus, storageBackend)
	log.Println("Reconciliation controller initialized.")
	snapshotReconciler := reconciliation.NewSnapshotReconciler(eventBus, apiStorageClient, reconLogger)
	if err := controller.RegisterReconciler(snapshotReconciler); err != nil {
		log.Fatalf("Failed to register reconciler: %v", err)
	}

	// --- 5. Start Controller --- (ADDED BACK)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		log.Println("Reconciliation controller starting...")
		if err := controller.Start(ctx); err != nil {
			log.Printf("Reconciliation controller error: %v", err)
		}
	}()

	// --- 6. Setup Router ---
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	if config.Debug {
		r.Mount("/debug", middleware.Profiler())
	}

	RegisterGeneratedRoutes(r) // This is the Fabrica "server"
	r.Get("/health", healthHandler)

	r.Post("/debug-event", debugEventHandler)

	// --- 7. Create and Start HTTP Server ---
	addr := fmt.Sprintf("%s:%d", config.Host, config.Port)
	server := &http.Server{
		Addr:         addr,
		Handler:      r,
		ReadTimeout:  time.Duration(config.ReadTimeout) * time.Second,
		WriteTimeout: time.Duration(config.WriteTimeout) * time.Second,
		IdleTimeout:  time.Duration(config.IdleTimeout) * time.Second,
	}

	go func() {
		log.Printf("Server starting on %s", addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed: %v", err)
		}
	}()

	// --- 8. Wait for Interrupt (Graceful Shutdown) ---
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Server shutting down...")

	// --- 9. Shut Down Controller --- (ADDED BACK)
	controller.Stop()
	log.Println("Reconciliation controller stopped.")

	// --- 10. Shut Down HTTP Server ---
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		return fmt.Errorf("server forced to shutdown: %w", err)
	}

	log.Println("Server exited")
	return nil
}

func debugEventHandler(w http.ResponseWriter, r *http.Request) {
	log.Println("--- DEBUG: Manual Event Trigger Received ---")

	// 1. Create a dummy snapshot resource
	// (This mimics what the real handler should do)
	testSnapshot := &discoverysnapshot.DiscoverySnapshot{}
	testSnapshot.Metadata.Name = "debug-snapshot-01"
	testSnapshot.Metadata.UID = "debug-" + fmt.Sprintf("%d", time.Now().Unix())
	
	// Create dummy rawData
	rawJSON, _ := json.Marshal(map[string]string{"debug": "true"})
	testSnapshot.Spec.RawData = rawJSON

	// 2. Save it to storage
	// (The real handler does this)
	if err := internal_storage.SaveDiscoverySnapshot(context.Background(), testSnapshot); err != nil {
		log.Printf("--- DEBUG: Failed to save snapshot: %v ---", err)
		http.Error(w, "Failed to save", 500)
		return
	}
	log.Printf("--- DEBUG: Saved dummy snapshot %s ---", testSnapshot.GetUID())

	// 3. Manually publish the "created" event
	// (This is what the generated handler is failing to do)
	event, err := events.NewResourceEvent(
		"io.fabrica.discoverysnapshot.created", // Default event type
		"DiscoverySnapshot",
		testSnapshot.GetUID(),
		testSnapshot,
	)
	if err != nil {
		log.Printf("--- DEBUG: Failed to create event: %v ---", err)
		http.Error(w, "Failed to create event", 500)
		return
	}

	if globalEventBus == nil {
		log.Println("--- DEBUG: globalEventBus is NIL. This is the problem. ---")
		http.Error(w, "globalEventBus is nil", 500)
		return
	}

	// --- THIS LINE IS CORRECTED ---
	// It compares the error 'err' to 'nil' (the error type), not "nil" (the string).
	if err := globalEventBus.Publish(context.Background(), *event); err != nil {
		log.Printf("--- DEBUG: Event publish error: %v ---", err)
	}
	// --- END CORRECTION ---

	log.Println("--- DEBUG: Event published manually ---")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Debug event published for " + testSnapshot.GetUID()))
}

// Health check handler
func healthHandler(w http.ResponseWriter, r *http.Request) {
	// (This function remains unchanged)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`{"status":"healthy","service":"inventory-api"}`))
}

var versionCmd = &cobra.Command{
	// (This command remains unchanged)
	Use:   "version",
	Short: "Print the version number",
	Long:  `Print the version number of inventory-api`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("inventory-api v1.0.0")
	},
}