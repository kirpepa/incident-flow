package main

import (
	"context"
	"log"
	"time"

	"github.com/kirpepa/incident-flow/internal/config"
	"github.com/kirpepa/incident-flow/internal/database"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	configuration, err := config.Load()
	if err != nil {
		log.Fatalf("load configuration: %v", err)
	}
	store, err := database.Open(ctx, configuration.DatabaseURL)
	if err != nil {
		log.Fatalf("open database: %v", err)
	}
	defer store.Close()
	tenantID, err := store.BootstrapTenant(ctx, database.TenantSeed{
		Slug:               configuration.TenantSlug,
		Name:               configuration.TenantName,
		APIKey:             configuration.APIKey,
		NotificationTarget: configuration.NotificationTarget,
	})
	if err != nil {
		log.Fatalf("seed demo tenant: %v", err)
	}
	log.Printf("demo tenant seeded: %s", tenantID)
}
