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
	if err := database.Migrate(ctx, store.Pool()); err != nil {
		log.Fatalf("apply migrations: %v", err)
	}
	log.Print("migrations applied")
}
