package database

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kirpepa/incident-flow/internal/domain"
)

type Store struct {
	pool *pgxpool.Pool
}

type TenantSeed struct {
	Slug               string
	Name               string
	APIKey             string
	NotificationTarget string
}

func Open(ctx context.Context, databaseURL string) (*Store, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database URL: %w", err)
	}
	config.MaxConns = 20
	config.MinConns = 2
	config.MaxConnLifetime = 30 * time.Minute
	config.MaxConnIdleTime = 5 * time.Minute
	config.HealthCheckPeriod = 30 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("create database pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() { s.pool.Close() }

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

func (s *Store) Pool() *pgxpool.Pool { return s.pool }

func (s *Store) BootstrapTenant(ctx context.Context, seed TenantSeed) (uuid.UUID, error) {
	hash := sha256.Sum256([]byte(seed.APIKey))
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return uuid.Nil, fmt.Errorf("begin tenant bootstrap: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	tenantID := uuid.New()
	if err := tx.QueryRow(ctx, `
		INSERT INTO tenants (id, slug, name, api_key_hash)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (slug) DO UPDATE
		SET name=EXCLUDED.name, api_key_hash=EXCLUDED.api_key_hash
		RETURNING id`, tenantID, seed.Slug, seed.Name, hash[:]).Scan(&tenantID); err != nil {
		return uuid.Nil, fmt.Errorf("upsert demo tenant: %w", err)
	}

	policyID := uuid.New()
	if err := tx.QueryRow(ctx, `
		INSERT INTO escalation_policies (id, tenant_id, name, is_default)
		VALUES ($1, $2, 'Default', true)
		ON CONFLICT (tenant_id, name) DO UPDATE SET is_default=true
		RETURNING id`, policyID, tenantID).Scan(&policyID); err != nil {
		return uuid.Nil, fmt.Errorf("upsert default escalation policy: %w", err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM escalation_policy_steps WHERE policy_id=$1`, policyID); err != nil {
		return uuid.Nil, fmt.Errorf("replace escalation steps: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO escalation_policy_steps (policy_id, step_number, delay_seconds, target_url, max_attempts)
		VALUES ($1, 0, 0, $2, 5), ($1, 1, 10, $2, 5)`, policyID, seed.NotificationTarget); err != nil {
		return uuid.Nil, fmt.Errorf("seed escalation steps: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return uuid.Nil, fmt.Errorf("commit tenant bootstrap: %w", err)
	}
	return tenantID, nil
}

func (s *Store) TenantByAPIKey(ctx context.Context, apiKey string) (uuid.UUID, error) {
	hash := sha256.Sum256([]byte(apiKey))
	var tenantID uuid.UUID
	err := s.pool.QueryRow(ctx, "SELECT id FROM tenants WHERE api_key_hash=$1", hash[:]).Scan(&tenantID)
	if errors.Is(err, pgx.ErrNoRows) {
		return uuid.Nil, domain.ErrUnauthorized
	}
	if err != nil {
		return uuid.Nil, fmt.Errorf("look up tenant: %w", err)
	}
	return tenantID, nil
}
