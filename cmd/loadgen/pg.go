package main

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

func openPG(dsn string) (*pgxpool.Pool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return pgxpool.New(ctx, dsn)
}

func contextBG() context.Context { return context.Background() }

func toTextArray(ids []string) []string { return ids }
