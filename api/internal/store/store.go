package store

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dvasconcellos/elo/api/internal/db"
)

type Store interface {
	db.Querier
	ExecTx(ctx context.Context, fn func(*db.Queries) error) error
}

type SQLStore struct {
	*db.Queries
	db *sql.DB
}

func NewStore(database *sql.DB) Store {
	return &SQLStore{
		db:      database,
		Queries: db.New(database),
	}
}

func (store *SQLStore) ExecTx(ctx context.Context, fn func(*db.Queries) error) error {
	tx, err := store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	q := db.New(tx)
	err = fn(q)
	if err != nil {
		if rbErr := tx.Rollback(); rbErr != nil {
			return fmt.Errorf("tx err: %v, rb err: %v", err, rbErr)
		}
		return err
	}

	return tx.Commit()
}
