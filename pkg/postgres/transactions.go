package postgres

import (
	"context"
	"errors"

	"github.com/jmoiron/sqlx"
)

var (
	ErrUnableGetTransactionFromContext = errors.New("unable get transaction from context")
	ErrNotInContextualTxStatement = errors.New("unable to commit transaction statement - not in tx statement")
)

type transactionCtxKey string

var transactionKey = transactionCtxKey("transaction")

// BeginTx ....
func (c *Connection) BeginTx() (*sqlx.Tx, error) {
	return c.Dbx.Beginx()
}

// BeginContextualTxStatement ....
func (c *Connection) BeginContextualTxStatement(ctx context.Context) (context.Context, error) {
	txStmt, err := c.Dbx.Beginx()
	if err != nil {
		return nil, err
	}

	return context.WithValue(ctx, transactionKey, txStmt), nil
}

// CommitContextualTxStatement ....
func (c *Connection) CommitContextualTxStatement(ctx context.Context) error {
	tx, inTransaction := ctx.Value(transactionKey).(*sqlx.Tx)
	if !inTransaction {
		return ErrNotInContextualTxStatement
	}

	return tx.Commit()
}

// RollbackContextualTxStatement ....
func (c *Connection) RollbackContextualTxStatement(ctx context.Context) error {
	tx, inTransaction := ctx.Value(transactionKey).(*sqlx.Tx)
	if !inTransaction {
		return ErrNotInContextualTxStatement
	}

	return tx.Rollback()
}

func (c *Connection) TryWithTransaction(ctx context.Context, fn func(stmt sqlx.Ext) error) error {
	stmt := sqlx.Ext(c.Dbx)

	tx, inTransaction := ctx.Value(transactionKey).(*sqlx.Tx)
	if inTransaction {
		stmt = tx
	}

	return fn(stmt)
}

func (c *Connection) MustWithTransaction(ctx context.Context, fn func(stmt *sqlx.Tx) error) error {
	tx, inTransaction := ctx.Value(transactionKey).(*sqlx.Tx)
	if inTransaction {
		return fn(tx)
	}

	return ErrUnableGetTransactionFromContext
}