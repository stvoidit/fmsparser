package fmsparser

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v4/pgxpool"
)

// NewStore - инициализация экземпляра соединения к БД, умеет в pool соединений,
// но должен быть равен пулу заданному в конфиге постгреса. Если будет больше,
// то рано или поздно упадет, не получив от него новый коннект.
// В ином случае - все вызовы Acquire должны по умному ждать свою очередь
func NewStore(host, port, user, password, dbname string) (*Store, error) {
	url := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)
	poolConfig, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	pool, err := pgxpool.ConnectConfig(context.Background(), poolConfig)
	if err != nil {
		return nil, err
	}
	return &Store{pool}, nil
}

// Store - экземпляр пула соединений к БД
type Store struct {
	db *pgxpool.Pool
}

// Close - close connection, be graceful be happy
func (s *Store) Close() {
	if s != nil && s.db != nil {
		s.db.Close()
	}
}

// Cursor - get base connection and contexxt
func (s *Store) Cursor() (*pgxpool.Conn, context.Context, error) {
	if s == nil {
		return nil, nil, errors.New("store not initialized")
	}
	ctx := context.Background()
	conn, err := s.db.Acquire(ctx)
	return conn, ctx, err
}
