package provider

import "context"

type DataProvider interface {
	SaveOne(ctx context.Context, data Kv) error
	TakeOne(ctx context.Context, data Kv) (any, error)
	FinishOne(ctx context.Context, data Kv) error
	NextPosition() error
	Close() error

	GenerateProcessingPK() (string, error)
	GenerateFallbackPK(pk string) (string, error)
}
