// internal/replicator/replicator.go

// логика чтения WAL
package replicator

import (
	"context"

	"github.com/Tkach360/tk_cdc/internal/config"
)

type Replicator struct {
}

func New(cfg *config.Config) (*Replicator, error) {
	// TODO: сделать replicator.New
	return &Replicator{}, nil
}

// основной цикл работы replicator
// - читает WAL
// - обновляет кеш
func (r *Replicator) Run(ctx context.Context) error {
	// TODO: сделать метод Replicator.Run
	return nil
}
