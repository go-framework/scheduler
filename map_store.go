package scheduler

import (
	"context"
	"sync"

	"github.com/json-iterator/go"
)

// MapStore implement RealtimeStore interface.
type MapStore struct {
	sync.Map
}

// Initialize Store with context.
func (this MapStore) Init(context.Context) error {
	return nil
}

// Get Runner from Store with id.
func (this MapStore) Get(id string) (Runner, error) {
	if face, ok := this.Map.Load(id); ok {
		if runner, ok := face.(Runner); ok {
			return runner, nil
		}
	}

	return nil, InvalidRunner
}

// Put Runner into Store.
func (this MapStore) Put(runner Runner) error {
	this.Map.Store(runner.GetId(), runner)
	return nil
}

// Delete Runner in Store.
func (this MapStore) Delete(runner Runner) error {
	this.Map.Delete(runner.GetId())
	return nil
}

// List Runners from Store with context and record count and offset.
func (this MapStore) List(ctx context.Context, count, offset uint64) (runners []Runner, err error) {
	this.Map.Range(func(key, value interface{}) bool {
		if runner, ok := value.(Runner); ok {
			runners = append(runners, runner)
		}

		return true
	})

	return
}

// Implement Marshaler.
func (this MapStore) MarshalJSON() ([]byte, error) {
	var runners []interface{}

	this.Range(func(key, value interface{}) bool {
		runners = append(runners, value)

		return true
	})

	return jsoniter.Marshal(runners)
}

// New map RealtimeStore.
func NewMapStore() RealtimeStore {
	return &MapStore{}
}
