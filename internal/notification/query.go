package notification

import (
	"context"
	"errors"
	"fmt"
	"time"
)

var (
	ErrTaskNotFound = errors.New("notification not found")
	ErrNotDead      = errors.New("notification is not dead")
)

type View struct {
	ID             string     `json:"notification_id"`
	DestinationID  string     `json:"destination_id"`
	Status         Status     `json:"status"`
	AttemptCount   int        `json:"attempt_count"`
	NextAttemptAt  *time.Time `json:"next_attempt_at,omitempty"`
	LastHTTPStatus int        `json:"last_http_status,omitempty"`
	LastErrorCode  string     `json:"last_error_code,omitempty"`
	LastError      string     `json:"last_error_message,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
	DeliveredAt    *time.Time `json:"delivered_at,omitempty"`
	DeadAt         *time.Time `json:"dead_at,omitempty"`
}

type QueryRepository interface {
	GetForCaller(context.Context, string, string) (Task, error)
	ReplayDeadAtomic(context.Context, string, time.Time, string) (bool, error)
}
type QueryService struct{ repository QueryRepository }

func NewQueryService(repository QueryRepository) *QueryService {
	return &QueryService{repository: repository}
}
func (service *QueryService) Get(ctx context.Context, callerID, notificationID string) (View, error) {
	task, err := service.repository.GetForCaller(ctx, callerID, notificationID)
	if err != nil {
		return View{}, err
	}
	return View{ID: task.ID, DestinationID: task.Snapshot.DestinationID, Status: task.Status, AttemptCount: task.AttemptCount, NextAttemptAt: task.NextAttemptAt,
		LastHTTPStatus: task.LastHTTPStatus, LastErrorCode: task.LastErrorCode, LastError: task.LastError, CreatedAt: task.CreatedAt, UpdatedAt: task.UpdatedAt,
		DeliveredAt: task.DeliveredAt, DeadAt: task.DeadAt}, nil
}

type ReplayService struct {
	repository QueryRepository
	now        func() time.Time
	newID      func() string
}

func NewReplayService(repository QueryRepository, now func() time.Time, newID func() string) *ReplayService {
	return &ReplayService{repository: repository, now: now, newID: newID}
}
func (service *ReplayService) Replay(ctx context.Context, _ string, notificationID string) error {
	applied, err := service.repository.ReplayDeadAtomic(ctx, notificationID, service.now(), service.newID())
	if err != nil {
		return fmt.Errorf("replay notification: %w", err)
	}
	if !applied {
		return ErrNotDead
	}
	return nil
}
