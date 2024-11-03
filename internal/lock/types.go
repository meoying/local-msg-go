package dlock

import "context"

type Lock interface {
	Lock(ctx context.Context) error
	Unlock(ctx context.Context) error

	// Refresh 续约，延长过期时间
	Refresh(ctx context.Context) error
}
