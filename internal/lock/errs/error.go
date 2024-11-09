package errs

import "errors"

var (
	// ErrLockNotHold 一般是出现在你预期你本来持有锁，结果却没有持有锁的地方
	// 比如说当你尝试释放锁的时候，可能得到这个错误
	// 这一般意味着有人绕开了分布式锁的控制，直接操作了 Redis
	ErrLockNotHold = errors.New("未持有锁")
	// ErrLocked 锁被人持有了，一般是加锁的时候发现的
	ErrLocked = errors.New("加锁失败，锁被人持有")
)
