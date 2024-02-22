package internal

import "errors"

var (
	ErrVersionMismatch = errors.New("version mismatch")
	ErrResourceExists  = errors.New("resource exists")
)

type Cond[T any] func(curr VersionContainer[T]) error

func WhenVersionMatches[T any](target int64) Cond[T] {
	return func(curr VersionContainer[T]) error {
		if curr.Version != target {
			return ErrVersionMismatch
		}
		return nil
	}
}

func WhenNotExists[T any]() Cond[T] {
	return func(curr VersionContainer[T]) error {
		if curr.Version != 0 {
			return ErrResourceExists
		}
		return nil
	}
}
