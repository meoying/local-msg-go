package glock

import "github.com/ecodeclub/ekit/bean/option"

type Mode string

func WithTableName(tableName string) option.Option[Lock] {
	return func(t *Lock) {
		t.tableName = tableName
	}
}

func WithMode(mode string) option.Option[Lock] {
	return func(t *Lock) {
		t.mode = mode
	}
}
