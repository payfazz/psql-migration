package migration

import (
	"context"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
)

type Item struct {
	ID   string
	Hash string
}

func (m *Migration) All() []Item {
	var r []Item
	for _, e := range m.entries {
		r = append(r, Item{
			ID:   e.id,
			Hash: e.hash,
		})
	}
	return r
}

var bgCtx = context.Background()

func setupConn(target string, onNestedTx func()) (*pgx.Conn, error) {
	config, err := pgx.ParseConfig(target)
	if err != nil {
		return nil, err
	}
	if onNestedTx != nil {
		config.OnNotice = func(pc *pgconn.PgConn, n *pgconn.Notice) {
			if n.Code == "25001" {
				onNestedTx()
			}
		}
	}

	conn, err := pgx.ConnectConfig(bgCtx, config)
	if err != nil {
		return nil, err
	}
	connMoved := false
	defer func() {
		if !connMoved {
			conn.Close(bgCtx)
		}
	}()

	if _, err := conn.Exec(bgCtx, ``+
		`create schema if not exists go_migration;`+
		`create table if not exists go_migration.meta`+
		`(id text primary key, hash text, at timestamp with time zone default now())`,
	); err != nil {
		return nil, err
	}

	connMoved = true
	return conn, nil
}
