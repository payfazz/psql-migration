package migration

import (
	"context"
	"database/sql"
	"fmt"
	"log"
)

type MigrateParam struct {
	Database      *sql.DB
	ErrorLog      *log.Logger
	ApplicationID string
	Statements    []string
}

func Migrate(ctx context.Context, p MigrateParam) error {
	if p.ApplicationID == "" {
		panic("migrate: invalid params: ApplicationID can't be empty string")
	}

	p.Database.ExecContext(ctx, ``+
		`create table if not exists `+
		`__meta(key text primary key, value text);`+

		`insert into __meta(key, value) values `+
		`('application_id', ''), `+
		`('user_version', '0') `+
		`on conflict do nothing;`,
	)

	conn, err := p.Database.Conn(ctx)
	if err != nil {
		p.ErrorLog.Println(err)
		return err
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx,
		"begin isolation level serializable;",
	); err != nil {
		p.ErrorLog.Println(err)
		return err
	}
	commited := false
	defer func() {
		if !commited {
			conn.ExecContext(ctx, "rollback;")
		}
	}()

	var curAppID string
	if err := conn.QueryRowContext(ctx,
		"select value from __meta where key='application_id';",
	).Scan(&curAppID); err != nil {
		p.ErrorLog.Println(err)
		return err
	}
	if curAppID == "" {
		if _, err := conn.ExecContext(ctx,
			"update __meta set value=$1 where key='application_id';",
			p.ApplicationID,
		); err != nil {
			p.ErrorLog.Println(err)
			return err
		}
		curAppID = p.ApplicationID
	}
	if curAppID != p.ApplicationID {
		return fmt.Errorf("Invalid application_id on database")
	}

	var userVersion int
	if err := conn.QueryRowContext(ctx,
		"select value from __meta where key='user_version';",
	).Scan(&userVersion); err != nil {
		p.ErrorLog.Println(err)
		return err
	}
	for ; userVersion < len(p.Statements); userVersion++ {
		statement := p.Statements[userVersion]
		if _, err := conn.ExecContext(ctx, statement); err != nil {
			p.ErrorLog.Println(err)
			return err
		}
	}
	if _, err := conn.ExecContext(ctx,
		"update __meta set value=$1 where key='user_version';",
		userVersion,
	); err != nil {
		p.ErrorLog.Println(err)
		return err
	}

	if _, err := conn.ExecContext(ctx, "commit"); err != nil {
		p.ErrorLog.Println(err)
		return err
	}
	commited = true

	return nil
}
