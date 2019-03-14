package migration

import (
	"context"
	"database/sql"
	"fmt"
)

// Migrate do the sql migration
func Migrate(ctx context.Context, db *sql.DB, appID string, statements []string) error {
	if appID == "" {
		panic("migrate: invalid params: ApplicationID can't be empty string")
	}

	if _, err := db.ExecContext(ctx, ``+
		`create table if not exists `+
		`__meta(key text primary key, value text);`+

		`insert into __meta(key, value) values `+
		`('application_id', ''), `+
		`('user_version', '0') `+
		`on conflict do nothing;`,
	); err != nil {
		return err
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx,
		"begin isolation level serializable;",
	); err != nil {
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
		return err
	}
	if curAppID == "" {
		if _, err := conn.ExecContext(ctx,
			"update __meta set value=$1 where key='application_id';",
			appID,
		); err != nil {
			return err
		}
		curAppID = appID
	}
	if curAppID != appID {
		return fmt.Errorf("Invalid application_id on database")
	}

	var userVersion int
	if err := conn.QueryRowContext(ctx,
		"select value from __meta where key='user_version';",
	).Scan(&userVersion); err != nil {
		return err
	}
	for ; userVersion < len(statements); userVersion++ {
		statement := statements[userVersion]
		if _, err := conn.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	if _, err := conn.ExecContext(ctx,
		"update __meta set value=$1 where key='user_version';",
		userVersion,
	); err != nil {
		return err
	}

	if _, err := conn.ExecContext(ctx, "commit"); err != nil {
		return err
	}
	commited = true

	return nil
}
