package migration

import (
	"context"
	"database/sql"
	"fmt"

	pg_query "github.com/lfittl/pg_query_go"
)

// HashError indicate that the hash for given statements doesn't match with hash in database
type HashError struct {
	StatementIndex int
	StatementHash  string
	ExpectedHash   string
}

func (e *HashError) Error() string {
	return "hash doesn't not match"
}

// InvalidAppIDError indicate that given application id and application id on database doesn't not match
type InvalidAppIDError struct {
	AppID string
}

func (e *InvalidAppIDError) Error() string {
	return "application id doesn't not match"
}

const stmtHashKeyFormat = "stmt_hash_%d"

// Migrate do the sql migration
func Migrate(ctx context.Context, db *sql.DB, appID string, statements []string) error {
	return migrate(ctx, db, appID, statements, false)
}

// DryRun do the sql migration but do not commit the changes, just check for error
func DryRun(ctx context.Context, db *sql.DB, appID string, statements []string) error {
	return migrate(ctx, db, appID, statements, true)
}

func migrate(ctx context.Context, db *sql.DB, appID string, statements []string, dryrun bool) error {
	if appID == "" {
		panic(fmt.Errorf("appID cannot be empty string"))
	}

	if _, err := db.ExecContext(ctx, ``+
		`create table if not exists `+
		`__meta(key text primary key, value text);`+

		`with d(k, v) as (values ('application_id', ''), ('user_version', '0')) `+
		`insert into __meta(key, value) `+
		`select k, v from d where not exists (select 1 from __meta where key = k);`,
	); err != nil {
		return err
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, ``+
		`begin isolation level serializable;`+
		`lock table __meta in access exclusive mode;`,
	); err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
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
		return &InvalidAppIDError{AppID: curAppID}
	}

	var userVersion int
	if err := conn.QueryRowContext(ctx,
		"select value from __meta where key='user_version';",
	).Scan(&userVersion); err != nil {
		return err
	}

	for i := 0; i < userVersion; i++ {
		key := fmt.Sprintf(stmtHashKeyFormat, i)
		statement := statements[i]
		statementHash, err := computeHash(statement)
		if err != nil {
			return &HashError{StatementIndex: i}
		}

		var expectedHash string
		if err := conn.QueryRowContext(ctx,
			"select value from __meta where key=$1;",
			key,
		).Scan(&expectedHash); err != nil && err != sql.ErrNoRows {
			return err
		}
		if expectedHash == "" {
			if _, err := conn.ExecContext(ctx,
				"insert into __meta(key, value) values($1, $2);",
				key, statementHash,
			); err != nil {
				return err
			}
			expectedHash = statementHash
		}

		if expectedHash != statementHash {
			return &HashError{
				StatementIndex: i,
				StatementHash:  statementHash,
				ExpectedHash:   expectedHash,
			}
		}
	}

	for ; userVersion < len(statements); userVersion++ {
		statement := statements[userVersion]
		if _, err := conn.ExecContext(ctx, statement); err != nil {
			return err
		}

		computedHash, err := computeHash(statement)
		if err != nil {
			return &HashError{StatementIndex: userVersion}
		}

		key := fmt.Sprintf(stmtHashKeyFormat, userVersion)
		if _, err := conn.ExecContext(ctx,
			"insert into __meta(key, value) values($1, $2);",
			key, computedHash,
		); err != nil {
			return err
		}
	}

	if _, err := conn.ExecContext(ctx,
		"update __meta set value=$1 where key='user_version';",
		userVersion,
	); err != nil {
		return err
	}

	if dryrun {
		return nil
	}

	if _, err := conn.ExecContext(ctx, "commit;"); err != nil {
		return err
	}
	committed = true

	return nil
}

func computeHash(input string) (string, error) {
	return pg_query.FastFingerprint(input)
}
