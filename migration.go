package migration

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
)

// HashError indicate that the hash for given statements doesn't match with hash in database
type HashError struct {
	StatementIndex     int
	NormalizeStatement string
	ComputedHash       string
	ExpectedHash       string
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
		return fmt.Errorf("appID cannot be empty string")
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
		normalizeStatement := normalize(statement)
		computedHash := computeHash(normalizeStatement)
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
				key, computedHash,
			); err != nil {
				return err
			}
			expectedHash = computedHash
		}
		if expectedHash != computedHash {
			return &HashError{
				StatementIndex:     i,
				NormalizeStatement: normalizeStatement,
				ExpectedHash:       expectedHash,
				ComputedHash:       computedHash,
			}
		}
	}

	for ; userVersion < len(statements); userVersion++ {
		statement := statements[userVersion]
		if _, err := conn.ExecContext(ctx, statement); err != nil {
			return err
		}

		computedHash := computeHash(normalize(statement))
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

func normalize(input string) string {
	inputLines := strings.Split(input, "\n")
	outputLines := make([]string, 0, len(inputLines))

	first := true
	for _, line := range inputLines {
		if first {
			tmp := strings.TrimSpace(line)
			if len(tmp) == 0 || strings.HasPrefix(tmp, "--") {
				continue
			}
		}
		first = false
		outputLines = append(outputLines, line)
	}

	for i := len(outputLines) - 1; i >= 0; i-- {
		tmp := strings.TrimSpace(outputLines[i])
		if len(tmp) == 0 || strings.HasPrefix(tmp, "--") {
			outputLines = outputLines[:i]
		} else {
			break
		}
	}

	return strings.Join(outputLines, "\n")
}

func computeHash(input string) string {
	output := sha256.Sum256([]byte(input))
	return hex.EncodeToString(output[:])
}
