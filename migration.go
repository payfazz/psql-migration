package migration

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
)

type entry struct {
	id        string
	statement string
	hash      string
}

type Migration struct {
	conn       *pgx.Conn
	entries    []entry
	revEntries map[string]int

	nestedTxDetected bool
}

// New return new Migration connection.
//
// do not forget to call Close.
//
// source must contains exactly one directory, and that directory must contains only *.sql file.
// each sql file must have lowercase name.
//
// the migration is sorted by sql file name.
func New(ctx context.Context, source embed.FS, targetConn string) (*Migration, error) {
	list, err := fs.ReadDir(source, ".")
	if err != nil {
		panic(err)
	}
	if len(list) != 1 || !list[0].IsDir() {
		panic("migration: source should have single dir in the root")
	}

	sub, err := fs.Sub(source, list[0].Name())
	if err != nil {
		panic(err)
	}

	m := &Migration{revEntries: make(map[string]int)}

	if err := fs.WalkDir(sub, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			panic(err)
		}
		if path == "." {
			return nil
		}

		name := d.Name()
		if d.IsDir() {
			panic(fmt.Sprintf("migration: cannot include directory: %s", name))
		}
		if !strings.HasSuffix(name, ".sql") {
			panic(fmt.Sprintf("migration: must ending with .sql: %s", name))
		}
		if strings.ToLower(name) != name {
			panic(fmt.Sprintf("migration: must have lowercase name: %s", name))
		}

		data, err := fs.ReadFile(sub, name)
		if err != nil {
			panic(err)
		}

		stmt := string(data)
		e := entry{name, stmt, hash(stmt)}
		m.entries = append(m.entries, e)

		return nil
	}); err != nil {
		panic(err)
	}

	sort.Slice(m.entries, func(i, j int) bool { return m.entries[i].id < m.entries[j].id })

	for i, e := range m.entries {
		if _, ok := m.revEntries[e.id]; ok {
			panic(fmt.Sprintf("migration: duplicate entry: %s", e.id))
		}
		m.revEntries[e.id] = i
	}

	config, err := pgx.ParseConfig(targetConn)
	if err != nil {
		return nil, err
	}

	config.OnNotice = m.onPgxNotice

	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return nil, err
	}
	connMoved := false
	defer func() {
		if !connMoved {
			conn.Close(ctx)
		}
	}()

	m.conn = conn
	connMoved = true

	return m, nil
}

// Close the underlying connection.
func (m *Migration) Close(ctx context.Context) error {
	return m.conn.Close(ctx)
}

func (m *Migration) onPgxNotice(c *pgconn.PgConn, n *pgconn.Notice) {
	if m.conn.PgConn() != c {
		panic("wrong conn")
	}
	m.nestedTxDetected = n.Code == "25001"
}

func (m *Migration) ensureMetatable(ctx context.Context) error {
	if _, err := m.conn.Exec(ctx, ``+
		`create table if not exists `+
		`__go_migration_meta(id text primary key, hash text, at timestamp with time zone default now())`,
	); err != nil {
		return err
	}
	return nil
}

// Check the current state of the database.
//
// will return list of migration that need to be run.
//
// also will return *MismatchHashError error if the database already execute a migration file
// but it has different hash with source
func (m *Migration) Check(ctx context.Context) ([]string, error) {
	if err := m.ensureMetatable(ctx); err != nil {
		return nil, err
	}

	rows, err := m.conn.Query(ctx, ``+
		`select id, hash from __go_migration_meta`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	alreadyInDB := make(map[string]struct{})
	for rows.Next() {
		var id, hash string
		if err := rows.Scan(&id, &hash); err != nil {
			return nil, err
		}
		i, ok := m.revEntries[id]
		if !ok {
			return nil, &MismatchHashError{ID: id, HashInDB: hash}
		}
		e := m.entries[i]
		if e.hash != hash {
			return nil, &MismatchHashError{ID: id, CurrentHash: e.hash, HashInDB: hash}
		}
		alreadyInDB[id] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	var ret []string
	for _, e := range m.entries {
		if _, ok := alreadyInDB[e.id]; ok {
			continue
		}
		ret = append(ret, e.id)
	}

	return ret, nil
}

// Run the migration.
//
// will return list of migration that executed.
//
// also will return *MismatchHashError error if the database already execute a migration file
// but it has different hash with source
func (m *Migration) Run(ctx context.Context) ([]string, error) {
	// if err := m.ensureMetatable(ctx, db); err != nil {
	// 	return err
	// }

	if _, err := m.conn.Exec(ctx, ``+
		`begin isolation level serializable; `+
		`lock table __go_migration_meta in access exclusive mode;`,
	); err != nil {
		return nil, err
	}
	committed := false
	defer func() {
		if !committed {
			m.conn.Exec(ctx, `rollback;`)
		}
	}()

	list, err := m.Check(ctx)
	if err != nil {
		return nil, err
	}
	for _, l := range list {
		e := m.entries[m.revEntries[l]]
		m.nestedTxDetected = false
		if _, err := m.conn.Exec(ctx, e.statement); err != nil {
			return nil, fmt.Errorf("cannot execute \"%s\": %w", e.id, err)
		}
		if m.nestedTxDetected {
			return nil, fmt.Errorf("cannot execute \"%s\": migration statement is already in transaction", e.id)
		}
		if _, err := m.conn.Exec(ctx, ``+
			`insert into __go_migration_meta(id, hash) values ($1, $2);`,
			e.id, e.hash,
		); err != nil {
			return nil, err
		}
	}

	if _, err := m.conn.Exec(ctx, `commit;`); err != nil {
		return nil, err
	}
	committed = true

	return list, nil
}
