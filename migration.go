package migration

import (
	"embed"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/jackc/pgx/v4"
)

type entry struct {
	id        string
	statement string
	hash      string
}

type Migration struct {
	entries    []entry
	revEntries map[string]int
}

// New return new Migration object.
//
// source must contains exactly one directory, and that directory must contains only *.sql file.
// each sql file must have lowercase name.
//
// the migration is sorted by sql file name.
func New(source embed.FS) *Migration {
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

	return m
}

// Check the current state of the database.
//
// will return list of migration that need to be executed.
//
// also will return *MismatchHashError error if the database already execute a migration file
// but it has different hash with source.
func (m *Migration) Check(target string) ([]string, error) {
	conn, err := setupConn(target, nil)
	if err != nil {
		return nil, err
	}
	defer conn.Close(bgCtx)
	return m.check(conn)
}

func (m *Migration) check(conn *pgx.Conn) ([]string, error) {
	rows, err := conn.Query(bgCtx, ``+
		`select id, hash from go_migration.meta`,
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
			return nil, &MismatchHashError{Item: Item{ID: id}, HashInDB: hash}
		}
		e := m.entries[i]
		if e.hash != hash {
			return nil, &MismatchHashError{Item: Item{ID: id, Hash: e.hash}, HashInDB: hash}
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
// but it has different hash with source.
func (m *Migration) Run(target string) ([]string, error) {
	nestedTxDetected := false
	conn, err := setupConn(target, func() { nestedTxDetected = true })
	if err != nil {
		return nil, err
	}
	defer conn.Close(bgCtx)

	if _, err := conn.Exec(bgCtx, ``+
		`begin isolation level serializable;`+
		`lock table go_migration.meta in access exclusive mode`,
	); err != nil {
		return nil, err
	}
	committed := false
	defer func() {
		if !committed {
			conn.Exec(bgCtx, `rollback`)
		}
	}()

	list, err := m.check(conn)
	if err != nil {
		return nil, err
	}
	for _, l := range list {
		e := m.entries[m.revEntries[l]]
		nestedTxDetected = false
		if _, err := conn.Exec(bgCtx, `reset all;`+e.statement); err != nil {
			return nil, fmt.Errorf("cannot execute \"%s\": %w", e.id, err)
		}
		if nestedTxDetected {
			return nil, fmt.Errorf("cannot execute \"%s\": migration statement is already in transaction", e.id)
		}
		if _, err := conn.Exec(bgCtx, ``+
			`insert into go_migration.meta(id, hash) values ($1, $2)`,
			e.id, e.hash,
		); err != nil {
			return nil, err
		}
	}

	if _, err := conn.Exec(bgCtx, `commit`); err != nil {
		return nil, err
	}
	committed = true

	return list, nil
}
