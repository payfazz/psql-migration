package migration

import "fmt"

func (m *Migration) UnsafeMarkAsExecued(target string, id string) error {
	eIdx, ok := m.revEntries[id]
	if !ok {
		panic(fmt.Sprintf("migration: entry not found; %s", id))
	}
	entry := m.entries[eIdx]

	conn, err := setupConn(target, nil)
	if err != nil {
		return err
	}
	defer conn.Close(bgCtx)

	if _, err := conn.Exec(bgCtx, ``+
		`insert into go_migration.meta(id, hash) values ($1, $2) `+
		`on conflict (id) do update set hash = excluded.hash`,
		entry.id, entry.hash,
	); err != nil {
		return err
	}

	return nil
}
