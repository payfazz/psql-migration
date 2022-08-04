package migration

import "embed"

// Migrate is the shortcut to create a migration object, and run it.
func Migrate(source embed.FS, target string) ([]string, error) {
	m, err := New(source, target)
	if err != nil {
		return nil, err
	}
	defer m.Close()

	return m.Run()
}

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
