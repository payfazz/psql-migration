package migration

import "fmt"

type MismatchHashError struct {
	Item
	HashInDB string
}

func (d *MismatchHashError) Error() string {
	return fmt.Sprintf("\"%s\" has different hash in the database", d.ID)
}
