package migration

import "fmt"

type MismatchHashError struct {
	ID          string
	CurrentHash string
	HashInDB    string
}

func (d *MismatchHashError) Error() string {
	return fmt.Sprintf("\"%s\" has different hash in the database", d.ID)
}
