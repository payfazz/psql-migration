package migration

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"unicode"
)

// this hash function will ignore case sensitivity, whitespace and sql comment
func hash(sql string) string {
	sql = strings.ReplaceAll(sql, "\r\n", "\n")
	sql = strings.ReplaceAll(sql, "\r", "\n")
	sql = strings.ToLower(sql)

	const (
		normal = iota
		lineComment
		blockComment
	)

	sum := sha256.New()

	state := normal
	blockCommentCount := 0

	for i := 0; i < len(sql); i++ {
		switch state {
		case normal:
			c := sql[i]
			switch {
			case unicode.IsSpace(rune(c)):
			case c == '-' && i+1 < len(sql) && sql[i+1] == '-':
				i++
				state = lineComment
			case c == '/' && i+1 < len(sql) && sql[i+1] == '*':
				i++
				state = blockComment
				blockCommentCount = 1
			default:
				sum.Write([]byte{c})
			}

		case lineComment:
			for ; i < len(sql) && sql[i] != '\n'; i++ {
			}
			state = normal

		case blockComment:
		loop:
			for ; i < len(sql); i++ {
				c := sql[i]
				switch {
				case c == '/' && i+1 < len(sql) && sql[i+1] == '*':
					i++
					blockCommentCount++
				case c == '*' && i+1 < len(sql) && sql[i+1] == '/':
					i++
					blockCommentCount--
					if blockCommentCount == 0 {
						break loop
					}
				}
			}
			state = normal

		default:
			panic("unreachable")
		}
	}

	return hex.EncodeToString(sum.Sum(nil))
}
