package migration

import "testing"

func TestHash(t *testing.T) {
	tc := []struct {
		name, a, b, hash string
	}{
		{
			"it should be case insensitive",
			"AB",
			"ab",
			"fb8e20fc2e4c3f248c60c39bd652f3c1347298bb977b8b4d5903b85055620603",
		},
		{
			"it should be ignoring whitespaces",
			`A
						BC`,
			"abc",
			"ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
		},
		{
			"it should be ignoring line comment",
			`
				A--comment
				-- test comment
				BCd
			`,
			"abcd",
			"88d4266fd4e6338d13b845fcf289579d209c897823b9217da3e161936f031589",
		},
		{
			"it should be ignoring block comment",
			`AB/* asdf asdfafs */CDE`,
			"abcde",
			"36bbe50ed96841d10443bcb670d6554f0a34b761be67ec9c4a8ad2c0c44ca42c",
		},
		{
			"it should be ignoring nested block comment",
			`AB/* asdf asdfaf/* asdf asdfafs */s /* asdf asdfa/* asdf asdfafs */fs */e*/CDEf`,
			"abcdef",
			"bef57ec7f53a6d40beb640a780a639c83bc29ac8a9816f1fc6c5c6dcd93c4721",
		},
		{
			"it should be ignoring nested block comment",
			`AB/* asdf asdfaf/* asdf asdfafs */s /* asdf asdfa/* asdf asdfafs */fs */e*/CDEg/* asdfaw`,
			"abcdeg",
			"a5a511ec5899cadabdc4e2bbefb106e731c718d2cc022cf0f48f364d51ed02bb",
		},
		{
			"it should be ignoring last semicolon",
			`ab;;;cd;`,
			`ab;;;cd`,
			"f3e0be0ac9b54823c959ffb60f5d3cea5d4a8f7219c1a9d7bd3dca5263749035",
		},
	}

	for _, c := range tc {
		t.Run(c.name, func(t *testing.T) {
			if a, b := hash(c.a), hash(c.b); a != b || b != c.hash {
				t.Fatalf("invalid hash %s %s %s", a, b, c.hash)
			}
		})
	}
}
