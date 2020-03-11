package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	_ "github.com/lib/pq"
	"github.com/payfazz/go-errors"
	migration "github.com/payfazz/psql-migration"
)

const appIDFile = "__APP_ID__.txt"

func main() {
	ctx, cancelCtx := context.WithCancel(context.Background())
	defer cancelCtx()
	go handleInterrupt(ctx, cancelCtx)

	dryrun, _ := strconv.ParseBool(os.Getenv("DryRun"))
	flag.BoolVar(&dryrun, "DryRun", false, "if DryRun set to true, the changes is not commited")

	dir := os.Getenv("Dir")
	flag.StringVar(&dir, "Dir", "./", "which directory contains the migration statements")

	conn := os.Getenv("Conn")
	flag.StringVar(&conn, "Conn", "", "postgres connection string")

	verbose, _ := strconv.ParseBool(os.Getenv("Verbose"))
	flag.BoolVar(&verbose, "Verbose", false, "verbose output")

	flag.Parse()

	data, err := ioutil.ReadFile(filepath.Join(dir, appIDFile))
	if os.IsNotExist(err) {
		crashf("cannot open file %s in %s\n", appIDFile, dir)
	} else if err != nil {
		crash(errors.Wrap(err))
	}

	appID := strings.TrimSpace(string(data))

	allFiles, err := ioutil.ReadDir(dir)
	if err != nil {
		crash(errors.Wrap(err))
	}

	var files []string

	for _, info := range allFiles {
		if info.IsDir() {
			continue
		}
		if filepath.Ext(info.Name()) == ".sql" {
			files = append(files, info.Name())
		}
	}

	sort.Strings(files)

	var statements []string

	for _, f := range files {
		data, err = ioutil.ReadFile(filepath.Join(dir, f))
		if err != nil {
			crash(errors.NewWithCause("cannot read file:"+f, err))
		}
		statements = append(statements, string(data))
	}

	db, err := sql.Open("postgres", conn)
	if err != nil {
		crash(errors.NewWithCause("Cannot open database", err))
	}
	err = db.PingContext(ctx)
	if err != nil {
		crash(errors.NewWithCause("Cannot ping database", err))
	}

	if dryrun {
		err = migration.DryRun(ctx, db, appID, statements)
	} else {
		err = migration.Migrate(ctx, db, appID, statements)
	}
	if err != nil {
		if err, ok := err.(*migration.InvalidAppIDError); ok {
			crashf(
				"application id in '"+appIDFile+"' does't match with database: %s != %s\n",
				appID, err.AppID,
			)
		}
		if err, ok := err.(*migration.HashError); ok {
			if !verbose {
				crashf("hash for file '%s' does't match with database\n", files[err.StatementIndex])
			} else {
				crashf(""+
					"hash for file '%s' does't match with database\n\n"+
					"normalized statement:\n"+
					"%s\n\n"+
					"computed hash    : %s\n"+
					"hash on database : %s\n",
					files[err.StatementIndex],
					err.NormalizeStatement,
					err.ComputedHash,
					err.ExpectedHash,
				)
			}
		}
		crash(errors.Wrap(err))
	}
	if dryrun {
		fmt.Println("Migration complete, but not commited because of DryRun")
	} else {
		fmt.Println("Migration complete")
	}
}

func crash(err error) {
	fmt.Fprintln(os.Stderr, errors.Format(err))
	os.Exit(1)
}

func crashf(f string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, f, args...)
	os.Exit(1)
}

func handleInterrupt(ctx context.Context, cancelCtx context.CancelFunc) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	select {
	case <-c:
	case <-ctx.Done():
	}
	signal.Stop(c)
	cancelCtx()
}
