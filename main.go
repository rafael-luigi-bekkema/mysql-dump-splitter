package main

import (
	"bufio"
	"compress/gzip"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

const lineCount uint64 = 61102

type Scanner struct {
	rdr     *bufio.Scanner
	count   uint64
	line    string
	outdir  string
	dryRun  bool
	created map[string]struct{}
	file    *os.File
}

func (s *Scanner) create(fName string) error {
	if s.file != nil {
		s.file.Close()
	}
	if err := s.ensureOut(); err != nil {
		return err
	}
	fName = filepath.Join(s.outdir, fName)
	if _, ok := s.created[fName]; ok {
		var err error
		s.file, err = os.OpenFile(fName, os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
	}
	if s.created == nil {
		s.created = map[string]struct{}{}
	}
	s.created[fName] = struct{}{}
	var err error
	s.file, err = os.Create(fName)
	if err != nil {
		return err
	}
	return nil
}

func (s *Scanner) Scan() bool {
	if s.rdr.Scan() {
		s.count++
		s.line = s.rdr.Text()
		return true
	}
	return false
}

func (s *Scanner) Next() bool {
	for s.Scan() {
		if s.line == "" || strings.HasPrefix(s.line, "--") {
			continue
		}
		return true
	}
	return false
}

func (s *Scanner) ensureOut() error {
	fi, err := os.Stat(s.outdir)
	if errors.Is(err, fs.ErrNotExist) {
		if err := os.Mkdir(s.outdir, 0700); err != nil {
			return err
		}
		return nil
	}
	if !fi.IsDir() {
		return fmt.Errorf("%s exists but is not a directory", s.outdir)
	}
	return nil
}

func (s *Scanner) isViewStart() bool {
	return strings.HasPrefix(s.line, "/*!50001 DROP VIEW")
}

func (s *Scanner) isSchemaStart() bool {
	return strings.HasPrefix(s.line, "DROP TABLE")
}

func (s *Scanner) isDataStart() bool {
	return strings.HasPrefix(s.line, "LOCK TABLES")
}

func (s *Scanner) start() error {
	var table string
	for s.Next() {
		if s.line == "" || strings.HasPrefix(s.line, "--") {
			continue
		}

		if s.isSchemaStart() {
			table = s.line[strings.IndexByte(s.line, '`')+1 : strings.LastIndexByte(s.line, '`')]
			fmt.Printf("Start schema for %q\n", table)
			if err := s.create(table + "_schema.sql"); err != nil {
				return err
			}
		}

		if s.isViewStart() {
			table = s.line[strings.IndexByte(s.line, '`')+1 : strings.LastIndexByte(s.line, '`')]
			fmt.Printf("Start view for %q\n", table)
			if err := s.create(table + "_schema.sql"); err != nil {
				return err
			}
		}

		if s.isDataStart() {
			fmt.Printf("Start data for %q\n", table)
			if err := s.create(table + "_data.sql"); err != nil {
				return err
			}
		}

		fmt.Fprint(s.file, s.line, "\r\n")
	}

	if s.file != nil {
		s.file.Close()
	}

	fmt.Println()
	if err := s.rdr.Err(); err != nil {
		fmt.Printf("At line %d\n%s\n", s.count+1, err)
	}
	return nil
}

func run() error {
	var s Scanner

	flag.StringVar(&s.outdir, "out", "out", "Directory to output to. Defaults to 'out' in working directory.")
	flag.BoolVar(&s.dryRun, "dry-run", false, "Don't output any files.")
	flag.Parse()

	fPath := flag.Arg(0)

	if fPath == "" {
		return fmt.Errorf("Please provide the path to the sql dump.")
	}

	f, err := os.Open(fPath)
	if err != nil {
		return err
	}
	defer f.Close()

	stream := io.ReadCloser(f)
	if strings.HasSuffix(fPath, ".gz") {
		stream, err = gzip.NewReader(stream)
		if err != nil {
			return fmt.Errorf("could not open gzip stream on %s: %w", fPath, err)
		}
		defer stream.Close()
	}

	rdr := bufio.NewScanner(stream)
	buf := make([]byte, 0, 64*1024)
	rdr.Buffer(buf, 1024*1024*1024) // max line: 1G

	s.rdr = rdr

	return s.start()
}

func main() {
	if err := run(); err != nil {
		fmt.Printf("Error: %s\n", err)
		os.Exit(1)
	}
}
