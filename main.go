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
	rdr    *bufio.Scanner
	count  uint64
	line   string
	outdir string
	dryRun bool
	skip   bool
	created map[string]struct{}
}

func (s *Scanner) create(fName string) (*os.File, error) {
	if _, ok := s.created[fName]; ok {
		return os.OpenFile(fName, os.O_APPEND|os.O_WRONLY, 0644)
	}
	if s.created == nil {
		s.created = map[string]struct{}{}
	}
	s.created[fName] = struct{}{}
	return os.Create(fName)
}

func (s *Scanner) Scan() bool {
	if s.skip {
		s.skip = false
		return true
	}
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

func (s *Scanner) scanSchema(tbl string) error {
	if s.dryRun {
		for s.Next() {
			if isSchemaEnd(s.line) {
				break
			}
		}
		return nil
	}

	if err := s.ensureOut(); err != nil {
		return err
	}
	f, err := s.create(filepath.Join(s.outdir, tbl+"_schema.sql"))
	if err != nil {
		return err
	}
	defer f.Close()
	fmt.Fprint(f, s.line, "\r\n")
	for s.Next() {
		if isSchemaEnd(s.line) {
			break
		}
		fmt.Fprint(f, s.line, "\r\n")
	}
	return nil
}

func (s *Scanner) scanTable(tbl string) error {
	if isSchemaStart(s.line) {
		s.skip = true
		return nil
	}

	if s.dryRun {
		for s.Next() {
			if isSchemaStart(s.line) {
				s.skip = true
				break
			}
		}
		return nil
	}

	if err := s.ensureOut(); err != nil {
		return err
	}
	f, err := s.create(filepath.Join("out", tbl+".sql"))
	if err != nil {
		return err
	}
	defer f.Close()
	fmt.Fprint(f, s.line, "\r\n")
	for s.Next() {
		if isSchemaStart(s.line) {
			s.skip = true
			break
		}
		fmt.Fprint(f, s.line, "\r\n")
	}
	return nil
}

func isSchemaStart(line string) bool {
	return strings.HasPrefix(line, "DROP TABLE") || strings.HasPrefix(line, "/*!50001 DROP VIEW")
}

func isSchemaEnd(line string) bool {
	return isSchemaStart(line) || strings.HasPrefix(line, "LOCK TABLES") || strings.HasPrefix(line, "INSERT INTO")
}

func (s *Scanner) start() error {
	for s.Next() {
		if s.line == "" || strings.HasPrefix(s.line, "--") {
			continue
		}
		if !isSchemaStart(s.line) {
			continue
		}

		tbl := s.line[strings.IndexByte(s.line, '`')+1 : strings.LastIndexByte(s.line, '`')]

		fmt.Printf("Start output for %q\n", tbl)
		if err := s.scanSchema(tbl); err != nil {
			return err
		}
		if err := s.scanTable(tbl); err != nil {
			return err
		}

		fmt.Printf("Finished output for %q\n", tbl)
	}
	fmt.Println()
	if err := s.rdr.Err(); err != nil {
		fmt.Printf("At line %d\n%s\n", s.count+1, err)
	}
	fmt.Printf("%d lines counted\n", s.count)
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
