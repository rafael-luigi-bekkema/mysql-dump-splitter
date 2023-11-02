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
	"slices"
	"strings"
)

const lineCount uint64 = 61102

type Scanner struct {
	rdr     *bufio.Scanner
	count   uint64
	line    string
	created map[string]struct{}
	file    *os.File
	table   string
	ignore  bool
	typ     string

	cfg struct {
		outdir     string
		singleFile string

		include, exclude []string
	}
}

func (s *Scanner) create(fName string) error {
	if s.cfg.singleFile != "" {
		if s.file == nil {
			var err error
			s.file, err = os.Create(s.cfg.singleFile)
			if err != nil {
				return err
			}
		}
		return nil
	}

	if s.file != nil {
		s.file.Close()
	}
	if err := s.ensureOut(); err != nil {
		return err
	}
	fName = filepath.Join(s.cfg.outdir, fName)
	if _, ok := s.created[fName]; ok {
		var err error
		s.file, err = os.OpenFile(fName, os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		return nil
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

func (s *Scanner) writeLine() {
	fmt.Fprint(s.file, s.line, "\r\n")
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
	fi, err := os.Stat(s.cfg.outdir)
	if errors.Is(err, fs.ErrNotExist) {
		if err := os.Mkdir(s.cfg.outdir, 0700); err != nil {
			return err
		}
		return nil
	}
	if !fi.IsDir() {
		return fmt.Errorf("%s exists but is not a directory", s.cfg.outdir)
	}
	return nil
}

func (s *Scanner) scanTableName() {
	s.table = s.line[strings.IndexByte(s.line, '`')+1 : strings.LastIndexByte(s.line, '`')]
	s.ignore = (s.cfg.include != nil && !slices.Contains(s.cfg.include, s.table)) || slices.Contains(s.cfg.exclude, s.table)
}

func (s *Scanner) isViewStart() bool {
	v := strings.HasPrefix(s.line, "/*!50001 DROP VIEW")
	if v {
		s.typ = "view"
	}
	return v
}

func (s *Scanner) isSchemaStart() bool {
	v := strings.HasPrefix(s.line, "DROP TABLE")
	if v {
		s.typ = "schema"
	}
	return v
}

func (s *Scanner) isDataStart() bool {
	v := strings.HasPrefix(s.line, "LOCK TABLES")
	if v {
		s.typ = "data"
	}
	return v
}

func (s *Scanner) start() error {
	for s.Next() {
		if s.line == "" || strings.HasPrefix(s.line, "--") {
			continue
		}

		if s.isViewStart() || s.isSchemaStart() || s.isDataStart() {
			s.scanTableName()

			if s.ignore {
				fmt.Printf("Ignoring %s for %q\n", s.typ, s.table)
				continue
			}

			fmt.Printf("Start %s for %q\n", s.typ, s.table)
			if err := s.create(s.table + ".sql"); err != nil {
				return err
			}
		}

		if !s.ignore {
			s.writeLine()
		}
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

	flag.StringVar(&s.cfg.outdir, "out", "out", "Directory or file to output to.")
	flag.StringVar(&s.cfg.singleFile, "single-file", "", "Output to a single file.")
	exclude := flag.String("exclude", "", "Tables to exclude.")
	include := flag.String("include", "", "Tables to include.")
	flag.Parse()

	if *exclude != "" {
		s.cfg.exclude = strings.Split(*exclude, ",")
	}
	if *include != "" {
		s.cfg.include = strings.Split(*include, ",")
	}

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
