package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/spf13/cobra"
)

type Scanner struct {
	rdr      *bufio.Scanner
	count    uint64
	line     string
	created  map[string]struct{}
	out      io.Writer
	outClose func()
	table    string
	ignore   bool
	skip     bool
	typ      string

	headers bytes.Buffer

	cfg struct {
		outdir, outfile               string
		include, exclude, excludeData []string
		verbose                       bool
		mode                          string
		compress                      bool
	}
}

func (s *Scanner) setFile(f *os.File) {
	if s.cfg.compress {
		gw := gzip.NewWriter(f)
		s.out = gw
		s.outClose = func() {
			gw.Close()
			f.Close()
		}
	} else {
		s.out = f
		s.outClose = func() { f.Close() }
	}
}

func (s *Scanner) create(fName string) error {
	if s.cfg.outfile != "" {
		if s.out != nil {
			return nil
		}

		if s.cfg.outfile == "-" {
			s.cfg.outfile = "/dev/stdout"
		}

		f, err := os.Create(s.cfg.outfile)
		if err != nil {
			return err
		}
		s.setFile(f)

		fmt.Fprintln(s.out, s.headers.String())

		return nil
	}

	if s.out != nil {
		s.outClose()
	}
	if err := s.ensureOut(); err != nil {
		return err
	}
	if s.cfg.compress {
		fName += ".gz"
	}
	fName = filepath.Join(s.cfg.outdir, fName)
	if _, ok := s.created[fName]; ok {
		f, err := os.OpenFile(fName, os.O_APPEND|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		s.setFile(f)
		return nil
	}

	if s.created == nil {
		s.created = map[string]struct{}{}
	}
	s.created[fName] = struct{}{}

	file, err := os.Create(fName)
	if err != nil {
		return err
	}
	s.setFile(file)

	fmt.Fprintln(s.out, s.headers.String())

	return nil
}

func (s *Scanner) writeLine() {
	fmt.Fprint(s.out, s.line, "\r\n")
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
	if s.skip {
		s.skip = false
		return true
	}
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
	s.ignore = (s.cfg.include != nil && !slices.Contains(s.cfg.include, s.table)) || slices.Contains(s.cfg.exclude, s.table) ||
		(s.typ == "data" && s.cfg.mode == "schema") ||
		(s.typ == "schema" && s.cfg.mode == "data") ||
		(s.typ == "data" && slices.Contains(s.cfg.excludeData, s.table))
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
		if !strings.HasPrefix(s.line, "/*!") {
			s.skip = true
			break
		}
		fmt.Fprintln(&s.headers, s.line)
	}

	for s.Next() {
		if s.line == "" || strings.HasPrefix(s.line, "--") {
			continue
		}

		if s.isViewStart() || s.isSchemaStart() || s.isDataStart() {
			s.scanTableName()

			if s.ignore {
				if s.cfg.verbose {
					fmt.Fprintf(os.Stderr, "Ignoring %s for %q\n", s.typ, s.table)
				}
				continue
			}

			if s.cfg.verbose {
				fmt.Fprintf(os.Stderr, "Start %s for %q\n", s.typ, s.table)
			}

			if err := s.create(s.table + ".sql"); err != nil {
				return err
			}
		}

		if !s.ignore {
			s.writeLine()
		}
	}

	if s.out != nil {
		s.outClose()
	}

	if err := s.rdr.Err(); err != nil {
		return fmt.Errorf("At line %d\n%w\n", s.count+1, err)
	}
	return nil
}

func run(s *Scanner, fPath string) error {
	if (s.cfg.outfile == "") == (s.cfg.outdir == "") {
		return fmt.Errorf("Provider either -outfile or -outdir.")
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
	var s Scanner

	cmd := cobra.Command{
		Use:          "mysql-dump-splitter PATH_TO_DUMP",
		Short:        "Split or process Mysql dumps.",
		Args:         cobra.ExactArgs(1),
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if !slices.Contains([]string{"data", "schema", "both"}, s.cfg.mode) {
				return fmt.Errorf("mode should be one of: data, schema or both")
			}
			return run(&s, args[0])
		},
	}

	cmd.Flags().StringVarP(&s.cfg.outfile, "outfile", "f", "", "Single file to output to. Pass - for stdout.")
	cmd.Flags().StringVarP(&s.cfg.outdir, "outdir", "d", "", "Directory to output files to.")
	cmd.Flags().BoolVarP(&s.cfg.verbose, "verbose", "v", false, "Output more info.")
	cmd.Flags().BoolVarP(&s.cfg.compress, "compress", "c", false, "Compress output.")
	cmd.Flags().StringSliceVarP(&s.cfg.include, "include", "i", nil, "Tables to include.")
	cmd.Flags().StringSliceVarP(&s.cfg.exclude, "exclude", "e", nil, "Tables to exclude.")

	cmd.Flags().StringSliceVar(&s.cfg.excludeData, "exclude-data", nil, "Exclude data for these tables.")

	cmd.Flags().StringVarP(&s.cfg.mode, "mode", "m", "both", "Output mode: data/schema/both")

	cmd.MarkFlagsOneRequired("outfile", "outdir")
	cmd.MarkFlagsMutuallyExclusive("outfile", "outdir")

	if err := cmd.Execute(); err != nil {
		os.Exit(1)
	}
}
