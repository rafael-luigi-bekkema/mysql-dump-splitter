package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const lineCount uint64 = 61102

type Scanner struct {
	rdr   *bufio.Scanner
	count uint64
	line  string
}

func (s *Scanner) Scan() bool {
	if s.rdr.Scan() {
		s.count++
		if s.count%1000 == 0 {
			fmt.Printf("Line %d at %.01f %%\n", s.count, float64(s.count)/float64(lineCount)*100)
		}
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

func (s *Scanner) scanSchema(tbl string) {
	f, err := os.Create(filepath.Join("out", tbl+"_schema.sql"))
	if err != nil {
		panic(err)
	}
	defer f.Close()
	fmt.Fprint(f, s.line, "\r\n")
	for s.Next() {
		if strings.HasPrefix(s.line, "LOCK TABLES") {
			break
		}
		fmt.Fprint(f, s.line, "\r\n")
	}
}

func (s *Scanner) scanTable(tbl string) {
	f, err := os.Create(filepath.Join("out", tbl+".sql"))
	if err != nil {
		panic(err)
	}
	defer f.Close()
	fmt.Fprint(f, s.line, "\r\n")
	for s.Next() {
		fmt.Fprint(f, s.line, "\r\n")
		if s.line == "UNLOCK TABLES;" {
			break
		}
	}
}

func (s *Scanner) start() {
	for s.Next() {
		if s.line == "" || strings.HasPrefix(s.line, "--") {
			continue
		}
		// DROP TABLE IF EXISTS `account_businesses`;
		if strings.HasPrefix(s.line, "DROP TABLE") {
			tbl := s.line[strings.IndexByte(s.line, '`')+1 : strings.LastIndexByte(s.line, '`')]
			fmt.Printf("Start output for %q\n", tbl)
			s.scanSchema(tbl)
			s.scanTable(tbl)
			fmt.Printf("Finished output for %q\n", tbl)
			continue
		}
	}
	fmt.Println()
	if err := s.rdr.Err(); err != nil {
		fmt.Printf("At line %d\n%s\n", s.count+1, err)
	}
	fmt.Printf("%d lines counted\n", s.count)
}

func run() {
	fPath := os.Args[1]
	f, err := os.Open(fPath)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	rdr := bufio.NewScanner(f)
	buf := make([]byte, 0, 64*1024)
	rdr.Buffer(buf, 1024*1024*1024) // max line: 1G

	s := Scanner{rdr: rdr}
	s.start()
}

func main() {
	run()
}
