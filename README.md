# Mysql Dump Splitter

Split huge mysql dumps per table, or create a new dump include/excluding tables.


## Usage
```
Split or process Mysql dumps.

Usage:
  mysql-dump-splitter PATH_TO_DUMP [flags]

Flags:
  -e, --exclude strings   Tables to exclude.
  -h, --help              help for mysql-dump-splitter
  -i, --include strings   Tables to include.
  -m, --mode string       Output mode: data/schema/both (default "both")
  -d, --outdir string     Directory to output files to.
  -f, --outfile string    Single file to output to. Pass - for stdout.
  -v, --verbose           Output more info.
```
