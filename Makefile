install_dir := /usr/local/bin
bin_name := mysql-dump-splitter
build_path := build/${bin_name}
install_path := ${install_dir}/${bin_name}

${build_path}: $(shell find . -iname '*.go') go.mod go.sum
	CGO_ENABLED=0 go build -o ${build_path}

.PHONY: install
install:
	install -D --compare ${build_path} ${install_path}
