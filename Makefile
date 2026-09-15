all: release

test:
	crystal spec --verbose --tag '~slow' --order random

test-all:
	crystal spec --verbose --release --order random

linter:
	crystal run lib/ameba/bin/ameba.cr

linter-fix:
	crystal run lib/ameba/bin/ameba.cr -- --fix

format-check:
	crystal tool format --check

format-apply:
	crystal tool format

start-moto:
	docker run -p 127.0.0.1:4566:5000 --rm -it motoserver/moto

.PHONY: dist-clean
dist-clean:
	rm -rf lib
	rm -rf bin

