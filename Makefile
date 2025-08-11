all: release

test:
	crystal spec --verbose --tag '~slow' --order random

test-all:
	crystal spec --verbose --release --order random

linter:
	ameba

linter-fix:
	ameba --fix

format-check:
	crystal tool format --check

format-apply:
	crystal tool format

start-minio:
	docker run -p 127.0.0.1:9000:9000 -p 127.0.0.1:9001:9001 -e "MINIO_ROOT_USER=admin" -e "MINIO_ROOT_PASSWORD=password" --rm  -it quay.io/minio/minio server /data --console-address ":9001"

.PHONY: dist-clean
dist-clean:
	rm -rf lib
	rm -rf bin

