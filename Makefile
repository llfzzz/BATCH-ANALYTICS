.PHONY: install run verify test serve
install:
	./scripts/bootstrap.sh
run:
	./run-demo.sh
verify:
	./run-demo.sh --verify
test:
	./scripts/test.sh
serve:
	./serve.sh
