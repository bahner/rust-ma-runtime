BINARY   := ma
CARGO    := cargo
RELEASE  := target/release/$(BINARY)
DEBUG    := target/debug/$(BINARY)
CLIPPY_STRICT := --all-targets --all-features -- -D warnings -W clippy::pedantic -W clippy::nursery
SRCS     := Cargo.toml $(shell find src -name '*.rs')
PREFIX   ?= $(HOME)/.local/bin
PUBLISH  := ma:bin/
RUN_ARGS ?=

.PHONY: all clean distclean install lint publish test gen-locales-cid

all: $(BINARY)

lint:
	$(CARGO) clippy -- -D warnings
	$(CARGO) fmt --check
	mdl *.md

test:
	$(CARGO) clippy $(CLIPPY_STRICT)

gen-locales-cid:
	$(CARGO) run $(RUN_ARGS) -- --gen-locales-cid --locales-dir locales

# Release build, binary copied to project root
$(BINARY): $(RELEASE)
	cp $(RELEASE) $(BINARY)

$(RELEASE): $(SRCS)
	$(CARGO) build --release

clean:
	$(CARGO) clean
	rm -f $(BINARY)

install: $(RELEASE)
	mkdir -p $(PREFIX)
	install -m 0755 $(RELEASE) $(PREFIX)/$(BINARY)

publish: $(BINARY)
	scp $(BINARY) $(PUBLISH)

distclean: clean
	rm -rf Cargo.lock
