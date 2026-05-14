BINARY   := ma-ipfs-publisher
CARGO    := cargo
RELEASE  := target/release/$(BINARY)
DEBUG    := target/debug/$(BINARY)
CLIPPY_STRICT := --all-targets --all-features -- -D warnings -W clippy::pedantic -W clippy::nursery

.PHONY: all build release clean distclean lint test $(BINARY)

all: $(BINARY)

lint:
	$(CARGO) clippy -- -D warnings
	$(CARGO) fmt --check
	mdl *.md

test:
	$(CARGO) clippy $(CLIPPY_STRICT)

# Release build, binary copied to project root
$(BINARY): $(RELEASE)
	cp $(RELEASE) $(BINARY)

$(RELEASE):
	$(CARGO) build --release

clean:
	$(CARGO) clean
	rm -f $(BINARY)

distclean: clean
	rm -rf Cargo.lock
