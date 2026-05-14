BINARY   := ma-ipfs-publisher
CARGO    := cargo
RELEASE  := target/release/$(BINARY)
DEBUG    := target/debug/$(BINARY)

.PHONY: all build release clean distclean lint $(BINARY)

all: $(BINARY)

lint:
	$(CARGO) clippy -- -D warnings
	$(CARGO) fmt --check
	mdl *.md

# Release build, binary copied to project root
$(BINARY): $(RELEASE)
	cp $(RELEASE) $(BINARY)

$(RELEASE):
	$(CARGO) build --release

$(RELEASE):
	$(CARGO) build --release

clean:
	$(CARGO) clean
	rm -f $(BINARY)

distclean: clean
	rm -rf Cargo.lock
