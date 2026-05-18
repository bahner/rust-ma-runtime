TARGET   := wasm32-unknown-unknown
PROFILE  := release
OUT_DIR  := target/$(TARGET)/$(PROFILE)
WASM     := $(OUT_DIR)/ma_root_plugin.wasm

.PHONY: all build check test clean ipfs

all: build

build:
	cargo build --target $(TARGET) --release

$(WASM): build

check:
	cargo check --target $(TARGET)

# Run tests natively (handlers.rs tests compile for the host target)
test:
	cargo test

clean:
	cargo clean

# Publish to IPFS and print the resulting CID
ipfs: $(WASM)
	@ipfs add --quiet --cid-version 1 $(WASM)
