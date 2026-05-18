TARGET  := wasm32-unknown-unknown
OUT_DIR := target/$(TARGET)/release
WASM    := $(OUT_DIR)/ma_ping_plugin.wasm

.PHONY: all build check test clean ipfs

all: build

build:
	cargo build --target $(TARGET) --release

$(WASM): build

check:
	cargo check --target $(TARGET)

test:
	cargo test

clean:
	cargo clean

ipfs: $(WASM)
	@ipfs add --quiet --cid-version 1 $(WASM)
