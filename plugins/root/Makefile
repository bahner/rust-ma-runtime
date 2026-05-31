TARGET  = wasm32-unknown-unknown
RELEASE = ../target/$(TARGET)/release
WASM    = $(RELEASE)/root.wasm

.PHONY: build ipfs clean

build: $(WASM)

$(WASM):
	cargo build --release

ipfs: $(WASM)
	ipfs add --quieter $(WASM)

clean:
	cargo clean
