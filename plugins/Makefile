PLUGINS = counter register set root

TARGET  = wasm32-unknown-unknown
RELEASE = target/$(TARGET)/release

.PHONY: build ipfs clean $(PLUGINS)

build:
	@for p in $(PLUGINS); do \
		echo "==> Building $$p"; \
		$(MAKE) -C $$p build || exit 1; \
	done

ipfs:
	@for p in $(PLUGINS); do \
		echo "==> Publishing $$p to IPFS"; \
		$(MAKE) -C $$p ipfs || exit 1; \
	done

clean:
	@for p in $(PLUGINS); do \
		$(MAKE) -C $$p clean; \
	done
