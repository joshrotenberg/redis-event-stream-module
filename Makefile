# Packaging helpers for the container image (issue #102) and the Redis
# Enterprise RAMP bundle (issue #103). These wrap the same commands CI runs so
# the artifacts are reproducible locally. Day-to-day development still uses
# cargo directly (see CONTRIBUTING.md).

VERSION := $(shell cargo metadata --no-deps --format-version 1 \
	| python3 -c 'import json,sys; print(json.load(sys.stdin)["packages"][0]["version"])')
TARGET  ?= linux-x86_64
IMAGE   ?= ghcr.io/joshrotenberg/redis-event-stream-module
# The image builds the server from source (see Dockerfile); SERVER_KIND and
# SERVER_VERSION select and pin it, defaulting to the CI redis-8 pin.
SERVER_KIND    ?= redis
SERVER_VERSION ?= 8.8.0

.PHONY: docker docker-valkey ramp clean-dist

# Build the preloaded image locally (single arch). Override SERVER_KIND/
# SERVER_VERSION for other servers, or use the docker-valkey target.
docker:
	docker build \
		--build-arg SERVER_KIND=$(SERVER_KIND) \
		--build-arg SERVER_VERSION=$(SERVER_VERSION) \
		-t $(IMAGE):$(VERSION) -t $(IMAGE):latest .

docker-valkey:
	$(MAKE) docker SERVER_KIND=valkey SERVER_VERSION=8.1.6 \
		IMAGE="$(IMAGE)" && \
	docker tag $(IMAGE):$(VERSION) $(IMAGE):$(VERSION)-valkey8

# Build the RAMP bundle from an already-built release .so. Requires
# `pip install ramp-packer` and a redis-server 7.2+ on PATH (ramp-packer loads
# the module into a throwaway server to enumerate commands); on a pre-7.2
# redis-server the load aborts, so a modern redis-server is mandatory here.
# The .so must already exist at dist/<name>.so (see the release workflow).
ramp: dist/redis-event-stream-module-$(VERSION)-$(TARGET).so
	ramp pack $< -m ramp.yml \
		-o dist/redis-event-stream-module-$(VERSION)-$(TARGET).zip

dist/redis-event-stream-module-$(VERSION)-$(TARGET).so:
	cargo build --release --lib
	mkdir -p dist
	cp target/release/libredis_event_stream_module.so $@

clean-dist:
	rm -rf dist
