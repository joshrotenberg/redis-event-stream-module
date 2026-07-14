
## Install

Download the module for your platform, verify it, and load it. Prebuilt
targets: `linux-x86_64`, `linux-aarch64`, `macos-aarch64`, `macos-x86_64`.

```
shasum -a 256 -c redis-event-stream-module-<version>-<target>.<ext>.sha256
gh attestation verify redis-event-stream-module-<version>-<target>.<ext> \
  --repo joshrotenberg/redis-event-stream-module
redis-server --loadmodule ./redis-event-stream-module-<version>-<target>.<ext>
```

Requires Redis 7.2+ or Valkey 8.x. Artifacts are attached by CI within a few
minutes of the release being created.
