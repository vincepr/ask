# Docker Orphan Containers Block Networking

An orphan container (`ask-api-reference`) from a previous compose project
retained a reference to a now-deleted Docker network. When running
`docker compose up`, the daemon tried to attach this orphan to the missing
network and failed.

```
Error response from daemon: failed to set up container networking:
network 902bbaa1be33... not found
```

Docker Compose warned about the orphan but did not remove it automatically:
```
WARN Found orphan containers ([ask-api-reference]) for this project.
```

The fix was `docker rm --force ask-api-reference`, after which networking
worked normally.

## Questions

- Is this a one-time environment issue, or does the project structure make it
  likely to recur (e.g., services being added/removed from compose profiles)?
- Should `docker compose down --remove-orphans` be part of the documented
  teardown workflow, or should the project adopt static container names /
  profiles to avoid orphans entirely?
- Should the compose file define an explicit network with a fixed name to make
  the networking setup more predictable and debuggable?
- Is there a way to make the startup more resilient to this class of failure,
  or is the correct response always "clean up and retry"?

## Recommended Direction

- Treat this as an operational recovery/documentation feature, not an
  application-code feature.
- Favor a documented cleanup workflow over adding repo complexity to compensate
  for stale local Docker state.

## Implementation Notes

- The most useful fix is likely documentation or a helper command that makes the
  cleanup action explicit:
  - `docker compose down --remove-orphans`
  - targeted removal for known stale containers
- Avoid static `container_name` settings as a general solution. They often make
  parallel development and multiple project copies worse.
- An explicit named network may improve debuggability but does not eliminate
  orphan-container cleanup needs by itself.

## Dependencies and Sequencing

- Lowest implementation priority. It does not block core repo behavior once a
  developer's local Docker state is clean.

## Test Expectations

- This is primarily a docs/ops feature. If implemented in code or scripts, add
  tests for the helper behavior where practical.

---
_This document captures problems observed during exploration. Update or close when the corresponding implementation resolves the underlying concern._
