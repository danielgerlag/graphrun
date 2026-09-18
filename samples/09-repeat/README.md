# 09 Repeat

`repeat` runs a body a fixed number of times. This sample starts at `{value: 1}` and increments three times, finishing at `{value: 4}`. The loop result is the last carry, not a vector of intermediates.

YAML can also bind `count` from a request object; the builder sample uses a typed literal so the two graphs match.

## Run

```sh
cargo run -p graphrun-samples --bin 09-repeat
```

The bin compiles this YAML against `catalog.json` in this directory, builds the same graph with `RegionBuilder`, checks IR parity, and runs both on `Engine::local`.
