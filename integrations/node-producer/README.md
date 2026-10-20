# click-producer

Node.js (20+) producer that records every URL-shortener redirect as a click event on the
Redpanda/Kafka topic `clicks`, using the event schema in the repository's `SPEC.md`
(schema_version 1). It is built on `@confluentinc/kafka-javascript` (KafkaJS-compatible API).

- **Non-blocking:** `recordClick()` returns immediately; the redirect never waits on Kafka.
- **Never throws:** send and connection errors are logged and the click is dropped.
- **Keyed by short code**, idempotent producer with `acks=all`; `produced_at` is set right
  before sending. The client IP comes from the first `X-Forwarded-For` entry (or `req.ip`)
  and is only used downstream for GeoIP, never stored.
- Call `disconnect()` on shutdown to flush pending clicks.

## Add it to an Express redirect route

```js
import { createClickProducer } from './integrations/node-producer/clickProducer.js';
const clicks = createClickProducer({ brokers: ['127.0.0.1:19092'] });
clicks.recordClick(req, req.params.code); // in the /:code handler, right after res.redirect(302, url)
```

## Run the example and tests

```sh
npm install
npm test                                        # node:test, no broker needed
KAFKA_BROKERS=127.0.0.1:19092 node example/server.js   # needs `make up` in the repo root
curl -i http://localhost:3000/abc5              # 302 to https://example.com
```
