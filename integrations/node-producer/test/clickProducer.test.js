import assert from 'node:assert/strict';
import { test } from 'node:test';
import { MAX_USER_AGENT_LENGTH, buildEvent, createClickProducer } from '../clickProducer.js';

const UUID_V4 = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;
const silentLogger = { error() {} };

function fakeProducer({ send }) {
  const calls = { connect: 0, send: 0, flush: 0, disconnect: 0 };
  return {
    calls,
    async connect() {
      calls.connect += 1;
    },
    send(record) {
      calls.send += 1;
      return send(record);
    },
    async flush() {
      calls.flush += 1;
    },
    async disconnect() {
      calls.disconnect += 1;
    },
  };
}

const request = {
  headers: {
    'x-forwarded-for': '203.0.113.7, 10.0.0.1',
    'user-agent': 'Mozilla/5.0',
    referer: 'https://www.google.com/',
  },
  ip: '10.0.0.1',
};

const tick = () => new Promise((resolve) => setImmediate(resolve));

test('buildEvent returns every SPEC field with the right types', () => {
  const now = 1700000000123;
  const event = buildEvent({
    shortCode: 'abc5',
    ip: '203.0.113.7',
    userAgent: 'x'.repeat(600),
    referrer: undefined,
    now,
  });

  assert.deepEqual(Object.keys(event).sort(), [
    'event_id',
    'event_time',
    'ip',
    'produced_at',
    'referrer',
    'schema_version',
    'short_code',
    'user_agent',
  ]);
  assert.match(event.event_id, UUID_V4);
  assert.equal(event.short_code, 'abc5');
  assert.equal(event.event_time, now);
  assert.equal(event.produced_at, now);
  assert.ok(Number.isInteger(event.event_time));
  assert.equal(event.ip, '203.0.113.7');
  assert.equal(event.user_agent.length, MAX_USER_AGENT_LENGTH);
  assert.equal(event.referrer, null);
  assert.equal(event.schema_version, 1);
});

test('recordClick never throws or rejects when send() rejects', async () => {
  const errors = [];
  const producer = fakeProducer({ send: () => Promise.reject(new Error('broker down')) });
  const clicks = createClickProducer({ producer, logger: { error: (m) => errors.push(m) } });

  const unhandled = [];
  const onUnhandled = (reason) => unhandled.push(reason);
  process.on('unhandledRejection', onUnhandled);
  try {
    assert.doesNotThrow(() => clicks.recordClick(request, 'abc5'));
    await tick();
    await tick();
  } finally {
    process.off('unhandledRejection', onUnhandled);
  }

  assert.equal(producer.calls.send, 1);
  assert.deepEqual(unhandled, []);
  assert.equal(errors.length, 1);
  assert.match(errors[0], /dropped click for abc5: broker down/);
});

test('recordClick returns immediately without waiting for send()', async () => {
  let sent;
  const producer = fakeProducer({
    send: (record) => {
      sent = record;
      return new Promise(() => {}); // never settles
    },
  });
  const clicks = createClickProducer({ producer, logger: silentLogger });

  const result = clicks.recordClick(request, 'abc5');
  assert.equal(result, undefined); // nothing for the caller to await
  assert.equal(producer.calls.send, 0); // send happens after the caller has moved on

  await tick();
  assert.equal(producer.calls.connect, 1);
  assert.equal(producer.calls.send, 1);
  assert.equal(sent.topic, 'clicks');
  assert.equal(sent.messages[0].key, 'abc5');
  const event = JSON.parse(sent.messages[0].value);
  assert.equal(event.ip, '203.0.113.7'); // first X-Forwarded-For entry
  assert.equal(event.user_agent, 'Mozilla/5.0');
  assert.equal(event.referrer, 'https://www.google.com/');
  assert.ok(event.produced_at >= event.event_time);
});

test('connects once and disconnect() flushes before disconnecting', async () => {
  const producer = fakeProducer({ send: async () => [] });
  const clicks = createClickProducer({ producer, logger: silentLogger });
  clicks.recordClick(request, 'a');
  clicks.recordClick(request, 'b');
  await clicks.disconnect();

  assert.deepEqual(producer.calls, { connect: 1, send: 2, flush: 1, disconnect: 1 });
});

test('disconnect() finishes even if the broker never becomes reachable', async () => {
  const errors = [];
  const producer = fakeProducer({ send: async () => [] });
  producer.connect = () => new Promise(() => {}); // broker down: connect never settles
  const clicks = createClickProducer({ producer, logger: { error: (m) => errors.push(m) } });
  clicks.recordClick(request, 'abc5');

  await clicks.disconnect({ timeoutMs: 50 });

  assert.equal(producer.calls.send, 0);
  assert.equal(producer.calls.disconnect, 1);
  assert.match(errors[0], /not flushed: connect timed out after 50 ms/);
});
