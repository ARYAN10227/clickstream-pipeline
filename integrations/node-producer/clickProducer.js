// Non-blocking click-event producer for the URL shortener.
// Events follow SPEC.md "Event schema" (schema_version 1) and go to Redpanda topic `clicks`.

import { randomUUID } from 'node:crypto';
import kafkaJavascript from '@confluentinc/kafka-javascript';

const { Kafka } = kafkaJavascript.KafkaJS;

export const MAX_USER_AGENT_LENGTH = 512;
const SHUTDOWN_STEP_TIMEOUT_MS = 5000;

function withTimeout(promise, ms, what) {
  let timer;
  const timeout = new Promise((_, reject) => {
    timer = setTimeout(() => reject(new Error(`${what} timed out after ${ms} ms`)), ms);
  });
  return Promise.race([promise, timeout]).finally(() => clearTimeout(timer));
}

/**
 * Build one click event. Pure apart from the random event_id.
 * @param {{shortCode: string, ip?: string|null, userAgent?: string|null,
 *          referrer?: string|null, now: number}} input  now = epoch milliseconds
 */
export function buildEvent({ shortCode, ip = null, userAgent = null, referrer = null, now }) {
  return {
    event_id: randomUUID(),
    short_code: shortCode,
    event_time: now,
    produced_at: now,
    ip: ip || null,
    user_agent: userAgent ? userAgent.slice(0, MAX_USER_AGENT_LENGTH) : null,
    referrer: referrer || null,
    schema_version: 1,
  };
}

function header(req, name) {
  const value = req.headers?.[name];
  return Array.isArray(value) ? value[0] : value;
}

function clientIp(req) {
  const forwarded = header(req, 'x-forwarded-for');
  const first = forwarded?.split(',')[0]?.trim();
  return first || req.ip || null;
}

/**
 * Create a producer that records clicks without ever blocking or failing the redirect.
 * @param {{brokers?: string[], topic?: string, producer?: object, logger?: Console}} options
 *   Pass `producer` (anything with connect/send/flush/disconnect) to inject one in tests.
 */
export function createClickProducer({ brokers, topic = 'clicks', producer, logger = console } = {}) {
  const kafkaProducer =
    producer ??
    new Kafka({ kafkaJS: { brokers } }).producer({ kafkaJS: { idempotent: true, acks: -1 } });

  // Connect exactly once; every send waits on this same promise.
  const ready = Promise.resolve().then(() => kafkaProducer.connect());
  ready.catch((err) => logger.error(`click producer: connect failed: ${err?.message ?? err}`));

  /**
   * Fire-and-forget: returns immediately, never throws, never returns a promise to await.
   */
  function recordClick(req, shortCode) {
    try {
      const event = buildEvent({
        shortCode,
        ip: clientIp(req),
        userAgent: header(req, 'user-agent'),
        referrer: header(req, 'referer'),
        now: Date.now(),
      });
      ready
        .then(() => {
          event.produced_at = Date.now(); // set right before sending
          return kafkaProducer.send({
            topic,
            messages: [{ key: shortCode, value: JSON.stringify(event) }],
          });
        })
        .catch((err) =>
          logger.error(`click producer: dropped click for ${shortCode}: ${err?.message ?? err}`),
        );
    } catch (err) {
      logger.error(`click producer: failed to record click for ${shortCode}: ${err?.message ?? err}`);
    }
  }

  /**
   * Flush pending clicks and disconnect; call on graceful shutdown.
   * Each step is time-limited, so shutdown finishes even if the broker is unreachable.
   */
  async function disconnect({ timeoutMs = SHUTDOWN_STEP_TIMEOUT_MS } = {}) {
    try {
      await withTimeout(ready, timeoutMs, 'connect');
      await kafkaProducer.flush?.({ timeout: timeoutMs });
    } catch (err) {
      logger.error(`click producer: pending clicks not flushed: ${err?.message ?? err}`);
    }
    try {
      await withTimeout(Promise.resolve().then(() => kafkaProducer.disconnect()), timeoutMs, 'disconnect');
    } catch (err) {
      logger.error(`click producer: disconnect failed: ${err?.message ?? err}`);
    }
  }

  return { recordClick, disconnect };
}
