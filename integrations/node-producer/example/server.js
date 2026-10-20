// Tiny URL-shortener stand-in: every /:code redirects, then records the click.
// Run: KAFKA_BROKERS=127.0.0.1:19092 PORT=3000 node example/server.js
import express from 'express';
import { createClickProducer } from '../clickProducer.js';

const brokers = (process.env.KAFKA_BROKERS ?? '127.0.0.1:19092').split(',');
const port = Number(process.env.PORT ?? 3000);
const clicks = createClickProducer({ brokers });

const app = express();
app.set('trust proxy', true); // honour X-Forwarded-For behind a proxy

app.get('/:code', (req, res) => {
  res.redirect(302, 'https://example.com'); // respond first...
  clicks.recordClick(req, req.params.code); // ...then record, without awaiting
});

const server = app.listen(port, () => console.log(`example shortener on http://localhost:${port}`));

async function shutdown(signal) {
  console.log(`${signal}: closing server and flushing clicks`);
  server.close();
  await clicks.disconnect();
  process.exit(0);
}
process.on('SIGINT', () => shutdown('SIGINT'));
process.on('SIGTERM', () => shutdown('SIGTERM'));
