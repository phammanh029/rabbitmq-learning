import amqp from 'amqplib';

const RABBIT_URL =
  process.env.RABBIT_URL ?? 'amqp://guest:guest@localhost:5672';
const QUEUE = process.env.QUEUE ?? 'demo.jobs';

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

async function main() {
  const conn = await amqp.connect(RABBIT_URL);
  const ch = await conn.createChannel();

  await ch.assertQueue(QUEUE, { durable: true });

  // Only process 1 message at a time (good for practicing backpressure)
  ch.prefetch(1);

  console.log(`[consumer] waiting on queue: ${QUEUE}`);

  await ch.consume(
    QUEUE,
    async (msg) => {
      if (!msg) return;

      const text = msg.content.toString('utf-8');
      const payload = JSON.parse(text);

      try {
        console.log('[consumer] received', payload);

        // simulate work
        await sleep(800);

        // fail sometimes to practice retry
        if (payload.n % 7 === 0) {
          throw new Error('simulated failure (n divisible by 7)');
        }

        ch.ack(msg);
        console.log('[consumer] ack', payload.id);
      } catch (e) {
        console.error('[consumer] error -> requeue', (e as Error).message);
        // requeue=true to retry
        ch.nack(msg, false, true);
      }
    },
    { noAck: false }
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
