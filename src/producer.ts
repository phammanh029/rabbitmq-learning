import amqp from "amqplib";

const RABBIT_URL = process.env.RABBIT_URL ?? "amqp://guest:guest@localhost:5672";
const QUEUE = process.env.QUEUE ?? "demo.jobs";

function sleep(ms: number) {
  return new Promise((r) => setTimeout(r, ms));
}

async function main() {
  const conn = await amqp.connect(RABBIT_URL);
  const ch = await conn.createChannel();

  await ch.assertQueue(QUEUE, { durable: true });

  let i = 0;
  while (true) {
    i += 1;
    const payload = {
      id: crypto.randomUUID(),
      n: i,
      createdAt: new Date().toISOString(),
    };

    const body = Buffer.from(JSON.stringify(payload));
    const ok = ch.sendToQueue(QUEUE, body, { persistent: true });

    console.log(`[producer] sent #${i}`, payload, ok ? "" : "(backpressure)");
    await sleep(500);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
