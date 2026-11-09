import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "payment-service",
  brokers: ["localhost:9094"],
});

const consumer = kafka.consumer({
  groupId: "analytic-service",
});

const run = async () => {
  try {
    await consumer.connect();

    // ĐĂNG KÍ VỚI TOPIC
    await consumer.subscribe({
      topic: "payment-successful",
      fromBeginning: true, // NẾU MẤT KN SAU ĐÓ ONLINE TRỞ LẠI NÓ SẼ LẤY LẠI CÁC TIN NHẮN TỪ KAFKA
    });

    //
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const value = message.value.toString();

        // CÁC GIÁ TRỊ GỬI Ở producer.send
        const { userId, cart } = JSON.parse(value);

        const total = cart.reduce((acc, item) => acc + item.price, 0);

        console.log(` - ANALYTIC CONSUMER: USER : ${userId} - PAID: ${total}`);
      },
    });
  } catch (error) {
    console.log(" - ERROR: ", error);
  }
};

run();
