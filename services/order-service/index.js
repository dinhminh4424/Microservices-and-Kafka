import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "order-service",
  brokers: ["localhost:9094"],
});

const producer = kafka.producer();

const consumer = kafka.consumer({
  groupId: "order-service",
});

const connectToKafka = async () => {
  try {
    await producer.connect();

    console.log(" - PRODUCER CONNECTED 🎉");
  } catch (error) {
    console.log(" - ERROR CONNECTING TO KAFKA 💣: ", error);
  }
};

const run = async () => {
  try {
    await connectToKafka();
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

        //   TODO: CREATE ORDER ON DB
        const dummyOrder = "123456789";

        console.log(
          `ORDER CONSUMER: Created Order For  USERID: ${userId} - ORDERID: ${dummyOrder}`,
        );

        await producer.send({
          topic: "order-successful",
          messages: [
            { value: JSON.stringify({ userId, orderId: dummyOrder }) },
          ],
        });
      },
    });
  } catch (error) {
    console.log(" - ERROR: ", error);
  }
};

run();
