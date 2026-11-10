import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "email-service",
  brokers: ["localhost:9094"],
});

const producer = kafka.producer();

const consumer = kafka.consumer({
  groupId: "email-service",
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
      topic: "order-successful",
      fromBeginning: true, // NẾU MẤT KN SAU ĐÓ ONLINE TRỞ LẠI NÓ SẼ LẤY LẠI CÁC TIN NHẮN TỪ KAFKA
    });

    //
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const value = message.value.toString();

        // CÁC GIÁ TRỊ GỬI Ở producer.send
        const { userId, orderId } = JSON.parse(value);

        //   TODO: SEND EMAIL TO THE USER
        const dummyEmailId = "910101010";

        console.log(
          `EMAIL CONSUMER: Email sent to USERID: ${userId} - EMAIL: ${dummyEmailId}`,
        );

        await producer.send({
          topic: "email-successful",
          messages: [
            { value: JSON.stringify({ userId, emailId: dummyEmailId }) },
          ],
        });
      },
    });
  } catch (error) {
    console.log(" - ERROR: ", error);
  }
};

run();
