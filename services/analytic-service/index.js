import { Kafka } from "kafkajs";

const kafka = new Kafka({
  clientId: "analytic-service",
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
      topics: ["payment-successful", "order-successful", "email-successful"],
      fromBeginning: true, // NẾU MẤT KN SAU ĐÓ ONLINE TRỞ LẠI NÓ SẼ LẤY LẠI CÁC TIN NHẮN TỪ KAFKA
    });

    //
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const value = message.value.toString();

        switch (topic) {
          case "payment-successful": {
            // CÁC GIÁ TRỊ GỬI Ở producer.send
            const { userId, cart } = JSON.parse(value);

            const total = cart
              .reduce((acc, item) => acc + item.price, 0)
              .toFixed(2);

            console.log(
              ` - ANALYTIC CONSUMER FOR TOPIC PAYMENT-SUCCESSFUL: USERID : ${userId} - PAID: ${total}`,
            );

            break;
          }
          case "order-successful": {
            // CÁC GIÁ TRỊ GỬI Ở producer.send
            const { userId, orderId } = JSON.parse(value);

            console.log(
              ` - ANALYTIC CONSUMER FOR TOPIC ORDER-SUCCESSFUL: ORDER CREATED FOR USERID : ${userId} - ORDERID: ${orderId}`,
            );

            break;
          }
          case "email-successful": {
            // CÁC GIÁ TRỊ GỬI Ở producer.send
            const { userId, emailId } = JSON.parse(value);

            console.log(
              ` - ANALYTIC CONSUMER FOR TOPIC EMAIL-SUCCESSFUL: SENDER EMAIL TO USERID : ${userId} - EMAIL: ${emailId}`,
            );

            break;
          }
          default:
            break;
        }
      },
    });
  } catch (error) {
    console.log(" - ERROR: ", error);
  }
};

run();
