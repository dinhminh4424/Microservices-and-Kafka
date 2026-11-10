import express, { json } from "express";
import cors from "cors";
import { Kafka } from "kafkajs";

const app = express();

app.use(
  cors({
    origin: ["http://localhost:3000"],
  }),
);

app.use(express.json());

// KHỞI TẠO TOPIC 'payment-service'
const kafka = new Kafka({
  clientId: "payment-service",
  brokers: ["localhost:9094", "localhost:9095", "localhost:9096"],
});

const producer = kafka.producer(); // KHỞI TẠO PRODUCER (ĐỂ GỬI THÔNG TIN)

const connectToKafka = async () => {
  try {
    await producer.connect(); // KẾT NỐI

    console.log(" - PRODUCER CONNECTED 🎉");
  } catch (error) {
    console.log(" - ERROR CONNECTING TO KAFKA 💣: ", error);
  }
};

app.post("/payment-service", async (req, res) => {
  const { cart } = req.body;

  // ASSUME THAT WE GET THE COOKIE AND DECRYPT THE USER ID

  const userId = "123";

  // TODO: Payment
  console.log("API ENDPOINT HIT!");

  // KAFKA
  // GỬI ĐẾN CONSUMER
  await producer.send({
    topic: "payment-successful",
    messages: [{ value: JSON.stringify({ userId, cart }) }],
  });

  res.status(200).send(" - Payment Successful!  🎉");
});

app.use((err, req, res) => {
  res.status(err.status || 500).send(err.message);
});

app.listen(8000, () => {
  connectToKafka();

  console.log(" - Payment Service is running http://localhost:8000");
});
