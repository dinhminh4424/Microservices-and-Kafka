import express from "express";
import cors from "cors";
import { Kafka } from "kafkajs";

const app = express();

app.use(
  cors({
    origin: ["http://localhost:3000"],
  }),
);

app.use(express.json());

const kafka = new Kafka({
  clientId: "payment-service",
  brokers: ["localhost:9094"],
});

const producer = kafka.producer();

const connectToKafka = async () => {
  try {
    await producer.connect();

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

  res.status(200).send("Payment Successful!  🎉");
});

app.use((err, req, res) => {
  res.status(err.status || 500).send(err.message);
});

app.listen(8000, () => {
  connectToKafka();

  console.log("Payment Service is running http://localhost:8000");
});
