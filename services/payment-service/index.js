import express from "express";
import cors from "cors";

const app = express();

app.use(
  cors({
    origin: ["http://localhost:3000"],
  }),
);

app.use(express.json());

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
  console.log("Payment Service is running http://localhost:8000");
});
