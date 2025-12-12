const mongoose = require("mongoose");

const SleepDataSchema = new mongoose.Schema({
  deviceId: { type: String, required: true },
  userId: { type: mongoose.Schema.Types.ObjectId, ref: "User" },
  sleepQuality: String,
  duration: Number,
  timestamp: { type: Date, default: Date.now }
});

module.exports = mongoose.model("SleepData", SleepDataSchema);
