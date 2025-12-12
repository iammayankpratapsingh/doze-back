const mongoose = require("mongoose");

const loginAttemptSchema = new mongoose.Schema({
    userId: { type: mongoose.Schema.Types.ObjectId, ref: "User", required: true },
    email: { type: String, required: true },
    success: { type: Boolean, required: true },
    timestamp: { type: Date, default: Date.now },
    deviceInfo: {
        ip: String,
        userAgent: String,
        os: String,
        browser: String,
    },
    location: {
        latitude: Number,
        longitude: Number,
        country: String,
        city: String,
    },
});

module.exports = mongoose.model("LoginAttempt", loginAttemptSchema);
