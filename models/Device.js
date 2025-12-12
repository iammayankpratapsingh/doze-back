const mongoose = require("mongoose");

const DeviceSchema = new mongoose.Schema({
  deviceId: { type: String, unique: true, required: true },  // Unique device identifier
  deviceType: { type: String, required: true },             // Device Type
  manufacturer: { type: String, required: true },           // Manufacturer Name
  // NEW
  accountId: { type: String }, // ties back to account/email
  profileId: { type: mongoose.Schema.Types.ObjectId, ref: "Profile"},

  firmwareVersion: { type: String, required: true },        // Firmware Version
  location: { type: String, required: true },               // Device Location
  status: {
    type: String,
    enum: ["active", "inactive", "under maintenance"],
    default: "inactive"
  },  // Device Status
  lastActiveAt: { type: Date },                              // Last Active Date
  createdAt: { type: Date, default: Date.now },              // Created Date
  validity: { type: Date, required: true },                 // Validity Date
  userId: { type: mongoose.Schema.Types.ObjectId, ref: "User", default: null }, // User who owns the device
  profileVersion: { type: Number, default: 1 }
});

DeviceSchema.index({ profileId: 1 });

module.exports = mongoose.model("Device", DeviceSchema);
