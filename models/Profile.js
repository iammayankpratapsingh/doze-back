const mongoose = require("mongoose");

const ProfileSchema = new mongoose.Schema({
  accountId: { type: String, required: true, index: true }, // e.g. email
  userId:    { type: mongoose.Schema.Types.ObjectId, ref: "User", required: true },
  identifier:{ type: String, trim: true, required: true },  // "Master", "Bed1", ...
  devices:   [{ type: mongoose.Schema.Types.ObjectId, ref: "Device" }],
  activeDevice: { type: mongoose.Schema.Types.ObjectId, ref: "Device", default: null }
}, { timestamps: true });

ProfileSchema.index({ accountId: 1, identifier: 1 }, { unique: true }); // "Bed1" unique per account

module.exports = mongoose.model("Profile", ProfileSchema);
