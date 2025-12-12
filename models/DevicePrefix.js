// models/DevicePrefix.js
const mongoose = require('mongoose');

const DevicePrefixSchema = new mongoose.Schema({
  prefix: { type: String, unique: true, required: true, match: /^\d{7}$/ }, // e.g. "0102130"

  // 7-digit code parts
  deviceNameCode:   { type: String, required: true, match: /^\d{2}$/ }, // 01..06
  manufacturerCode: { type: String, required: true, match: /^\d{2}$/ }, // 01..04
  sectorCode:       { type: String, required: true, match: /^\d{1}$/ }, // 0..6
  technologyCode:   { type: String, required: true, match: /^\d{1}$/ }, // 0..6
  portsCode:        { type: String, required: true, match: /^\d{1}$/ }, // 0..6

  // denormalized labels
  deviceName:   { type: String, required: true },
  manufacturer: { type: String, required: true },
  sector:       { type: String, required: true },
  technology:   { type: String, required: true },
  ports:        { type: String, required: true },

  // running number for the second part
  sequence: { type: Number, default: 0 },

  createdBy: { type: mongoose.Schema.Types.ObjectId, ref: 'User' },
}, { timestamps: true });

DevicePrefixSchema.index({ deviceName: 1, manufacturer: 1 });

module.exports = mongoose.model('DevicePrefix', DevicePrefixSchema);
