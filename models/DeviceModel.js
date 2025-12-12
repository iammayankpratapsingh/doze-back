// models/DeviceModel.js
const mongoose = require('mongoose');

const MetricSchema = new mongoose.Schema({
  key: String,
  label: String,
  graphType: { type: String, default: 'line' },
  averagingSec: Number,
  averagingSeconds: Number,
  avgSec: Number,
  min: Number,
  max: Number,
  alertMin: Number,
  alertMax: Number,
}, { _id: false });

const DeviceModelSchema = new mongoose.Schema({
  code: { type: String, required: true, unique: true }, // make code unique
  name: { type: String, required: true, trim: true },
  manufacturerCode: { type: String, trim: true },
  manufacturerId: { type: mongoose.Schema.Types.ObjectId, ref: 'Manufacturer' },
  metrics: [MetricSchema],
}, { collection: 'deviceModels', timestamps: true });

module.exports = mongoose.model('DeviceModel', DeviceModelSchema);
