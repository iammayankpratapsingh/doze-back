// models/Manufacturer.js
const mongoose = require('mongoose');

const ManufacturerSchema = new mongoose.Schema({
  code: { type: String, required: true, unique: true, trim: true }, // ensure uniqueness
  name: { type: String, required: true, trim: true }
}, { collection: 'manufacturers', timestamps: true });

module.exports = mongoose.model('Manufacturer', ManufacturerSchema);
