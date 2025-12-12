const mongoose = require('mongoose');

const PortSchema = new mongoose.Schema(
  {
    code: { type: String, required: true, trim: true }, // "0".."9"
    name: { type: String, required: true, trim: true }
  },
  { timestamps: true, collection: 'ports' }
);

PortSchema.index({ code: 1 }, { unique: true });
PortSchema.index({ name: 1 });

module.exports = mongoose.model('Port', PortSchema);
