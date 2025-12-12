const mongoose = require('mongoose');

const TechnologySchema = new mongoose.Schema(
  {
    code: { type: String, required: true, trim: true }, // "0".."9"
    name: { type: String, required: true, trim: true }
  },
  { timestamps: true, collection: 'technologies' }
);

TechnologySchema.index({ code: 1 }, { unique: true });
TechnologySchema.index({ name: 1 });

module.exports = mongoose.model('Technology', TechnologySchema);
