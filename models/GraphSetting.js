const mongoose = require("mongoose");

const graphSettingSchema = new mongoose.Schema({
  metric: { type: String, required: true },
  selectedType: { type: String, enum: ["NA", "Value", "Line"], default: "NA" },
  avgSec: { type: Number, default: null },
  updatedBy: { type: mongoose.Schema.Types.ObjectId, ref: "User" },
  alertMin: { type: Number, default: null },
  alertMax: { type: Number, default: null },
  updatedAt: { type: Date, default: Date.now },
  userId: { type: mongoose.Schema.Types.ObjectId, ref: "User", required: true },
  updatedAt: { type: Date, default: Date.now }
});
// Ensure uniqueness: one metric per user (or global if userId=null)
graphSettingSchema.index({ metric: 1, userId: 1 }, { unique: true });

module.exports = mongoose.model("GraphSetting", graphSettingSchema);
