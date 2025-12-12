// controllers/publicController.js
const DeviceModel = require("../models/DeviceModel");
const Manufacturer = require("../models/Manufacturer");

exports.getCodes = async (req, res) => {
  try {
    const models = await DeviceModel.find({}, "code name").lean();
    const manufacturers = await Manufacturer.find({}, "code name").lean();

    res.json({
      models,
      manufacturers,
      modelsByCode: Object.fromEntries(models.map(m => [m.code, m])),
      manufacturersByCode: Object.fromEntries(manufacturers.map(m => [m.code, m])),
    });
  } catch (err) {
    console.error("[API:/public/codes] error:", err.message);
    res.status(500).json({ message: "Failed to load codes" });
  }
};
