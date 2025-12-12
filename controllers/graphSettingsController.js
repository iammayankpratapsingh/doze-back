const GraphSetting = require("../models/GraphSetting");

// Get all settings
exports.getSettings = async (req, res) => {
  try {
    const settings = await GraphSetting.find().lean();
    res.json(settings);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};

// Update multiple settings
exports.updateSettings = async (req, res) => {
  try {
    const updates = req.body.updates || []; // safer fallback

    if (!Array.isArray(updates) || !updates.length) {
      return res.status(400).json({ error: "No updates provided" });
    }

    const bulkOps = updates.map(u => ({
      updateOne: {
        filter: { metric: u.metric },
        update: {
          $set: {
            selectedType: u.selectedType,
            avgSec: u.avgSec ?? null,
            alertMin: u.alertMin ?? null,   // ✅ handle alert min
            alertMax: u.alertMax ?? null,   // ✅ handle alert max
            updatedAt: new Date()
          }
        },
        upsert: true,
      }
    }));

    await GraphSetting.bulkWrite(bulkOps);

    res.json({ message: "Settings updated successfully" });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};

// controllers/graphSettingsController.js
exports.getUserSettings = async (req, res) => {
  try {
    const settings = await GraphSetting.find({ userId: req.user.userId }).lean();
    res.json({ settings });
  } catch (err) {
    console.error("Error fetching graph settings:", err);
    res.status(500).json({ error: err.message });
  }
};
