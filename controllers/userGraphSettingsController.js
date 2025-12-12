const GraphSetting = require("../models/GraphSetting");

// Get settings for current logged-in user
exports.getUserSettings = async (req, res) => {
  try {
    const settings = await GraphSetting.find({ userId: req.user.userId }).lean();
    res.json(settings);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};

// Update settings for current user
exports.updateUserSettings = async (req, res) => {
  try {
    const updates = req.body.updates || [];

    if (!Array.isArray(updates) || !updates.length) {
      return res.status(400).json({ error: "No updates provided" });
    }

    const bulkOps = updates.map(u => ({
      updateOne: {
        filter: { userId: req.user.userId, metric: u.metric },
        update: {
          $set: {
            selectedType: u.selectedType,
            avgSec: u.avgSec ?? null,
            alertMin: u.alertMin ?? null,
            alertMax: u.alertMax ?? null,
            updatedAt: new Date(),
            userId: req.user.userId,
          },
        },
        upsert: true,
      },
    }));

    await GraphSetting.bulkWrite(bulkOps);

    res.json({ message: "User settings updated successfully" });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
};
