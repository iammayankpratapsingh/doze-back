const express = require("express");
const router = express.Router();
const graphSettingsController = require("../controllers/graphSettingsController");
const authMiddleware = require("../middleware/authMiddleware");

// GET all settings
router.get("/", graphSettingsController.getSettings);

// UPDATE settings
router.post("/", graphSettingsController.updateSettings);

router.get("/graph-settings", authMiddleware, graphSettingsController.getUserSettings);

module.exports = router;
