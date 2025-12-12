const express = require("express");
const router = express.Router();
const deviceManagementController = require("../controllers/deviceManagementController");
const authMiddleware = require("../middleware/authMiddleware");
const superadminMiddleware = require("../middleware/superadminMiddleware");
const Device = require('../models/Device');
const User = require("../models/User");

// All routes require authentication and superadmin access
router.use(authMiddleware);
router.use(superadminMiddleware);

// Search devices
router.get("/search", deviceManagementController.searchDevices);

// Get all devices (with pagination)
router.get("/", deviceManagementController.getAllDevices);

// Get a single device by MongoDB ID
router.get("/:id", deviceManagementController.getDeviceById);

// Update a device by MongoDB ID
router.put("/:id", deviceManagementController.updateDevice);

// Delete a device by MongoDB ID
router.delete("/:id", deviceManagementController.deleteDevice);

// Add Device
router.post("/add", authMiddleware, async (req, res) => {
  // Debug: Log entry into the route and important objects.
  console.log(">>> POST /add called");
  console.log("Request body:", req.body);
  console.log("User from auth:", req.user);

  const { deviceId, deviceType, manufacturer, firmwareVersion, location, status, validity } = req.body;
  const currentDate = new Date();

  // Add more debugging: log received status and its processed value
  console.log("Received status:", status);
  const processedStatus = status ? status.toLowerCase() : 'inactive';
  console.log("Processed status:", processedStatus);

  try {
    // Create a new device document; note that we force the status to lowercase.
    const device = new Device({
      deviceId,
      deviceType,
      manufacturer,
      firmwareVersion,
      location,
      status: processedStatus,
      validity,
      createdAt: currentDate,
      lastActiveAt: currentDate,
      userId: req.user.userId
    });

    console.log("Adding device:", device);
    await device.save();
    console.log("Device saved successfully:", device._id);

    // Update user's devices array
    // Update user's devices array and set activeDevice if not already set
    const user = await User.findById(req.user.userId);
    if (user) {
      user.devices.push(device._id);
      if (!user.activeDevice) {
        user.activeDevice = device._id;
      }
      await user.save();
      console.log("Updated user devices:", user.devices, "Active device:", user.activeDevice);
    }

    return res.status(201).json({
      message: "Device added successfully",
      device,
      createdAt: device.createdAt,
      formattedDate: device.createdAt.toLocaleDateString('en-US', {
        year: 'numeric',
        month: 'long',
        day: 'numeric',
        hour: '2-digit',
        minute: '2-digit'
      })
    });

  } catch (error) {
    console.error("Error adding device:", error);

    // Handle Mongoose validation errors
    if (error.name === 'ValidationError') {
      return res.status(400).json({
        status: "fail",
        message: error.message
      });
    }

    // Handle duplicate key errors (e.g., unique deviceId)
    if (error.code === 11000) {
      return res.status(409).json({
        status: "fail",
        message: `A device with this Device ID already exists.`
      });
    }

    // Generic fallback for other errors
    return res.status(500).json({
      status: "fail",
      message: "An internal server error occurred while adding the device."
    });
  }
});

module.exports = router;