const express = require("express");
const Device = require("../models/Device");
const User = require("../models/User");
const authMiddleware = require("../middleware/authMiddleware");
const adminMiddleware = require("../middleware/adminMiddleware");
const mongoose = require('mongoose');
const DevicePrefix = require('../models/DevicePrefix');
const Profile = require("../models/Profile");
const router = express.Router();
const ID_RX = /^\d{4}-[0-9A-F]{12}$/i;; // 4 digits 12 hex chars
const pad5 = (n) => String(n).padStart(5, '0');
const deviceController= require('../controllers/deviceManagementController');

// --- local handlers so we don't need another controller import ---
async function getByDeviceId(req, res) {
  try {
    const device = await Device.findOne({ deviceId: req.params.deviceId });
    if (!device) return res.status(404).json({ message: 'Device not found' });
    res.json({ data: { device } });
  } catch (e) {
    res.status(500).json({ message: 'Server error' });
  }
}

async function validateDeviceId(req, res) {
  try {
    const { deviceId } = req.query;
    if (!deviceId) return res.status(400).json({ ok: false, message: 'deviceId required' });

    const d = await Device.findOne({ deviceId });
    res.json({
      ok: true,
      exists: !!d,
      assigned: !!d?.userId,
      device: d || null
    });
  } catch (e) {
    res.status(500).json({ ok: false, message: e.message });
  }
}


// Add Device
router.post("/add", authMiddleware, async (req, res) => {
  const {
    // legacy/manual fields (still supported)
    deviceId,
    deviceType,
    manufacturer,
    prefixId,
    firmwareVersion,
    location,
    status,
    validity,
    accountId,
    profileId,
  } = req.body;

  const now = new Date();
  const processedStatus = (status || 'inactive').toLowerCase();

  try {
    const payload = {
      firmwareVersion,
      location,
      status: processedStatus,
      validity,
      createdAt: now,
      lastActiveAt: now,
      userId: req.user.userId,
      accountId,  
      profileId  
    };
    if (profileId) {
      payload.profileId = new mongoose.Types.ObjectId(profileId);
    }

    if (prefixId) {
      // --- New: server issues deviceId from prefix ---
      const p = await DevicePrefix.findByIdAndUpdate(
        prefixId,
        { $inc: { sequence: 1 } },
        { new: true, session }
      );
      if (!p) throw new Error("Invalid prefixId");

      const second = pad5(p.sequence);                    
      payload.deviceId = `${p.prefix}-${second}`;        
      payload.deviceType = p.deviceName;                
      payload.manufacturer = p.manufacturer;       
    } else {
     
      if (!deviceId || !deviceType || !manufacturer) {
        throw new Error("deviceId, deviceType and manufacturer are required (or provide prefixId)");
      }
      if (!ID_RX.test(deviceId)) {
        throw new Error("deviceId must match ######-XXXXXXXXXXXX (4 digits, hyphen, 12 hex chars)");
      }
      payload.deviceId = deviceId.trim().toUpperCase();
      payload.deviceType = deviceType.trim();
      payload.manufacturer = manufacturer.trim();
    }

    console.log(">>> Creating device with payload:", payload);
    const [device] = await Device.create([payload]);
    console.log(">>> Device created:", device._id, "deviceId:", device.deviceId, "status:", device.status);

    const user = await User.findById(req.user.userId);
    console.log(">>> Found user:", req.user.userId, "=>", user ? "YES" : "NO");

    if (user) {
      console.log(">>> Before push, user.devices:", user.devices);

      user.devices.push(device._id);
      console.log(">>> After push, user.devices:", user.devices);

      if (!user.activeDevice && device.status === "active") {
        console.log(">>> No activeDevice set, assigning device:", device._id);
        user.activeDevice = new mongoose.Types.ObjectId(device._id);
      } else {
        console.log(">>> activeDevice already set or device not active:",
          "activeDevice:", user.activeDevice,
          "device.status:", device.status
        );
      }

      await user.save();
      console.log(">>> User saved with activeDevice:", user.activeDevice);
    } else {
      console.log(">>> No user found with ID:", req.user.userId);
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
  }
  catch (error) {

    // duplicate key (unique deviceId)
    if (error && error.code === 11000) {
      return res.status(409).json({
        status: "fail",
        message: "A device with this Device ID already exists."
      });
    }

    // validation / format errors
    if (error && (error.name === 'ValidationError' || error.message)) {
      return res.status(400).json({
        status: "fail",
        message: error.message || "Validation failed"
      });
    }

    console.error("Error adding device:", error);
    return res.status(500).json({
      status: "fail",
      message: "An internal server error occurred while adding the device."
    });
  }
});


// GET /devices/organization/:organizationId - Fetch devices by organizationId (Admin only)
router.get('/devices/organization/:organizationId', authMiddleware, adminMiddleware, async (req, res) => {
  try {
    const { organizationId } = req.params;

    // Convert organizationId string to ObjectId
    if (!mongoose.Types.ObjectId.isValid(organizationId)) {
      return res.status(400).json({
        status: "fail",
        message: "Invalid organization ID format"
      });
    }

    const orgObjectId = new mongoose.Types.ObjectId(organizationId);

    // Pagination
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 10;
    const skip = (page - 1) * limit;

    // Step 1: Find all users in this organization using ObjectId
    const usersInOrg = await User.find({ organizationId: orgObjectId }).select('devices');

    if (usersInOrg.length === 0) {
      return res.status(200).json({
        status: "success",
        results: 0,
        totalPages: 0,
        currentPage: page,
        total: 0,
        organizationId,
        data: []
      });
    }

    // Step 2: Extract all device IDs from users' devices arrays
    const deviceIds = [];
    usersInOrg.forEach(user => {
      if (user.devices && user.devices.length > 0) {
        deviceIds.push(...user.devices);
      }
    });

    if (deviceIds.length === 0) {
      return res.status(200).json({
        status: "success",
        results: 0,
        totalPages: 0,
        currentPage: page,
        total: 0,
        organizationId,
        data: []
      });
    }

    // Step 3: Build filter for devices
    const filter = {
      _id: { $in: deviceIds }
    };

    // Additional filters
    if (req.query.status) {
      filter.status = req.query.status.toLowerCase().trim();
    }

    if (req.query.deviceType) {
      filter.deviceType = req.query.deviceType;
    }

    // Search by deviceId or manufacturer
    if (req.query.search) {
      filter.$or = [
        { deviceId: { $regex: req.query.search, $options: "i" } },
        { manufacturer: { $regex: req.query.search, $options: "i" } }
      ];
    }

    // Step 4: Fetch device details with pagination
    const devices = await Device.find(filter)
      .populate('userId', 'name email organizationId')
      .sort({ createdAt: -1 })
      .skip(skip)
      .limit(limit);

    // Get total count for pagination
    const total = await Device.countDocuments(filter);

    res.status(200).json({
      status: "success",
      results: devices.length,
      totalPages: Math.ceil(total / limit),
      currentPage: page,
      total,
      organizationId,
      data: devices
    });

  } catch (error) {
    console.error("Error fetching devices by organization:", error);
    res.status(500).json({
      status: "fail",
      message: "Server error",
      error: error.message
    });
  }
});

router.get('/devices/by-deviceId/:deviceId', getByDeviceId);

router.get('/validate', validateDeviceId);

router.get("/history", authMiddleware, deviceController.getDeviceHistory);
router.get("/history/respiration", authMiddleware, deviceController.getRespirationLive);
router.get("/history/stress", authMiddleware, deviceController.getStressAggregates);

// in the same router that serves other /public routes

router.get('/public/available', async (req, res) => {
  try {
    const deviceId = String(req.query.deviceId || '').trim().toUpperCase();
    if (!deviceId) return res.status(400).json({ ok: false, message: 'deviceId required' });

    const d = await Device.findOne({ deviceId }).lean();
    return res.json({
      ok: true,
      exists: !!d,
      assigned: !!d?.userId,
      device: d || null
    });
  } catch (e) {
    res.status(500).json({ ok: false, message: e.message });
  }
});

// was: router.get('/users/suggest', ...
router.get('/users/suggest', async (req, res) => {
  try {
    const q = (req.query.q || '').trim();
    const limit = Math.min(parseInt(req.query.limit || '10', 10), 25);
    if (q.length < 2) return res.json({ data: [], note: 'q too short' });

    const rx = new RegExp(q.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i');
    const users = await User.find(
      { $or: [{ email: rx }, { name: rx }] },
      { _id: 1, email: 1, name: 1 }
    ).limit(limit).lean();

    res.json({ data: users });
  } catch (e) {
    console.error('users/suggest error:', e);
    res.status(500).json({ message: 'Server error' });
  }
});



// PUT /activate/:deviceId?profileId=xxxx
router.put("/activate/:deviceId", authMiddleware, async (req, res) => {
  try {
    const { deviceId } = req.params;
    const { profileId } = req.query;
    if (!profileId) return res.status(400).json({ message: "profileId required" });

    const device = await Device.findOne({ deviceId });
    if (!device) return res.status(404).json({ message: "Device not found" });

    // --- if already active under another profile
    if (device.status === "active" && device.profileId && device.profileId.toString() !== profileId) {
      const activeProfile = await Profile.findById(device.profileId).lean();
      return res.status(409).json({
        message: `Device ${deviceId} is already active on profile "${activeProfile?.identifier || device.profileId}"`
      });
    }

    // --- deactivate all devices for this profile
    await Device.updateMany({ profileId }, { $set: { status: "inactive" } });


    device.status = "active";
    device.profileId = new mongoose.Types.ObjectId(profileId);
    device.lastActiveAt = new Date();
    await device.save();
    await User.findByIdAndUpdate(
      device.userId,
      { $set: { activeDevice: new mongoose.Types.ObjectId(device._id) } },
      { new: true }
    );

    // --- get profile name for success message
    const newProfile = await Profile.findById(profileId).lean();

    return res.json({
      message: `Device ${deviceId} activated on profile "${newProfile?.identifier || profileId}"`,
      deviceId,
      profileId
    });

  } catch (e) {
    console.error("activate error:", e);
    res.status(500).json({ message: e.message });
  }
});


// GET /mapping/:profileId
router.get("/mapping/:profileId", authMiddleware, async (req, res) => {
  try {
    const { profileId } = req.params;
    const devices = await Device.find({ profileId }).lean();
    res.json({
      profileId,
      devices: devices.map(d => ({
        deviceId: d.deviceId,
        status: d.status,
        active: d.status === "active"
      }))
    });
  } catch (e) {
    console.error("mapping error:", e);
    res.status(500).json({ message: e.message });
  }
});


// GET /profiles/:profileId/active-device
router.get("/profiles/:profileId/active-device", authMiddleware, async (req, res) => {
  try {
    const { profileId } = req.params;
    const active = await Device.findOne({ profileId, status: "active" }).lean();
    res.json({
      profileId,
      activeDevice: active ? { deviceId: active.deviceId } : null
    });
  } catch (e) {
    res.status(500).json({ message: e.message });
  }
});


module.exports = router;