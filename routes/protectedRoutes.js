const express = require("express");
const User = require("../models/User");
const Device = require("../models/Device");
const HealthData = require("../models/HealthData");
const SleepData = require("../models/SleepData");
// Adjust paths/file names to your project
const Manufacturer = require('../models/Manufacturer');
const DeviceModel = require('../models/DeviceModel');
const Technology = require('../models/Technology');
const Port = require('../models/Port');
const authMiddleware = require("../middleware/authMiddleware");
const adminMiddleware = require("../middleware/adminMiddleware");
const mongoose = require("mongoose");
const deviceController = require("../controllers/deviceManagementController");

const router = express.Router();

// GET /devices/details/:deviceId - Fetch device details by deviceId
router.get('/devices/details/:deviceId', authMiddleware, async (req, res) => {
    try {
        const { deviceId } = req.params;
        const device = await Device.findOne({ deviceId });
        if (!device) {
            return res.status(404).json({ message: 'Device not found' });
        }


        // derive model code (first 2 chars or digits)
        const modelCode = device.deviceId?.slice(0, 2);

        // lookup model  manufacturer
        const model = modelCode
            ? await DeviceModel.findOne({ code: modelCode }).populate("manufacturerId", "code name").lean()
            : null;

        const modelName = model?.name || null;
        const manufacturerName = model?.manufacturerId?.name || null;

        res.json({
            data: {
                ...device.toObject(),
                modelCode,
                modelName,
                manufacturerName,
            }
        });


    } catch (error) {
        res.status(500).json({ message: 'Server error', error: error.message });
    }
});

// ✅ Fetch Devices Assigned to Logged-in User
router.get("/devices/user", authMiddleware, async (req, res) => {
  try {
    const user = await User.findById(req.user.userId).lean();
    if (!user) return res.status(404).json({ message: "User not found" });

    // fetch all devices that belong to this user
    const devices = await Device.find(
      { userId: req.user.userId },
      "deviceId name deviceType manufacturer firmwareVersion location status"
    ).lean();

    console.log("📋 Returning devices for user:", req.user.userId, devices.map(d => d.deviceId));

    res.json({
      devices,
      activeDevice: user.activeDevice
    });
  } catch (error) {
    console.error("Error fetching devices:", error);
    res.status(500).json({ message: "Server error" });
  }
});


// ✅ Assign a Device to User (resilient to legacy orgId values)
router.post('/devices/assign', authMiddleware, async (req, res) => {
    try {
        const { deviceId } = req.body;
        const userId = req.user.userId;

        if (!deviceId?.trim()) {
            return res.status(400).json({ status: 'fail', message: 'Device ID is required' });
        }

        const device = await Device.findOne({ deviceId });
        if (!device) {
            return res.status(404).json({ status: 'fail', message: 'Device not found' });
        }

        // 1) assign device to user
        await Device.updateOne({ _id: device._id }, { $set: { userId } });

        // 2) add to user's devices and set activeDevice (no full validation)
        const updatedUser = await User.findByIdAndUpdate(
            userId,
            {
                $addToSet: { devices: device._id },
                $set: { activeDevice: device._id }
            },
            { new: true } // runValidators defaults to false -> we avoid orgId cast
        ).select('devices role');

        // 3) promote to admin if >1 device
        if (updatedUser?.devices?.length > 1 && updatedUser.role !== 'admin') {
            await User.updateOne({ _id: userId }, { $set: { role: 'admin' } });
        }

        return res.json({
            status: 'success',
            message: 'Device assigned to logged-in user',
            data: { deviceId: device.deviceId }
        });
    } catch (error) {
        return res.status(500).json({ status: 'fail', message: error.message });
    }
});


// ✅ Remove a Device from User Profile
router.delete("/devices/remove/:deviceId", authMiddleware, async (req, res) => {
  try {
    // Find the device by deviceId
    const device = await Device.findOne({ deviceId: req.params.deviceId });

    if (!device) {
      return res.status(404).json({ message: "Device not found" });
    }

    // Remove device reference from user
    const user = await User.findByIdAndUpdate(
      req.user.userId,
      { $pull: { devices: device._id } },
      { new: true }
    );

    if (!user) return res.status(404).json({ message: "User not found" });

    // Delete the actual device document from DB
    await Device.deleteOne({ _id: device._id });

    res.json({ message: `Device ${device.deviceId} removed completely` });
  } catch (error) {
    console.error("Error removing device:", error);
    res.status(500).json({ message: "Server error" });
  }
});

// ✅ Get health data for a specific device
router.get("/data/health/:deviceId", authMiddleware, async (req, res) => {
    try {
        // Get query parameters for filtering
        const { start, end, limit } = req.query;

        // Build query
        const query = { deviceId: req.params.deviceId };
        if (start && end) {
            query.timestamp = {
                $gte: new Date(start),
                $lte: new Date(end)
            };
        }

        // Query with optional limit and sorting
        const healthData = await HealthData.find(query)
            .sort({ timestamp: -1 }) // Newest first
            .limit(limit ? parseInt(limit) : 1000)
            .select("+metrics +signals +raw");

        res.json(healthData);
    } catch (error) {
        console.error("Error fetching health data:", error);
        res.status(500).json({ message: "Server error" });
    }
});

// ✅ Get sleep data for a specific device
router.get("/data/sleep/:deviceId", authMiddleware, async (req, res) => {
    try {
        // Get query parameters for filtering
        const { start, end, limit } = req.query;

        // Build query
        const query = { deviceId: req.params.deviceId };
        if (start && end) {
            query.timestamp = {
                $gte: new Date(start),
                $lte: new Date(end)
            };
        }

        // Query with optional limit and sorting
        const sleepData = await SleepData.find(query)
            .sort({ timestamp: -1 }) // Newest first
            .limit(limit ? parseInt(limit) : 100);

        res.json(sleepData);
    } catch (error) {
        console.error("Error fetching sleep data:", error);
        res.status(500).json({ message: "Server error" });
    }
});

// ✅ Get historical data with aggregation - using only HealthData model
router.get("/data/history/:deviceId", authMiddleware, async (req, res) => {
    try {
        const { period = '24h' } = req.query; // Default to 24 hours
        const deviceId = req.params.deviceId;

        // Calculate time periods and aggregation intervals
        const endDate = new Date();
        let startDate, aggregationMinutes;

        switch (period) {
            case '24h':
                startDate = new Date(endDate - 24 * 60 * 60 * 1000);
                aggregationMinutes = 5; // 5-minute intervals
                break;
            case '48h':
                startDate = new Date(endDate - 48 * 60 * 60 * 1000);
                aggregationMinutes = 10; // 10-minute intervals
                break;
            case '72h':
                startDate = new Date(endDate - 72 * 60 * 60 * 1000);
                aggregationMinutes = 15; // 15-minute intervals
                break;
            case '7d':
                startDate = new Date(endDate - 7 * 24 * 60 * 60 * 1000);
                aggregationMinutes = 60; // 1-hour intervals
                break;
            case '30d':
                startDate = new Date(endDate - 30 * 24 * 60 * 60 * 1000);
                aggregationMinutes = 360; // 6-hour intervals
                break;
            default:
                startDate = new Date(endDate - 24 * 60 * 60 * 1000);
                aggregationMinutes = 5;
        }

        console.log(`Fetching historical data for ${deviceId} from ${startDate} to ${endDate} with ${aggregationMinutes}min intervals`);

        // Comprehensive aggregation pipeline for health data including all metrics
        const healthData = await HealthData.aggregate([
            {
                $match: {
                    deviceId: deviceId,
                    timestamp: { $gte: startDate, $lte: endDate }
                }
            },
            {
                $group: {
                    _id: {
                        // Group by time interval (truncate to nearest aggregationMinutes)
                        year: { $year: "$timestamp" },
                        month: { $month: "$timestamp" },
                        day: { $dayOfMonth: "$timestamp" },
                        hour: { $hour: "$timestamp" },
                        interval: {
                            $subtract: [
                                { $minute: "$timestamp" },
                                { $mod: [{ $minute: "$timestamp" }, aggregationMinutes] }
                            ]
                        }
                    },
                    timestamp: { $first: "$timestamp" },
                    temp: { $avg: "$temp" },
                    humidity: { $avg: "$humidity" },
                    iaq: { $avg: "$iaq" },
                    eco2: { $avg: "$eco2" },
                    tvoc: { $avg: "$tvoc" },
                    etoh: { $avg: "$etoh" },
                    hrv: { $avg: "$hrv" },
                    stress: { $avg: "$stress" },
                    heartRate: { $avg: "$heartRate" },
                    respiration: { $avg: "$respiration" },

                    // ✅ HRV UART Metrics
                    sdnn: { $avg: "$metrics.sdnn" },
                    rmssd: { $avg: "$metrics.rmssd" },
                    lf: { $avg: "$metrics.lf" },
                    hf: { $avg: "$metrics.hf" },
                    lfhf: { $avg: "$metrics.lfhf" }
                }
            },
            {
                $project: {
                    _id: 0,
                    timestamp: 1,
                    // Round all values to nearest integer
                    temp: { $round: ["$temp", 0] },
                    humidity: { $round: ["$humidity", 0] },
                    iaq: { $round: ["$iaq", 0] },
                    eco2: { $round: ["$eco2", 0] },
                    tvoc: { $round: ["$tvoc", 0] },
                    etoh: { $round: ["$etoh", 2] },
                    hrv: { $round: ["$hrv", 0] },
                    stress: { $round: ["$stress", 0] },
                    heartRate: { $round: ["$heartRate", 0] },
                    hr: { $round: ["$heartRate", 0] },
                    respiration: { $round: ["$respiration", 0] },
                    resp: { $round: ["$respiration", 0] },
                    sdnn: { $round: ["$sdnn", 0] },
                    rmssd: { $round: ["$rmssd", 0] },
                    lf: { $round: ["$lf", 0] },
                    hf: { $round: ["$hf", 0] },
                    lfhf: { $round: ["$lfhf", 2] }
                }
            },
            {
                $sort: { "timestamp": 1 }
            }
        ]);

        res.json({
            period,
            aggregationMinutes,
            data: healthData
        });

    } catch (error) {
        console.error("Error fetching historical data:", error);
        res.status(500).json({ message: "Server error" });
    }
});

// ✅ Get raw health data for a specific device (no averaging/preprocessing)
router.get("/data/health/raw/:deviceId", authMiddleware, async (req, res) => {
    try {
        const { deviceId } = req.params;
        const { start, end, limit, page } = req.query;
        const query = { deviceId: deviceId };
        if (start && end) {
            query.timestamp = {
                $gte: new Date(start),
                $lte: new Date(end)
            };
        } else if (start && end) {
            query.timestamp = { $gte: new Date(start), $lte: new Date(end) };
        } else if (start) {
            query.timestamp = { $gte: new Date(start) };
        } else if (end) {
            query.timestamp = { $lte: new Date(end) };
        }

        // Pagination
        const pageNumber = page ? parseInt(page) : 1;
        const limitNumber = limit ? parseInt(limit) : 1000;
        const skip = (pageNumber - 1) * limitNumber;

        // Fetch with UART fields
        const healthData = await HealthData.find(query)
            .sort({ timestamp: -1 })
            .skip(skip)
            .limit(limitNumber)
            .select("+metrics +signals +raw");

        const totalCount = await HealthData.countDocuments(query);

        res.json({
            status: "success",
            data: healthData,
            pagination: {
                currentPage: pageNumber,
                totalPages: Math.ceil(totalCount / limitNumber),
                totalRecords: totalCount,
                recordsPerPage: limitNumber,
                hasNextPage: pageNumber < Math.ceil(totalCount / limitNumber),
                hasPrevPage: pageNumber > 1
            },
            deviceId
        });

    } catch (error) {
        console.error("Error fetching raw health data:", error);
        res.status(500).json({
            status: "fail",
            message: "Server error",
            error: error.message
        });
    }
});

// ✅ Manually update device statuses (for testing)
router.post("/devices/update-status", authMiddleware, async (req, res) => {
    try {
        // Only admins can manually trigger status updates
        const user = await User.findById(req.user.userId);
        if (user.role !== "admin") {
            return res.status(403).json({ message: "Unauthorized. Admin access required." });
        }

        const { updateAllDeviceStatuses } = require('../utils/deviceStatusManagement');
        const result = await updateAllDeviceStatuses();
        res.json(result);
    } catch (error) {
        console.error("Error updating device statuses:", error);
        res.status(500).json({ message: "Server error" });
    }
});

// ✅ Fetch Active Device ID by User Email (Admin/Superadmin only)
router.get("/devices/active-device/:userEmail", adminMiddleware, async (req, res) => {
    try {
        const { userEmail } = req.params;
        console.log("Fetching active device for user email:", userEmail);

        // Validate email format (basic validation)
        const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
        if (!emailRegex.test(userEmail)) {
            return res.status(400).json({
                status: "fail",
                message: "Invalid email format"
            });
        }

        // Get the user by email and populate activeDevice to get deviceId
        const user = await User.findOne({ email: userEmail })
            .select('activeDevice email name')
            .populate('activeDevice', 'deviceId');

        if (!user) {
            return res.status(404).json({
                status: "fail",
                message: "User not found"
            });
        }

        console.log("User found:", user.email, "Active device:", user.activeDevice);

        // Extract deviceId from populated device or return null if no active device
        const activeDeviceId = user.activeDevice ? user.activeDevice.deviceId : null;

        res.json({
            status: "success",
            data: {
                activeDeviceId: activeDeviceId,
                user: {
                    email: user.email,
                    name: user.name
                }
            }
        });
    } catch (error) {
        console.error("Error fetching active device:", error);
        res.status(500).json({
            status: "fail",
            message: "Server error",
            error: error.message
        });
    }
});

// GET /api/public/codes
// returns { manufacturersByCode, modelsByCode, technologiesByCode, portsByCode }
router.get('/public/codes', async (req, res, next) => {
    try {
        // --- fetch all in parallel ---
        const [manufacturers, models, technologies, ports] = await Promise.all([
            Manufacturer.find({}, 'code name').lean(),
            DeviceModel.aggregate([
                {
                    $lookup: {
                        from: "manufacturers",
                        localField: "manufacturerId",
                        foreignField: "_id",
                        as: "manufacturer"
                    }
                },
                { $unwind: { path: "$manufacturer", preserveNullAndEmptyArrays: true } },
                { $project: { code: 1, name: 1, manufacturer: { _id: 1, code: 1, name: 1 } } }
            ]),
            Technology.find({}, 'code name').lean(),
            Port.find({}, 'code name').lean(),
        ]);

        // --- build maps ---
        const byCode = (arr, pad = 0) =>
            Object.fromEntries(
                (arr || [])
                    .filter(x => x.code != null && x.name)
                    .map(x => {
                        const key = pad ? String(x.code).padStart(pad, '0') : String(x.code);
                        return [key, { id: String(x._id), name: x.name }];
                    })
            );

        const manufacturersByCode = byCode(manufacturers, 2);

        const modelsByCode = Object.fromEntries(
            (models || [])
                .filter(x => x.code != null && x.name)
                .map(x => {
                    const key = String(x.code).padStart(2, '0');
                    return [
                        key,
                        {
                            id: String(x._id),
                            name: x.name,
                            manufacturer: x.manufacturer
                                ? {
                                    id: String(x.manufacturer._id),
                                    code: x.manufacturer.code,
                                    name: x.manufacturer.name
                                }
                                : null
                        }
                    ];
                })
        );

        const technologiesByCode = Object.fromEntries(
            (technologies || [])
                .filter(x => x.code != null && x.name)
                .map(x => [String(x.code), x.name])
        );

        const portsByCode = Object.fromEntries(
            (ports || [])
                .filter(x => x.code != null && x.name)
                .map(x => [String(x.code), x.name])
        );

        res.json({ manufacturersByCode, modelsByCode, technologiesByCode, portsByCode });
    } catch (err) {
        console.error("[public/codes] error:", err.message);
        next(err);
    }
});

// GET /api/public/device-models/:code
router.get('/public/device-models/:code', async (req, res, next) => {
    try {
        const raw = String(req.params.code || '');
        const code = raw.padStart(2, '0'); // accept "2" or "02"

        const model = await DeviceModel.aggregate([
            { $match: { code } },
            {
                $lookup: {
                    from: "manufacturers",
                    localField: "manufacturerId",
                    foreignField: "_id",
                    as: "manufacturer"
                }
            },
            { $unwind: { path: "$manufacturer", preserveNullAndEmptyArrays: true } },
            {
                $project: {
                    _id: 1,
                    code: 1,
                    name: 1,
                    manufacturer: { _id: 1, code: 1, name: 1 },
                    metrics: 1
                }
            }
        ]);

        if (!model || !model.length) {
            return res.status(404).json({ status: 'fail', message: 'Device model not found' });
        }

        const m = model[0];
        res.json({
            data: {
                _id: String(m._id),
                code: m.code,
                name: m.name,
                manufacturer: m.manufacturer ? {
                    id: String(m.manufacturer._id),
                    code: m.manufacturer.code,
                    name: m.manufacturer.name
                } : null,
                metrics: (m.metrics || []).map(metric => ({
                    key: metric.key || metric.label,
                    label: metric.label,
                    graphType: String(metric.graphType || 'line').toLowerCase(),
                    averagingSec: (Number.isFinite(metric.averagingSec) ? metric.averagingSec
                        : Number.isFinite(metric.averagingSeconds) ? metric.averagingSeconds
                            : Number.isFinite(metric.avgSec) ? metric.avgSec : null),
                    min: metric.min,
                    max: metric.max,
                    alertMin: metric.alertMin,
                    alertMax: metric.alertMax,
                }))
            }
        });
    } catch (err) {
        console.error("[public/device-models] error:", err.message);
        next(err);
    }
});

// POST /api/device-models
router.post('/device-models', async (req, res, next) => {
    try {
        const { code, name, manufacturerCode, metrics } = req.body;
        if (!code || !name) {
            return res.status(400).json({ ok: false, message: "code and name are required" });
        }

        const DeviceModel = require('../models/DeviceModel');
        const doc = await DeviceModel.findOneAndUpdate(
            { code },
            { $set: { name, manufacturerCode, metrics } },
            { new: true, upsert: true }
        );

        console.info("[device-models] saved:", doc.code, doc.name);
        res.json({ ok: true, data: doc });
    } catch (err) {
        console.error("[device-models] error:", err.message);
        next(err);
    }
});

// ✅ Set an Active Device for User
router.patch("/devices/activate/:deviceId", authMiddleware, async (req, res) => {
    try {
        const { deviceId } = req.params;
        const userId = req.user.userId;
        const userObjectId = new mongoose.Types.ObjectId(userId);

        console.log("🔑 Activate device route called");
        console.log("👉 Requested deviceId:", deviceId);
        console.log("👉 UserId (string):", userId);
        console.log("👉 UserId (ObjectId):", userObjectId);

        // 1) Find the device
        const device = await Device.findOne({ deviceId });
        if (!device) {
            console.log("❌ Device not found:", deviceId);
            return res.status(404).json({ message: "Device not found" });
        }

        console.log("✅ Found device:", device.deviceId, "status:", device.status, "userId:", device.userId);

        // 2) Ensure it belongs to user
        if (!device.userId || String(device.userId) !== String(userId)) {
            console.log("⚠️ Device does not belong to this user. device.userId:", device.userId);
            return res.status(403).json({ message: "Device does not belong to this user" });
        }

        // 3) Log all devices for user before update
        const beforeDevices = await Device.find({ userId: userObjectId }).lean();
        console.log("📋 Devices BEFORE update:", beforeDevices.map(d => ({
            deviceId: d.deviceId, status: d.status
        })));

        // 4) Deactivate others
        const updateResult = await Device.updateMany(
            { userId: userObjectId, deviceId: { $ne: deviceId } },
            { $set: { status: "inactive" } }
        );
        console.log("🔄 updateMany result:", updateResult);

        // 5) Activate current device
        device.status = "active";
        device.lastActiveAt = new Date();
        await device.save();
        console.log("✅ Activated device:", device.deviceId);

        // 6) Update user's activeDevice
        const user = await User.findByIdAndUpdate(
            userId,
            { activeDevice: device._id },
            { new: true }
        );

        console.log("👤 User updated activeDevice:", user?.activeDevice);

        // 7) Log all devices after update
        const afterDevices = await Device.find({ userId: userObjectId }).lean();
        console.log("📋 Devices AFTER update:", afterDevices.map(d => ({
            deviceId: d.deviceId, status: d.status
        })));

        res.json({
            message: `Device ${deviceId} set as active`,
            activeDevice: device.deviceId
        });
    } catch (error) {
        console.error("❌ Error in activate route:", error);
        res.status(500).json({ message: "Server error" });
    }
});

router.patch("/devices/deactivate/:deviceId", authMiddleware, deviceController.setInactiveDevice);

// POST /api/user/devices/save
router.post('/user/devices/save', authMiddleware, async (req, res) => {
    try {
        const userId = req.user.userId;

        // normalize incoming ids
        const raw = [
            ...(Array.isArray(req.body.deviceIds) ? req.body.deviceIds : []),
            ...(Array.isArray(req.body.devices) ? req.body.devices.map(d => d.deviceId) : []),
            ...(req.body.deviceId ? [req.body.deviceId] : []),
        ];

        const ids = [...new Set(
            raw.map(s => String(s || '').trim().toUpperCase()).filter(Boolean)
        )];

        if (!ids.length) {
            return res.status(400).json({ status: 'fail', message: 'deviceId(s) required' });
        }

        const results = [];

        // assign each device to this user & add to user's devices[]
        for (const deviceId of ids) {
            const device = await Device.findOne({ deviceId });

            if (!device) {
                results.push({ deviceId, ok: false, status: 404, message: 'Device not found' });
                continue;
            }

            // set userId on device (idempotent)
            if (!device.userId || String(device.userId) !== String(userId)) {
                await Device.updateOne({ _id: device._id }, { $set: { userId } });
            }

            // push into user's devices (no duplicates)
            await User.updateOne(
                { _id: userId },
                { $addToSet: { devices: device._id } }
            );

            results.push({ deviceId, ok: true, status: 200 });
        }

        // fetch user to finalize activeDevice + role
        const updatedUser = await User.findById(userId).select('devices role activeDevice');
        if (updatedUser && !updatedUser.activeDevice && updatedUser.devices?.length) {
            await User.updateOne({ _id: userId }, { $set: { activeDevice: updatedUser.devices[0] } });
        }
        if (updatedUser && updatedUser.devices?.length > 1 && updatedUser.role !== 'admin') {
            await User.updateOne({ _id: userId }, { $set: { role: 'admin' } });
        }

        const allOk = results.every(r => r.ok);
        return res.status(200).json({
            status: allOk ? 'success' : 'partial',
            saved: results.filter(r => r.ok).map(r => r.deviceId),
            failed: results.filter(r => !r.ok),
            count: { requested: ids.length, saved: results.filter(r => r.ok).length }
        });
    } catch (err) {
        console.error('save devices error:', err);
        return res.status(500).json({ status: 'fail', message: err.message });
    }
});

module.exports = router;


