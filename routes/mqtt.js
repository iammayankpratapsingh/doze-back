const express = require("express");
const mqtt = require("mqtt");
const dotenv = require("dotenv");
const HealthData = require("../models/HealthData");
const SleepData = require("../models/SleepData");
const Device = require("../models/Device");
const User = require("../models/User");
const SPEC = require("../config/metricSpec");

dotenv.config();

const router = express.Router();

const MQTT_BROKER_URL = process.env.MQTT_BROKER_URL || "mqtt://172.105.98.123:1883";
const MQTT_USERNAME = process.env.MQTT_USERNAME || "doze";
const MQTT_PASSWORD = process.env.MQTT_PASSWORD || "bK67ZwBHSWkl";

let client;

// Helper function to convert string to number
const toNum = (v) => {
    if (v === "" || v === null || v === undefined) return undefined;
    const n = Number(v);
    return Number.isFinite(n) ? n : undefined;
};

// Parse abbreviated JSON format -> { patch, metrics, signals, raw }
function parseAbbreviatedFormat(data) {
    if (!data || typeof data !== "object") return null;
    
    const patch = {};
    const metrics = {};
    const signals = {};
    const raw = {};
    
    // Map abbreviated fields to database fields
    if (data.T !== undefined && data.T !== "") patch.temp = toNum(data.T);
    if (data.H !== undefined && data.H !== "") patch.humidity = toNum(data.H);
    if (data.HR !== undefined && data.HR !== "") patch.heartRate = toNum(data.HR);
    if (data.RE !== undefined && data.RE !== "") patch.respiration = toNum(data.RE);
    if (data.S !== undefined && data.S !== "") patch.stress = toNum(data.S);
    if (data.IA !== undefined && data.IA !== "") patch.iaq = toNum(data.IA);
    if (data.CO !== undefined && data.CO !== "") patch.eco2 = toNum(data.CO);
    if (data.VO !== undefined && data.VO !== "") patch.tvoc = toNum(data.VO);
    if (data.ET !== undefined && data.ET !== "") patch.etoh = toNum(data.ET);
    
    // Battery/Voltage
    if (data.V !== undefined && data.V !== "") signals.battery = toNum(data.V);
    
    // Store all fields in raw for reference
    Object.assign(raw, data);
    
    return { patch, metrics, signals, raw };
}

// Parse one UART CSV line -> { patch, metrics, signals }
function parseUartLine(line) {
    if (!line || typeof line !== "string") return null;
    const parts = line.trim().split(",").map(s => s.trim());
    const tag = (parts[0] || "").toUpperCase();

    const patch = {};
    const metrics = {};
    const signals = {};

    switch (tag) {
        case "HRV_DATA": {
            if (parts.length >= 17) {
                const [
                    _,
                    mean_rr, sdnn, rmssd, pnn50, hr_median, rr_tri_index, tin_rmssd,
                    sd1, sd2, lf, hf, lfhf, sample_entropy, sd1sd2, sns_index, pns_index
                ] = parts;

                Object.assign(metrics, {
                    mean_rr: toNum(mean_rr),
                    sdnn: toNum(sdnn),
                    rmssd: toNum(rmssd),
                    pnn50: toNum(pnn50),
                    hr_median: toNum(hr_median),
                    rr_tri_index: toNum(rr_tri_index),
                    tin_rmssd: toNum(tin_rmssd),
                    sd1: toNum(sd1),
                    sd2: toNum(sd2),
                    lf: toNum(lf),
                    hf: toNum(hf),
                    lfhf: toNum(lfhf),
                    sample_entropy: toNum(sample_entropy),
                    sd1sd2: toNum(sd1sd2),
                    sns_index: toNum(sns_index),
                    pns_index: toNum(pns_index),
                });

                if (metrics.rmssd !== undefined) patch.hrv = metrics.rmssd;
                if (metrics.hr_median !== undefined) patch.heartRate = metrics.hr_median;
            }
            break;
        }
        case "TEMP_HUM":
            patch.temp = toNum(parts[1]);
            patch.humidity = toNum(parts[2]);
            break;
        case "HR":
            patch.heartRate = toNum(parts[1]);
            break;
        case "RES":
            patch.respiration = toNum(parts[1]);
            break;
        case "STRESS":
            patch.stress = toNum(parts[1]);
            break;
        case "RR":
            break;
        case "MOTION":
            signals.motion = parts[1] !== undefined ? Number(parts[1]) === 1 : undefined;
            break;
        case "PRESENCE":
            signals.presence = parts[1] !== undefined ? Number(parts[1]) === 1 : undefined;
            break;
        case "ACT":
        case "ACTIVITY":
            signals.activity = toNum(parts[1]);
            break;
        case "BAT":
            signals.battery = toNum(parts[1]);
            break;
        case "MIC":
            signals.mic = toNum(parts[1]);
            break;
        default:
            break;
    }

    return { patch, metrics, signals, raw: line };
}

// ✅ Connect to MQTT Broker with MQTT v5 support
const connectMQTT = () => {
    client = mqtt.connect(MQTT_BROKER_URL, {
        username: MQTT_USERNAME,
        password: MQTT_PASSWORD,
        protocolVersion: 5, // Enable MQTT v5
        clean: true,
        connectTimeout: 4000,
        reconnectPeriod: 1000,
        properties: {
            sessionExpiryInterval: 60 * 60, // Session expiry interval in seconds
        },
    });

    client.on("connect", async (connack) => {
        // console.log("✅ Connected to MQTT broker with MQTT v5");
        // console.log("Connack properties:", connack.properties);

        // Get all devices and subscribe to their topics
        try {
            const devices = await Device.find().select("deviceId");
            // console.log(`✅ Found ${devices.length} devices to subscribe`);

            devices.forEach((device) => {
                if (device.deviceId) {
                    subscribeToDeviceTopics(device.deviceId);
                }
            });
        } catch (err) {
            console.error("❌ Error fetching devices:", err);
        }
    });

    client.on("message", async (topic, message) => {
        console.log("📩 Raw MQTT message:", topic, message.toString());

        try {
            const data = JSON.parse(message.toString());
            const topicParts = topic.split("/");
            const deviceId = topicParts[1];

            if (!deviceId) {
                console.warn("⚠️ No device ID found in topic:", topic);
                return;
            }

            const device = await Device.findOne({ deviceId });
            if (device) {
                await Device.findByIdAndUpdate(device._id, {
                    status: "active",
                    lastActiveAt: new Date(),
                });
                // console.log(`✅ Updated device status to active: ${deviceId}`);
            } else {
                console.warn(`⚠️ Device not found in database: ${deviceId}`);
            }

            if (topic.includes("/health")) {

                const newData = data; // incoming payload

                // Base document - only essential fields, no mapped fields
                const base = {
                    deviceId,
                    timestamp: new Date(),
                    metrics: {
                        ...(newData.metrics || {})
                    },
                    signals: {
                        motion: newData.signals?.motion ?? null,
                        presence: newData.signals?.presence ?? null,
                        battery: newData.signals?.battery ?? null,
                        activity: newData.signals?.activity ?? null,
                        mic: newData.signals?.mic ?? null,
                        rrIntervals: newData.signals?.rrIntervals || [],
                        rawWaveform: newData.signals?.rawWaveform || []
                    },
                    raw: {}  // Empty for abbreviated format
                };

                // --- presence gating ---
                const presence = Number(base.signals.presence ?? 1);
                const state = presenceState.get(deviceId) || { lastPresence: 1, lastValues: {} };
                if (state.lastPresence === 1 && presence === 0) {
                    // retract last 12s
                    const cutoff = new Date(Date.now() - 12000);
                    const res = await HealthData.deleteMany({ deviceId, timestamp: { $gte: cutoff } });
                    console.log(`🔴 presence 1→0 for ${deviceId}, retracted ${res.deletedCount} docs`);
                }
                if (state.lastPresence === 0 && presence === 1) {
                    console.log(`🟢 presence 0→1 for ${deviceId}, resume immediately`);
                }
                state.lastPresence = presence;
                presenceState.set(deviceId, state);
                if (presence === 0) {
                    console.log(`⏸ skipped store (presence=0) for ${deviceId}`);
                    return;
                }

                // --- apply spec rules ---
                const rules = SPEC[device.deviceType]?.params || {};
                let violations = [];
                let changes = [];
                for (const [key, rule] of Object.entries(rules)) {
                    const value = newData[key] ?? base[key];
                    if (value == null) continue;
                    if (rule.min != null && value < rule.min) violations.push(`${key}<min`);
                    if (rule.max != null && value > rule.max) violations.push(`${key}>max`);
                    if (rule.mode === "onchange") {
                        if (state.lastValues[key] === value) continue; // skip no-change
                    }
                    changes.push(key);
                    state.lastValues[key] = value;
                }
                presenceState.set(deviceId, state);
                if (violations.length) {
                    console.log(`⚠️ ${deviceId} skipped (violations: ${violations.join(",")})`);
                    return;
                }
                if (!changes.length) {
                    console.log(`⚠️ ${deviceId} skipped (no-change values)`);
                    return;
                }

                // --- optional UART CSV parsing (if device sends UART lines) ---
                // Also handle abbreviated JSON format (TS, T, H, HR, RE, etc.)
                let mergedPatch = {};
                let mergedMetrics = {};
                let mergedSignals = {};
                let raws = [];

                // Check if data is in abbreviated format (has TS, T, H, HR, etc.)
                const isAbbreviatedFormat = newData.hasOwnProperty("TS") || newData.hasOwnProperty("T") || 
                                           newData.hasOwnProperty("H") || newData.hasOwnProperty("HR");
                
                if (isAbbreviatedFormat) {
                    const parsed = parseAbbreviatedFormat(newData);
                    if (parsed) {
                        Object.assign(mergedPatch, parsed.patch);
                        Object.assign(mergedMetrics, parsed.metrics);
                        Object.assign(mergedSignals, parsed.signals);
                        // Store abbreviated format in raw
                        raws.push(JSON.stringify(parsed.raw));
                    }
                }

                const lines = Array.isArray(newData.lines)
                    ? newData.lines
                    : (newData.line ? [newData.line] : []);

                for (const ln of lines) {
                    const parsed = parseUartLine(String(ln));
                    if (!parsed) continue;
                    Object.assign(mergedPatch, parsed.patch);
                    Object.assign(mergedMetrics, parsed.metrics);
                    Object.assign(mergedSignals, parsed.signals);
                    raws.push(parsed.raw);
                }

                // collect extra flat metric keys if present
                const extraMetrics = {};
                [
                    "nn50", "sdsd", "mxdmn", "mo", "amo", "stress_ind",
                    "lf_pow", "hf_pow", "lf_hf_ratio", "bat", "mean_rr", "mean_hr",
                    "snore_num", "snore_freq", "pressure", "bvoc", "co2", "gas_percent",
                    "HRrest", "HRmax", "VO2max", "LactateThres", "TemperatureSkin",
                    "TemperatureEnv", "TemperatureCore", "ECG", "Barometer", "Accel",
                    "Gyro", "Magneto", "Steps", "Calories", "Distance", "BloodPressureSys",
                    "BloodPressureDia", "MuscleOxygenation", "GSR", "SleepStage",
                    "SleepQuality", "PostureFront", "PostureSide", "Fall", "BMI",
                    "BodyIndex", "ABSI", "Sports", "Start", "End", "NormalSinusRhythm",
                    "CHFAnalysis", "Diabetes", "TMT", "sdnn", "rmssd", "pnn50", "hr_median",
                    "rr_tri_index", "tin_rmssd", "sd1", "sd2", "lf", "hf", "lfhf",
                    "sample_entropy", "sd1sd2", "sns_index", "pns_index"
                ].forEach(k => {
                    if (newData[k] !== undefined) extraMetrics[k] = newData[k];
                });

                // Remove 'temperature' if present and ensure only 'temp' is used
                const { temperature, ...mergedPatchClean } = mergedPatch;
                
                // For abbreviated format, don't store in raw (fields are top-level)
                // Only store raw for UART CSV lines
                let finalRaw = {};
                if (isAbbreviatedFormat) {
                    // Don't store abbreviated fields in raw - they're top-level fields
                    finalRaw = {};
                } else if (raws.length > 0) {
                    finalRaw = raws.join("\n");
                } else {
                    finalRaw = base.raw || {};
                }
                
                // Build final document - only abbreviated fields and essential fields
                // Removed mapped fields: temp, humidity, iaq, eco2, tvoc, etoh, hrv, stress, respiration, heartRate
                const finalDoc = {
                    deviceId: base.deviceId,
                    timestamp: base.timestamp,
                    metrics: { ...(newData.metrics || {}), ...mergedMetrics, ...extraMetrics },
                    signals: { ...(newData.signals || {}), ...mergedSignals },
                    raw: finalRaw
                };
                
                // Add all abbreviated format fields directly to the document as top-level fields
                if (isAbbreviatedFormat && newData) {
                    finalDoc.TS = newData.TS;
                    finalDoc.TS_ms = newData.TS_ms;
                    finalDoc.T = newData.T;
                    finalDoc.H = newData.H;
                    finalDoc.MS = newData.MS;
                    finalDoc.MST = newData.MST;
                    finalDoc.AS = newData.AS;
                    finalDoc.AST = newData.AST;
                    finalDoc.SS = newData.SS;
                    finalDoc.SST = newData.SST;
                    finalDoc.SF = newData.SF;
                    finalDoc.RST = newData.RST;
                    finalDoc.RS = newData.RS;
                    finalDoc.V = newData.V;
                    finalDoc.L = newData.L;
                    finalDoc.S = newData.S;
                    finalDoc.HR = newData.HR;
                    finalDoc.RE = newData.RE;
                    finalDoc.IA = newData.IA;
                    finalDoc.CO = newData.CO;
                    finalDoc.VO = newData.VO;
                    finalDoc.ET = newData.ET;
                }
                
                // CRITICAL: Explicitly remove temperature and mapped fields if somehow present
                delete finalDoc.temperature;
                delete finalDoc.temp;
                delete finalDoc.humidity;
                delete finalDoc.iaq;
                delete finalDoc.eco2;
                delete finalDoc.tvoc;
                delete finalDoc.etoh;
                delete finalDoc.hrv;
                delete finalDoc.stress;
                delete finalDoc.respiration;
                delete finalDoc.heartRate;
                
                // Create document using set() method to ensure only valid fields
                const newHealthData = new HealthData();
                
                // Explicitly set each field - only abbreviated fields and essential fields
                // Removed mapped fields: temp, humidity, iaq, eco2, tvoc, etoh, hrv, stress, respiration, heartRate
                const fieldsToSet = {
                    deviceId: finalDoc.deviceId,
                    timestamp: finalDoc.timestamp,
                    metrics: finalDoc.metrics || {},
                    signals: finalDoc.signals || {},
                    raw: finalDoc.raw || {}  // Empty for abbreviated format
                };
                
                // Add all abbreviated format fields as top-level fields
                if (finalDoc.TS !== undefined) fieldsToSet.TS = finalDoc.TS;
                if (finalDoc.TS_ms !== undefined) fieldsToSet.TS_ms = finalDoc.TS_ms;
                if (finalDoc.T !== undefined) fieldsToSet.T = finalDoc.T;
                if (finalDoc.H !== undefined) fieldsToSet.H = finalDoc.H;
                if (finalDoc.MS !== undefined) fieldsToSet.MS = finalDoc.MS;
                if (finalDoc.MST !== undefined) fieldsToSet.MST = finalDoc.MST;
                if (finalDoc.AS !== undefined) fieldsToSet.AS = finalDoc.AS;
                if (finalDoc.AST !== undefined) fieldsToSet.AST = finalDoc.AST;
                if (finalDoc.SS !== undefined) fieldsToSet.SS = finalDoc.SS;
                if (finalDoc.SST !== undefined) fieldsToSet.SST = finalDoc.SST;
                if (finalDoc.SF !== undefined) fieldsToSet.SF = finalDoc.SF;
                if (finalDoc.RST !== undefined) fieldsToSet.RST = finalDoc.RST;
                if (finalDoc.RS !== undefined) fieldsToSet.RS = finalDoc.RS;
                if (finalDoc.V !== undefined) fieldsToSet.V = finalDoc.V;
                if (finalDoc.L !== undefined) fieldsToSet.L = finalDoc.L;
                if (finalDoc.S !== undefined) fieldsToSet.S = finalDoc.S;
                if (finalDoc.HR !== undefined) fieldsToSet.HR = finalDoc.HR;
                if (finalDoc.RE !== undefined) fieldsToSet.RE = finalDoc.RE;
                if (finalDoc.IA !== undefined) fieldsToSet.IA = finalDoc.IA;
                if (finalDoc.CO !== undefined) fieldsToSet.CO = finalDoc.CO;
                if (finalDoc.VO !== undefined) fieldsToSet.VO = finalDoc.VO;
                if (finalDoc.ET !== undefined) fieldsToSet.ET = finalDoc.ET;
                
                // CRITICAL: Double-check no temperature field
                delete fieldsToSet.temperature;
                
                newHealthData.set(fieldsToSet);

                console.log("🚀 Final Save Payload:", JSON.stringify({
                    metrics: { ...(newData.metrics || {}), ...mergedMetrics },
                    signals: { ...(newData.signals || {}), ...mergedSignals }
                }, null, 2));

                try {
                    const savedDoc = await newHealthData.save();
                    console.log(`✅ Saved to healthdata_new collection. Device: ${deviceId}, Document ID: ${savedDoc._id}`);
                    console.log("📊 Collection name:", savedDoc.collection.name);
                } catch (saveError) {
                    console.error(`❌ Error saving to healthdata_new for device ${deviceId}:`, saveError);
                    throw saveError;
                }
            } else if (topic.includes("/sleep")) {
                const newSleepData = new SleepData({
                    deviceId,
                    timestamp: new Date(),
                    sleepQuality: data.sleepQuality || "Unknown",
                    duration: data.duration || 0,
                });

                await newSleepData.save();
                // console.log(`✅ Sleep data saved for device ${deviceId}`);
            }
        } catch (error) {
            console.error("❌ Error processing MQTT message:", error);
        }
    });

    client.on("error", (error) => {
        console.error("❌ MQTT connection error:", error);
    });

    client.on("disconnect", () => {
        console.log("❌ Disconnected from MQTT broker");
    });

    client.on("reconnect", () => {
        console.log("🔄 Attempting to reconnect to MQTT broker");
    });

    return client;
};

// ✅ Subscribe to topics for a device
const subscribeToDeviceTopics = (deviceId) => {
    if (!client || !deviceId) {
        console.error("❌ Cannot subscribe: MQTT client not initialized or deviceId missing");
        return;
    }

    const healthTopic = `/${deviceId}/health`;
    const sleepTopic = `/${deviceId}/sleep`;

    client.subscribe([healthTopic, sleepTopic], { qos: 1 }, (err) => {
        if (err) {
            console.error(`❌ Failed to subscribe to topics for ${deviceId}:`, err);
        } else {
            // console.log(`✅ Subscribed to topics: ${healthTopic}, ${sleepTopic}`);
        }
    });
};

// ✅ Create API endpoint to subscribe to a new device's topics
router.post("/subscribe", async (req, res) => {
    try {
        const { deviceId } = req.body;

        if (!deviceId) {
            return res.status(400).json({ message: "Device ID is required" });
        }

        if (!client) {
            return res.status(500).json({ message: "MQTT client not initialized" });
        }

        subscribeToDeviceTopics(deviceId);
        res.json({ message: `Subscribed to topics for device ${deviceId}` });
    } catch (error) {
        console.error("Error subscribing to device topics:", error);
        res.status(500).json({ message: "Server error" });
    }
});


// ✅ Disconnect from MQTT Broker
const disconnectMQTT = () => {
    if (client) {
        client.end();
        console.log("❌ Disconnected from MQTT broker");
    }
};

// ✅ API Route to get MQTT connection status
router.get("/status", (req, res) => {
    const isConnected = client && client.connected;
    res.json({
        status: isConnected ? "connected" : "disconnected",
        message: isConnected ? "MQTT client is connected" : "MQTT client is not connected",
    });
});

// ✅ Export the router and functions properly
module.exports = {
    router,
    connectMQTT,
    disconnectMQTT,
    subscribeToDeviceTopics,
};