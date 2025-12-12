const express = require("express");
const dotenv = require("dotenv");
const HealthData = require("../models/HealthData");
const SleepData = require("../models/SleepData");
const Device = require("../models/Device");

dotenv.config();

const router = express.Router();

// --- UART helpers (keep inline to avoid new files)
const toNum = (v) => {
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
  
  // Helper to convert string to number
  const toNum = (v) => {
    if (v === "" || v === null || v === undefined) return undefined;
    const n = Number(v);
    return Number.isFinite(n) ? n : undefined;
  };
  
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

  const patch = {};    // goes to flat fields (temp, heartRate, respiration, hrv, stress…)
  const metrics = {};  // goes to HealthData.metrics (HRV detail)
  const signals = {};  // goes to HealthData.signals (flags)

  switch (tag) {
    case "HRV_DATA": {
      // Expected order (17 values after tag):
      // mean_rr, sdnn, rmssd, pnn50, hr_median, rr_tri_index, tin_rmssd,
      // sd1, sd2, lf, hf, lfhf, sample_entropy, sd1sd2, sns_index, pns_index
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

        // Optional: keep legacy flats filled if present
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
      // optional raw RR sample (not always present)
      if (!("sample_entropy" in metrics)) metrics.sample_entropy = undefined;
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
      // leave unrecognized as raw only
      break;
  }

  return { patch, metrics, signals, raw: line };
}

router.post("/ingest", async (req, res) => {
  try {
    const { deviceId, type, data } = req.body;

    if (!deviceId || !type || !data) {
      return res.status(400).json({ message: "deviceId, type, and data are required" });
    }

    const device = await Device.findOne({ deviceId });
    if (!device) {
      return res.status(404).json({ message: `Device ${deviceId} not found` });
    }

    await Device.findByIdAndUpdate(device._id, {
      status: "active",
      lastActiveAt: new Date(),
    });

    if (type === "health") {

      console.log("Incoming data:", JSON.stringify(data, null, 2));


      // CRITICAL: Remove temperature from data if present, use temp only
      // Store original data for raw field (but remove temperature)
      const originalDataForRaw = { ...data };
      delete originalDataForRaw.temperature;
      
      const cleanData = { ...data };
      delete cleanData.temperature;
      
      // Base document - only essential fields, no mapped fields
      const base = {
        deviceId,
        timestamp: new Date(),
        metrics: {
          ...(data.metrics || {})
        },
        signals: {
          motion: data.signals?.motion ?? null,
          presence: data.signals?.presence ?? null,
          battery: data.signals?.battery ?? null,
          activity: data.signals?.activity ?? null,
          mic: data.signals?.mic ?? null,
          rrIntervals: data.signals?.rrIntervals || [],
          rawWaveform: data.signals?.rawWaveform || []
        },
        raw: {}  // Empty for abbreviated format
      };


      // NEW: accept a single UART line or an array of lines and fold into the same doc
      // Also handle abbreviated JSON format (TS, T, H, HR, RE, etc.)
      let mergedPatch = {};
      let mergedMetrics = {};
      let mergedSignals = {};
      let raws = [];
      let isAbbreviatedFormat = false;

      // Check if data is in abbreviated format (has TS, T, H, HR, etc.)
      isAbbreviatedFormat = cleanData.hasOwnProperty("TS") || cleanData.hasOwnProperty("T") || 
                            cleanData.hasOwnProperty("H") || cleanData.hasOwnProperty("HR");
      
      if (isAbbreviatedFormat) {
        const parsed = parseAbbreviatedFormat(cleanData);
        if (parsed) {
          Object.assign(mergedPatch, parsed.patch);
          Object.assign(mergedMetrics, parsed.metrics);
          Object.assign(mergedSignals, parsed.signals);
          // Store ALL original fields in raw (including empty strings)
          // Use originalDataForRaw which has all fields except temperature
          raws.push(JSON.stringify(originalDataForRaw));
        }
      }

      // Also handle UART CSV lines if present
      const lines = Array.isArray(data.lines) ? data.lines : (data.line ? [data.line] : []);
      for (const ln of lines) {
        const parsed = parseUartLine(String(ln));
        if (!parsed) continue;
        Object.assign(mergedPatch, parsed.patch);
        Object.assign(mergedMetrics, parsed.metrics);
        Object.assign(mergedSignals, parsed.signals);
        raws.push(parsed.raw);
      }

      // Final document (legacy fields preserved, UART merged if present)
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
        metrics: { ...base.metrics, ...mergedMetrics },
        signals: { ...base.signals, ...mergedSignals },
        raw: finalRaw
      };
      
      // Add all abbreviated format fields directly to the document as top-level fields
      if (isAbbreviatedFormat && originalDataForRaw) {
        finalDoc.TS = originalDataForRaw.TS;
        finalDoc.TS_ms = originalDataForRaw.TS_ms;
        finalDoc.T = originalDataForRaw.T;
        finalDoc.H = originalDataForRaw.H;
        finalDoc.MS = originalDataForRaw.MS;
        finalDoc.MST = originalDataForRaw.MST;
        finalDoc.AS = originalDataForRaw.AS;
        finalDoc.AST = originalDataForRaw.AST;
        finalDoc.SS = originalDataForRaw.SS;
        finalDoc.SST = originalDataForRaw.SST;
        finalDoc.SF = originalDataForRaw.SF;
        finalDoc.RST = originalDataForRaw.RST;
        finalDoc.RS = originalDataForRaw.RS;
        finalDoc.V = originalDataForRaw.V;
        finalDoc.L = originalDataForRaw.L;
        finalDoc.S = originalDataForRaw.S;
        finalDoc.HR = originalDataForRaw.HR;
        finalDoc.RE = originalDataForRaw.RE;
        finalDoc.IA = originalDataForRaw.IA;
        finalDoc.CO = originalDataForRaw.CO;
        finalDoc.VO = originalDataForRaw.VO;
        finalDoc.ET = originalDataForRaw.ET;
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
      
      try {
        const savedDoc = await newHealthData.save();
        console.log("✅ Saved to healthdata_new collection. Document ID:", savedDoc._id);
        console.log("📊 Collection name:", savedDoc.collection.name);
        return res.json({ 
          message: "Health data saved via http",
          collection: "healthdata_new",
          documentId: savedDoc._id
        });
      } catch (saveError) {
        console.error("❌ Error saving to healthdata_new:", saveError);
        throw saveError;
      }
    }

    if (type === "sleep") {
      const newSleepData = new SleepData({
        deviceId,
        timestamp: new Date(),
        sleepQuality: data.sleepQuality || "Unknown",
        duration: data.duration || 0,
      });

      await newSleepData.save();
      return res.json({ message: "Sleep data saved" });
    }

    return res.status(400).json({ message: "Invalid type. Use 'health' or 'sleep'" });

  } catch (err) {
    console.error("❌ Error saving data via HTTP:", err);
    res.status(500).json({ message: "Internal server error" });
  }
});

module.exports = router;