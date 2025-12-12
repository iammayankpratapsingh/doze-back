// routes/devicePrefixes.js
const express = require('express');
const mongoose = require('mongoose');

const DevicePrefix = require('../models/DevicePrefix');
// Optional: if you keep these collections, we can resolve labels from them.
// Fallback constant maps are provided below.
let Manufacturer, Technology, Port;
try { Manufacturer = require('../models/Manufacturer'); } catch {}
try { Technology   = require('../models/Technology'); } catch {}
try { Port         = require('../models/Port'); } catch {}

const auth = require('../middleware/authMiddleware');
const admin = require('../middleware/adminMiddleware'); // treat admin as super-admin here

const router = express.Router();

/* ------------------------- Codebook (fallback) ------------------------- */
// You can delete/replace these if you read from DB exclusively.
const DEVICE_NAME_MAP = {
  '01': 'Dozemate',
  '02': 'Sensabit',
  '03': 'Smart ring',
  '04': 'GPSmart',
  '05': 'Stethopod',
  '06': 'Environment',
};
const SECTOR_MAP = {
  '0': 'Medical',
  '1': 'Environment',
  '2': 'Power',
  '3': 'IT',
  '4': 'Energy',
  '5': '—',
  '6': '—',
};
const TECHNOLOGY_MAP = { // 0..6 (example matches your sheet)
  '0': 'BLE',
  '1': 'WiFi',
  '2': 'BLE+WiFi',
  '3': 'UWB',
  '4': 'GPS/GNSS',
  '5': 'UWB+GPS',
  '6': '—',
};
const PORTS_MAP = {
  '0': 'I2C',
  '1': 'SPI',
  '2': 'UART+I2C',
  '3': 'UART+SPI',
  '4': 'I2C+SPI',
  '5': 'C+SPI+UART',
  '6': 'UART',
};

/* ----------------------- Helpers: resolve labels ----------------------- */
async function resolveManufacturerLabel(code) {
  if (Manufacturer) {
    const m = await Manufacturer.findOne({ code: code }).lean();
    if (m?.name) return m.name;
    // Some teams store it as code2 -> name
    const mm = await Manufacturer.findOne({ code2: code }).lean();
    if (mm?.name) return mm.name;
  }
  // fallback map from the screenshot
  const fallback = { '01': 'ABC corp', '02': 'Slimiot', '03': 'Sensabit', '04': 'Dozemate' };
  return fallback[code] || `Manufacturer-${code}`;
}
async function resolveTechnologyLabel(code) {
  if (Technology) {
    const t = await Technology.findOne({ code: code }).lean() || await Technology.findOne({ code1: code }).lean();
    if (t?.label) return t.label;
    if (t?.name)  return t.name;
  }
  return TECHNOLOGY_MAP[code] || `Tech-${code}`;
}
async function resolvePortsLabel(code) {
  if (Port) {
    const p = await Port.findOne({ code: code }).lean() || await Port.findOne({ code1: code }).lean();
    if (p?.label) return p.label;
    if (p?.name)  return p.name;
  }
  return PORTS_MAP[code] || `Ports-${code}`;
}
function resolveDeviceNameLabel(code) {
  return DEVICE_NAME_MAP[code] || `Device-${code}`;
}
function resolveSectorLabel(code) {
  return SECTOR_MAP[code] || `Sector-${code}`;
}
function buildPrefix({ deviceNameCode, manufacturerCode, sectorCode, technologyCode, portsCode }) {
  return `${deviceNameCode}${manufacturerCode}${sectorCode}${technologyCode}${portsCode}`;
}

/* --------------------------------- POST --------------------------------
   Create a new 7-digit prefix (first part), with duplicate prevention */
router.post('/', auth, admin, async (req, res) => {
  try {
    const { deviceNameCode, manufacturerCode, sectorCode, technologyCode, portsCode } = req.body || {};

    if (!/^\d{2}$/.test(deviceNameCode) ||
        !/^\d{2}$/.test(manufacturerCode) ||
        !/^\d{1}$/.test(sectorCode) ||
        !/^\d{1}$/.test(technologyCode) ||
        !/^\d{1}$/.test(portsCode)) {
      return res.status(400).json({ message: 'Invalid codes provided' });
    }

    const prefix = buildPrefix({ deviceNameCode, manufacturerCode, sectorCode, technologyCode, portsCode });

    const [deviceName, manufacturer, sector, technology, ports] = await Promise.all([
      resolveDeviceNameLabel(deviceNameCode),
      resolveManufacturerLabel(manufacturerCode),
      resolveSectorLabel(sectorCode),
      resolveTechnologyLabel(technologyCode),
      resolvePortsLabel(portsCode),
    ]);

    // upsert only if not exists (enforce uniqueness)
    const exists = await DevicePrefix.findOne({ prefix }).lean();
    if (exists) return res.status(409).json({ message: 'Prefix already exists', prefix });

    const doc = await DevicePrefix.create({
      prefix,
      deviceNameCode, manufacturerCode, sectorCode, technologyCode, portsCode,
      deviceName, manufacturer, sector, technology, ports,
      createdBy: req.user.userId,
    });

    res.status(201).json({ message: 'Prefix created', data: doc });
  } catch (e) {
    console.error('device-prefixes POST error:', e);
    res.status(500).json({ message: 'Server error' });
  }
});

/* --------------------------------- GET ---------------------------------
   List prefixes (with filters: deviceName, manufacturer, q) */
router.get('/', auth, admin, async (req, res) => {
  try {
    const { deviceName = '', manufacturer = '', q = '', page = 1, limit = 20 } = req.query;

    const rx = (s) => new RegExp(String(s).replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i');
    const filter = {};
    const ors = [];
    if (deviceName) ors.push({ deviceName: rx(deviceName) });
    if (manufacturer) ors.push({ manufacturer: rx(manufacturer) });
    if (q) ors.push({ prefix: rx(q) }, { deviceName: rx(q) }, { manufacturer: rx(q) });

    if (ors.length) filter.$or = ors;

    const pageN = Math.max(parseInt(page, 10) || 1, 1);
    const limitN = Math.min(Math.max(parseInt(limit, 10) || 20, 1), 100);

    const [rows, total] = await Promise.all([
      DevicePrefix.find(filter).sort({ createdAt: -1 }).skip((pageN - 1) * limitN).limit(limitN).lean(),
      DevicePrefix.countDocuments(filter),
    ]);

    res.json({ data: rows, total, page: pageN, limit: limitN });
  } catch (e) {
    console.error('device-prefixes GET error:', e);
    res.status(500).json({ message: 'Server error' });
  }
});

/* ------------------------------ /validate ------------------------------
   Real-time duplicate check. Body or query can carry codes. */
router.get('/validate', auth, admin, async (req, res) => {
  try {
    const {
      deviceNameCode, manufacturerCode, sectorCode, technologyCode, portsCode,
    } = req.query;

    if (!deviceNameCode || !manufacturerCode || !sectorCode || !technologyCode || !portsCode) {
      return res.status(400).json({ ok: false, message: 'All 5 codes are required' });
    }

    const prefix = buildPrefix({ deviceNameCode, manufacturerCode, sectorCode, technologyCode, portsCode });
    const exists = await DevicePrefix.exists({ prefix });
    res.json({ ok: true, exists: !!exists, prefix });
  } catch (e) {
    res.status(500).json({ ok: false, message: e.message });
  }
});

/* ------------------------------- /suggest ------------------------------
   Type-ahead by device name / manufacturer */
router.get('/suggest', auth, admin, async (req, res) => {
  try {
    const q = String(req.query.q || '').trim();
    if (q.length < 2) return res.json({ data: [], note: 'q too short' });

    const rx = new RegExp(q.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'), 'i');
    const rows = await DevicePrefix.find({
      $or: [{ deviceName: rx }, { manufacturer: rx }, { prefix: rx }],
    }).limit(15).lean();

    res.json({ data: rows.map(r => ({
      _id: r._id,
      prefix: r.prefix,
      deviceName: r.deviceName,
      manufacturer: r.manufacturer,
      sector: r.sector,
      technology: r.technology,
      ports: r.ports,
      nextHint: `${r.prefix}-XXXXX`,
    })) });
  } catch (e) {
    res.status(500).json({ message: e.message });
  }
});

module.exports = router;
