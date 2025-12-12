
const express = require("express");
const PendingUser = require('../models/PendingUser');
const router = express.Router();
const publicController = require("../controllers/publicController");
const organizationController = require("../controllers/organizationController");


// POST /api/public/pending-users
router.post('/pending-users', async (req, res) => {
  const { batchId, user } = req.body || {};
  if (!batchId || !user?.email) return res.status(400).json({ ok: false, message: 'batchId & user.email required' });
  try {
    const doc = await PendingUser.findOneAndUpdate(
      { batchId, email: user.email.toLowerCase() },
      { $set: user, $setOnInsert: { status: 'pending' } },
      { new: true, upsert: true }
    );
    res.json({ ok: true, data: doc });
  } catch (e) { res.status(500).json({ ok: false, message: e.message }); }
});

// GET /api/pending-users?batchId=...&q=...
router.get('/pending-users', async (req, res) => {
  res.set('Cache-Control', 'no-store');
  const { batchId, q } = req.query;
  if (!batchId) return res.status(400).json({ ok: false, message: 'batchId required' });
  const filter = { batchId };
  if (q) filter.$or = [
    { email: new RegExp(q, 'i') },
    { firstName: new RegExp(q, 'i') },
    { lastName: new RegExp(q, 'i') },
    { identifier: new RegExp(q, 'i') },
  ];
  const rows = await PendingUser.find(filter).select('email firstName lastName').lean();
  res.json({ ok: true, data: rows.map(r => ({ _id: r._id, email: r.email, name: [r.firstName, r.lastName].filter(Boolean).join(' ') })) });
});

// routes/users.js  (or wherever your user routes live)
router.get('/identifier/available', async (req, res) => {
  const raw = (req.query.value || '').trim();
  if (!raw) return res.status(400).json({ ok: false, message: 'Missing value' });

  const key = raw.replace(/\s+/g, ' ').toLowerCase();
  const exists = await User.exists({ identifierKey: key });
  return res.json({ ok: true, available: !exists });
});


// GET /api/public/codes
router.get("/codes", publicController.getCodes);

router.get("/organizations", organizationController.getPublicOrganizations);



module.exports = router;
