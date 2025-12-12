// helpers/promotePending.js
const PendingUser = require('../models/PendingUser');
const User = require('../models/User');
const bcrypt = require('bcryptjs');
const { sendNewUserCredentials } = require('../utils/mailer');
const { sendWelcomeEmail } = require('../utils/mailer');

function genTempPassword() {
  // simple, readable temp password
  const base = Math.random().toString(36).slice(-8); // 8 chars
  const digits = Math.floor(100 + Math.random() * 900); // 3 digits
  return `${base}${digits}`;
}


async function promotePendingUsers(batchId, orgId, adminId) {
  if (!batchId) return { created: [], emailResults: [] };

  const pendings = await PendingUser.find({ batchId, status: 'pending' }).lean();
  const created = [];
  const emailResults = [];

  for (const p of pendings) {
    try {
      const plain = Math.random().toString(36).slice(-8);
      const u = await User.create({
        name: [p.firstName, p.lastName].filter(Boolean).join(' '),
        email: p.email,
        password: await bcrypt.hash(plain, 12),
        role: 'user',
        organizationId: orgId,
        addedBy: adminId,
        countryCode: p.countryCode,
        mobile: p.mobile,
        country: p.country,
        pincode: p.pincode,
        address: p.address,
        city: p.city,
        status: 'invited'
      });

      // try sending the email
      let ok = false, error = null;
      try {
        const info = await sendWelcomeEmail(p.email, plain);
        ok = Array.isArray(info.accepted) && info.accepted.includes(p.email);
      } catch (e) {
        error = e.message || 'send failed';
      }

      created.push(u);
      emailResults.push({ email: p.email, ok, error });

    } catch (e) {
      // user create failed — still report email failure
      emailResults.push({ email: p.email, ok: false, error: e.message || 'create failed' });
    }
  }

  await PendingUser.deleteMany({ batchId });
  return { created, emailResults };
}

module.exports = { promotePendingUsers };
