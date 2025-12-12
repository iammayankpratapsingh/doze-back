// middleware/accountAccess.js
const User = require('../models/User');

/**
 * Allow:
 *  - superadmin/admin → any account
 *  - user            → only their own accountId
 */
module.exports = async function accountAccess(req, res, next) {
  try {
    const authUserId = req.user?.userId;
    const targetAccountId = String(req.params.accountId || '');

    if (!authUserId || !targetAccountId) {
      return res.status(401).send('Unauthorized3');
    }

    const u = await User.findById(authUserId).select('role accountId').lean();
    if (!u) return res.status(401).send('Unauthorized4');

    if (u.role === 'superadmin' || u.role === 'admin') return next();
    if (String(u.accountId) === targetAccountId) return next();

    return res.status(403).send('Forbidden');
  } catch (e) {
    return next(e);
  }
};
