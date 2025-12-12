const mongoose = require('mongoose');

const AccountSchema = new mongoose.Schema({
  // numeric-as-string to avoid int range issues, stays sortable
  accountId: { type: String, unique: true, index: true, match: [/^\d+$/, 'accountId must be numeric'] },
  primaryEmail: { type: String, index: true, sparse: true },

  // shared info kept at account level (diagram)
  mobile: { type: String },
  countryCode: { type: String },
  address: { type: String },
  pincode: { type: Number },
  country: { type: String },
  city: { type: String },
  organizationId: { type: mongoose.Schema.Types.ObjectId, ref: 'Organization', default: null },

  userProfiles: [{ type: mongoose.Schema.Types.ObjectId, ref: 'User' }],
  defaultUser: { type: mongoose.Schema.Types.ObjectId, ref: 'User', default: null },
}, { timestamps: true });

module.exports = mongoose.model('Account', AccountSchema);
