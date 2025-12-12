// models/PendingUser.js
const mongoose = require('mongoose');

const PendingUserSchema = new mongoose.Schema({
  batchId: { type: String, index: true, required: true },

  email: { type: String, required: true, lowercase: true, trim: true },
  firstName: String,
  lastName: String,
  countryCode: String,
  mobile: String,
  country: String,
  pincode: String,
  address: String,
  city: String,

  // Added to match your controller usage / auditing
  organizationId: { type: String, index: true }, // can hold ObjectId string or external org code
  identifier: String,                             // e.g., "Bed no 1"
  addedBy: { type: mongoose.Schema.Types.ObjectId, ref: 'User' },

  status: { type: String, enum: ['pending', 'invited', 'promoted'], default: 'pending', index: true },

  invitedAt: Date,
  promotedAt: Date,

  // TTL only when set; cleaner than expiring everything by createdAt
  expireAt: { type: Date, index: { expireAfterSeconds: 0 } },
}, { timestamps: true });

// one email per batch
PendingUserSchema.index({ batchId: 1, email: 1 }, { unique: true });

// optional: make identifier unique within an org (if you need it)
// PendingUserSchema.index(
//   { organizationId: 1, identifier: 1 },
//   { unique: true, partialFilterExpression: { identifier: { $type: 'string' } } }
// );

PendingUserSchema.virtual('fullName').get(function () {
  return [this.firstName, this.lastName].filter(Boolean).join(' ');
});

module.exports = mongoose.model('PendingUser', PendingUserSchema);
