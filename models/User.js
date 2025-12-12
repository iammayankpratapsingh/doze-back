// filepath: models/User.js
const mongoose = require("mongoose");

const UserSchema = new mongoose.Schema({
  name: { type: String, required: true },
  email: { type: String, unique: true, required: true },

  address: { type: String, default: "" },
  password: {
    type: String,
    required: function () {
      // required only if no oauth AND this is a default profile
      return this.isDefaultProfile && !(this.oauth && this.oauth.length);
    }
  },

  pincode: {
    type: Number,
    required: function () {
      return this.isDefaultProfile && !(this.oauth && this.oauth.length);
    }
  },

  mobile: {
    type: Number,
    required: function () {
      return this.isDefaultProfile && !(this.oauth && this.oauth.length);
    }
  },

  countryCode: { type: String },
  country: { type: String },
  city: { type: String },

  profileImage: { type: String, default: "/uploads/defaults/default-profile.jpg" },
  role: { type: String, enum: ["user", "admin", "superadmin"], default: "user" },

  devices: { type: [{ type: mongoose.Schema.Types.ObjectId, ref: "Device" }], default: [] },
  grid: { type: { x: Number, y: Number }, default: undefined },
  displayedDevices: { type: [{ type: mongoose.Schema.Types.ObjectId, ref: "Device" }], default: [] },
  activeDevices: [{ type: mongoose.Schema.Types.ObjectId, ref: "Device" }],


  // OAuth identities
  oauth: [{
    provider: { type: String, enum: ["google", "apple", "github"], required: true },
    providerId: { type: String, required: true, index: true },
    email: { type: String },
    name: { type: String },
    avatar: { type: String },
    linkedAt: { type: Date, default: Date.now }
  }],

  // Account/profile linkage
  account: { type: mongoose.Schema.Types.ObjectId, ref: "Account", default: null },
  accountId: { type: String, index: true, sparse: true },    // e.g. "12345"
  // userId: { type: String, unique: true, required: true },  // e.g. "12345a"
  isDefaultProfile: { type: Boolean, default: false },

  nickname: { type: String, trim: true },

  // New health-related fields
  dateOfBirth: {
    type: Date
  },
  gender: {
    type: String,
    enum: ["male", "female", "other", "prefer-not-to-say"]
  },
  weight: {
    type: Number // in kg
  },
  height: {
    type: Number // in cm
  },
  waist: {
    type: Number // in cm
  },
  createdAt: { type: Date, default: Date.now },
  passwordMustChange: { type: Boolean, default: false },
  members: [{ type: mongoose.Schema.Types.ObjectId, ref: 'User' }],
  passwordChangedAt: Date,
  passwordResetToken: String,
  passwordResetExpires: Date,
  tempPasswordIssuedAt: Date,
  identifier: { type: String, trim: true },              // human name you show
  identifierKey: { type: String, index: true, unique: true, sparse: true }, // normalized
  isVerified: {
    type: Boolean,
    default: false
  }
});

function makeIdentifierKey(v) {
  if (!v) return undefined;
  return String(v).trim().replace(/\s+/g, ' ').toLowerCase();
}

UserSchema.pre('save', function (next) {
  if (this.isModified('identifier')) {
    this.identifierKey = makeIdentifierKey(this.identifier);
  }
  next();
});

UserSchema.index({ account: 1 });
UserSchema.index({ "oauth.provider": 1, "oauth.providerId": 1 });

module.exports = mongoose.model("User", UserSchema);