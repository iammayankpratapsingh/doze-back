const User = require("../models/User");
const Organization = require("../models/Organization");
const createError = require("../utils/appError");
const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");
const LoginAttempt = require("../models/LoginAttempts");
const geoip = require("geoip-lite");
const useragent = require("useragent");
const Device = require('../models/Device');// <-- add
const { createAdmin } = require("./adminController");
const crypto = require("crypto");
const { sendEmail } = require("../utils/mailer");
const dbg = require("../utils/dlog");
const mongoose = require("mongoose");
const Account = require("../models/Account");
const { OAuth2Client } = require("google-auth-library");

// ---- simple debug helpers ----
const DEBUG_AUTH = String(process.env.DEBUG_AUTH || 'true').toLowerCase() === 'true';
const log = (...a) => { if (DEBUG_AUTH) console.log('[AUTH]', ...a); };
const elog = (...a) => console.error('[AUTH:ERR]', ...a);
const mask = (s) => (s ? `${String(s).slice(0, 2)}*** (${String(s).length} chars)` : 'nil');

const gClient = new OAuth2Client({
  clientId: process.env.OAUTH_GOOGLE_CLIENT_ID,
  clientSecret: process.env.OAUTH_GOOGLE_CLIENT_SECRET,
  redirectUri: process.env.OAUTH_GOOGLE_REDIRECT_URI,
});

const APP_BASE_URL = ((process.env.APP_BASE_URL || "").trim() || "https://admin.dozemate.com").replace(/\/$/, "");
const IS_LOCAL_APP = /^(https?:\/\/)?(localhost|127\.0\.0\.1)/i.test(APP_BASE_URL);

function generateTempPassword() {
  // 12–16 chars, mixed; avoids +/=
  const raw = crypto.randomBytes(12).toString("base64").replace(/[+/=]/g, "");
  // ensure at least one lower/upper/digit/special (quick tweak)
  return (raw.slice(0, 10) + "aA1!").slice(0, 14);
}

function makeIdentifierKey(v) {
  if (!v) return undefined;
  return String(v).trim().replace(/\s+/g, ' ').toLowerCase();
}

// Unique numeric accountId allocator (retry a few times)
async function allocAccountId() {
  for (let i = 0; i < 5; i++) {
    const cand = String(Math.floor(10000 + Math.random() * 90000)); // 5-digit
    const exists = await Account.exists({ accountId: cand });
    if (!exists) return cand;
  }
  // fallback: timestamp-based
  return String(Date.now()).slice(-8);
}
async function nextProfileSuffix(account) {
  const n = (account.userProfiles?.length || 0);      // 0 -> 'a', 1 -> 'b' ...
  return String.fromCharCode(97 + n);
}

// POST /api/auth/register
exports.register = async (req, res, next) => {
  dbg("auth.register:start", {
    email: req.body?.email,
    role: req.body?.role,
    devicesCount: Array.isArray(req.body?.devices) ? req.body.devices.length : 0,
  });

  try {
    const {
      email,
      password,
      name,
      address = "",
      pincode,
      mobile,
      countryCode,
      country,
      city,
      organizationId,
      organizationName,
      role = "user",
      devices = [],             // [{ deviceId }]
      weightProfile = {},       // optional
      grid = {},                // optional
      displayDeviceIds = [],    // optional
      identifier
    } = req.body;

    // 1) Validate required fields
    const missing = [];
    if (!email) missing.push("email");
    if (!name) missing.push("name");
    if (pincode === undefined || pincode === null) missing.push("pincode");
    if (mobile === undefined || mobile === null) missing.push("mobile");

    if (missing.length) {
      dbg("auth.register:missing_fields", { missing });
      return res
        .status(400)
        .json({ status: "fail", message: `Missing: ${missing.join(", ")}` });
    }

    // 2) Validate email format + uniqueness
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
    if (!emailRegex.test(email)) {
      return res.status(400).json({ status: "fail", message: "Invalid email format" });
    }

    // 3) Resolve organization (optional)
    let resolvedOrgId = null;
    if (organizationId) {
      resolvedOrgId = organizationId;
      dbg("auth.register:org_by_id", { organizationId });
    } else if (organizationName && organizationName.trim()) {
      const orgName = organizationName.trim();
      let org = await Organization.findOne({ name: orgName });
      if (!org) org = await Organization.create({ name: orgName });
      resolvedOrgId = org._id;
      dbg("auth.register:org_by_name", { organizationName: orgName, resolvedOrgId });
    }

    // [IDENTIFIER] Enforce uniqueness (global OR per‑org — choose one)
    const identifierKey = makeIdentifierKey(identifier);
    if (identifierKey) {
      // (A) Global unique:
      // const dup = await User.exists({ identifierKey });

      // (B) Per‑organization unique (recommended):
      const dup = await User.exists({
        identifierKey,
        ...(resolvedOrgId ? { organizationId: resolvedOrgId } : {}),
      });

      if (dup) {
        return res.status(409).json({ status: "fail", message: "Identifier already in use" });
      }
    }

    // 4) Hash password
    const isTempPassword = !password || !String(password).trim();
    const plainPassword = isTempPassword ? generateTempPassword() : String(password).trim();
    const hashedPassword = await bcrypt.hash(plainPassword, 12);
    dbg("auth.register:password_hashed", { hashLen: hashedPassword.length, isTempPassword });

    // 5) Devices from payload (optional)
    const incomingIds = Array.isArray(devices)
      ? devices
        .map((d) =>
          d && d.deviceId ? String(d.deviceId).trim().toUpperCase() : null
        )
        .filter(Boolean)
      : [];

    dbg("auth.register:devices_incoming", { incomingIdsCount: incomingIds.length });

    let deviceDocs = [];
    if (incomingIds.length) {
      deviceDocs = await Device.find(
        { deviceId: { $in: incomingIds } },
        { _id: 1, deviceId: 1 }
      ).lean();
    }
    const deviceObjectIds = deviceDocs.map((d) => d._id);
    const activeDevice = deviceObjectIds[0] || null;
    dbg("auth.register:devices_found", {
      found: deviceObjectIds.length,
      activeDevice: activeDevice ? String(activeDevice) : null,
    });

    // 6) Auto-promote to admin if more than one device
    const resolvedRole = deviceObjectIds.length > 1 ? "admin" : role;
    dbg("auth.register:role_resolved", { resolvedRole });

    // 7) Build displayedDevices list (only ACTIVE, capped by grid capacity)
    const cap = Number(grid?.x || 0) * Number(grid?.y || 0);
    let displayIds = Array.isArray(displayDeviceIds)
      ? [...new Set(displayDeviceIds.map((s) => String(s).trim().toUpperCase()))]
      : [];
    let displayDocs = [];
    if (displayIds.length) {
      displayDocs = await Device.find(
        { deviceId: { $in: displayIds } },
        { _id: 1, deviceId: 1, status: 1 }
      ).lean();
      displayDocs = displayDocs.filter(
        (d) => String(d.status || "").toLowerCase() === "active"
      );
      if (cap > 0 && displayDocs.length > cap) displayDocs = displayDocs.slice(0, cap);
    }
    dbg("auth.register:display_devices", {
      requested: displayIds.length,
      acceptedActive: displayDocs.length,
      cap,
    });


    // --- BEFORE creating the user (right before step 8) ---
    const acctId = await allocAccountId();
    const accountDoc = await Account.create({
      accountId: acctId,
      primaryEmail: email,
      mobile: String(mobile || ''),
      countryCode: countryCode || undefined,
      address,
      pincode,
      country,
      city,
      organizationId: resolvedOrgId || null,
      userProfiles: [],
      defaultUser: null
    });
    // --- END insert ---

    // 8) Create user
    const newUser = await User.create({
      email,
      password: hashedPassword,
      name,
      address,
      pincode,
      mobile,
      organizationId: resolvedOrgId,
      countryCode,
      country,
      city,
      role: resolvedRole,
      devices: deviceObjectIds,
      activeDevice,
      identifier: identifier || undefined,
      identifierKey: identifierKey || undefined,
      dateOfBirth: weightProfile?.dob || undefined,
      gender: weightProfile?.gender || undefined,
      weight: weightProfile?.weight || undefined,
      height: weightProfile?.height || undefined,
      waist: weightProfile?.waist || undefined,
      createdAt: new Date(),
      grid: grid || undefined,
      displayedDevices: displayDocs.map((d) => d._id),
      passwordMustChange: isTempPassword ? true : false,
      account: accountDoc._id,
      accountId: accountDoc.accountId,
      userId: `${accountDoc.accountId}a`,
      isDefaultProfile: true,
      isVerified: false
    });
    dbg("auth.register:user_created", { userId: String(newUser._id) });

    await Account.updateOne(
      { _id: accountDoc._id },
      { $push: { userProfiles: newUser._id }, $set: { defaultUser: newUser._id } }
    );

    // 9) Reflect assignment on Device docs
    if (deviceObjectIds.length) {
      // Assign devices to this user
      await Device.updateMany(
        { _id: { $in: deviceObjectIds } },
        { $set: { userId: newUser._id, status: "inactive" } }  // mark all as inactive first
      );

      // Mark the first one as active
      if (activeDevice) {
        await Device.updateOne(
          { _id: activeDevice },
          { $set: { status: "active", lastActiveAt: new Date(), profileId: newUser._id } }
        );
      }

      dbg("auth.register:devices_assigned", {
        count: deviceObjectIds.length,
        activeDevice: String(activeDevice),
      });
    }

    // 9.4) Attach members array onto the new account (optional)
    let membersSet = [];
    try {
      const rawIds = Array.isArray(req.body.userIds) ? req.body.userIds : [];
      if (rawIds.length) {
        const validIds = rawIds
          .map(id => {
            try { return new mongoose.Types.ObjectId(String(id)); } catch { return null; }
          })
          .filter(Boolean)
          .filter(oid => String(oid) !== String(newUser._id)); // avoid self

        // de-dup and stringify for response
        const uniq = [...new Set(validIds.map(String))];
        membersSet = uniq;

        // persist on the new user
        // NOTE: requires `members: [{ type: mongoose.Schema.Types.ObjectId, ref: 'User' }]` in User schema
        await User.updateOne(
          { _id: newUser._id },
          { $set: { members: validIds } },
          { strict: false } // if schema doesn't yet have `members`, this allows setting it
        );

        dbg("auth.register:members_set", {
          provided: rawIds.length,
          valid: validIds.length,
          savedOnUserId: String(newUser._id),
          membersCount: uniq.length
        });
      } else {
        dbg("auth.register:members_set", { provided: 0 });
      }
    } catch (e) {
      elog("auth.register:members_set_error", e?.message || e);
    }


    // 9.5) Optionally link pre-created users to this account's organization
    let linkedMemberIds = [];
    let linkUpdatedCount = 0;

    try {
      const rawIds = Array.isArray(req.body.userIds) ? req.body.userIds : [];
      if (rawIds.length) {
        const validIds = rawIds
          .map((id) => {
            try { return new mongoose.Types.ObjectId(String(id)); } catch { return null; }
          })
          .filter(Boolean);

        if (validIds.length) {
          const orgToSet = resolvedOrgId || null; // reuse the resolved org
          linkedMemberIds = validIds.map(String);

          if (orgToSet) {
            const result = await User.updateMany(
              { _id: { $in: validIds } },
              { $set: { organizationId: orgToSet } }
            );
            linkUpdatedCount = result?.modifiedCount ?? result?.nModified ?? 0;
          }
          dbg("auth.register:link_members", {
            provided: rawIds.length,
            valid: validIds.length,
            linkedMemberIds,
            members: membersSet,
            linkUpdatedCount,
            org: orgToSet ? String(orgToSet) : null
          });
        }
      }
    } catch (e) {
      elog("auth.register:link_members_error", e?.message || e);
    }

        // 9.7) If Admin → create subUsers (inactive + send activation)
    if (String(resolvedRole).toLowerCase() === "admin" 
        && Array.isArray(req.body.subUsers) 
        && req.body.subUsers.length) {
      dbg("auth.register:subUsers:create", { count: req.body.subUsers.length });

      for (const s of req.body.subUsers) {
        try {
          if (!s.email) continue;
          const subPlainPass = generateTempPassword();
          const subHashed = await bcrypt.hash(subPlainPass, 12);

          const subUser = await User.create({
            email: s.email,
            name: [s.firstName, s.lastName].filter(Boolean).join(" "),
            address: s.address || "",
            pincode: s.pincode,
            mobile: s.mobile,
            countryCode: s.countryCode || "+91",
            country: s.country,
            city: s.city,
            role: "user",
            password: subHashed,
            passwordMustChange: true,
            isVerified: false,
            organizationId: newUser.organizationId,
            account: newUser.account,
            accountId: newUser.accountId,
            userId: `${newUser.accountId}m${Date.now().toString(36)}`
          });

          // 🔑 Generate verification link
          const verifyToken = jwt.sign(
            { userId: String(subUser._id) },
            process.env.JWT_SECRET,
            { expiresIn: "1d" }
          );
          const apiBase = process.env.API_BASE_URL || APP_BASE_URL;
          const verifyUrl = `${apiBase}/api/auth/verify/${verifyToken}`;

          await sendEmail({
            to: subUser.email,
            subject: "Activate your Dozemate account",
            text: `Hello ${subUser.name},\n\nYour account has been created by an Admin.\nPlease verify & activate using this link: ${verifyUrl}\n\n— Dozemate Team`,
            html: `<p>Hello ${subUser.name},</p>
                   <p>Your account has been created by an Admin.</p>
                   <p><a href="${verifyUrl}">Click here to verify & activate</a></p>
                   <p>— Dozemate Team</p>`
          });

          dbg("auth.register:subUser_created", { email: subUser.email });
        } catch (e) {
          elog("auth.register:subUser_error", e?.message || e);
        }
      }
    }

    // 10) Generate email verification token (24h expiry)
    const verifyToken = jwt.sign(
      { userId: String(newUser._id) },
      process.env.JWT_SECRET,
      { expiresIn: "1d" }
    );


    const apiBase = process.env.API_BASE_URL || APP_BASE_URL;
    const appBase = process.env.APP_BASE_URL || APP_BASE_URL;

    const verifyUrl = `${apiBase}/api/auth/verify/${verifyToken}`;

    dbg("auth.register:verify_token_issued", { userId: String(newUser._id) });    // ---- MAIL: begin

    dbg("auth.register:mail:begin", {
      to: String(newUser.email),
      isTempPassword,
      appBase: process.env.APP_BASE_URL || 'https://admin.dozemate.com',
      hasSendEmailFn: typeof sendEmail === 'function'
    });

    let mailAttempted = false, mailSent = false, mailError = null, mailMeta = null;

    try {
      const appBase = (process.env.APP_BASE_URL || 'https://admin.dozemate.com').replace(/\/+$/, '');


      const subject = "Verify your Dozemate account";
      const lines = [
        `Hi ${newUser.name || 'there'},`,
        "",
        "Thanks for registering with Dozemate.",
        "Please verify your email by clicking the link below:",
        verifyUrl,
        "",
        "This link will expire in 24 hours.",
        "",
        "— Dozemate Team"
      ];


      // Guard missing/incorrect mailer export early
      if (typeof sendEmail !== 'function') {
        throw new Error("sendEmail export is not a function (check ../utils/mailer)");
      }

      mailAttempted = true;

      // Race with a timeout so we log even if SMTP stalls
      const result = await Promise.race([
        Promise.resolve(
          sendEmail({
            to: newUser.email,
            subject,
            text: lines.join("\n"),
            html: lines.map(l => l ? `<p>${l}</p>` : '<br/>').join('')
          })
        ).then((r) => (r === false ? 'returned_false' : 'ok')),
        new Promise((_, rej) => setTimeout(() => rej(new Error('MAIL_TIMEOUT_12s')), 12000))
      ]);

      mailSent = (result === 'ok');
      dbg("auth.register:mail_result", { result, mailSent });
    } catch (mailErr) {
      mailError = mailErr?.message || String(mailErr);
      elog("auth.register:mail_error", mailError);
    }
    // ---- MAIL: end instrumentation ----


    return res.status(201).json({
      status: "success",
      message: "User registered. Verification email sent.",
      user: {
        id: newUser._id,
        email: newUser.email,
        name: newUser.name,
        role: newUser.role
      },
      mailAttempted,
      mailSent,
      mailError
    });

  } catch (err) {
    dbg("auth.register:error", { message: err?.message });
    next(err);
  }
};

// POST /api/auth/login 
exports.login = async (req, res, next) => {
  dbg("auth.login:start", { email: req.body?.email, role: req.body?.role });

  try {
    const { email: emailRaw, password, role } = req.body;

    const accountId =
      req.body?.accountId ??
      req.body?.account_id ??
      req.body?.accountID ??
      req.query?.accountId ??
      req.headers?.["x-account-id"] ??
      null;

    let email = emailRaw;

    // LOG: trace raw inputs (mask password length only)
    dbg("auth.login:payload", {
      email: emailRaw,
      role,
      passwordLen: typeof password === 'string' ? password.length : null
    });



    // --- resolve email via accountId (default profile) ---
    if (!email && accountId) {
      const acct = await Account.findOne({ accountId }).select('defaultUser userProfiles');
      if (!acct) {
        dbg("auth.login:fail_no_account", { accountId });
        return res.status(400).json({ status: "fail", message: "Invalid account ID" });
      }
      const defaultUser = await User.findById(acct.defaultUser || acct.userProfiles?.[0]);
      if (!defaultUser) {
        return res.status(400).json({ status: "fail", message: "No user available for this account" });
      }
      email = defaultUser.email;
      dbg("auth.login:resolved_email_from_account", { accountId, email });
    }

    const user = await User.findOne({ email });
    dbg("auth.login:user_lookup", { found: !!user, email });

    // capture environment details (for attempts log)
    const ip =
      req.ip || req.headers["x-forwarded-for"] || req.connection?.remoteAddress;
    const agent = useragent.parse(req.headers["user-agent"] || "");
    const location = geoip.lookup(ip);

    // LOG: environment snapshot
    dbg("auth.login:env", {
      ip,
      ua: req.headers["user-agent"] || "",
      os: agent.os && agent.os.toString ? agent.os.toString() : null,
      browser: agent && agent.toString ? agent.toString() : null,
      geo: location ? {
        country: location.country || null,
        city: location.city || null,
        ll: location.ll || null
      } : null
    });

    const attemptBase = {
      userId: user ? user._id : null,
      email,
      success: false,
      deviceInfo: {
        ip,
        userAgent: req.headers["user-agent"] || "",
        os: agent.os.toString(),
        browser: agent.toString(),
      },
      location: location
        ? {
          latitude: location.ll ? location.ll[0] : null,
          longitude: location.ll ? location.ll[1] : null,
          country: location.country || null,
          city: location.city || null,
        }
        : null,
      attemptedRole: role,
    };

    if (!user) {
      dbg("auth.login:fail_no_user", { email });
      await LoginAttempt.create({ ...attemptBase, failReason: "User not found" });
      // LOG: response about to return
      dbg("auth.login:resp", { status: 400, reason: "User not found" });
      return res.status(400).json({ status: "fail", message: "User not found" });
    }

    // Check verification
    if (!user.isVerified) {
      return res.status(403).json({ status: "fail", message: "Please verify your email before logging in." });
    }

    // Strict role check
    if (!user.role) {
      dbg("auth.login:fail_role_missing", { email });
      return res.status(403).json({ status: "fail", message: "Access denied. Missing role in DB." });
    }

    // Password check
    const ok = await bcrypt.compare(password || "", user.password || "");
    dbg("auth.login:password_compare", { ok, email });

    if (!ok) {
      await LoginAttempt.create({ ...attemptBase, failReason: "Invalid password" });
      // LOG: response about to return
      dbg("auth.login:resp", { status: 400, reason: "Invalid credentials" });
      return res.status(400).json({ status: "fail", message: "Invalid credentials" });
    }

    // Success — issue token and record attempt
    const token = jwt.sign(
      { userId: user._id, role: user.role },
      process.env.JWT_SECRET,
      { expiresIn: "30d" }
    );
    // LOG: token issued (never log token content)
    dbg("auth.login:token_issued", { userId: String(user._id) });

    await LoginAttempt.create({ ...attemptBase, success: true });
    dbg("auth.login:success", {
      userId: String(user._id),
      email,
      ip,
      country: attemptBase.location?.country || null,
      city: attemptBase.location?.city || null
    });

    // LOG: response about to return
    dbg("auth.login:resp", { status: 200, message: "User Logged in Successfully" });

    return res.status(200).json({
      status: "success",
      message: "User Logged in Successfully",
      token,
      user: {
        id: user._id,
        email: user.email,
        role: user.role,
        name: user.name,
        accountId: user.accountId || null
      },
    });
  } catch (error) {
    dbg("auth.login:error", { message: error?.message });
    // LOG: error stack (short)
    elog("auth.login:error_stack", error && error.stack ? error.stack.split('\n')[0] : String(error));
    next(error);
  }
};

// POST /api/auth/change-password  (auth required)
exports.changePassword = async (req, res, next) => {
  try {
    const userId = req.user.userId; // set by your auth middleware
    const { currentPassword, newPassword } = req.body;

    if (!newPassword || newPassword.length < 8) {
      return res.status(400).json({ status: "fail", message: "New password must be at least 8 characters." });
    }


    const user = await User.findById(userId).select("+password");
    if (!user) return res.status(404).json({ status: "fail", message: "User not found" });

    const ok = await bcrypt.compare(String(currentPassword || ""), user.password);
    if (!ok) return res.status(400).json({ status: "fail", message: "Current password is incorrect" });

    user.password = await bcrypt.hash(String(newPassword), 12);
    user.passwordMustChange = false;
    user.passwordChangedAt = new Date();
    await user.save();

    return res.status(200).json({ status: "success", message: "Password updated" });
  } catch (err) { next(err); }
};

// POST /api/auth/forgot
exports.forgotPassword = async (req, res, next) => {
  try {
    const { email } = req.body;
    const user = await User.findOne({ email });
    // don't reveal existence
    if (!user) return res.status(200).json({ status: "success", message: "If that email exists, a reset link has been sent." });

    const tokenRaw = crypto.randomBytes(32).toString("hex");
    const tokenHash = crypto.createHash("sha256").update(tokenRaw).digest("hex");
    user.passwordResetToken = tokenHash;
    user.passwordResetExpires = new Date(Date.now() + 1000 * 60 * 30); // 30 minutes
    await user.save({ validateBeforeSave: false });

    const resetUrl = `${process.env.APP_BASE_URL || 'https://admin.dozemate.com'}/reset-password/${tokenRaw}`;
    await sendEmail({
      to: user.email,
      subject: 'Reset your Dozemate password',
      text: `Click the link to reset your password:\n\n${resetUrl}\n\nIf you didn't request this, ignore this email.`,
      html: `<p>Click the link to reset your password:</p><p><a href="${resetUrl}">${resetUrl}</a></p><p>If you didn't request this, you can ignore this email.</p>`
    });

    return res.status(200).json({ status: "success", message: "If that email exists, a reset link has been sent." });
  } catch (err) { next(err); }
};

// POST /api/auth/reset/:token
exports.resetPassword = async (req, res, next) => {
  try {
    const tokenHash = crypto.createHash("sha256").update(req.params.token).digest("hex");
    const user = await User.findOne({
      passwordResetToken: tokenHash,
      passwordResetExpires: { $gt: new Date() }
    }).select("+password");

    if (!user) return res.status(400).json({ status: "fail", message: "Token invalid or expired" });

    const { newPassword } = req.body;
    if (!newPassword || newPassword.length < 8) {
      return res.status(400).json({ status: "fail", message: "New password must be at least 8 characters." });
    }

    user.password = await bcrypt.hash(String(newPassword), 12);
    user.passwordMustChange = false;
    user.passwordResetToken = undefined;
    user.passwordResetExpires = undefined;
    user.passwordChangedAt = new Date();
    await user.save();

    return res.status(200).json({ status: "success", message: "Password has been reset." });
  } catch (err) { next(err); }
};

// GET /api/auth/google
exports.googleAuth = (req, res) => {
  const url = gClient.generateAuthUrl({
    scope: ["openid", "email", "profile"],
    prompt: "consent",
  });
  return res.redirect(url);
};

// GET /api/auth/google/callback
exports.googleCallback = async (req, res, next) => {
  try {
    const { code } = req.query;
    if (!code) return res.status(400).json({ status: "fail", message: "Missing code" });

    // 1) Exchange code
    const { tokens } = await gClient.getToken({ code });
    if (!tokens.id_token) throw new Error("No id_token from Google");

    const ticket = await gClient.verifyIdToken({
      idToken: tokens.id_token,
      audience: process.env.OAUTH_GOOGLE_CLIENT_ID,
    });
    const p = ticket.getPayload(); // {sub, email, name, picture}

    // 2) Find or create user
    let user = await User.findOne({ email: p.email });
    if (!user) {

      // ---- create Account first ----
      const acctId = await allocAccountId();
      const accountDoc = await Account.create({
        accountId: acctId,
        primaryEmail: p.email,
        userProfiles: [],
        defaultUser: null,
      });

      // ---- then create User linked to Account ----
      user = await User.create({
        email: p.email,
        name: p.name,
        profileImage: p.picture,
        role: "user",
        oauth: [{ provider: "google", providerId: p.sub, email: p.email }],
        account: accountDoc._id,
        accountId: accountDoc.accountId,
        userId: `${accountDoc.accountId}a`,
        isDefaultProfile: true,
        isVerified: false
      });

      // link back defaultUser
      await Account.updateOne(
        { _id: accountDoc._id },
        { $push: { userProfiles: user._id }, $set: { defaultUser: user._id } }
      );

      console.info("[google-oauth] Created Account  User", {
        email: user.email,
        accountId: accountDoc.accountId,
        userId: user.userId
      });

    }

    // 3) JWT
    const token = jwt.sign({ userId: user._id, role: user.role }, process.env.JWT_SECRET, {
      expiresIn: "30d",
    });

    // 4) Redirect back to frontend

    res.cookie("auth_token", token, {
      httpOnly: false,     // set to true if you want it inaccessible to JS
      secure: !IS_LOCAL_APP,
      sameSite: "lax",
    });
    return res.redirect(`${APP_BASE_URL}/oauth/success`);

  } catch (err) {
    next(err);
  }
};

// GET /api/auth/verify/:token
exports.verifyEmail = async (req, res, next) => {
  try {
    const { token } = req.params;

    console.log("👉 Full incoming token (length):", token?.length);
    console.log("👉 First 40 chars:", token?.slice(0, 40));
    console.log("👉 Last 40 chars :", token?.slice(-40));

    let decoded;
    try {
      decoded = jwt.verify(token, process.env.JWT_SECRET);   // don’t redeclare with `const`
      console.log("✅ Decoded:", decoded);
    } catch (err) {
      console.error("❌ JWT verify failed:", err.message);
      return res.status(401).send("Unauthorized1: " + err.message);
    }

    const user = await User.findById(new mongoose.Types.ObjectId(decoded.userId));
    console.log("👉 DB User found:", !!user);

    if (!user) {
      return res.status(400).json({ status: "fail", message: "Invalid token (user not found)" });
    }

    if (user.isVerified) {
      console.log("⚠️ Already verified");
      return res.redirect(`${process.env.APP_BASE_URL}/login?verified=1`);
    }

    user.isVerified = true;
    await user.save();
    console.log("✅ User verified and saved");

    return res.redirect(`${process.env.APP_BASE_URL}/login?verified=1`);
  } catch (err) {
    console.error("❌ verifyEmail error:", err.message);
    return res.redirect(`${process.env.APP_BASE_URL}/login?verified=0`);
  }
};
