// controllers/profileController.js
const jwt = require("jsonwebtoken");
const multer = require("multer");
const path = require("path");
const fs = require("fs");
const Account = require("../models/Account");
const User = require("../models/User");

// ---------- helpers ----------
async function nextProfileSuffix(account) {
  const n = (account.userProfiles?.length || 0);           // 0 -> 'a', 1 -> 'b', ...
  return String.fromCharCode(97 + n);
}
function canManageAccount(req, account) {
  if (!req.user) return false;
  if (req.user.role === "superadmin" || req.user.role === "admin") return true;

  return account.userProfiles.some(p =>
    String(p._id || p) === String(req.user.userId)
  );
}

// ---------- avatar upload (multer) ----------
const uploadDir = path.join("uploads", "profiles");
fs.mkdirSync(uploadDir, { recursive: true });
const upload = multer({ dest: uploadDir });
exports.avatarUploadMw = upload.single("image");

// ---------- create profile ----------
exports.addProfile = async (req, res, next) => {
  try {
    const { accountId } = req.params;
    const { name, nickname, dateOfBirth, gender, weight, height, waist } = req.body;

    const account = await Account.findOne({ accountId });
    if (!account) return res.status(404).json({ status: "fail", message: "Account not found" });
    if (!canManageAccount(req, account)) return res.sendStatus(403);

    const suffix = await nextProfileSuffix(account);

    const profile = await User.create({
      name,
      nickname,
      dateOfBirth,
      gender,
      weight,
      height,
      waist,
      role: "user",
      email: `${account.accountId}${suffix}@dozemate.local`,
      account: account._id,
      accountId: account.accountId,
      userId: `${account.accountId}${suffix}`,
      isDefaultProfile: false,
    });

    await Account.updateOne(
      { _id: account._id },
      { $push: { userProfiles: profile._id } }
    );

    res.status(201).json({ status: "success", userId: profile.userId, profile });
  } catch (err) { next(err); }
};

// ---------- list profiles by account ----------
exports.listProfiles = async (req, res, next) => {
  try {
    const { accountId } = req.params;
    const account = await Account.findOne({ accountId }).populate({
      path: "userProfiles",
      select: "userId name nickname gender dateOfBirth profileImage isDefaultProfile",
    });
    if (!account) return res.status(404).json({ status: "fail", message: "Account not found" });
    if (!canManageAccount(req, account)) return res.sendStatus(403);

    res.json({ status: "success", profiles: account.userProfiles, accountId: account.accountId });
  } catch (err) { next(err); }
};

// ---------- get single profile by userId ----------
exports.getProfile = async (req, res, next) => {
  try {
    const { userId } = req.params;
    const profile = await User.findOne({ userId });
    if (!profile) return res.status(404).json({ status: "fail", message: "Profile not found" });

    const account = await Account.findById(profile.account);
    if (!account) return res.status(404).json({ status: "fail", message: "Account not found" });
    if (!canManageAccount(req, account)) return res.sendStatus(403);

    res.json({ status: "success", profile });
  } catch (err) { next(err); }
};

// ---------- update profile by userId ----------
exports.updateProfile = async (req, res, next) => {
  try {
    const { userId } = req.params;
    const profile = await User.findOne({ userId });
    if (!profile) return res.status(404).json({ status: "fail", message: "Profile not found" });

    const account = await Account.findById(profile.account);
    if (!account) return res.status(404).json({ status: "fail", message: "Account not found" });
    if (!canManageAccount(req, account)) return res.sendStatus(403);

    const allowed = ["name", "nickname", "dateOfBirth", "gender", "weight", "height", "waist"];
    for (const k of allowed) if (k in req.body) profile[k] = req.body[k];
    await profile.save();

    res.json({ status: "success", profile });
  } catch (err) { next(err); }
};

// ---------- delete profile by userId ----------
exports.deleteProfile = async (req, res, next) => {
  try {
    const { userId } = req.params;
    const profile = await User.findOne({ userId });
    if (!profile) return res.status(404).json({ status: "fail", message: "Profile not found" });

    const account = await Account.findById(profile.account);
    if (!account) return res.status(404).json({ status: "fail", message: "Account not found" });
    if (!canManageAccount(req, account)) return res.sendStatus(403);
    if (profile.isDefaultProfile) {
      return res.status(400).json({ status: "fail", message: "Default profile cannot be deleted" });
    }

    await Account.updateOne({ _id: account._id }, { $pull: { userProfiles: profile._id } });
    await User.deleteOne({ _id: profile._id });

    res.json({ status: "success" });
  } catch (err) { next(err); }
};

// ---------- upload/change avatar ----------
exports.uploadAvatar = async (req, res, next) => {
  try {
    const { userId } = req.params;
    const profile = await User.findOne({ userId });
    if (!profile) return res.status(404).json({ status: "fail", message: "Profile not found" });

    const account = await Account.findById(profile.account);
    if (!account) return res.status(404).json({ status: "fail", message: "Account not found" });
    if (!canManageAccount(req, account)) return res.sendStatus(403);
    if (!req.file) return res.status(400).json({ status: "fail", message: "Image file missing" });

    const relPath = `/${req.file.path.replace(/\\/g, "/")}`; // e.g. /uploads/profiles/xxx
    profile.profileImage = relPath;
    await profile.save();

    res.json({ status: "success", profileImage: relPath });
  } catch (err) { next(err); }
};
