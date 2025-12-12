// routes/profileRoutes.js
const express = require("express");
const jwt = require("jsonwebtoken");
const {
  addProfile, listProfiles, getProfile, updateProfile, deleteProfile,
  uploadAvatar, avatarUploadMw
} = require("../controllers/profileController");

const router = express.Router();

// ---- minimal JWT check ----
function requireAuth(req, res, next) {
  const hdr = req.headers.authorization || "";
  const token = hdr.startsWith("Bearer ") ? hdr.slice(7) : null;
  if (!token) return res.sendStatus(401);

  try {
    const decoded = jwt.verify(token, process.env.JWT_SECRET);
    req.user = { userId: decoded.userId || decoded.id, role: decoded.role };
    console.log("[PROFILE:AUTH]", {
      route: req.originalUrl,
      userId: req.user.userId,
      role: req.user.role
    });
    return next();
  } catch (e) {
    console.error("[PROFILE:AUTH_ERR]", e.message);
    return res.sendStatus(401);
  }
}

router.use(requireAuth);

// ---- Account-scoped profiles ----
router.post("/account/:accountId/profiles", addProfile);
router.get("/account/:accountId/profiles", listProfiles);

// ---- Profile by userId ----
router.get("/profiles/:userId", getProfile);
router.put("/profiles/:userId", updateProfile);
router.delete("/profiles/:userId", deleteProfile);
router.post("/profiles/:userId/avatar", avatarUploadMw, uploadAvatar);

module.exports = router;
