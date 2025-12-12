const express = require("express");
const authController = require("../controllers/authController");


const router = express.Router();

// Register
router.post("/register", authController.register);

// Login
router.post("/login", authController.login);

// Email verification
router.get("/verify/:token", authController.verifyEmail);

// OAuth2 (Google)
router.get("/google", authController.googleAuth);
router.get("/google/callback", authController.googleCallback);

+router.post("/forgot", authController.forgotPassword);
+router.post("/reset/:token", authController.resetPassword);

module.exports = router;
