const express = require("express");
const adminController = require("../controllers/adminController");
const authMiddleware = require("../middleware/authMiddleware");
const superadminMiddleware = require("../middleware/superadminMiddleware");
const Device = require('../models/Device');
const User = require('../models/User');
const adminMiddleware = require('../middleware/adminMiddleware'); 

const router = express.Router();

// Apply authentication and superadmin middleware to all admin routes
router.use(authMiddleware);

// must mount before superadminMiddleware if admins can call it
router.post("/add-user", authMiddleware, adminMiddleware, adminController.addUser);

router.use(superadminMiddleware);

// Admin management routes
router.post("/", adminController.createAdmin);
router.get("/", adminController.getAllAdmins);
router.get("/:id", adminController.getAdmin);
router.put("/:id", adminController.updateAdmin);
router.delete("/:id", adminController.deleteAdmin);

router.use((req, res, next) => {
  console.log(`Admin route hit: ${req.method} ${req.originalUrl}`);
  next();
});


module.exports = router;