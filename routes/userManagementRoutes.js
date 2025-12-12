const express = require("express");
const userController = require("../controllers/userManagementController");
const authMiddleware = require("../middleware/authMiddleware");
const adminMiddleware = require("../middleware/adminMiddleware");
const router = express.Router();
const userManagementController = require("../controllers/userManagementController");

// must come first
router.get("/me", authMiddleware, userController.getMe);

// All routes require authentication and admin privileges
router.use(authMiddleware);
router.use(adminMiddleware);

// User CRUD routes
router.post("/", userController.createUser);
router.get("/", userController.getAllUsers);

router.get("/:id", userController.getUserById);
router.put("/:id", userController.updateUser);
router.delete("/:id", userController.deleteUser);

// Additional user management routes
router.put("/:id/role", userController.changeUserRole);
router.get("/organization/:organizationId", userController.getUsersByOrganization);

// Organization routes
const organizationController = require("../controllers/organizationController");
router.get("/organizations", organizationController.getAllOrganizations);

module.exports = router;