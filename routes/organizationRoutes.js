const express = require("express");
const organizationController = require("../controllers/organizationController");
const authMiddleware = require("../middleware/authMiddleware");
const adminMiddleware = require("../middleware/adminMiddleware");
const { getPublicOrganizations } = require('../controllers/organizationController');

const router = express.Router();

// Apply authentication middleware to all organization routes
router.use(authMiddleware);

// Public routes (just need authentication, not admin rights)
router.get("/name/:organizationId", organizationController.getOrganizationNameByOrgId);


// Admin-only routes
router.use(adminMiddleware);

// Organization CRUD routes
router.post("/", organizationController.createOrganization);
router.get("/", organizationController.getAllOrganizations);

router.get('/public', getPublicOrganizations);

router.get("/:id", organizationController.getOrganization);
router.get("/byOrgId/:organizationId", organizationController.getOrganizationByOrgId);
router.put("/:id", organizationController.updateOrganization);
router.delete("/:organizationId", organizationController.deleteOrganization);

// Organization users route
router.get("/:organizationId/users", organizationController.getOrganizationUsers);

module.exports = router;