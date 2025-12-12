// controllers/adminController.js
const User = require("../models/User");
const Organization = require("../models/Organization");
const bcrypt = require("bcryptjs");
const createError = require("../utils/appError");
const mongoose = require("mongoose");
const { register } = require("./authController");
const { promotePendingUsers } = require("../helpers/promotePending");
const PendingUser = require("../models/PendingUser");
const authController = require("./authController");

// ---- Helpers ----
const getOrganizationName = async (orgId) => {
  if (!orgId) return "No Organization";
  try {
    if (mongoose.Types.ObjectId.isValid(orgId)) {
      const byId = await Organization.findById(orgId);
      if (byId) return byId.name;
    }
    const byField = await Organization.findOne({ organizationId: orgId });
    return byField ? byField.name : "Unknown Organization";
  } catch {
    return "Unknown Organization";
  }
};

// ---- Create Admin (+ optional pending users batch) ----
exports.createAdmin = async (req, res, next) => {
  try {
    const {
      email,
      password,
      name,
      address,
      pincode,
      mobile,
      organizationId,
      batchId: incomingBatchId, // optional
      usersToAdd = [],          // optional
    } = req.body;

    // 1) Uniqueness
    if (await User.findOne({ email })) {
      return res.status(400).json({ status: "fail", message: "Email already in use" });
    }

    // 2) Organization check
    const organizationName = await getOrganizationName(organizationId);
    if (organizationName === "Unknown Organization") {
      return res.status(400).json({ status: "fail", message: "Organization not found" });
    }

    // 3) Create admin
    const hashedPassword = await bcrypt.hash(password, 12);
    const newAdmin = await User.create({
      email,
      password: hashedPassword,
      name,
      address,
      pincode,
      mobile,
      organizationId,
      role: "admin",
      createdAt: new Date(),
    });

    // 4) Queue pending users (optional)
    let batchId = incomingBatchId || null;
    if (Array.isArray(usersToAdd) && usersToAdd.length > 0) {
      batchId = batchId || new mongoose.Types.ObjectId().toString();
      const docs = usersToAdd.map((u) => ({
        batchId,
        status: "pending",
        organizationId,
        firstName: u.firstName,
        lastName: u.lastName,
        email: u.email,
        countryCode: u.countryCode,
        mobile: u.mobile,
        country: u.country,
        pincode: u.pincode,
        address: u.address,
        city: u.city,
        identifier: u.identifier,
      }));
      try {
        await PendingUser.insertMany(docs, { ordered: false });
      } catch (e) {
        console.warn("PendingUser insertMany warning:", e?.message);
      }
    }

    // 5) Promote pending users now (optional)
    let promotedCount = 0;
    let emailResults = [];
    if (batchId) {
      try {
        const { created, emailResults: er } =
          await promotePendingUsers(batchId, organizationId, newAdmin._id);
        promotedCount = Array.isArray(created) ? created.length : 0;
        emailResults = Array.isArray(er) ? er : [];
      } catch (e) {
        console.error("promotePendingUsers failed:", e?.message);
      }
    }

    // 6) Response
    const adminResponse = newAdmin.toObject();
    delete adminResponse.password;
    adminResponse.organizationName = organizationName;

    return res.status(201).json({
      status: "success",
      message: "Admin user created successfully",
      data: {
        admin: adminResponse,
        promotedUsers: promotedCount,
        batchId: batchId || null,
        emailResults   // <── includes [{email, ok, error}]
      }
    });

  } catch (error) {
    console.error("Error creating admin:", error);
    next(error);
  }
};

// ---- List Admins ----
exports.getAllAdmins = async (req, res, next) => {
  try {
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 10;
    const skip = (page - 1) * limit;

    const filter = { role: "admin" };
    if (req.query.organizationId) filter.organizationId = req.query.organizationId;
    if (req.query.search) {
      filter.$or = [
        { name: { $regex: req.query.search, $options: "i" } },
        { email: { $regex: req.query.search, $options: "i" } },
      ];
    }

    const admins = await User.find(filter)
      .select("-password")
      .sort({ createdAt: -1 })
      .skip(skip)
      .limit(limit);

    const total = await User.countDocuments(filter);
    const adminsWithOrgName = await Promise.all(
      admins.map(async (a) => {
        const obj = a.toObject();
        obj.organizationName = await getOrganizationName(a.organizationId);
        return obj;
      })
    );

    res.status(200).json({
      status: "success",
      results: adminsWithOrgName.length,
      totalPages: Math.ceil(total / limit),
      currentPage: page,
      total,
      data: { admins: adminsWithOrgName },
    });
  } catch (error) {
    console.error("Error fetching admins:", error);
    next(error);
  }
};

// ---- Get One Admin ----
exports.getAdmin = async (req, res, next) => {
  try {
    const admin = await User.findOne({ _id: req.params.id, role: "admin" }).select("-password");
    if (!admin) return next(new createError(404, "Admin not found"));

    const adminObj = admin.toObject();
    adminObj.organizationName = await getOrganizationName(admin.organizationId);

    res.status(200).json({ status: "success", data: { admin: adminObj } });
  } catch (error) {
    console.error("Error fetching admin:", error);
    next(error);
  }
};

// ---- Update Admin ----
exports.updateAdmin = async (req, res, next) => {
  try {
    const allowedFields = [
      "name",
      "address",
      "pincode",
      "mobile",
      "organizationId",
      "profileImage",
      "dateOfBirth",
      "gender",
      "weight",
      "height",
      "waist",
    ];

    const updateData = {};
    for (const f of allowedFields) if (req.body[f] !== undefined) updateData[f] = req.body[f];

    if (updateData.organizationId) {
      const orgName = await getOrganizationName(updateData.organizationId);
      if (orgName === "Unknown Organization") {
        return res.status(400).json({ status: "fail", message: "Organization not found" });
      }
    }

    if (req.body.password) updateData.password = await bcrypt.hash(req.body.password, 12);

    const admin = await User.findOneAndUpdate(
      { _id: req.params.id, role: "admin" },
      updateData,
      { new: true, runValidators: true }
    ).select("-password");

    if (!admin) return next(new createError(404, "Admin not found"));

    const adminObj = admin.toObject();
    adminObj.organizationName = await getOrganizationName(admin.organizationId);

    res.status(200).json({ status: "success", message: "Admin updated successfully", data: { admin: adminObj } });
  } catch (error) {
    console.error("Error updating admin:", error);
    next(error);
  }
};

// ---- Delete Admin ----
exports.deleteAdmin = async (req, res, next) => {
  try {
    const admin = await User.findOneAndDelete({ _id: req.params.id, role: "admin" });
    if (!admin) return next(new createError(404, "Admin not found"));

    res.status(200).json({ status: "success", message: "Admin deleted successfully" });
  } catch (error) {
    console.error("Error deleting admin:", error);
    next(error);
  }
};

// ---- Admins by Organization ----
exports.getAdminsByOrganization = async (req, res, next) => {
  try {
    const { organizationId } = req.params;

    let organization = null;
    if (mongoose.Types.ObjectId.isValid(organizationId)) {
      organization = await Organization.findById(organizationId);
    }
    if (!organization) organization = await Organization.findOne({ organizationId });
    if (!organization) return next(new createError(404, "Organization not found"));

    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 10;
    const skip = (page - 1) * limit;

    const orgIdString = organizationId.toString();
    const orgObjectId = organization._id.toString();

    const admins = await User.find({
      $or: [{ organizationId: orgIdString }, { organizationId: orgObjectId }],
      role: "admin",
    })
      .select("-password")
      .sort({ createdAt: -1 })
      .skip(skip)
      .limit(limit);

    const total = await User.countDocuments({
      $or: [{ organizationId: orgIdString }, { organizationId: orgObjectId }],
      role: "admin",
    });

    const adminsWithOrgName = admins.map((a) => {
      const obj = a.toObject();
      obj.organizationName = organization.name;
      return obj;
    });

    res.status(200).json({
      status: "success",
      results: adminsWithOrgName.length,
      totalPages: Math.ceil(total / limit),
      currentPage: page,
      total,
      data: {
        organizationId: organization._id,
        organizationIdField: organization.organizationId,
        organizationName: organization.name,
        admins: adminsWithOrgName,
      },
    });
  } catch (error) {
    console.error("Error fetching admins by organization:", error);
    next(error);
  }
};

// ---- Change Admin Status ----
exports.changeAdminStatus = async (req, res, next) => {
  try {
    const { isActive } = req.body;
    if (typeof isActive !== "boolean") {
      return res.status(400).json({ status: "fail", message: "isActive must be a boolean value" });
    }

    const admin = await User.findOne({ _id: req.params.id, role: "admin" });
    if (!admin) return next(new createError(404, "Admin not found"));

    admin.isActive = isActive;
    await admin.save();

    const organizationName = await getOrganizationName(admin.organizationId);

    res.status(200).json({
      status: "success",
      message: `Admin ${isActive ? "activated" : "deactivated"} successfully`,
      data: {
        id: admin._id,
        email: admin.email,
        isActive: admin.isActive,
        organizationId: admin.organizationId,
        organizationName,
      },
    });
  } catch (error) {
    console.error("Error changing admin status:", error);
    next(error);
  }
};

// ---- Diagnostic endpoint ----
exports.checkOrganization = async (req, res, next) => {
  try {
    const { organizationId } = req.params;
    const results = { checks: [] };

    if (mongoose.Types.ObjectId.isValid(organizationId)) {
      const orgById = await Organization.findById(organizationId);
      results.checks.push({
        method: "findById",
        success: !!orgById,
        result: orgById
          ? { _id: orgById._id, name: orgById.name, organizationId: orgById.organizationId }
          : null,
      });
    } else {
      results.checks.push({ method: "findById", success: false, error: "Invalid ObjectId format" });
    }

    const orgByField = await Organization.findOne({ organizationId });
    results.checks.push({
      method: "findOne by organizationId field",
      success: !!orgByField,
      result: orgByField
        ? { _id: orgByField._id, name: orgByField.name, organizationId: orgByField.organizationId }
        : null,
    });

    const allOrgs = await Organization.find().limit(5);
    results.checks.push({
      method: "Sample organizations",
      count: allOrgs.length,
      samples: allOrgs.map((o) => ({ _id: o._id, name: o.name, organizationId: o.organizationId })),
    });

    res.status(200).json({ status: "success", data: results });
  } catch (error) {
    console.error("Error in organization diagnostic check:", error);
    next(error);
  }
};

// ---- Add User (by Admin) ----
exports.addUser = async (req, res, next) => {
  console.log("[addUser] called by admin:", req.user?.userId);

  try {
    // Force role to "user" (admins can only add users)
    req.body.role = "user";
    console.log("[addUser] using authController.register type:", typeof authController.register);

    // Reuse the same register flow (this handles hashing, isVerified=false, verify email)
    return authController.register(req, res, next);
  } catch (err) {
    console.error("[addUser] error:", err.message);
    next(err);
  }
};