const express = require("express");
const User = require("../models/User");
const bcrypt = require("bcryptjs");
const authMiddleware = require("../middleware/authMiddleware");
const router = express.Router();
const multer = require("multer");
const path = require("path");

// Configure multer for profile image uploads
const storage = multer.diskStorage({
  destination: function(req, file, cb) {
    cb(null, 'uploads/profiles/');
  },
  filename: function(req, file, cb) {
    cb(null, `user-${req.user.userId}-${Date.now()}${path.extname(file.originalname)}`);
  }
});

const upload = multer({ 
  storage: storage,
  limits: { fileSize: 5 * 1024 * 1024 }, // 5MB limit
  fileFilter: function(req, file, cb) {
    if (!file.originalname.match(/\.(jpg|jpeg|png|gif)$/)) {
      return cb(new Error('Only image files are allowed!'), false);
    }
    cb(null, true);
  }
});

// Get current user profile
router.get("/profile", authMiddleware, async (req, res) => {
  try {
    // Ensure we have userId from the decoded token
    const userId = req.user?.userId || req.user?.id;
    console.log("[PROFILE] Request received, userId:", userId);
    
    if (!userId) {
      console.error("[PROFILE] No userId found in token:", req.user);
      return res.status(401).json({ message: "User ID not found in token" });
    }

    // Try to fetch user without populate first to see if user exists
    let user = await User.findById(userId).select("-password");
    
    if (!user) {
      console.error("[PROFILE] User not found with userId:", userId);
      return res.status(404).json({ message: "User not found" });
    }

    console.log("[PROFILE] User found, populating devices...");

    // Populate devices - handle errors gracefully
    try {
      user = await User.findById(userId)
        .select("-password")
        .populate({
          path: "devices",
          select: "deviceId deviceType manufacturer status lastActiveAt",
          options: { strictPopulate: false }
        })
        .populate({
          path: "activeDevices",
          select: "deviceId deviceType manufacturer status lastActiveAt",
          options: { strictPopulate: false }
        });
    } catch (populateError) {
      console.warn("[PROFILE] Populate error, returning user without populated devices:", populateError.message);
      // Return user without populated devices if populate fails
      user = await User.findById(userId).select("-password");
    }

    console.log("[PROFILE] Successfully fetched profile for user:", user.email);
    res.json({
      status: "success",
      data: user
    });
  } catch (error) {
    console.error("[PROFILE] Error fetching user profile:", error);
    console.error("[PROFILE] Error stack:", error.stack);
    res.status(500).json({ message: "Server error", error: error.message });
  }
});

// Update user profile
router.put("/profile", authMiddleware, async (req, res) => {
  try {
    const { 
      name, 
      email, 
      address, 
      pincode, 
      mobile, 
      dateOfBirth, 
      gender, 
      weight, 
      height, 
      waist 
    } = req.body;
    
    // Check if email is already in use by another user
    if (email && email.trim() !== '') {
      const existingUser = await User.findOne({ email, _id: { $ne: req.user.userId } });
      if (existingUser) {
        return res.status(400).json({ message: "Email already in use" });
      }
    }

    // Build update object only with valid values
    const updateData = {};
    
    // Only add fields that are not null, undefined, or empty strings
    if (name && name.trim() !== '') updateData.name = name.trim();
    if (email && email.trim() !== '') updateData.email = email.trim();
    if (address && address.trim() !== '') updateData.address = address.trim();
    if (pincode && pincode.toString().trim() !== '') updateData.pincode = pincode;
    if (mobile && mobile.toString().trim() !== '') updateData.mobile = mobile;
    
    // Handle date field
    if (dateOfBirth && dateOfBirth !== null) updateData.dateOfBirth = dateOfBirth;
    
    // Handle enum field
    if (gender && gender.trim() !== '') updateData.gender = gender.trim();
    
    // Handle numeric fields (allow 0 as valid value)
    if (weight !== null && weight !== undefined && weight !== '') updateData.weight = weight;
    if (height !== null && height !== undefined && height !== '') updateData.height = height;
    if (waist !== null && waist !== undefined && waist !== '') updateData.waist = waist;

    // Check if there's anything to update
    if (Object.keys(updateData).length === 0) {
      return res.status(400).json({ message: "No valid fields to update" });
    }

    // Update profile with only the valid fields
    const updatedUser = await User.findByIdAndUpdate(
      req.user.userId,
      { $set: updateData },
      { new: true, runValidators: true }
    ).select("-password");

    if (!updatedUser) {
      return res.status(404).json({ message: "User not found" });
    }

    res.json({
      status: "success",
      message: "Profile updated successfully",
      data: updatedUser,
      updatedFields: Object.keys(updateData) // Show which fields were updated
    });
  } catch (error) {
    console.error("Error updating user profile:", error);
    res.status(500).json({ message: "Server error", error: error.message });
  }
});

// Update profile image
router.put("/profile/image", [authMiddleware, upload.single('profileImage')], async (req, res) => {
  try {
    if (!req.file) {
      return res.status(400).json({ message: "No image uploaded" });
    }

    // Get profile image path
    const profileImage = `/uploads/profiles/${req.file.filename}`;

    const updatedUser = await User.findByIdAndUpdate(
      req.user.userId,
      { $set: { profileImage } },
      { new: true }
    ).select("-password");

    if (!updatedUser) {
      return res.status(404).json({ message: "User not found" });
    }

    res.json({
      status: "success",
      message: "Profile image updated successfully",
      data: {
        profileImage: updatedUser.profileImage
      }
    });
  } catch (error) {
    console.error("Error updating profile image:", error);
    res.status(500).json({ message: "Server error" });
  }
});

// Update password
router.put("/profile/password", authMiddleware, async (req, res) => {
  try {
    const { currentPassword, newPassword } = req.body;
    
    if (!currentPassword || !newPassword) {
      return res.status(400).json({ message: "Both current and new password are required" });
    }

    // Find the user and check current password
    const user = await User.findById(req.user.userId);
    if (!user) {
      return res.status(404).json({ message: "User not found" });
    }

    const isMatch = await bcrypt.compare(currentPassword, user.password);
    if (!isMatch) {
      return res.status(401).json({ message: "Current password is incorrect" });
    }

    // Hash the new password
    const hashedPassword = await bcrypt.hash(newPassword, 12);
    
    // Update password
    await User.findByIdAndUpdate(
      req.user.userId,
      { $set: { password: hashedPassword } }
    );

    res.json({
      status: "success",
      message: "Password updated successfully"
    });
  } catch (error) {
    console.error("Error updating password:", error);
    res.status(500).json({ message: "Server error" });
  }
});

// Delete user account
router.delete("/profile", authMiddleware, async (req, res) => {
  try {
    // Option to require password confirmation
    const { password } = req.body;
    
    const user = await User.findById(req.user.userId);
    if (!user) {
      return res.status(404).json({ message: "User not found" });
    }

    // Verify password if provided
    if (password) {
      const isMatch = await bcrypt.compare(password, user.password);
      if (!isMatch) {
        return res.status(401).json({ message: "Password is incorrect" });
      }
    }

    // Delete user account
    await User.findByIdAndDelete(req.user.userId);

    res.json({
      status: "success",
      message: "Account deleted successfully"
    });
  } catch (error) {
    console.error("Error deleting account:", error);
    res.status(500).json({ message: "Server error" });
  }
});

// GET /user/organization-id - Fetch organizationId of the logged-in user
router.get('/user/organization-id', authMiddleware, async (req, res) => {
  try {
    const userId = req.user && (req.user.userId || req.user.id);
    if (!userId) {
      return res.status(401).json({ status: 'fail', message: 'Unauthorized: User not found in request' });
    }

    const user = await User.findById(userId).select('organizationId');
    if (!user) {
      return res.status(404).json({ status: 'fail', message: 'User not found' });
    }

    res.json({ status: 'success', organizationId: user.organizationId });
  } catch (error) {
    res.status(500).json({ status: 'fail', message: error.message });
  }
});

module.exports = router;