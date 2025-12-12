const User = require("../models/User");
const bcrypt = require("bcryptjs");
const createError = require("../utils/appError");
// Create a new user
exports.createUser = async (req, res) => {
  try {
    const { 
      email, 
      password, 
      name, 
      address, 
      pincode, 
      mobile, 
      role,
      organizationId,
      dateOfBirth,
      gender,
      weight,
      height,
      waist,
      devices
    } = req.body;
    
    // Check if user already exists
    const existingUser = await User.findOne({ email });
    if (existingUser) {
      return res.status(400).json({
        status: "fail",
        message: "Email already in use"
      });
    }
    
    // Hash password
    const hashedPassword = await bcrypt.hash(password, 12);
    
    // Create new user
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
    
    // Remove password from response
    const userResponse = newUser.toObject();
    delete userResponse.password;
    
    res.status(201).json({
      status: "success",
      message: "User created successfully",
      data: userResponse
    });
  } catch (error) {
    console.error("Error creating user:", error);
    res.status(500).json({ message: "Server error", error: error.message });
  }
};

// Get all users with pagination, filtering and search
exports.getAllUsers = async (req, res) => {
  try {
    // Pagination
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 10;
    const skip = (page - 1) * limit;
    
    // Build filter object
    let filter = {};
    
    // Filter by role if provided
    if (req.query.role) {
      filter.role = req.query.role;
    }
    
    // Filter by organization if provided
    if (req.query.organizationId) {
      filter.organizationId = req.query.organizationId;
    }
    
    // Search by name or email
    if (req.query.search) {
      filter.$or = [
        { name: { $regex: req.query.search, $options: "i" } },
        { email: { $regex: req.query.search, $options: "i" } }
      ];
    }
    
    // Execute query with pagination
    const users = await User.find(filter)
      .select("-password")
      .sort({ createdAt: -1 })
      .skip(skip)
      .limit(limit);
    
    // Get total count for pagination
    const total = await User.countDocuments(filter);
    
    res.status(200).json({
      status: "success",
      results: users.length,
      totalPages: Math.ceil(total / limit),
      currentPage: page,
      total,
      data: users
    });
  } catch (error) {
    console.error("Error fetching users:", error);
    res.status(500).json({ message: "Server error", error: error.message });
  }
};

// Get user by ID
exports.getUserById = async (req, res) => {
  try {
    const user = await User.findById(req.params.id)
      .select("-password")
      .populate("devices")
      .populate("activeDevice");
      
    if (!user) {
      return res.status(404).json({ 
        status: "fail", 
        message: "User not found" 
      });
    }
    
    res.status(200).json({
      status: "success",
      data: user
    });
  } catch (error) {
    console.error("Error fetching user:", error);
    res.status(500).json({ message: "Server error", error: error.message });
  }
};

// Update user
exports.updateUser = async (req, res) => {
  try {
    const { 
      name, 
      email, 
      address, 
      pincode, 
      mobile, 
      role,
      organizationId,
      dateOfBirth,
      gender,
      weight,
      height,
      waist 
    } = req.body;
    
    // Check if email exists for another user
    if (email) {
      const existingUser = await User.findOne({ 
        email, 
        _id: { $ne: req.params.id } 
      });
      
      if (existingUser) {
        return res.status(400).json({ 
          status: "fail",
          message: "Email already in use" 
        });
      }
    }
    
    // Build update object with only provided fields
    const updateData = {};
    
    if (name) updateData.name = name;
    if (email) updateData.email = email;
    if (address) updateData.address = address;
    if (pincode) updateData.pincode = pincode;
    if (mobile) updateData.mobile = mobile;
    if (role) updateData.role = role;
    if (organizationId) updateData.organizationId = organizationId;
    if (dateOfBirth) updateData.dateOfBirth = dateOfBirth;
    if (gender) updateData.gender = gender;
    if (weight) updateData.weight = weight;
    if (height) updateData.height = height;
    if (waist) updateData.waist = waist;
    
    // If password is provided, hash it
    if (req.body.password) {
      updateData.password = await bcrypt.hash(req.body.password, 12);
    }
    
    const updatedUser = await User.findByIdAndUpdate(
      req.params.id,
      { $set: updateData },
      { new: true, runValidators: true }
    ).select("-password");
    
    if (!updatedUser) {
      return res.status(404).json({ 
        status: "fail",
        message: "User not found" 
      });
    }
    
    res.status(200).json({
      status: "success",
      message: "User updated successfully",
      data: updatedUser
    });
  } catch (error) {
    console.error("Error updating user:", error);
    res.status(500).json({ message: "Server error", error: error.message });
  }
};

// Delete user
exports.deleteUser = async (req, res) => {
  try {
    const user = await User.findById(req.params.id);
    
    if (!user) {
      return res.status(404).json({ 
        status: "fail",
        message: "User not found" 
      });
    }
    
    // Prevent deleting yourself (for safety)
    if (user._id.toString() === req.user.userId) {
      return res.status(400).json({ 
        status: "fail",
        message: "Cannot delete your own account through this API" 
      });
    }
    
    await User.findByIdAndDelete(req.params.id);
    
    res.status(200).json({
      status: "success",
      message: "User deleted successfully"
    });
  } catch (error) {
    console.error("Error deleting user:", error);
    res.status(500).json({ message: "Server error", error: error.message });
  }
};

// Change user role
exports.changeUserRole = async (req, res) => {
  try {
    const { role } = req.body;
    
    if (!["user", "admin"].includes(role)) {
      return res.status(400).json({ 
        status: "fail",
        message: "Invalid role. Must be 'user' or 'admin'" 
      });
    }
    
    const user = await User.findById(req.params.id);
    
    if (!user) {
      return res.status(404).json({ 
        status: "fail",
        message: "User not found" 
      });
    }
    
    // Prevent changing your own role (for safety)
    if (user._id.toString() === req.user.userId) {
      return res.status(400).json({ 
        status: "fail",
        message: "Cannot change your own role" 
      });
    }
    
    // Prevent changing superadmin role (for safety)
    if (user.role === "superadmin") {
      return res.status(403).json({ 
        status: "fail",
        message: "Cannot change role of a superadmin" 
      });
    }
    
    user.role = role;
    await user.save();
    
    res.status(200).json({
      status: "success",
      message: "User role updated successfully",
      data: {
        id: user._id,
        email: user.email,
        role: user.role
      }
    });
  } catch (error) {
    console.error("Error changing user role:", error);
    res.status(500).json({ message: "Server error", error: error.message });
  }
};

// Get users by organization
exports.getUsersByOrganization = async (req, res) => {
  try {
    const { organizationId } = req.params;
    
    // Pagination
    const page = parseInt(req.query.page) || 1;
    const limit = parseInt(req.query.limit) || 10;
    const skip = (page - 1) * limit;
    
    // Build filter for the organization
    const filter = { 
      organizationId,
      role: "user" // Only return users, not admin or superadmin
    };
    
    // Search by name or email
    if (req.query.search) {
      filter.$or = [
        { name: { $regex: req.query.search, $options: "i" } },
        { email: { $regex: req.query.search, $options: "i" } }
      ];
    }
    
    // Execute query with pagination
    const users = await User.find(filter)
      .select("-password")
      .sort({ createdAt: -1 })
      .skip(skip)
      .limit(limit);
    
    // Get total count for pagination
    const total = await User.countDocuments(filter);
    
    res.status(200).json({
      status: "success",
      results: users.length,
      totalPages: Math.ceil(total / limit),
      currentPage: page,
      total,
      organizationId,
      data: users
    });
  } catch (error) {
    console.error("Error fetching users by organization:", error);
    res.status(500).json({ message: "Server error", error: error.message });
  }
};

exports.getMe = async (req, res) => {
  try {
    const userId = req.user?.userId; // from authMiddleware
    if (!userId) return res.status(401).json({ message: "Unauthorized - invalid token" });

    const user = await User.findById(userId)
      .populate("devices", "deviceId deviceType manufacturer status");

    if (!user) return res.status(404).json({ message: "User not found" });

    res.json(user);
  } catch (err) {
    console.error("Error in getMe:", err);
    res.status(500).json({ message: "Server error", error: err.message });
  }
};
