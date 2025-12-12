const jwt = require('jsonwebtoken');

const adminMiddleware = (req, res, next) => {
  try {
    const token = req.header("Authorization");
    if (!token) {
      return res.status(401).json({ message: "Access Denied. No token provided." });
    }

    const decoded = jwt.verify(token.replace("Bearer ", ""), process.env.JWT_SECRET);
    
    // Allow both admin and superadmin
    if (decoded.role !== 'admin' && decoded.role !== 'superadmin') {
      return res.status(403).json({ message: "Access Denied. Admin role required." });
    }
    
    req.user = decoded;
    next();
  } catch (error) {
    console.error('Admin middleware error:', error);
    res.status(500).json({ message: "Server error during authorization" });
  }
};

module.exports = adminMiddleware;