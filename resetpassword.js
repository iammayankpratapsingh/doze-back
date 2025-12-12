const bcrypt = require("bcryptjs");

(async () => {
  const hash = await bcrypt.hash("12345678", 12);
  console.log("Hash:", hash);
})();