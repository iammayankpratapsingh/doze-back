// Accept "0102-AC:DE:48:11:22:33" or "0102-ACDE48112233"
const PREFIX_TO_TYPE = { "0102": { deviceType: "Dozemate", manufacturer: "Dozemate" } };
// extend table if you add more prefixes later

function normalizeDeviceId(raw) {
  if (!raw) throw new Error("deviceId required");
  let s = raw.toUpperCase().trim();
  s = s.replace(/:/g, ""); // strip colons from MAC portion if any
  // enforce "NNNN-XXXXXXXXXXXX" (4 digits + '-' + 12 hex)
  const m = s.match(/^(\d{4})-?([0-9A-F]{12})$/);
  if (!m) throw new Error("Invalid deviceId format. Expected 4 digits + 12-hex (with or without '-')");
  return `${m[1]}-${m[2]}`;
}

function deriveMetaFromPrefix(deviceId) {
  const prefix = deviceId.split("-")[0];
  return PREFIX_TO_TYPE[prefix] || {};
}

function macFromDeviceId(deviceId) {
  return deviceId.split("-")[1]; // already 12-hex, no colons
}

module.exports = { normalizeDeviceId, deriveMetaFromPrefix, macFromDeviceId };
