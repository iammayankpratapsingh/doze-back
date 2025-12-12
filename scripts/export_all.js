// D:\export_param.js
// Usage examples:
// 🔹 Export ALL parameters:
// mongosh D:\export_param.js --eval "FROM='2025-10-01T00:00:00Z'; TO='2025-10-06T23:59:59Z'; PARAM='ALL'"
// 🔹 Export only heartRate:
// mongosh D:\export_param.js --eval "FROM='2025-10-01T00:00:00Z'; TO='2025-10-06T23:59:59Z'; PARAM='heartRate'"

if (typeof FROM === 'undefined' || typeof TO === 'undefined' || typeof PARAM === 'undefined') {
  print("❌ Missing inputs. Please specify FROM, TO, and PARAM.");
  print("Example:");
  print('mongosh D:\\export_param.js --eval "FROM=\'2025-10-01T00:00:00Z\'; TO=\'2025-10-06T23:59:59Z\'; PARAM=\'ALL\'"');
  quit(1);
}

const fromDate = ISODate(FROM);
const toDate = ISODate(TO);

const projectionAll = {
  timestamp: 1,
  temp: 1,
  humidity: 1,
  iaq: 1,
  eco2: 1,
  tvoc: 1,
  etoh: 1,
  hrv: 1,
  stress: 1,
  respiration: 1,
  heartRate: 1,
  _id: 0
};

let projection = {};
let header = "";

if (PARAM === "ALL") {
  projection = projectionAll;
  header = "timestamp,temp,humidity,iaq,eco2,tvoc,etoh,hrv,stress,respiration,heartRate";
} else {
  projection = { timestamp: 1 };
  projection[PARAM] = 1;
  header = "timestamp," + PARAM;
}

const cursor = db.healthdatas.find(
  { timestamp: { $gte: fromDate, $lte: toDate } },
  projection
).sort({ timestamp: 1 });

print(header);

cursor.forEach(doc => {
  if (PARAM === "ALL") {
    const row = [
      doc.timestamp,
      doc.temp ?? "",
      doc.humidity ?? "",
      doc.iaq ?? "",
      doc.eco2 ?? "",
      doc.tvoc ?? "",
      doc.etoh ?? "",
      doc.hrv ?? "",
      doc.stress ?? "",
      doc.respiration ?? "",
      doc.heartRate ?? ""
    ].join(",");
    print(row);
  } else if (doc[PARAM] !== undefined && doc[PARAM] !== null) {
    print(`${doc.timestamp},${doc[PARAM]}`);
  }
});
