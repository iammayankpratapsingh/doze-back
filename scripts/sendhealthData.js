// Total runtime: 9 minutes = 540 seconds
// Interval: 6 seconds → 90 iterations
// Presence pattern: 30 iterations (3 min) = 1, 1 iteration (≈6 sec) = 0, repeat.

const deviceId = "0102-B95AA8AF0427";
const totalIterations = 90;  // 9 minutes at 6s interval
const intervalMs = 6000;

function randomFloat(min, max, decimals = 2) {
  return +(Math.random() * (max - min) + min).toFixed(decimals);
}
function randomInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

function generateDoc(i) {
  // Presence pattern: 30 iterations with 1, then 1 iteration with 0
  const presence = (i % 31 === 30) ? 0 : 1;

  return {
    deviceId,
    timestamp: new Date(),
    heartRate: randomInt(60, 100),
    respiration: randomInt(12, 18),
    SpO2: randomInt(94, 99),
    stress: randomInt(10, 40),
    temp: randomFloat(35.5, 37.5, 1),
    humidity: randomInt(40, 65),
    iaq: randomInt(50, 100),
    eco2: randomInt(400, 700),
    tvoc: randomFloat(0, 2, 2),
    etoh: randomFloat(0, 0.1, 2),
    metrics: {
      sdnn: randomFloat(30, 70, 2),
      rmssd: randomFloat(20, 50, 2),
      lf_hf_ratio: randomFloat(0.1, 2, 2),
      stress_index: randomInt(10, 30)
    },
    signals: {
      motion: randomInt(0, 1),
      presence: presence,
      activity: randomInt(0, 1),
      battery: randomInt(50, 100),
      mic: randomInt(0, 1),
      rrIntervals: Array.from({ length: 4 }, () => randomInt(800, 900)),
      rawWaveform: Array.from({ length: 4 }, () => randomFloat(0.05, 0.08, 2))
    },
    __v: 0
  };
}

// Insert loop with delay
function runSimulation() {
  for (let i = 0; i < totalIterations; i++) {
    const doc = generateDoc(i);
    db.healthdatas.insertOne(doc);
    print(`Inserted doc ${i + 1}/${totalIterations} with presence=${doc.signals.presence}`);
    sleep(intervalMs); // wait 6 seconds
  }
  print("✅ Simulation complete (9 minutes)");
}

runSimulation();
