// Backend | config/metricSpec.js
// Single source of truth for device-specific param rules.
// Only rules — no business logic here.

const COMMON_ON_CHANGE = [
  "Stress", "SpO2", "HRrest", "HRmax", "VO2max", "LactateThres",
  "TemperatureSkin", "TemperatureEnv", "TemperatureCore", "ECG",
  "Snoring", "BloodPressureSys", "BloodPressureDia", "MuscleOxygenation",
  "GSR", "SleepStage", "SleepQuality", "Fall", "BMI", "BodyIndex", "ABSI",
  "Sports", "Start", "End", "NormalSinusRhythm", "CHFAnalysis", "Diabetes", "TMT"
];

// IMPORTANT: keys below are normalized metric ids used across backend+frontend.
const DEVICES = {
  Dozemate: {
    presenceField: "signals.presence",
    params: {
      // Core vitals
      HR:         { min: 48, max: 200, mode: "periodic", avgSec: 6 },   // was 12 → now 6s per Excel
      HRV:        { min: 300, max: 1600, mode: "periodic", avgSec: 540 },
      Respiration:{ min: 2,   max: 20,  mode: "periodic", avgSec: 6 },  // was 12 → now 6s
      Stress:     { min: 0,   max: 50,  mode: "onchange" },
      SpO2:       { min: 50,  max: 100, mode: "onchange" },
      Temp:       { min: 0,   max: 100, mode: "periodic", avgSec: 6 },
      Hum:        { min: 0,   max: 100, mode: "periodic", avgSec: 6 },

      // On-change
      Motion:     { min: 0,   max: 1,   mode: "onchange", avgSec: 1 },
      Human:      { min: 0,   max: 1,   mode: "onchange", avgSec: 12 },

      // Environment (every 12s)
      IAQ:        { min: 0, max: 500,  mode: "periodic", avgSec: 12 },
      BVOC:       { min: 0, max: 500,  mode: "periodic", avgSec: 12 },
      CO2:        { min: 0, max: 5000, mode: "periodic", avgSec: 12 },
      TVOC:       { min: 0, max: 500,  mode: "periodic", avgSec: 12 },
      GasPercent: { min: 0, max: 100,  mode: "periodic", avgSec: 12 },
      Pressure:   { min: 0, max: 2000, mode: "periodic", avgSec: 12 },
      SnoreNum:   { min: 0, max: 500,  mode: "periodic", avgSec: 12 },
      SnoreFreq:  { min: 0, max: 500,  mode: "periodic", avgSec: 12 },

      // HRV metrics (9 min = 540s)
      SDNN:       { min: 0, max: 500,  mode: "periodic", avgSec: 540 },
      RMSSD:      { min: 0, max: 500,  mode: "periodic", avgSec: 540 },
      NN50:       { min: 0, max: 500,  mode: "periodic", avgSec: 540 },
      PNN50:      { min: 0, max: 100,  mode: "periodic", avgSec: 540 },
      SDSD:       { min: 0, max: 500,  mode: "periodic", avgSec: 540 },
      SD1:        { min: 0, max: 500,  mode: "periodic", avgSec: 540 },
      SD2:        { min: 0, max: 500,  mode: "periodic", avgSec: 540 },
      MXDMN:      { min: 0, max: 500,  mode: "periodic", avgSec: 540 },
      MO:         { min: 0, max: 500,  mode: "periodic", avgSec: 540 },
      AMO:        { min: 0, max: 500,  mode: "periodic", avgSec: 540 },
      StressIndex:{ min: 0, max: 100,  mode: "periodic", avgSec: 540 },
      LF_POW:     { min: 0, max: 1000, mode: "periodic", avgSec: 540 },
      HF_POW:     { min: 0, max: 1000, mode: "periodic", avgSec: 540 },
      LF_HF_Ratio:{ min: 0, max: 100,  mode: "periodic", avgSec: 540 },
    }
  },

  Hexaskin: {
    presenceField: "signals.presence",
    // Largely same constraints per sheet; includes posture/fall/VO2/etc.
    params: {
      HR: { min: 48, max: 200, mode: "periodic", avgSec: 12 },
      HRV: { min: 300, max: 1600, mode: "periodic", avgSec: 12 },
      Respiration: { min: 2, max: 20, mode: "periodic", avgSec: 12 },
      Stress: { min: 0, max: 50, mode: "onchange" },
      SpO2: { min: 50, max: 100, mode: "onchange" },
      HRrest: { min: 48, max: 200, mode: "onchange" },
      HRmax: { min: 48, max: 200, mode: "onchange" },
      VO2max: { min: 10, max: 100, mode: "onchange" },
      LactateThres: { min: 10, max: 500, mode: "onchange" },
      TemperatureSkin: { min: 0, max: 100, mode: "onchange", avgSec: 12 },
      TemperatureEnv: { min: 0, max: 100, mode: "periodic", avgSec: 12 },
      TemperatureCore: { min: 0, max: 200, mode: "periodic", avgSec: 12 },
      ECG: { min: null, max: null, mode: "onchange" },
      Barometer: { min: 0, max: 10000, mode: "periodic", avgSec: 60 },
      Accel: { min: null, max: null, mode: "periodic", avgSec: 6 },
      Gyro: { min: null, max: null, mode: "periodic", avgSec: 6 },
      Magneto: { min: null, max: null, mode: "periodic", avgSec: 6 },
      Steps: { min: 0, max: 100000, mode: "periodic", avgSec: 6 },
      Calories: { min: 0, max: 10000, mode: "periodic", avgSec: 6 },
      Distance: { min: 0, max: 100000, mode: "periodic", avgSec: 6 },
      Snoring: { min: 0, max: 1, mode: "onchange" },
      BloodPressureSys: { min: 20, max: 200, mode: "onchange" },
      BloodPressureDia: { min: 20, max: 200, mode: "onchange" },
      MuscleOxygenation: { min: 0, max: 100, mode: "onchange" },
      GSR: { min: 0, max: 100, mode: "onchange" },
      SleepStage: { min: 1, max: 5, mode: "onchange" },
      SleepQuality: { min: 0, max: 100, mode: "onchange" },
      PostureFront: { min: -90, max: 90, mode: "onchange" },
      PostureSide: { min: -90, max: 90, mode: "onchange" },
      Fall: { min: 0, max: 1, mode: "onchange" },
      BMI: { min: 0, max: 100, mode: "onchange" },
      BodyIndex: { min: 0, max: 100, mode: "onchange" },
      ABSI: { min: 0, max: 100, mode: "onchange" },
      Sports: { min: 1, max: 20, mode: "onchange" },
      Start: { min: 0, max: 1, mode: "onchange" },
      End: { min: 0, max: 1, mode: "onchange" },
      // HRV block
      SDNN: { min: 0, max: 200, mode: "periodic", avgSec: 540 },
      RMSSD: { min: 0, max: 500, mode: "periodic", avgSec: 540 },
      LFHF: { min: 0, max: 100, mode: "periodic", avgSec: 540 },
      PNN50: { min: 0, max: 200, mode: "periodic", avgSec: 540 },
      StressIndex: { min: 0, max: 50, mode: "periodic", avgSec: 540 },
      NormalSinusRhythm: { min: 0, max: 1, mode: "onchange" },
      CHFAnalysis: { min: 0, max: 1, mode: "onchange" },
      Diabetes: { min: 0, max: 1, mode: "onchange" },
      TMT: { min: 0, max: 1, mode: "onchange" },
    }
  }
};

module.exports = DEVICES;
