const mongoose = require("mongoose");

const HealthDataSchema = new mongoose.Schema({
    deviceId: { type: String, required: true },
    timestamp: { type: Date, default: Date.now },
    
    // Removed mapped fields: temp, humidity, iaq, eco2, tvoc, etoh, hrv, stress, respiration, heartRate
    // Only abbreviated fields are stored as top-level fields
    
    // Abbreviated format fields - stored as top-level fields
    TS: { type: Number },
    TS_ms: { type: Number },
    T: { type: String },
    H: { type: String },
    MS: { type: String },
    MST: { type: String },
    AS: { type: String },
    AST: { type: String },
    SS: { type: String },
    SST: { type: String },
    SF: { type: String },
    RST: { type: String },
    RS: { type: String },
    V: { type: String },
    L: { type: String },
    S: { type: String },
    HR: { type: String },
    RE: { type: String },
    IA: { type: String },
    CO: { type: String },
    VO: { type: String },
    ET: { type: String },
    
    metrics: {
        nn50: { type: Number },
        sdsd: { type: Number },
        mxdmn: { type: Number },
        mo: { type: Number },
        amo: { type: Number },
        stress_ind: { type: Number },
        lf_pow: { type: Number },
        hf_pow: { type: Number },
        lf_hf_ratio: { type: Number },
        bat: { type: Number },
        mean_hr: { type: Number },
        snore_num: { type: Number },
        snore_freq: { type: Number },
        pressure: { type: Number },
        bvoc: { type: Number },
        co2: { type: Number },
        gas_percent: { type: Number },

        HRrest: { type: Number },
        HRmax: { type: Number },
        VO2max: { type: Number },
        LactateThres: { type: Number },
        TemperatureSkin: { type: Number },
        TemperatureEnv: { type: Number },
        TemperatureCore: { type: Number },
        ECG: { type: Number },
        Barometer: { type: Number },
        Accel: { type: Number },
        Gyro: { type: Number },
        Magneto: { type: Number },
        Steps: { type: Number },
        Calories: { type: Number },
        Distance: { type: Number },
        BloodPressureSys: { type: Number },
        BloodPressureDia: { type: Number },
        MuscleOxygenation: { type: Number },
        GSR: { type: Number },
        SleepStage: { type: Number },
        SleepQuality: { type: Number },
        PostureFront: { type: Number },
        PostureSide: { type: Number },
        Fall: { type: Number },
        BMI: { type: Number },
        BodyIndex: { type: Number },
        ABSI: { type: Number },
        Sports: { type: Number },
        Start: { type: Number },
        End: { type: Number },
        NormalSinusRhythm: { type: Number },
        CHFAnalysis: { type: Number },
        Diabetes: { type: Number },
        TMT: { type: Number },
        sdnn: { type: Number },
        rmssd: { type: Number },
        pnn50: { type: Number },
        hr_median: { type: Number },
        rr_tri_index: { type: Number },
        tin_rmssd: { type: Number },
        sd1: { type: Number },
        sd2: { type: Number },
        lf: { type: Number },
        hf: { type: Number },
        lfhf: { type: Number },
        sample_entropy: { type: Number },
        sd1sd2: { type: Number },
        sns_index: { type: Number },
        pns_index: { type: Number }
    },
    signals: {
        motion: { type: Number },
        presence: { type: Number },
        activity: { type: Number },
        battery: { type: Number },
        mic: { type: Number },
        rrIntervals: [{ type: Number }],
        rawWaveform: [{ type: Number }]
    },

    // ✅ Raw storage for debugging/logging UART packets
    raw: {
        type: Object,  // catch-all raw UART JSON payload
        default: {}
    }

}, {
    strict: true,  // Prevent saving fields not defined in schema
    collection: 'healthdata_new'  // Use custom collection name
});

// Pre-save hook to ensure mapped fields are never saved
HealthDataSchema.pre('save', function(next) {
    // CRITICAL: Remove temperature and all mapped fields
    const mappedFields = ['temperature', 'temp', 'humidity', 'iaq', 'eco2', 'tvoc', 'etoh', 'hrv', 'stress', 'respiration', 'heartRate'];
    mappedFields.forEach(field => {
        if (this.get(field) !== undefined) {
            this.unset(field);
        }
        if (this[field] !== undefined) {
            delete this[field];
            this.set(field, undefined);
        }
    });
    next();
});

// Pre-validate hook to catch mapped fields before validation
HealthDataSchema.pre('validate', function(next) {
    // Remove mapped fields if somehow present
    const mappedFields = ['temperature', 'temp', 'humidity', 'iaq', 'eco2', 'tvoc', 'etoh', 'hrv', 'stress', 'respiration', 'heartRate'];
    mappedFields.forEach(field => {
        if (this[field] !== undefined) {
            delete this[field];
        }
    });
    next();
});

HealthDataSchema.index({ deviceId: 1, timestamp: -1 });

module.exports = mongoose.model("HealthData", HealthDataSchema);
