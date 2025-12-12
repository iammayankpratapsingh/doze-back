const Device = require('../models/Device');
const HealthData = require('../models/HealthData');

/**
 * Updates the status of all devices based on recent activity
 * A device is considered active if it has sent data within the last 24 hours
 */
async function updateAllDeviceStatuses() {
    try {
        const twentyFourHoursAgo = new Date(Date.now() - 24 * 60 * 60 * 1000);
        const devices = await Device.find();
        
        const statusUpdates = {
            active: 0,
            inactive: 0,
            total: devices.length
        };

        for (let device of devices) {
            // Check if there's any health data in the last 24 hours
            const recentData = await HealthData.findOne({
                deviceId: device.deviceId,
                timestamp: { $gte: twentyFourHoursAgo }
            }).sort({ timestamp: -1 });

            // Determine device status based on recent activity
            const newStatus = recentData ? "active" : "inactive";
            const lastActiveAt = recentData ? recentData.timestamp : device.lastActiveAt;
            
            // Only update if status changed or lastActiveAt changed
            if (device.status !== newStatus || 
                (recentData && (!device.lastActiveAt || device.lastActiveAt < recentData.timestamp))) {
                
                await Device.findByIdAndUpdate(device._id, {
                    status: newStatus,
                    lastActiveAt: lastActiveAt
                });
                
                statusUpdates[newStatus]++;
                console.log(`Updated device ${device.deviceId} status to ${newStatus}`);
            }
        }
        
        return {
            message: "Device statuses updated",
            updates: statusUpdates
        };
    } catch (error) {
        console.error("Error updating device statuses:", error);
        throw error;
    }
}

/**
 * Checks if a specific device is active (has data in the last 24 hours)
 */
async function checkDeviceStatus(deviceId) {
    try {
        const twentyFourHoursAgo = new Date(Date.now() - 24 * 60 * 60 * 1000);
        
        // Check if there's any health data in the last 24 hours
        const recentData = await HealthData.findOne({
            deviceId: deviceId,
            timestamp: { $gte: twentyFourHoursAgo }
        }).sort({ timestamp: -1 });

        return {
            isActive: !!recentData,
            lastActiveAt: recentData ? recentData.timestamp : null
        };
    } catch (error) {
        console.error(`Error checking device status for ${deviceId}:`, error);
        throw error;
    }
}

module.exports = {
    updateAllDeviceStatuses,
    checkDeviceStatus
};