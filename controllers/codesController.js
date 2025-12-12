// controllers/codesController.js
const DeviceModel = require("../models/DeviceModel");

exports.getDeviceModelsWithManufacturers = async (req, res, next) => {
  try {
    const results = await DeviceModel.aggregate([
      {
        $lookup: {
          from: "manufacturers",          // exact DB collection
          localField: "manufacturerId",   // from deviceModels
          foreignField: "_id",            // in manufacturers
          as: "manufacturer"
        }
      },
      { $unwind: "$manufacturer" },
      {
        $project: {
          _id: 1,
          code: 1,
          name: 1,
          "manufacturer._id": 1,
          "manufacturer.code": 1,
          "manufacturer.name": 1
        }
      }
    ]);

    console.log("[CodesController] deviceModels count:", results.length);

    return res.json({ status: "success", data: results });
  } catch (err) {
    console.error("[CodesController:error]", err.message);
    next(err);
  }
};
