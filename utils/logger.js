const winston = require("winston");

const logFormat = winston.format.combine(
  winston.format.timestamp(),
  winston.format.printf(({ level, message, timestamp, ...meta }) => {
    return `${timestamp} [${level.toUpperCase()}]: ${message} ${Object.keys(meta).length ? JSON.stringify(meta) : ""}`;
  })
);

const logger = winston.createLogger({
  level: "info",
  format: logFormat,
  transports: [
    new winston.transports.Console({
      format: winston.format.combine(
        winston.format.colorize(),
        winston.format.simple()
      )
    })
  ]
});

// Helper wrappers
logger.req = (req, meta) => logger.info(`${req.method} ${req.originalUrl}`, meta);
logger.err = (err, meta) => logger.error(err.message || err, { stack: err.stack, ...meta });

module.exports = { logger };
