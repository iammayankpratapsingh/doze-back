const path = require('path');
const webpack = require('webpack');
const dotenv = require('dotenv');
const nodeExternals = require('webpack-node-externals');

// Load env variables from .env.production
const env = dotenv.config({ path: './.env.production' }).parsed;

// Convert to webpack DefinePlugin format
const envKeys = Object.keys(env || {}).reduce((prev, next) => {
  prev[`process.env.${next}`] = JSON.stringify(env[next]);
  return prev;
}, {});

module.exports = {
  entry: './server.js',
  target: 'node',
  output: {
    path: path.resolve(__dirname, 'dist'),
    filename: 'index.js',
  },
  externals: [nodeExternals()],
  mode: 'production',
  module: {
    rules: [
      {
        test: /\.js$/,
        exclude: /node_modules/,
        use: 'babel-loader',
      },
    ],
  },
  plugins: [
    new webpack.DefinePlugin(envKeys),
  ],
};
