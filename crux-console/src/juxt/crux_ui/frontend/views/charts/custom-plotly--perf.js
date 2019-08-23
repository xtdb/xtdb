// in custom-plotly--perf.js
require('graceful-fs/graceful-fs');
require('gl-surface3d/lib/shaders');
require('gl-surface3d/surface');
require('gl-surface3d');
var Plotly = require('plotly.js/lib/core');

// Load in the trace types for pie, and choropleth
Plotly.register([
    require('plotly.js/lib/surface')
]);

module.exports = Plotly;