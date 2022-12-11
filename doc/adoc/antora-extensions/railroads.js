const fs = require('fs');
const matcher = /railroad::(?<filename>.*)\[\]/;

function embed(s, filename) {
  var path = './railroads/'.concat(filename);
  var contents;

  try {
    contents = fs.readFileSync(path, 'utf8');
//    console.log(contents);
  } catch (err) {
    console.error(err);
  }

  return s.replace(matcher, contents);
}

function tryEmbed(s) {
  var svg = s.match(matcher);
  if (svg != null) {
    return embed(s, svg.groups.filename);
  } else {
    return s;
  }
}

module.exports = function (registry) {
  registry.postprocessor('railroad', function () {
//    console.log('postprocessor');
    var self = this
    self.process(function (doc, output) {
      var proc = doc.attributes.$$smap.docfile;
//      console.log('processing:',proc);

      var svg = output.match(matcher);
      if (svg != null) {
        console.log('first matched railroad is:', svg.groups.filename);
        return output.split('\n').map(line => tryEmbed(line)).join('\n');
      } else {
        return output;
      }
    })
  })
}
