var geojsonStream = require('geojson-stream');
var AWS = require('aws-sdk');
var fs = require('fs');
var through2 = require('through2');
var _ = require('underscore');

// Config
var srcFile = './MapUnitPolys.geojson';
var creds = require('superenv')('aws-geo-data');
var bucket = 'aws-geo-data';
var putParams = {
    Bucket: bucket,
    ACL: 'public-read'
};

// S3 access
var s3 = new AWS.S3(creds);

// Read file up to S3, took 2.5 min
// fs.createReadStream(srcFile)
//     .pipe(geojsonStream.parse())
//     .on('data', function(feature) {
//         var f = _(putParams).extend({
//             Key: 'data/' + feature.properties.mapunitpolys_id + '.geojson',
//             Body: JSON.stringify(feature)
//         });
//         s3.putObject(f, function(err, data) {
//             if (err) console.log(err);
//         });
//     });

var Reader = function() {
    var reader = through2.obj(function(feature, enc, callback) {
        this.push(feature);
        callback();
    });

    function getData(next) {
        var params = {
            Bucket: bucket,
            Prefix: 'data/'
        };
        if (next) params.Marker = next;

        s3.listObjects(params, function(err, data) {
            if (err) throw err;
            
            var keys = _(data.Contents).pluck('Key');
            var total = data.Contents.length;
            var finished = total < 1000;
            var count = 0;

            if (data.IsTruncated) getData(keys[keys.length - 1]);

            keys.forEach(function(key) {
                s3.getObject({
                    Bucket: bucket,
                    Key: key
                }, function (err, data) {
                    if (err) throw err;
                    var feature = JSON.parse(data.Body);
                    feature.id = key;
                    reader.write(feature);
                    count++;

                    if (finished && count === total) reader.end();
                });
            });
        });
    }

    getData()
    return reader;
};

// What have we got?
// Reader().on('data', function(f) { console.log(f); });

// Try indexing, took 2min 10 sec
var rbush = require('rbush');
var tree = rbush(9, ['.w', '.s', '.e', '.n']);

function getBbox(coords) {
    // Pluck all the xCoords and all the yCoords
    var xAll = [], yAll = [];
    coords.forEach(function(coord) {
        xAll.push(coord[0]);
        yAll.push(coord[1]);
    });

    // Sort all the coords
    xAll = xAll.sort(function (a,b) { return a - b });
    yAll = yAll.sort(function (a,b) { return a - b });

    // Return the bbox
    return [xAll[0], yAll[0], xAll[xAll.length - 1], yAll[yAll.length - 1]];
}

Reader()
    .on('data', function(feature) {
        // Flatten any geometry into an array of coordinates
        var coords = (function(geom) {
            var type = geom.type;
            var coords = geom.coordinates;
            if (type === 'Point') return [coords];
            if (type === 'MultiPoint' || type === 'LineString') return coords;
            if (type === 'MultiLineString' || 'Polygon') return _(coords).flatten(true);
            if (type === 'MultiPolygon') return _(coords).chain().flatten(true).flatten(true).value();
        })(feature.geometry);

        // BBOX
        var bbox = getBbox(coords);

        // Format
        tree.insert({id: feature.id, w: bbox[0], s: bbox[1], e: bbox[2], n: bbox[3]});
    })
    .on('end', function() {
        var index = JSON.stringify(tree.toJSON());
        fs.writeFile('./spatial-index.json', index);
        s3.putObject({
            Bucket: bucket,
            ACL: 'public-read',
            Key: 'data/spatial-index.json',
            Body: index
        }, function(err, data) {
            if (err) console.log(err);
        });
    });