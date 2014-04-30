var geojsonStream = require('geojson-stream');
var AWS = require('aws-sdk');
var fs = require('fs');
var _ = require('underscore');
var async = require('async');
var events = require('events');
var rbush = require('rbush');

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

// --------------------------------------------------------------------------------
// --------------------------------------------------------------------------------

// Read file up to S3 and create spatial index took 2.5 min
var tree = rbush(9, ['.w', '.s', '.e', '.n']);

fs.createReadStream(srcFile)
    .pipe(geojsonStream.parse())
    .on('data', function(feature) {
        // Build spatial index
        var bbox = getBbox(feature);
        feature.id = 'data/' + feature.properties.mapunitpolys_id + '.geojson';
        tree.insert({id: feature.id, w: bbox[0], s: bbox[1], e: bbox[2], n: bbox[3]});

        // Put each feature to S3
        // var f = _(putParams).extend({
        //     Key: feature.id,
        //     Body: JSON.stringify(feature)
        // });
        // s3.putObject(f, function(err, data) {
        //     if (err) console.log(err);
        // });
    })
    .on('end', function() {
        writeSpatialIndex(tree);
    });

// --------------------------------------------------------------------------------
// --------------------------------------------------------------------------------

// A tool for reading the files from S3 individually
var Reader = function() {
    var reader = new events.EventEmitter();

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
                    reader.emit('data', feature);
                    count++;

                    if (finished && count === total) reader.emit('end');
                });
            });
        });
    }

    getData()
    return reader;
};

// --------------------------------------------------------------------------------
// --------------------------------------------------------------------------------

// Full read took 2min 34 sec, including about 500 lumped files
// Reader().on('data', function(f) { console.log(f); });

// --------------------------------------------------------------------------------
// --------------------------------------------------------------------------------

// // Indexing from files already on S3 took 2min 10 sec
// var tree = rbush(9, ['.w', '.s', '.e', '.n']);
// Reader()
//     .on('data', function(feature) {
//         // BBOX
//         var bbox = getBbox(feature);

//         // Format, insert
//         tree.insert({id: feature.id, w: bbox[0], s: bbox[1], e: bbox[2], n: bbox[3]});
//     })
//     .on('end', function() {
//         writeSpatialIndex(tree);
//     });

// --------------------------------------------------------------------------------
// --------------------------------------------------------------------------------

// // After building the r-tree, use it to make lumped data files
// var index = require('./spatial-index.json');
// var count = 0;

// function writeLump(lumpKey, keys) {
//     async.map(keys, function(key, callback) {
//         s3.getObject({
//             Bucket: bucket,
//             Key: key
//         }, function(err, data) {
//             if (err) return callback(err);
//             callback(null, JSON.parse(data.Body));
//         });
//     }, function(err, result) {
//         var fc = {
//             type: "FeatureCollection",
//             features: result
//         };
//         s3.putObject({
//             Bucket: bucket,
//             ACL: 'public-read',
//             Key: lumpKey,
//             Body: JSON.stringify(fc)
//         }, function(err, data) {
//             if (err) throw err;

//         });
//     });
// }

// function adjust(node) {
//     if (node.leaf) {
//         var ids = node.children.map(function(c) { return c.id; });
//         var lumpKey = 'data/lump.' + count + '.geojson';
//         // writeLump(lumpKey, ids);
//         count++;

//         node.children = [ {
//             id: lumpKey,
//             w: node.bbox[0],
//             s: node.bbox[1],
//             e: node.bbox[2],
//             n: node.bbox[3]
//         } ];
//     }
//     if (node.children) node.children.forEach(adjust);
// }

// adjust(index);
// fs.writeFile('./lumped-spatial-index.json', JSON.stringify(index, null, 4));

// --------------------------------------------------------------------------------
// --------------------------------------------------------------------------------

function getBbox(feature) {
    // Flatten any geometry into an array of coordinates
    var coords = (function(geom) {
        var type = geom.type;
        var coords = geom.coordinates;
        if (type === 'Point') return [coords];
        if (type === 'MultiPoint' || type === 'LineString') return coords;
        if (type === 'MultiLineString' || 'Polygon') return _(coords).flatten(true);
        if (type === 'MultiPolygon') return _(coords).chain().flatten(true).flatten(true).value();
    })(feature.geometry);

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

function writeSpatialIndex(tree) {
    var index = JSON.stringify(tree.toJSON());
    fs.writeFile('./spatial-index.json', index);
    // s3.putObject({
    //     Bucket: bucket,
    //     ACL: 'public-read',
    //     Key: 'data/spatial-index.json',
    //     Body: index
    // }, function(err, data) {
    //     if (err) console.log(err);
    // });
}