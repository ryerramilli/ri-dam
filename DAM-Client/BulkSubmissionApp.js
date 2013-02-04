// Monitor submission folder for batches
var log4js = require('log4js');
var logger = log4js.getLogger('BulkSubmissionApp');
var fs = require('fs');
var crypto = require('crypto');
var transport = require('./../Integration/Transport.js');
var fs = require('fs');

var thisKioskId = 'kiosk#1';

function submit(submissionFolder) {
    
    var submissionName = submissionFolder == rootFolder? 'submission' : submissionFolder.substr(rootFolder.length);
    
    fs.readdir(submissionFolder, function(err, files) {
        
        var fileInfos = new Array();
        var metadata = new Object();
        files.forEach(function(file) {
            
            var fullPath = submissionFolder + '/' + file;
            
            var stats = fs.statSync(fullPath);
            
            if(stats.isDirectory()) {
                submit(fullPath);
            }
            else if(file.match('mov$')) {
                fileInfos.push({ 'fileName' : file, 'mimeType' : 'video/quicktime', 'md5Hash' : calculatMD5(fullPath)});
            }
            else if(file.match('json$')) {
                var metadataString = fs.readFileSync(fullPath, 'utf-8');
                metadata = JSON.parse(metadataString);
            }
        });
        
        if(fileInfos.length > 0) {
            var payload = { 'kioskId' : thisKioskId, 'name' : submissionName, 'files' : fileInfos, 'metadata': metadata};
            var message = { 'payload' : new transport.serializers.json(payload)};
            transport.send('/submission', 'PUT', message, submissionResult);
        }
    });
    
}

function submissionResult(result) {
    logger.info(result);
}

function calculatMD5(file) {
    var buffer = fs.readFileSync( file);
    var hasher = crypto.createHash('md5');
    hasher.update(buffer);
    return hasher.digest('base64');
}

var rootFolder = fs.realpathSync('../_submissions_');
submit(rootFolder);
