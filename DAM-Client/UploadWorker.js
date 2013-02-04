// Upload file to the given location provided by the Asset-Central
var log4js = require('log4js');
var events = require('events');
var https = require('http');
var fs = require('fs');

var logger = log4js.getLogger('UploadWorker');
logger.setLevel('INFO');

var thisKioskId = 'kiosk#1';

function doUpload(uploadRequest, onComplete) {
    
    var uploadInfo = uploadRequest.payload;
    logger.info('%s) Recevied upload instruction %j ....', uploadRequest.header.cooridinationId, uploadInfo);
    
    function callback(response) {
                
        logger.info('%s) Upload activity completed with result: %s', uploadRequest.header.cooridinationId, response.statusCode);
        
        var status = response.statusCode == 200?'success':'error';
        onComplete({ 'header' : {
                            'status' : status ,
                            'cooridinationId' : uploadRequest.header.cooridinationId
                            },
                            'payload' : {
                            }
                        });
    }
    
    var filePath = '../_submissions_' + uploadInfo.name + '/' + uploadInfo.file.fileName;
    var fileBytes = fs.readFileSync(filePath);
    logger.debug('%s) File size: %s', uploadRequest.header.cooridinationId, fileBytes.length);
    var options = uploadInfo.destinationUrl;
    options.headers['content-length'] = fileBytes.length;
    options.headers['content-MD5'] = uploadInfo.file.md5Hash;
    options.headers['content-type'] = uploadInfo.file.mimeType;
    
    logger.debug('Uploading file: %s', filePath);
    console.log(options);
    
    var request = https.request(options, callback);
    
    request.end(fileBytes);
    logger.debug('%s) Request Ended', uploadRequest.header.cooridinationId);
    
}

require('./../Integration/SWF-Transport.js').bind('kiosk#1', function(activtyEvents) {
    activtyEvents.on('Upload', doUpload);
});