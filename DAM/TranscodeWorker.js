console.log('Loading module %s', module.filename);

var events = require('events');
var toxml = require('xml');
var encodingDotCom = require('./../CloudProviders/EncodingDotCom/EncodingDotCom.js');
var cloudStorage = require('./Storage.js');
var logger = require('log4js').getLogger('TranscodeWorker');
logger.setLevel('INFO');

function doTranscode(request, onComplete) {
    
    var transcodingInfo = request.payload;
    logger.info('%s) Preparing to transcode: %j', request.header.cooridinationId, transcodingInfo);
    
    function onSuccessCallback() {
        
        logger.info('%s) Transcoding is successful', request.header.cooridinationId);
        
        onComplete({ 'header' : {
                            'status' : 'success' ,
                            'cooridinationId' : request.header.cooridinationId
                            },
                            'payload' : {
                            }
                        });
    }
    
    function onErrorCallback() {
        
        onComplete({ 'header' : {
                            'status' : 'error' ,
                            'statusList' : [
                                            {'status' : {'type' : '', 'code' : '', 'message' : ''}}
                                            ],
                            'cooridinationId' : request.header.cooridinationId
                            },
                            'payload' : {    
                            }
                        });
    }
    
    function s3Url(url) {
        return 'http://' + url.bucket + '.s3.amazonaws.com' + '/' + url.filePath;
    }
    
    var encodingDotComRequest = {
                        'header' : {'cooridinationId' : request.header.cooridinationId},
                        'payload' : {'masterUrl' : s3Url(transcodingInfo.masterUrl), 'thumbUrl' : s3Url(transcodingInfo.thumbUrl), 'flashUrl' : s3Url(transcodingInfo.flashUrl)}
                    };
    
    encodingDotCom.transcode(onSuccessCallback, onErrorCallback, encodingDotComRequest);
}

require('./../Integration/SWF-Transport.js').bind('transcode', function(activtyEvents) {
    activtyEvents.on('Transcode', doTranscode);
});
