console.log('Loading module %s', module.filename);

var events = require('events');
var toxml = require('xml');
var search = require('./../CloudProviders/AWS/cloudSearch.js');
var http = require('http');
var urlHelper = require('url');

var logger = require('log4js').getLogger('AssetIndexWorker');
logger.setLevel('INFO');

function doIndexing(request, onComplete) {
    
    var assetInfo = request.payload;
    logger.debug('%s) Preparing to index: %j', request.header.cooridinationId, assetInfo);
    
    function onSuccessCallback() {
        
        logger.info('%s) Indexing is successful', request.header.cooridinationId);
        
        onComplete({ 'header' : {
                            'status' : 'success' ,
                            'cooridinationId' : request.header.cooridinationId
                            },
                            'payload' : {
                            }
                        });
    }
    
    function onErrorCallback(errorCode) {
        
        logger.error('%s) Indexing is NOT successful', request.header.cooridinationId);
        
        onComplete({ 'header' : {
                            'status' : 'error' ,
                            'statusList' : [
                                            {'status' : {'type' : '', 'code' : '', 'message' : errorCode}}
                                            ],
                            'cooridinationId' : request.header.cooridinationId
                            },
                            'payload' : {    
                            }
                        });
    }
    
    var indexingEvents = new events.EventEmitter;
    
    var parentKeywords = new Array();
    var keywords = new Array();
    assetInfo.keywords.forEach(function(kw) {keywords.push(kw);});
    
    indexingEvents.on('keyword denormalized', function() {
        
        logger.debug('Calculating parent keywords ==> %j', keywords);
        
        if(keywords.length == 0) {
            indexingEvents.emit('All keywords denormalized');
        }
        else {
            var keyword = keywords.shift();
            if(keyword) {
                logger.debug('Calculating parent keywords: %s', keyword);
                
                var request = http.request({ 'hostname' : 'localhost', 'port' : 8889, 'method': 'GET', 'path' : '/' + keyword}, function(response) {
                
                    if(response.statusCode == 200) {
                    
                        logger.debug('Successfully calculated parent keywords for: %s', keyword);
                    
                        var data = ''
                        response.on('data', function(chunk) { data += chunk;});
                        response.on('end', function() {
                            var pw = JSON.parse(data);
                            pw.forEach(function(w) {parentKeywords.push(w);});
                            indexingEvents.emit('keyword denormalized');
                        });
                    
                    }
                    else {
                        logger.error('Error calculating parent keywords for: %s', keyword);
                        indexingEvents.emit('All keywords denormalized');
                    }
                });
                request.end();
            }
        }
    });
    
    indexingEvents.on('All keywords denormalized', function() {
        
        assetInfo.keywords = assetInfo.keywords.concat(parentKeywords);
        
        var document =  [{
            'type' : 'add',
            'id' : assetInfo.masterId.toString(),
            'version' : assetInfo.version,
            'lang' : 'en',
            'fields' : {
                'caption' : assetInfo.caption,
                'keywords' : assetInfo.keywords,
                'original_file_name' : assetInfo.originalFilenName,
                'submission_id' : assetInfo.submissionId,
                'cutdowns' : JSON.stringify(assetInfo.cutdowns),
                'asset_object': JSON.stringify(assetInfo)
            }
        }];
    
        logger.debug('Indexing document: %j', assetInfo);
        search.index('doc-videopoc-usg77ebs72kytzogy4pjky62oi.us-east-1', document, onSuccessCallback, onErrorCallback);
    });
    
    indexingEvents.emit('keyword denormalized');
}

require('./../Integration/SWF-Transport.js').bind('SearchIndexAsset', function(activtyEvents) {
    logger.info('Listening for IndexAsset activity on SearchIndexAsset queue');
    activtyEvents.on('IndexAsset', doIndexing);
});

