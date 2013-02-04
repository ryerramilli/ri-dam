var credentials = require('./__credentials.js');
var toxml = require('xml');
var fromxml = require('xml2js');
var http = require('http');
var qs = require('querystring');
var events = require('events');

var logger = require('log4js').getLogger('EncodingDotCom');
logger.setLevel('INFO');

var statusEvents = new events.EventEmitter;

var commandQueue = new Array();
runCommand();

exports.transcode = function(onSuccess, onError, request) {
    
    enqueue(new submitMedia(onSuccess, onError, request));
    
}

function submitMedia(onSuccess, onError, request) {
    
    var _self = this;
    this.request = request;
    this.callbacks = { 'finished' : onSuccess, 'error' : onError};
    this.statusCheckDelaySeq = [0, 10];
    
    this.getQuery = function() {
        
        var _action = 'AddMedia';
        _self.query =  {
                    'action' : _action,
                    'source' : request.payload.masterUrl,
                    'region' : 'us-east-1',
                    'formats' : [
                                    {
                                        'output' : 'thumbnail',
                                        'time' : '10%',
                                        'height' : '66',
                                        'destination' : request.payload.thumbUrl
                                    },
                                    {
                                        'output' : 'flv',
                                        'destination' : request.payload.flashUrl
                                    }
                                ]
                };
                
        logger.info('%s) Submit Media: %j', request.header.cooridinationId, _self.query)
                
        return _self.query;
    }
    
    this.handleResponse = function responseHandler(data) {
            
        logger.debug('%s) %s', request.header.cooridinationId, JSON.stringify(data));
            
        data.response.MediaID.forEach( function(mediaId) {
            
            logger.info('%s) Media Id: %s', request.header.cooridinationId, mediaId);
            
            statusEvents.on( mediaId, _self.statusChangeListener);
            
            commandQueue.push(new checkStatus(request.header.cooridinationId, mediaId));
                
        });
                
    };
    
    this.statusChangeListener = function(mediaId, formats) {
        
        var successCount = 0;
        var errorCount = 0;
        formats.forEach( function(format) {
            
            logger.debug('%s) Analyzing status change for %j', request.header.cooridinationId, format);
            
            var status = ''
            format.status.forEach(function(s) {
                status = s.toLowerCase();
            });
            
            if( status == 'finished')
                successCount++;
                
            if(status == 'error')
                errorCount++;
            
        });
               
        if((successCount + errorCount) == _self.query.formats.length) {
            
            logger.info('%s) All formats have completed - success - %s, error = %s', request.header.cooridinationId, successCount, errorCount);
    
            statusEvents.removeListener( mediaId, _self.statusChangeListener);
            
            var mode = errorCount > 0 ? 'error' : 'finished'
;            
            if(_self.callbacks[mode] && typeof _self.callbacks[mode] == 'function' ) {
                logger.debug('%s) Invoking %s handler of the callee', request.header.cooridinationId, mode);
                _self.callbacks[mode](); 
            }
            
        }
        else {
            
            function progressiveBackoff() {
                var delay = _self.statusCheckDelaySeq[0] + _self.statusCheckDelaySeq[1];
            
                logger.info('%s) Next status check will be done after %s seconds', request.header.cooridinationId, delay);
                setTimeout(function() {
                        enqueue(new checkStatus(request.header.cooridinationId, mediaId));
                    }, delay * 1000);
            
                _self.statusCheckDelaySeq.shift();
                _self.statusCheckDelaySeq.push(delay);
            };
            
            progressiveBackoff();
            
        }
    }
}

function checkStatus(cooridinationId, mediaId) {
    
    var _self = this;
    this.mediaId = mediaId;
    
    this.getQuery = function() {
        var _action = 'GetStatus';
        return { 'action' : _action, 'mediaid' : mediaId};
    }
    
    this.handleResponse = function(data) {
            
            logger.debug('%s) %s', cooridinationId, JSON.stringify(data));
            statusEvents.emit(data.response.id, data.response.id, data.response.format);
            
        }
}

function enqueue(command) {
    commandQueue.push(command);
}

function runCommand() {
    
    function poll(delay) {
       if(!delay)
           delay = 10;
        
       logger.debug('setting a 10 second delay for the next command');
       timeoutId = setTimeout(runCommand, delay * 1000);
    }
    
    logger.debug('##');
    
    var command = commandQueue.shift();
    
    if(command)
        if(command.getQuery && command.handleResponse) {
            
            new function(cmd) {
                
                var _self = this;
                this.cmd = cmd;
            
                this.callback = function(statusCode, response) {
                
                    if(statusCode == 200) {
                       logger.debug('Invoking caller provided callback');
                        _self.cmd.handleResponse(response);
                    }
            
                    if (statusCode == 421){
                    
                        logger.warn('Encoding.com throttled us. Setting a 3 minute delay for the next command');
                        clearTimeout(timeoutId);
                        poll(3 * 60);
                        commandQueue.unshift(_self.cmd);
                    }
                    else {
                        poll();
                    }
                }
                
                logger.debug('Contacting encoding.com....');
                contactEncodingDotCom(_self.callback, _self.cmd.getQuery());
                
            }(command);
        }
        else
            logger.error('%j is not a valid command', command);
    else
        poll();
}

function contactEncodingDotCom(callback, q) {
    
    var query = {'query' : [
                        {'userid' : credentials.access.id},
                        {'userkey' : credentials.access.key},
                        {'action' : q.action},
                    ]};
    
    function add(p) {
        
        if(q[p]) {
            var element = new Object();
            element[p] = q[p];
            query.query.push(element);
        }
    }
    add('mediaid');
    add('source');
    add('region');
    if(q.formats) {
        
        logger.debug('Appending format defintions to the query');
        q.formats.forEach(function(format) {
            
            var f = new Array();
            for(p in format) {
                var o = new Object();
                o[p] = format[p];
                f.push(o);
            }
            query.query.push({'format': f});

        });
    }
        
    var queryXml = toxml(query, true);
    queryXml = '<?xml version="1.0" encoding="UTF-8"?>\n' + queryXml + "\n";
    logger.debug('Query xml: %s', queryXml);
    
    var requestParams = qs.stringify({'xml' : queryXml});

    var options = {
      'host' : 'manage.encoding.com',
      'method' : 'POST',
      'headers' : {
        'content-type' : 'application/x-www-form-urlencoded',
        'content-length' : requestParams.length
      }
    };
    
    function handleResponse(response) {
        
        logger.debug("Response status: %s", response.statusCode);
        logger.debug("Response headers: %j", response.headers);
        
        var xml = ''
        response.on('data', function (d) {
            xml += d;
        });
        
        response.on('end', function () {
                
            var parser = new fromxml.Parser();
            parser.on('end', function(obj) {
                logger.debug('Invoking caller provided callback');
                callback(response.statusCode, obj);
            });
    
            parser.parseString(xml);
            
        });
    }
    
    var request = http.request(options, handleResponse);
    request.end(requestParams);
    
}


/*
function runTest() {
    
    var masterUrl = 'https://ramencodingdotcom.s3.amazonaws.com/2.mov';
    var thumbUrl = "https://ramencodingdotcom.s3.amazonaws.com/thumb_2.jpg";
    var flashUrl = "https://ramencodingdotcom.s3.amazonaws.com/flash_2.flv";
    
    function onSuccess() {
        console.log('==========================');
        console.log('All is well');
    }
    
    function onError() {
        console.log('==========================');
        console.log('Houston we have a problem');
    }
    
    enqueue(new submitMedia(onSuccess, onError, masterUrl, thumbUrl, flashUrl));
    
}

//runTest();

<?xml version="1.0"?>

<query>
    
    <userid>14076</userid>
    <userkey>f3d1c8a84f58a8aa95743613cbb040b5</userkey>
    <action>AddMedia</action>
    <source>https://ramencodingdotcom.s3.amazonaws.com/74060150_n.mov</source>
    
    <format>
        <output>thumbnail</output>
        <thumb_time>5%</thumb_time>
        <thumb_size>66px</thumb_size>      
        <destination>https://AKIAIE5AP2AQIK3UDECQ:ImdV%2BE%2Fbc7aPX65tfiH6kFGSTt2fagS5Clw%2BqNeu@ramencodingdotcom.s3.amazonaws.com/thumb_74060150_n.jpg</destination>
 
   </format>

</query>
*/