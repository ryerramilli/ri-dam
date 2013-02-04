console.log('Loading module %s', module.filename);
var http = require('http');
var events = require('events');
var urlHelper = require('url');
var transport = require('./Transport.js');

var logger = require('log4js').getLogger('HTTP-Transport');
logger.setLevel('INFO');

var routes = new Array();
var routeMap = new Object();

exports.bind = function(port, onBindCallback) {
    
    activtyEvents = new events.EventEmitter;
    
    onBindCallback(new function() {
        
        var self = this;
        this.on = function(path, listener) {
            routes.push(path);
            routeMap[path] = listener;
            activtyEvents.on(path, listener);
        }
    });
    
    var server = http.createServer(requestHandler);
    server.listen(port);
    logger.info('Listening on port %d', port);
}

function requestHandler(inStream, outStream) {
    
    var data = '';
    inStream.on('data', function(chunk) {
        data += chunk;
    });
    
    inStream.on('end', function() {
       
       var method = inStream.method.toUpperCase();
       var canonicalUrl = inStream.url.toLowerCase();
       
       for(var i=0; i < routes.length; i++) {
            if(canonicalUrl.indexOf(routes[i]) == 0) {
                logger.info('Found hit for route: %s', routes[i]);
                var resource = urlHelper.parse(inStream.url.toLowerCase()).pathname.substr(routes[i].length);
                routeMap[routes[i]](resource, onResult);
                break;
            }
            else {
                logger.debug('route: %s is not a match', routes[i]);
            }
       }
       
       function onResult(result) {
                var content = result.payload ? result.payload.getString():'{}';
                console.log(content);
                outStream.writeHead(result.statusCode,  {'Content-Type' : 'application/json', 'Content-length' : content.length});
                outStream.write( content, 'utf-8');
                        outStream.end(); 
        }
        
    });
}
