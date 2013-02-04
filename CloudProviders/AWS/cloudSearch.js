var logger = require('log4js').getLogger('cloudSearch');
logger.setLevel('INFO');

var http = require('http');

exports.index = function(domain, document, onSuccess, onError) {
    
    var options = {
        'host' : domain + '.cloudsearch.amazonaws.com',
        'method' : 'POST',
        'path' : '/2011-02-01/documents/batch',
        'headers' : {
            'Accept' : 'application/json',
            'Content-Type' : 'application/json'
        }
    };
    
    var payload = JSON.stringify(document);
    
    logger.warn('Potential incorrect content-length calculation');
    options.headers['Content-Length'] = payload.length;
    
    function callback(response) {
        
        var data = ''
        response.on('data', function(chunk) {
            data+=chunk;
        });
        
        response.on('end', function() {
            
            logger.debug(data);
            if(response.statusCode == 200) {
                onSuccess();
            }
            else  {
                if(response.statusCode == 403) {
                    logger.error('AWS cloudSearch denied access. Please check if you server IP address is on the whitelist');
                }
                else {
                    logger.warn('Attempt failed to index document in cloudSearch: %s\n%j', response.statusCode, response.headers);
                }
                onError(response.statusCode);
            }
        });        
    }
    
    var request = http.request(options, callback);
    
    request.write(payload);
    request.end();
}

exports.search = function(domain, query, onSuccess, onError) {
    
    var options = {
        'host' : domain + '.cloudsearch.amazonaws.com',
        'method' : 'GET',
        'path' : '/2011-02-01/search?q=' + encodeURIComponent(query) + '&return-fields=caption,asset_object',
        'headers' : {
            'Accept' : 'application/json',
            'Content-Type' : 'application/json'
        }
    };
    
    function callback(response) {
        
        var data = ''
        response.on('data', function(chunk) {
            data+=chunk;
        });
        
        response.on('end', function() {
            
            logger.debug(data);
            if(response.statusCode == 200) {
                onSuccess(JSON.parse(data));
            }
            else  {
                if(response.statusCode == 403) {
                    logger.error('AWS cloudSearch denied access. Please check if you server IP address is on the whitelist');
                }
                else {
                    logger.warn('Attempt failed to index document in cloudSearch: %s\n%j', response.statusCode, response.headers);
                }
                onError(response.statusCode);
            }
        });        
    }
    
    var request = http.request(options, callback);
    
    request.end();
    
}