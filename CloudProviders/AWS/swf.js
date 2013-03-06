console.log('Loading module %s', module.filename);

var https = require('http');
var crypto = require('crypto');
var shell = require('module');
var credentials = require('./__credentials');
var logger = require('log4js').getLogger('SWF');
if(!logger)
    console.log('Logger could not be bootstrapped');
else
    logger.setLevel('WARN');

var access_id = credentials.access.id;
var access_key = credentials.access.key;
var awsHost = credentials.access.host;

Object['keys'] = function(obj)
{
    var array = new Array();
    
    for(var p in obj)
        array.push(p);
        
    return array;
}

function getSignature(method, headers, payload) {
    
    var newLine = '\u000A';

    var headersInCanonicalForm = 'host:' + awsHost + newLine;
    for(var p in headers){
        headersInCanonicalForm += p +":" + headers[p] + newLine;
    }
    logger.debug(headersInCanonicalForm);

    var message = method + newLine;
    message += "/" + newLine;
    message += newLine;
    message += headersInCanonicalForm + newLine;
    message += payload;

    var hash = crypto.createHash('SHA256');
    var messageHash = hash.update(message).digest();
    
    var hmac = crypto.createHmac('SHA256', access_key);
    var signature = hmac.update(messageHash).digest("base64");
    
    return signature;
}

exports.send = function(message, callback, onErrorCallback) {
    
    var httpMethod = 'POST';
    var encoding = 'utf-8';
    
    var headers = {};
    headers['x-amz-date'] = new Date().toUTCString();
    headers['x-amz-target'] = message.command;
    
    var payload = JSON.stringify(message.payload);

    var signature = getSignature(httpMethod, headers, payload);

    var authString = 'AWS3 AWSAccessKeyId=' + access_id + ',Algorithm=HmacSHA256,SignedHeaders=' + Object.keys(headers).join(';') + ',Signature=' + signature;

    headers['x-amzn-authorization'] = authString;
    headers['content-encoding'] = encoding;
    headers['content-length'] = payload.length;
    headers['content-type'] = 'application/x-amz-json-1.0';
    var options = {
        'host' : awsHost,
        'method' : httpMethod,
        'headers' : headers
    };

    var cb = function(response)
    {
        logger.debug('Got response from aws');
        response.setEncoding(encoding);
        if(response.statuCode != 200)
        logger.info('Status Code: %s\nHeaders: %j', response.statusCode, response.headers);
        
        var data = '';
        response.on('data',
                    function(chunk) {
                        logger.debug('On data event has fired');
                        data += chunk;
                        });
        response.on('end', function() {
            if(data == '') data = '{}';
                if(response.statusCode != 200 && onErrorCallback)
                    onErrorCallback(data);
                else
                    callback(data)
            }
        );
    };
    
    logger.debug(options);
    var request = https.request(options, cb);
    logger.debug(payload);
    request.end(payload, encoding);
}