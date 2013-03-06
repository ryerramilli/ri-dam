var crypto = require('crypto');
var util = require('util');
var qs = require('querystring');
var log4js = require('log4js');
var credentials = require('./__credentials.js');
var logger = log4js.getLogger('S3');
logger.setLevel('WARN');

exports.s3 = function(message, sharable) {
    
    _self = this;
    
    _self._message = message;
    if(sharable) _self._sharable = sharable;
    _self._timestamp = new Date();
    _self._timestamp['plus'] = function(minutes) {
        return new Date(this.getTime() + minutes * 60 * 1000);
    }
    
    _self._getAmznHeaders = function() {
        
        var headers = {};
        if(!this._sharable) {
            headers['x-amz-date'] = this._timestamp.toUTCString();
        }
        return headers;
    }
    
    _self._getCanonicalizedResource = function() {
        var uriPath = '';
        var subResources = '';
        return '/' + this._getSignableBucketName() + (this._message.object ? this._message.object.name : '') + subResources;
    }
    
    _self._getCanonicalizedHeaders = function() {
        var canonicalizedAmzHeader = '';
        var headers = this._getAmznHeaders(); 
        for(var h in headers) {
            canonicalizedAmzHeader += util.format('%s:%s', h.toLowerCase(), headers[h]);
        }
        logger.debug(canonicalizedAmzHeader);
        return canonicalizedAmzHeader;
    }
    
    _self._getExpiration = function() {
        return Math.ceil(this._timestamp.plus(10).getTime()/1000);
        //return 1356129620;
    }
    
    _self._getSignableBucketName = function() {
        return this._isBucketSpecified()?(this._message.bucket + '/'):'';
    }
    
    _self._getBucketHost = function() {
        var host = 's3.amazonaws.com';
        if(this._isBucketSpecified())
            host = util.format('%s.%s', this._message.bucket, host);
        return host;
    }
    
    _self._isBucketSpecified = function() {
        return this._message.bucket && this._message.bucket != '';
    }
    
    _self._getSignature = function() {
        
        function sign(key, s2s) {
            var s2sBytes = new Buffer(s2s, 'utf8');
            var keyBytes = new Buffer(key, 'utf8');
            var hmac = crypto.createHmac('SHA1', keyBytes);
            var signature = hmac.update(s2sBytes).digest("base64");
            return signature;
        }
        
        var stringToSign = '';
        var contentMD5 = '';
        
        function a(s) {
            stringToSign += s + '\n';
        }
        function o(k) {
            a( (_self._message.object && _self._message.object[k])?_self._message.object[k]:'');
        }
        a(this._message.action);
        o('md5');
        a((this._message.object && this._message.object.mimeType)?this._message.object.mimeType:'');
        a(this._sharable?this._getExpiration():'');
        if(!this._sharable)a(this._getCanonicalizedHeaders());
        stringToSign += this._getCanonicalizedResource();
        
        logger.info(stringToSign);
    
        return sign(credentials.access.key, stringToSign);
    }
    
    _self.getUrl = function() {
        
        var headers = this._getAmznHeaders();
        var path = '/';
        if(this._message.object && this._message.object.name)
            path += this._message.object.name;
        if(this._sharable) {
            '';
            path += "?" + qs.stringify({'AWSAccessKeyId' : credentials.access.id, 'Signature' : this._getSignature(), 'Expires' : this._getExpiration()});
        }
        else {
            headers['Authorization'] = util.format('AWS %s:%s',credentials.access.id, this._getSignature());
        }
        
        return options = {
            'host' : this._getBucketHost(),
            'path' : path,
            'method' : this._message.action,
            'headers' : headers
        };
    }
}

function test() {
    var test_key = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'

    var string2Sign = 'GET\n\n\nTue, 27 Mar 2007 19:36:42 +0000\n/johnsmith/photos/puppy.jpg';

    function sign(key, s2s) {
        var s2sBytes = new Buffer(s2s, 'utf8');
        var hmac = crypto.createHmac('SHA1', key);
        var signature = hmac.update(s2sBytes).digest("base64");
        return signature;
    }

    var expectedSigngature = 'bWq2s1WEIj+Ydj0vQ697zp+IXMU=';
    logger.debug(sign(test_key, string2Sign));
    logger.debug(expectedSigngature);
}