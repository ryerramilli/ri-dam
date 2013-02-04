var crypto = require('crypto');
var fs = require('fs');
var awsS3 = require('./../CloudProviders/AWS/s3.js');
var log4js = require('log4js');
var logger = log4js.getLogger('Storage');
require('module');


var provider = awsS3.s3;

exports.getStorageUrl = function (bucketName, fileName, mimeType, contentMD5) {
    
    var url = new provider({ 'action': 'PUT', 'bucket': bucketName, 'object' : {'name' : fileName, 'mimeType' : mimeType, 'md5' : contentMD5}}, true).getUrl();
    logger.info('Store url command: %j', url);
        
    return url;
}

exports.getViewUrl = function (bucketName, fileName) {
    
    var url = new provider({ 'action': 'GET', 'bucket': bucketName, 'object' : {'name' : fileName}}, true).getUrl();
    logger.info('View url command: %j', url);
        
    return url;
}

/*
function client() {

    var _fileName = '3.txt';
    var _mimeType = 'text/plain';
    var _bucketName = 'ramencodingdotcom';

    var buffer = fs.readFileSync(_fileName);
    var hasher = crypto.createHash('md5');
    hasher.update(buffer);
    var contentMD5 = hasher.digest('base64');
    
    var fileInfo = { 'fileName' : '3.txt', 'mimeType' : 'text/plain', 'md5Hash' : contentMD5};
    
    return fileInfo;
}

var fileInfo = client(); 
new cloudStorage(awsS3.s3).storeUrl(_bucketName, fileInfo.fileName, fileInfo.mimeType, fileInfo.md5Hash);
*/