var http = require('http');
var urlHelper = require('url');
var search = require('./../CloudProviders/AWS/cloudSearch.js');
var util = require('util');
var cloudStorage = require('./Storage.js');

var logger = require('log4js').getLogger('AssetEditorWebApp');
logger.setLevel('INFO');

var server = http.createServer(requestHandler);

var routeHandlers = {
        'GET' : { '/' :  showForm},
        'POST' : { '/' :  doSearch}
    };

function requestHandler(inStream, outStream) {
    
    var data = '';
    inStream.on('data', function(chunk) {
        data += chunk;
    });
    
    inStream.on('end', function() {
       
       var method = inStream.method.toUpperCase();
       var resource = urlHelper.parse(inStream.url.toLowerCase()).pathname;
       
       function onResult(result) {
                outStream.writeHead(result.statusCode, {'Content-length' : result.payload.length, 'Content-Type' : 'text/html'});
                if( result.payload) outStream.write( result.payload, 'utf-8');
                        outStream.end(); 
        }
       
       if(routeHandlers[method] && routeHandlers[method][resource])
            routeHandlers[method][resource](onResult, data);
       else
            onResult({ 'statusCode': 404, 'payload' : '<html><head></head><body>Page not found!!!</body></html>'});
        
    });
}

function page(content) {
    return "<html><head></head><body><form method='post'> <input type='test' name='term'></input> <input type='submit' name='search'></input></form>" + content + " </body></html>";
}

function showForm(callback) {
    callback({ 'statusCode' : 200 , 'payload' : page('')});
}

function doSearch(callback, data) {
    
    var query = urlHelper.parse("http://null/null?" + data, true).query;
    
    function onSuccess(results) {
        
        console.log(results);
    
        var images = '<ul>';    
        results.hits.hit.forEach(function(h) {
           /*images += ('<li>'
                + '<dl>'
                + '<dt>Id:</dt>'
                + '<dd>' + h.id + '</dd'>
                + '<dt>Catpion:</dt>'
                + '<dd>' + h.data.caption + '</dd'>
                + '</dl>'
                + '</li>');*/
           var asset = JSON.parse(h.data.asset_object);
           var keywords = asset.keywords.join();
           
           var thumbDef = cloudStorage.getViewUrl(asset.cutdowns['application/jpeg'].url.bucket, asset.cutdowns['application/jpeg'].url.filePath);
           var thumbUrl = 'http://' + asset.cutdowns['application/jpeg'].url.bucket + '.s3.amazonaws.com' + thumbDef.path;
           
           var quicktimeDef = cloudStorage.getViewUrl(asset.cutdowns['video/quicktime'].url.bucket, asset.cutdowns['video/quicktime'].url.filePath);
           var quicktimeUrl = 'http://' + asset.cutdowns['video/quicktime'].url.bucket + '.s3.amazonaws.com' + quicktimeDef.path;
           
           var flashDef = cloudStorage.getViewUrl(asset.cutdowns['video/x-flv'].url.bucket, asset.cutdowns['video/x-flv'].url.filePath);
           var flashUrl = 'http://' + asset.cutdowns['video/x-flv'].url.bucket + '.s3.amazonaws.com' + flashDef.path;
           
           images += util.format('<li><img src="%s"></img> <dl> <dt>Id:</dt><dd>%s</dd> <dt>Submit datetime:</dt><dd>%s</dd>  <dt>Caption:</dt><dd>%s</dd> <dt>Keywords:</dt><dd>%s</dd>  '
                                 + '<dt>Original filename::</dt><dd>%s</dd> '
                                 + '<dt>Downloadable files:</dt>'
                                 + '<dd> <ul> <li><a href="%s">Quicktime</a></li> <li><a href="%s">Flash</a></li> </ul></dd>'
                                 +'</dl></li>'
                                 , thumbUrl, h.id, asset.submitDateTime, h.data.caption, keywords, asset.originalFileName, quicktimeUrl, flashUrl);
        });
        images += '</ul>';
        console.log(images);
    
        callback({ 'statusCode' : 200 , 'payload' : page(images)});    
    }
    
    function onError() {
        callback({ 'statusCode' : 500 , 'payload' : page('Internal error')}); 
    }
    
    search.search('search-videopoc-usg77ebs72kytzogy4pjky62oi.us-east-1', query.term, onSuccess, onError);
}

var server = http.createServer(requestHandler);
var port = 8888;
server.listen(port);
logger.info('Listening on port %d for submisison', port);