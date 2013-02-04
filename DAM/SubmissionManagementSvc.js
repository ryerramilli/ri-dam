// This is  HTTP Service
var http = require('http');
var urlHelper = require('url');
var transport = require('./../Integration/Transport.js');
var swf = require('./../CloudProviders/AWS/swf.js');
var util = require('util');
var log4js = require('log4js');
var submisisonDb = require('mysql');

var logger = log4js.getLogger('SubmissionManagementSvc');
logger.setLevel('INFO');

var routeHandlers = {
        'GET' : { '/' :  hello},
        'POST' : { '/' :  hello}, 'PUT' : { '/submission' : receiveSubmission }
    };

function requestHandler(inStream, outStream) {
    
    var data = '';
    inStream.on('data', function(chunk) {
        data += chunk;
    });
    
    inStream.on('end', function() {
       
       var obj = JSON.parse(data);
       
       var method = inStream.method.toUpperCase();
       var resource = urlHelper.parse(inStream.url.toLowerCase()).pathname;
       
       function onResult(result) {
                outStream.writeHead(result.statusCode);
                if( result.payload) outStream.write( result.payload.getString(), 'utf-8');
                        outStream.end(); 
        }
       
       if(routeHandlers[method] && routeHandlers[method][resource])
            routeHandlers[method][resource](onResult, obj);
       else
            onResult({ 'statusCode': 404});
        
    });
}

function hello() {
    return { 'statusCode': 200, payload: new transport.serializers.json({'message' : 'hi'})};
}

function receiveSubmission(callback, submission) {
    
    logger.info('Received submission: %j', submission);
    
    var connection = submisisonDb.createConnection( {'host' : 'videopoc.ceoxxwsgttsc.us-east-1.rds.amazonaws.com', 'user' : 'yerramilli', 'password' : 'yerramilli', 'database' : 'SubmissionDB'} );
    
    var row = {'Name' : submission.name, 'ContentProvider' : submission.kioskId, 'SubmitDateTime' : new Date()};
    
    function closeConnection() {
        connection.end(function(err) {
                if(err)
                    logger.warn(_self.context.logFormatter('** Error closing connection **\n%j'), err);
        });
    }
    var query = connection.query('Insert Into Submission Set ?', row, function(err, result) {
        
        logger.warn(err);
        submissionResponse = {'statusCode' : 500, payload: {'masterId' : -1}};
        
        var submissionId = result.insertId;
        
        if(submissionId) {
                
                submission.submissionId = submissionId;
        
                submission.files.forEach(function(file) {
                        var metadata = submission.metadata[file.fileName];
                        for(var p in metadata) {
                                file[p] = metadata[p];
                        }
                });
        
                var message = {};
                message.payload = {'domain' : 'Yerramilli-ESP', 'workflowType' : { 'name' : 'SSBI', 'version' : '3.0'}, 'workflowId' : util.format('%s-%s', submissionId, submission.kioskId), 'input' : JSON.stringify(submission)};
                message.command = 'SimpleWorkflowService.StartWorkflowExecution';
    
                function handleSwfResponse(data)
                {
                    logger.debug('SWF response: %j', data);
                    callback({ 'statusCode': 200, payload: new transport.serializers.json({'submissioinId' : submissionId})});
                    closeConnection();
                }

                logger.info('Starting a new workflow: %j', message);
                swf.send(message, handleSwfResponse);
        }
        else {
                logger.error('Could not acquire a submission id');
                callback({'statusCode' : 500, payload: {'submissioinId' : -1}});
                closeConnection();
        }
    
    });
    logger.debug(query.sql);
    
}

function makeWorkflowName(s) {
        var wfName = s.replace(/[/:|/s]|arn/g, "$") 
        logger.debug("Workflow name is:%s, given that submision name is: %s", wfName, s);
        return wfName;
}

var server = http.createServer(requestHandler);

var port = 9999;
server.listen(port);
logger.info('Listening on port %d for submisison', port);