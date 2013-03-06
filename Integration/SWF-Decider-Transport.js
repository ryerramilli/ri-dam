var swf = require('./../CloudProviders/AWS/swf.js');
var swfConfig = require('./../Config/swf-config').config;
var util = require('util');
var events = require('events');
var log4js = require('log4js');
var logger = log4js.getLogger('SWF-Decider-Transport');
logger.setLevel('INFO');

exports.bind = function(desiredTaskList, decisionHandler) {
    
    taskList = desiredTaskList;
    activtyEvents = new events.EventEmitter;
    taskQueue = new events.EventEmitter;
    taskQueue.on('workflowTaskAvailable', routeTask);
    taskQueue.on('ResponseFor.SimpleWorkflowService.PollForActivityTask', doActivity);
    taskQueue.on('ResponseFor.SimpleWorkflowService.RespondActivityTaskCompleted', doDefault);
    
    onBindCallback(activtyEvents);
    
    doDefault();

}

var decisionQueue = new events.EventEmitter;
function getDecisionTask() {
    
    logger.debug('===================================================================');
    logger.debug('>>>>>>>>>>>>>>>> Polling for a decision task <<<<<<<<<<<<<<<<<<<<<<');
    
    var message = {
            'command' :'SimpleWorkflowService.PollForDecisionTask',
            'payload' : {'domain' : swfConfig.domain.name, 'taskList' : { 'name' : 'SSBITask'} }
        };

    swf.send(message, function(data) {decisionQueue.emit('decisionTaskAvailable', data);});
}

decisionQueue.on('decisionTaskAvailable', handleDecisionTask);
function handleDecisionTask(data)
{
    logger.debug('-----------------');
    logger.debug(data);
    logger.debug('-----------------');
    
    var decisionTask = JSON.parse(data);
    
    takeDecision(decisionTask);
}

function takeDecision(decisionTask)
{
    var workflowEvents = new events.EventEmitter;
    var eventLookup = new Object();
    var wf = new workflowWatcher(workflowEvents, function(id) {
                    return eventLookup[id];
                    }, decisionTask.workflowExecution);
    
    if(decisionTask.events) {
        
        decisionTask.events.forEach(function(event) {
            eventLookup[event.eventId] = event;
            workflowEvents.emit(event.eventType, event);    
        });
    
        var decisions = wf.getDecisions();
        
        logger.debug('Decisions: ===== %s', JSON.stringify(decisions));
    
        var message = {
            'command' :'SimpleWorkflowService.RespondDecisionTaskCompleted',
            'payload' : {}
        };    
        
        message.payload['taskToken'] = decisionTask.taskToken;
        message.payload['decisions'] = decisions;
    
        var x = function(data) {
            logger.debug('*******************');
            logger.debug(data);
            logger.debug('*******************');
            getDecisionTask();
        };
    
        swf.send(message, x);
    }
    else {
        getDecisionTask();
    }
}

function makeAwsName(s) {
        var awsName = s.replace(/[/:|/s]|arn/g, "$") 
        logger.debug("Aws name is:%s, given that input string is: %s", awsName, s);
        return awsName;
}

getDecisionTask();