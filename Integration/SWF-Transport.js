console.log('Loading module %s', module.filename);
var events = require('events');
var swf = require('./../CloudProviders/AWS/swf.js');
var swfConfig = require('./../Config/swf-config').config;
var logger = require('log4js').getLogger('SWF-Transport');
logger.setLevel('DEBUG');

exports.bind = function(desiredTaskList, onBindCallback) {
    
    taskList = desiredTaskList;
    activtyEvents = new events.EventEmitter;
    taskQueue = new events.EventEmitter;
    taskQueue.on('workflowTaskAvailable', routeTask);
    taskQueue.on('ResponseFor.SimpleWorkflowService.PollForActivityTask', doActivity);
    taskQueue.on('ResponseFor.SimpleWorkflowService.RespondActivityTaskCompleted', doDefault);
    
    onBindCallback(activtyEvents);
    
    doDefault();

}

function contactWorkflowQueue(message) {
    
    logger.debug('>>>>>>>>>>>>>>>> Sending %s  <<<<<<<<<<<<<<<<<<<<<<\n%j', message.command, message);
    
    function onSuccess(reply) {
        taskQueue.emit('workflowTaskAvailable', message, reply);
    }
    
    function onError(reply) {
        
        logger.warn('Problem contacting swf: %j\n.... Continuing doing default', reply);
        doDefault();
    }

    swf.send(message, onSuccess, onError);
}

function routeTask(taskRequestMessage, workflowTaskAvailableMessageString) {
        
    logger.debug("Routing response for request message: %j", taskRequestMessage);
    if(taskRequestMessage && taskRequestMessage.command)
        taskQueue.emit("ResponseFor." + taskRequestMessage.command, workflowTaskAvailableMessageString);
    else {
        logger.warn('Unknown request message =======> %s', JSON.stringify(badRequest));
        doDefault();
    }
}

function doActivity(activityTaskString)
{
    var activityTask = JSON.parse(activityTaskString);
    if(activityTask.workflowExecution) {
        
        var request = JSON.parse(activityTask .input);
        
        activtyEvents.emit(activityTask.activityType.name, request, function (response) {
        
            var taskResultMessage = {'payload' : { 'taskToken' : activityTask.taskToken}};
            if(response.header.status == 'success') {
                taskResultMessage['command'] = 'SimpleWorkflowService.RespondActivityTaskCompleted';
                taskResultMessage.payload['result'] = JSON.stringify(response);
            }
            else {
                taskResultMessage['command'] = 'SimpleWorkflowService.RespondActivityTaskFailed';
                taskResultMessage.payload['reason'] = JSON.stringify(response);
            }
        
            contactWorkflowQueue(taskResultMessage);
        
        });
    }
    
    doDefault();
}

function doDefault() {
    
    var getActivityTaskMessage = {
            'command' :'SimpleWorkflowService.PollForActivityTask',
            'payload' : {'domain' : swfConfig.domain.name, 'taskList' : {'name' : taskList}}
        };

    contactWorkflowQueue(getActivityTaskMessage);
}
