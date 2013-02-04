var swf = require('./../CloudProviders/AWS/swf.js');
var util = require('util');
var events = require('events');
var log4js = require('log4js');
var logger = log4js.getLogger('BulkSubmissionDecider');
logger.setLevel('INFO');
var cloudStorage = require('./Storage.js');

var decisionQueue = new events.EventEmitter;
function getDecisionTask() {
    
    logger.debug('===================================================================');
    logger.debug('>>>>>>>>>>>>>>>> Polling for a decision task <<<<<<<<<<<<<<<<<<<<<<');
    
    var message = {
            'command' :'SimpleWorkflowService.PollForDecisionTask',
            'payload' : {'domain' : 'Yerramilli-ESP', 'taskList' : { 'name' : 'SSBITask'} }
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

function workflowWatcher(workflowEvents, lookupEvent, workflowExecutionContext) {
    
    var self = this;
    self._decisionPending = false;
    self._submitOneWorkflows = new Object();
    
    workflowEvents.on('WorkflowExecutionStarted', function(event) {
        logger.debug('WorkflowExecutionStarted raised');
        if(event.workflowExecutionStartedEventAttributes && event.workflowExecutionStartedEventAttributes.input) {
            self._submission = JSON.parse(event.workflowExecutionStartedEventAttributes.input);
            logger.debug(self._submission);
        }
    });
    
    workflowEvents.on('DecisionTaskStarted', function(event) {
        logger.debug('DecisionTaskStarted raised');
        self._decisionPending = true;
        logger.debug(self._decisionPending);
    });
    
    workflowEvents.on('DecisionTaskCompleted', function(event) {
        logger.debug('DecisionTaskCompleted raised');
        self._decisionPending = false;
        logger.debug(self._decisionPending);
    });
    
    function fileNameFromEvent(initiatedEvent) {     
        
        if(initiatedEvent.startChildWorkflowExecutionInitiatedEventAttributes && initiatedEvent.startChildWorkflowExecutionInitiatedEventAttributes.input) {
            var input = JSON.parse(initiatedEvent.startChildWorkflowExecutionInitiatedEventAttributes.input);
            return input.file.fileName;
        }
        else {
            logger.error('The initiating event does not have input specified: %j', initiatedEvent);
            return '';
        }
        
    }
    
    workflowEvents.on('StartChildWorkflowExecutionInitiated', function(event) {
        
        logger.debug('StartChildWorkflowExecutionInitiated raised');

        self._submitOneWorkflows[fileNameFromEvent(event)] = { 'isDone' : function() { return false;}};

    });
    
    var doneStates = { 'ChildWorkflowExecutionCompleted' : 'childWorkflowExecutionCompletedEventAttributes', 'ChildWorkflowExecutionFailed' : 'childWorkflowExecutionFailedEventAttributes',
     'ChildWorkflowExecutionTimedOut' : 'childWorkflowExecutionTimedOutEventAttributes', 'ChildWorkflowExecutionCanceled' : 'childWorkflowExecutionCanceledEventAttributes',
     'ChildWorkflowExecutionTerminated' : 'childWorkflowExecutionTerminatedEventAttributes', 'StartChildWorkflowExecutionFailed' : 'startChildWorkflowExecutionFailedEventAttributes'};
    
    for(var doneState in doneStates) {
        
        logger.debug('Attaching event for doneState: %s', doneState)
        
        workflowEvents.on(doneState, function(doneEvent) {
        
            logger.debug('%s) %s raised', workflowExecutionContext.workflowId, doneEvent.eventType);
        
            var initiatingEvent = lookupEvent(doneEvent[doneStates[doneEvent.eventType]].initiatedEventId);
            
            self._submitOneWorkflows[fileNameFromEvent(initiatingEvent)] = { 'isDone' : function() { return true;}};
        
        });
        
    };    
    
    this.getDecisions = function() {
        
        var decisions = new Array();
        if(self._decisionPending) {
            
            self._submission.files.forEach(function(fileInfo) {
                    
                if(!self._submitOneWorkflows[fileInfo.fileName]) {
                    
                    logger.info('%s) Initiating workflow to submit %s', workflowExecutionContext.workflowId, fileInfo.fileName);
            
                    decisions.push( {
                            'decisionType' : 'StartChildWorkflowExecution',
                            'startChildWorkflowExecutionDecisionAttributes' : {
                                'childPolicy' : 'TERMINATE',
                                'input' : JSON.stringify({"kioskId": self._submission.kioskId,"submissionId": self._submission.submissionId, "submissionName" : self._submission.name, "file": fileInfo}),
                                'workflowId' : workflowExecutionContext.workflowId + '##' + fileInfo.fileName,
                                'workflowType' : {'name' : 'SubmitOne', 'version' : '1.0'}
                            }
                        });
                }
            });
            
            self._submission.files.forEach(function(fileInfo) {
                
                var doneCnt = 0;
                if(self._submitOneWorkflows[fileInfo.fileName] && self._submitOneWorkflows[fileInfo.fileName].isDone()) {
                    logger.info('%s) %s file submssion is complete', workflowExecutionContext.workflowId, fileInfo.fileName);
                    doneCnt++;
                }
                else {
                    logger.info('%s) %s file submssion is NOT yet complete', workflowExecutionContext.workflowId, fileInfo.fileName);
                }
                
                if(doneCnt == self._submission.files.length) {
                
                    logger.info('All files have been submitted');
                    
                    decisions.push( {
                        'decisionType' : 'CompleteWorkflowExecution',
                        'completeWorkflowExecutionDecisionAttributes' : {
                            'result' : 'Yahoooooo'
                        }
                    });
                }
                
            });
            
        }
        return decisions;
    }
}

function makeAwsName(s) {
        var awsName = s.replace(/[/:|/s]|arn/g, "$") 
        logger.debug("Aws name is:%s, given that input string is: %s", awsName, s);
        return awsName;
}

getDecisionTask();