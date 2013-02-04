var swf = require('./../CloudProviders/AWS/swf.js');
var util = require('util');
var events = require('events');
var log4js = require('log4js');
var logger = log4js.getLogger('SingleSubmissionDecider');
logger.setLevel('INFO');
var cloudStorage = require('./Storage.js');

var decisionQueue = new events.EventEmitter;
function getDecisionTask() {
    
    logger.debug('===================================================================');
    logger.debug('>>>>>>>>>>>>>>>> Polling for a decision task <<<<<<<<<<<<<<<<<<<<<<');
    
    var message = {
            'command' :'SimpleWorkflowService.PollForDecisionTask',
            'payload' : {'domain' : 'Yerramilli-ESP', 'taskList' : { 'name' : 'SubmitOneTask'} }
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
        
        function sendDecisions(decisions) {
            
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
        
        wf.makeDecisions(sendDecisions);
    }
    else {
        getDecisionTask();
    }
}

function workflowWatcher(workflowEvents, lookupEvent, workflowExecutionContext) {
    
    var self = this;
    self._decisionPending = false;
    
    function watchResultTask(resultType, attributeProperty, scheduledEventIdProperty) {
        
        workflowEvents.on(resultType, function(event) {
        
            logger.debug('%s raised', resultType);
            
            if(event.scheduleActivityTaskFailedEventAttributes) {
                
                workflowEvents.emit( event.scheduleActivityTaskFailedEventAttributes.activityType.name + ':' + resultType  , event);
                
            }
            else {
                
                logger.debug('=======> The scheduling event was: %s <=========', event[attributeProperty].scheduledEventId)
            
                var scheduledEvent = lookupEvent(event[attributeProperty].scheduledEventId);
            
                workflowEvents.emit( scheduledEvent.activityTaskScheduledEventAttributes.activityType.name + ':' + resultType  , event, scheduledEvent);
            
            }

        });   
    }
    
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
    
    workflowEvents.on('ActivityTaskScheduled', function(event) {
        
        logger.debug('ActivityTaskScheduled raised');

        workflowEvents.emit(event.activityTaskScheduledEventAttributes.activityType.name + ':ActivityTaskScheduled', event);

    });
    
    watchResultTask('ActivityTaskCompleted', 'activityTaskCompletedEventAttributes');
    
    var activityFailedStates = {
        'ScheduleActivityTaskFailed' : 'scheduleActivityTaskFailedEventAttributes',
        'ActivityTaskFailed' : 'activityTaskFailedEventAttributes' ,
        'ActivityTaskTimedOut' : 'activityTaskTimedOutEventAttributes',
        'ActivityTaskCanceled': 'activityTaskCanceledEventAttributes' };    
    
    for(var state in activityFailedStates) {
        watchResultTask(state, activityFailedStates[state]);
    }
    
    for(var state in activityFailedStates) {
        workflowEvents.on( 'Metadata:' + state, function(event) {
            logger.debug('%s) Metadata ingestion has failed', workflowExecutionContext.workflowId);
            self._metadata = 'ActivityTaskFailed';
        });
    }
    
    workflowEvents.on('Metadata:ActivityTaskScheduled', function(event) {
        
        logger.debug('%s) Metadata ingestion has been scheduled.', workflowExecutionContext.workflowId);
        self._metadata = 'ActivityTaskScheduled';
        
    });
    
    workflowEvents.on('Metadata:ActivityTaskCompleted', function(event, scheduledEvent) {
        
        logger.debug('%s) Metadata has been ingested with asset id = %s', workflowExecutionContext.workflowId, event.activityTaskCompletedEventAttributes.result );
        self._metadata = 'ActivityTaskCompleted';
        var result = JSON.parse(event.activityTaskCompletedEventAttributes.result);
        self._asset = result.payload.asset;
        
    });
    
    workflowEvents.on('Metadata:ActivityTaskFailed', function(event, scheduledEvent) {
        
        logger.error('%s) Metadata could not be ingested', workflowExecutionContext.workflowId, event.result );
        self._metadata = 'ActivityTaskFailed';
    });
    
    workflowEvents.on('Upload:ActivityTaskScheduled', function(event) {
        
        logger.debug('Content upload has been scheduled: %s', workflowExecutionContext.workflowId);
        self._content = 'scheduled';
        
    });
    
    workflowEvents.on('Upload:ActivityTaskCompleted', function(event, scheduledEvent) {
        
        logger.debug('%s) Content has been uploaded', workflowExecutionContext.workflowId );
        self._content = 'uploaded';
        
    });
    
    workflowEvents.on('IndexAsset:ActivityTaskScheduled', function(event) {
        
        logger.debug('%s) Asset Indexing has been scheduled', workflowExecutionContext.workflowId);
        self[event.activityTaskScheduledEventAttributes.activityType.name] = event.eventType;
        
    });
    
    workflowEvents.on('IndexAsset:ActivityTaskCompleted', function(event, scheduledEvent) {
        
        logger.debug('%s) Asset Indexing is complete', workflowExecutionContext.workflowId );
        self[scheduledEvent.activityTaskScheduledEventAttributes.activityType.name] = event.eventType;
        
    });
    
    workflowEvents.on('Transcode:ActivityTaskScheduled', function(event) {
        logger.debug('%s) Asset pack transcoding has been scheduled', workflowExecutionContext.workflowId);
        self._assetPack = 'scheduled';
    });
    
    workflowEvents.on('Transcode:ActivityTaskCompleted', function(event, scheduledEvent) {
        
        logger.debug('Asset pack transcoding is completed: %s', workflowExecutionContext.workflowId );
        self._assetPack = 'generated';
        
    });
    
    workflowEvents.on('Transcode:ActivityTaskFailed', function(event, scheduledEvent) {
        
        logger.debug('Asset pack transcoding is completed: %s', workflowExecutionContext.workflowId );
        self._assetPack = 'generated';
        
    });
    
    this.makeDecisions = function(callback) {
        
        var decisions = new Array();
        if(self._decisionPending) {
            
            if(!self._metadata) {
            
                var metadataActivityId = util.format('%s-metadata-ingest', workflowExecutionContext.workflowId);
                
                logger.info('%s) Decided to schedule metadata insert. Activity id = %s', workflowExecutionContext.workflowId, metadataActivityId);
                
                if(self._submission.file.mimeType.toLowerCase() == 'video/quicktime') {
                    
                    var request = {
                        'header' : {'cooridinationId' : workflowExecutionContext.workflowId},
                        'payload' : self._submission
                    };
                    
                    decisions.push( {
                        'decisionType' : 'ScheduleActivityTask',
                        'scheduleActivityTaskDecisionAttributes' : {
                            'activityType' : {'name' : 'Metadata', 'version' : '2.0'},
                            'scheduleToCloseTimeout' : 'NONE',
                            'startToCloseTimeout' : 'NONE',
                            'heartbeatTimeout' : 'NONE',
                            'input' : JSON.stringify(request),
                            'activityId' : metadataActivityId
                        }
                    });   
                }
            }
            else if (self._metadata == 'ActivityTaskFailed') {
                
                logger.error('%s) Decided to close out this workflow', workflowExecutionContext.workflowId);
                    
                decisions.push( {
                    'decisionType' : 'FailWorkflowExecution',
                    'failWorkflowExecutionDecisionAttributes' : {
                        'reason' : 'Metadata errors'
                    }
                });
            }
            
            if(self._metadata == 'ActivityTaskCompleted' &&  !self._content) {
                    
                if(self._submission.file.mimeType.toLowerCase() == 'video/quicktime') {
                    
                    uploadActivityId = util.format('%s-content-upload', workflowExecutionContext.workflowId);
                  
                    logger.info('%s) Decided to schedule content upload. Activity id = %s', workflowExecutionContext.workflowId, uploadActivityId);
                    
                    var contentFile = self._asset.cutdowns['video/quicktime'];
                    var destinationUrl = cloudStorage.getStorageUrl(contentFile.url.bucket, contentFile.url.filePath,
                                                                    self._submission.file.mimeType, self._submission.file.md5Hash);
                    
                    var uploadRequest = {
                        'header' : {'cooridinationId' : workflowExecutionContext.workflowId},
                        'payload' : {'name': self._submission.submissionName, 'file' : self._submission.file, 'destinationUrl': destinationUrl}
                    };
                    
                    decisions.push( {
                        'decisionType' : 'ScheduleActivityTask',
                        'scheduleActivityTaskDecisionAttributes' : {
                            'activityType' : {'name' : 'Upload', 'version' : '3.0'},
                            'scheduleToCloseTimeout' : 'NONE',
                            'startToCloseTimeout' : 'NONE',
                            'heartbeatTimeout' : 'NONE',
                            'taskList' : { 'name' : self._submission.kioskId},
                            'input' : JSON.stringify(uploadRequest),
                            'activityId' : uploadActivityId
                        }
                    });
                    
                    var indexActivityId = util.format('%s-index-asset', workflowExecutionContext.workflowId);
                    logger.info('%s) Decided to index (initial) metadata search. Activity id = %s', workflowExecutionContext.workflowId, indexActivityId);
                    
                    var indexRequest = {
                        'header' : {'cooridinationId' : workflowExecutionContext.workflowId},
                        'payload' : self._asset
                    };
                    
                    decisions.push( {
                        'decisionType' : 'ScheduleActivityTask',
                        'scheduleActivityTaskDecisionAttributes' : {
                            'activityType' : {'name' : 'IndexAsset', 'version' : '1.0'},
                            'scheduleToCloseTimeout' : 'NONE',
                            'startToCloseTimeout' : 'NONE',
                            'heartbeatTimeout' : 'NONE',
                            'taskList' : { 'name' : 'SearchIndexAsset'},
                            'input' : JSON.stringify(indexRequest),
                            'activityId' : indexActivityId
                        }
                    });
                }
            }

            if(self._content == 'uploaded' && !self._assetPack) {
                
                if(self._asset.assetType.toLowerCase() == 'video') {
                    
                    var transcodeActivityId = util.format('%s-content-transcode', workflowExecutionContext.workflowId);
                    logger.info('%s) Decided to transcode asset cutdowns. Activity id = %s', workflowExecutionContext.workflowId, transcodeActivityId);
                    
                    var activityInfo = {'masterUrl' : self._asset.cutdowns['video/quicktime'].url, 'thumbUrl' : self._asset.cutdowns['application/jpeg'].url, 'flashUrl' : self._asset.cutdowns['video/x-flv'].url};        
                    
                    var transcodeRequest = {
                        'header' : {'cooridinationId' : workflowExecutionContext.workflowId},
                        'payload' : activityInfo
                    };
                    
                    decisions.push( {
                        'decisionType' : 'ScheduleActivityTask',
                        'scheduleActivityTaskDecisionAttributes' : {
                            'activityType' : {'name' : 'Transcode', 'version' : '1.0'},
                            'input' : JSON.stringify(transcodeRequest),
                            'activityId' : transcodeActivityId
                        }
                    });
                }
                
                //self._assetPack = 'generated';
   
            }
            
            if(self._assetPack == 'generated' && self.IndexAsset == 'ActivityTaskCompleted') {
                
                logger.info('%s) Workflow is now complete', workflowExecutionContext.workflowId);
                    
                decisions.push( {
                    'decisionType' : 'CompleteWorkflowExecution',
                    'completeWorkflowExecutionDecisionAttributes' : {
                        'result' : 'Yahoooooo'
                    }
                });
                
            }
        }
        
        callback(decisions);
    
    }
}

function makeAwsName(s) {
        var awsName = s.replace(/[/:|/s]|arn/g, "$") 
        logger.debug("Aws name is:%s, given that input string is: %s", awsName, s);
        return awsName;
}

getDecisionTask();