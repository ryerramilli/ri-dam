var swf = require('./../CloudProviders/AWS/swf.js');
var swfConfig = require('./swf-config').config;
var join = require('join');
var workflowActiviitiesJoin = join.create();

var events = require('events');
var swfConfigEvents = new events.EventEmitter;

swfConfigEvents.on('attempt-domain-creation',
    
    function() {    
        swf.send({
                'command' :'SimpleWorkflowService.RegisterDomain',
                'payload' : swfConfig.domain
            },
            function() {
                swfConfigEvents.emit('domain-is-ready');
            },    
            function() {
                swfConfigEvents.emit('domain-creation-attempt-failed');
            }
        );
    }
    
);

swfConfigEvents.on('domain-is-ready',
    
    function() {
        
        function whenWorkflowAndActivitiesAreDone() {
        
            var result = { 'success' : 0, 'error' : 0, 'warning' : 0}
            for(var i=0; i < arguments.length; i++) {
                result[arguments[i]]++;
            }
        
            if(result.error > 0 || result.warning > 0)
                console.log(swfConfig);
        
            console.log('\n\n');
            console.log(result);
        }
            
        var configurables = [
                             { 'property' : 'workflows', 'command' : 'SimpleWorkflowService.RegisterWorkflowType'},
                             { 'property' : 'activities', 'command' : 'SimpleWorkflowService.RegisterActivityType'}
                        ];
    
        configurables.forEach(
            function(configurable) {
                swfConfig[configurable.property].forEach(
                    function(configurableItem) {
                        configurableItem['onConfigComplete'] = workflowActiviitiesJoin.add();
                    }
                );
            }
        );
            
        workflowActiviitiesJoin.when(whenWorkflowAndActivitiesAreDone);
    
        configurables.forEach(
            function(configurable) {
            
                swfConfig[configurable.property].forEach(
                    function(configurableItem) {
            
                        swf.send(
                            {
                                'command' : configurable.command,
                                'payload' : configurableItem
                            },
                            function() {
                                configurableItem.status = 'success';
                                configurableItem.onConfigComplete('success')
                            },
                            function(data) {
                                configurableItem.status = 'warning';
                                configurableItem.failedReason = data;
                                configurableItem.onConfigComplete('warning');
                            }
                        );        
                    }
                );    
            }
        );
    }
);

swfConfigEvents.on('domain-pre-exists',
    function(domainDescription) {
        
        console.log('Yes, Domain already exists, %j', domainDescription);    
        swfConfigEvents.emit('domain-is-ready');
        
    }
);
    
swfConfigEvents.on('domain-creation-attempt-failed', function() {
        
    swf.send(
        {
            'command' : 'SimpleWorkflowService.DescribeDomain',
            'payload' : {'name' : swfConfig.domain.name}
        },
        function(data) {
            swfConfigEvents.emit('domain-pre-exists', data);
        },
        function(data) {
            swfConfigEvents.emit('domain-setup-failed', data);
        });
    }
    
);
    
swfConfigEvents.on('domain-setup-failed' ,
    function(data) {
        console.log('Something else is wrong');
    }
);

swfConfigEvents.emit('attempt-domain-creation');


