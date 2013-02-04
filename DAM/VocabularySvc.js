// This is  HTTP Service
var util = require('util');
var log4js = require('log4js');
var logger = log4js.getLogger('Vocab');
logger.setLevel('INFO');
var transport = require('./../Integration/Transport.js');

function getParentKeywords(keyword, onComplete) {
        
        var vocab = {
                'usa' : {
                        'wa' : {
                                'seattle' : '',
                                'bellevue' : '',
                                'redmond' : ''},
                        'ma' : {
                                'boston' : '',
                                'cambridge' : '',
                                'waltham' : ''},
                        'ca' : {
                                'san diego' : '',
                                'los angeles' : '',
                                'san francisco' : ''}
                },
                'transportation' : {
                        'land transportation' : {
                                'car' : '',
                                'bus' : '',
                                'train' : ''},
                        'water transportation' : {
                                'boat' : '',
                                'ship' : '',
                                'yatch' : ''},
                        'air transportation' : {
                                'aeroplane' : '',
                                'helicopter' : '',
                                'glider' : ''}
                }
        };
        
        var lookup = new Object();
        
        function walk(parentWord, level)  {
                for(var word in level) {
                        
                        if(lookup[parentWord])
                                lookup[word] = lookup[parentWord].concat(parentWord);
                        else
                                lookup[word] = new Array();
                        
                        if( typeof(level[word]) != 'string' )
                                walk(word, level[word]);
                }
        }
        
        walk('_', vocab);
        
        var parentKeywords = lookup[keyword];
        
        if(parentKeywords)
                onComplete({ 'statusCode': 200, 'payload': new transport.serializers.json(parentKeywords)});
        else
                onComplete({ 'statusCode': 404, 'payload': new transport.serializers.json({})});
}

require('./../HTTP-Transport.js').bind(8889, function(activtyEvents) {
    activtyEvents.on('/', getParentKeywords);
});