console.log('Loading module %s', module.filename);
var events = require('events');
var assetDb = require('mysql');

var logger = require('log4js').getLogger('AssetManagementWorker');
logger.setLevel('DEBUG');

function doSubmitAsset(submitRequest, onCompleteCallback) {
    
    function onComplete(response) {
        assetStore.close();
        onCompleteCallback(response);
    }
    
    function fmt(str) {
        return '[' + submitRequest.header.cooridinationId + '] ' + str;
    }
    
    var submissionInfo = submitRequest.payload;
    logger.info(fmt('Preparing to submit: %j'), submissionInfo);
    
    var assetStoreEvents = new events.EventEmitter;
    
    var newAsset = {'version' : 1, 'assetType' : 'video', 'caption' : submissionInfo.file.caption, 'originalFileName' : submissionInfo.file.fileName, 'submissionId' : submissionInfo.submissionId,
        'submitDateTime' : new Date(),
        'keywords' : submissionInfo.file.keywords,
        'cutdowns' : { 'video/quicktime' : {'mimeType' : 'video/quicktime', 'status' : 'not created'},
                        'application/jpeg' : { 'mimeType' : 'application/jpeg', 'status' : 'not created'},
                        'video/x-flv' : { 'mimeType' : 'video/x-flv', 'status' : 'not created'}}
    }
    
    assetStoreEvents.on('Connected', function() {
        logger.debug(fmt('Successfully connected to datastore'));
        assetStore.beginTransaction( {'onSuccess' : 'Transaction started'});
    });
    
    assetStoreEvents.on('Transaction started', function() {
        
        logger.debug(fmt('Successfully started a transaction'));
        
        var row = {'ContentProvider' : submissionInfo.kioskId, 'SubmitDateTime' : newAsset.submitDateTime, 'Caption': newAsset.caption,
                        'OriginalFilename': newAsset.originalFileName, 'SubmissionId' : newAsset.submissionId};
                        
        assetStore.doStmt({ 'stmt' : 'Insert Into Asset Set ?',
                           'params' : row,
                            'onSuccess' : 'Asset Inserted'});
    });
    
    var cutdowns = shallowCloneArray(newAsset.cutdowns);
    function insertAssetCutdown() {
        
        if(cutdowns.length == 0) {
            assetStoreEvents.emit('All AssetCutdowns Inserted');
            return;
        }
    
        var cutdown = cutdowns.shift();
                                    
        assetStore.doStmt({
            'stmt' : 'Insert Into AssetCutdown Set ?',
            'params' : {'MasterId' : newAsset.masterId, 'mimeType': cutdown.mimeType, 'status' : cutdown.status},
            'onSuccess' : 'AssetCutdown Inserted'
        });
    }
    
    assetStoreEvents.on('Asset Inserted', function(result) {
        newAsset['masterId'] =  result.insertId;
        
        var cutdownMeta = {
            'video/quicktime' : { 'suffix' : 'mov', 'folder' : 'quicktime'},
            'application/jpeg' :  { 'suffix' : 'jpg', 'folder' : 'thumb'},
             'video/x-flv' : { 'suffix' : 'flv', 'folder' : 'flash'}
        };
        
        for(var mimeType in newAsset.cutdowns) {
            newAsset.cutdowns[mimeType]['url'] = {'bucket' : 'ramencodingdotcom', 'filePath' :  cutdownMeta[mimeType].folder + '/' + newAsset.masterId + "." + cutdownMeta[mimeType].suffix};
        }
    
        logger.info(fmt('Inserted (pre-committed) Asset %s'), newAsset.masterId);
        insertAssetCutdown();
    });
    
    assetStoreEvents.on( 'AssetCutdown Inserted', function() {
        logger.debug(fmt('Inserted (pre-committed) AssetCutdown'));
        insertAssetCutdown();
    });
    
    assetStoreEvents.on('All AssetCutdowns Inserted', function() {
        logger.debug(fmt('Inserted (pre-committed) all AssetCutdowns'));
        assetStore.commit({ 'onSuccess' : 'Committed'});
    });
    
    assetStoreEvents.on('Committed', function() {
        
        var response = { 'header' : {
                            'status' : 'success' ,
                            'cooridinationId' : submitRequest.header.cooridinationId
                            },
                            'payload' : {
                                'asset' : newAsset
                            }
                        };
                        
         onComplete(response);                       
    });
    
    assetStoreEvents.on('Error', function() {
        
        var response = { 'header' : {
                            'status' : 'error' ,
                            'statusList' : [
                                            {'status' : {'type' : '', 'code' : '', 'message' : ''}}
                                            ],
                            'cooridinationId' : submitRequest.header.cooridinationId
                            },
                            'payload' : {    
                            }
                        };
        
        onComplete(response);
        
    });
    
    var assetStore = new datastore({'eventer' : assetStoreEvents, 'logFormatter' : fmt});
    assetStore.connect({'onSuccess' : 'Connected'});    
}

function doSubmitCutdownFiles(request, onCompleteCallback) {
    
    function onComplete(response) {
        assetStore.close();
        onCompleteCallback(response);
    }
    
    function fmt(str) {
        return '[' + request.header.cooridinationId + '] ' + str;
    }
    
    var activityInfo = request.payload;
    logger.debug(fmt('Accepting asset pack metadata: %j'), activityInfo);
    
    var assetStoreEvents = new events.EventEmitter;
    
    assetStoreEvents.on('Connected', function() {
        assetStore.beginTransaction( {'onSuccess' : 'Transaction started'});
    });
    
    assetStoreEvents.on('Transaction started', function() {
        assetStore.doStmt({'stmt' : "Update Asset Set Version = Version + 1 Where MasterId = " + activityInfo.masterId,
                          'onSuccess' : 'Asset Versioned'});
    });
    
    var cutdowns = shallowCloneArray(activityInfo.cutdowns);
    ['Asset Versioned', 'AssetCudown updated'].forEach(function(evt) {
        assetStoreEvents.on( evt, function() {
            
            if(cutdowns.length == 0) {
                assetStoreEvents.emit('All AssetCudown updated');
            }
            else {
                var cutdown = cutdowns.shift();
                assetStore.doStmt({
                    'stmt' :  "Update AssetCutdown Set status = 'Available' Where MasterId = " + activityInfo.masterId + " And mimeType = '" + cutdown.mimeType + "'",
                    'onSuccess' : 'AssetCudown updated'});
                }
            }); 
    });
    
    assetStoreEvents.on('All AssetCudown updated', function() {
        
        asssetStore.doStmt({
            'stmt' : "Select * From Asset Where MasterId = " + activityInfo.masterId,
            'onSuccess' : 'Asset fetched'});
        
    });
    
    var fetchedAsset =  {'masterId' : activityInfo.masterId, 'cutdowns' : {}};
    assetStoreEvents.on('Asset fetched', function(records) {
    
        fetchedAsset['version'] = records[0].Version;
        fetchedAsset['caption'] = records[0].Caption;
        fetchedAsset['originalFileName'] = records[0].OriginalFilename;
        fetchedAsset['submissionId'] =  records[0].SubmissionId;
        
        asssetStore.doStmt({
            'stmt' : "Select * From AssetCutdown Where MasterId = " + activityInfo.masterId,
            'onSuccess' : 'Asset cutdowns fetched'});
    });
    
    assetStoreEvents.on('Asset cutdowns fetched', function(records) {
        
        records.forEach(function(record) {
           fetchedAsset.cutdowns[record.mimeType] = {'mimeType' : record.mimeType, 'status' : record.status};
           
        });
        
        assetStore.commit({'onSuccess' : 'Committed'});
    });
    
    assetStoreEvents.on('Committed', function() {
        
        var response = { 'header' : {
                            'status' : 'success' ,
                            'cooridinationId' : request.header.cooridinationId
                            },
                            'payload' : {
                                'asset' : fetchedAsset
                            }
                        };
                        
         onComplete(response);                        
    });
    
    assetStoreEvents.on('Error', function() {
        
        var response = { 'header' : {
                            'status' : 'error' ,
                            'statusList' : [
                                            {'status' : {'type' : '', 'code' : '', 'message' : ''}}
                                            ],
                            'cooridinationId' : request.header.cooridinationId
                            },
                            'payload' : {    
                            }
                        };
        
        onComplete(response);
        
    });
    
    var assetStore = new datastore('submit cutdown files');
    assetStore.connect({'onSuccess' : 'Connected'});
}

function shallowCloneArray(src) {
    var dest = new Array();
    for(var p in src) {
        dest.push(src[p]);
    }
    return dest;
}

function datastore(context, assetStoreEvents){
    
    var _self = this;
    
    this.connection = assetDb.createConnection( {'host' : 'videopoc.ceoxxwsgttsc.us-east-1.rds.amazonaws.com', 'user' : 'yerramilli', 'password' : 'yerramilli', 'database' : 'AssetDB'} );
    this.context = context;
    
    this.doStmt = function(cmd) {
        
        _self.connection.query(cmd.stmt, cmd.params, function(err, result) {
            
            if(!err) {
                if(cmd.onSuccess) _self.context.eventer.emit(cmd.onSuccess, result);
            }
            else {
                logger.error(_self.context.logFormatter('** Database error **\n    Error: %j\n    Statement: %j **'), err, cmd);
                _self.context.eventer.emit('Error', err);
            }
        });
    }
    
    this.connect = function() {
        
        _self.connection.connect(function(err) {
            
            if(err) {
                logger.error(_self.context.logFormatter('** Cannot connect to database **\n%j'), err);
                _self.context.eventer.emit('Error', err);
            }
            else {
                _self.context.eventer.emit('Connected', err);
            }
            
        });
    }
    
    this.beginTransaction = function(cmd) {
        
        cmd['stmt'] = 'Start Transaction';
        
        _self.doStmt (cmd);
    }
    
    this.rollback = function() {
        
        _self.doStmt ({
            'stmt' : 'Rollback',
            'onSucccess' : 'Error'
        });
        
    }
    
    this.commit = function() {
        _self.doStmt ({
            'stmt' : 'Commit',
            'onSuccess' : 'Committed'
        });
    }
    
    this.close = function() {
        _self.connection.end(function(err) {
            if(err)
                logger.warn(_self.context.logFormatter('** Error closing connection **\n%j'), err);
        });
    }
}

require('./../Integration/SWF-Transport.js').bind('metadata', function(activtyEvents) {
    activtyEvents.on('Metadata', doSubmitAsset);
    activtyEvents.on('Metadata.AssetPack', doSubmitCutdownFiles);
});
