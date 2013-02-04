var http = require('http');
require('module');

exports.send =  function(resource, method, message, responseHandler) {
    
    var request = http.request( {'hostname':'localhost', 'port':9999, 'path': resource, 'method': method} , function(response) {
        
        var data = '';
        response.on('data', function(chunk) {
            data += chunk;
        });
        
        response.on('end', function() {
            
            if(response.statusCode == 200) {
                var obj = JSON.parse(data);
                responseHandler(obj);
            }
            else {
                console.log(response.statusCode);
                console.log(response.headers);
            }
            
        });
        
    });
    
    if(message && message.payload) request.write(message.payload.getString(), 'utf-8');
        request.end();
    
}

exports.serializers = {
    'file' : function (fileName) {
    
        this._fileName = fileName;
    
        this.getString = function() {
            return file.readFileSync(fileName);
        }
    },
    
    'json' : function(obj) {
    
        this._obj = obj;
    
        this.getString = function() {
            console.log("===> %s", JSON.stringify(this._obj));
            return JSON.stringify(this._obj);
        }
    }

}