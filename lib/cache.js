'use strict';

var cluster = require('cluster');

/*
 * Shared cache is nothing more than worker local cache with ability to synchronize data with other workers
 * To implement redis, or any other caching mechanism, just override Cache.prototype methods "get", "put", and "del"
 *
 */
var Cache = function(){};
module.exports = new Cache();


/*
 * Worker to worker messaging:
 * Master catch message from worker and broadcasts it to all others,
 * 
 */
function broadcast(msg, excludeWorkerId){
    for(var id in cluster.workers){
        if(id !== excludeWorkerId) cluster.workers[id].send(msg);
    }
}

if(cluster.isMaster){
    setTimeout(function(){
        for(var id in cluster.workers){
            (function(id){
                cluster.workers[id].on('message', function(msg) {
                    msg = msg || {};
                    if(msg.broadcast === true) broadcast(msg, id);
                });
            })(id);
        }
    });
    
    // if application is using only single master process, there is no need to synchronize cache
    Cache.prototype.synchronizeCache = function(key, value, timeout){};
}
else {
    // receive msg from others
    process.on('message', function(msg) {
        msg = msg || {};
        
        // don't listen to messages from other modules
        if(msg.module!=='nodee-cache') return;
        
        if(msg.cmd === 'clear_cache_key') {
            module.exports.local.del(msg.key);
        }
        else if(msg.cmd === 'update_cache_key'){
            module.exports.local.put(msg.key, msg.value, msg.timeout);
        }
    });

    // synchronize only if isWorker
    Cache.prototype.synchronizeCache = function(key, value, timeout){
        if(!value) process.send({
            module:'nodee-cache',
            cmd: 'clear_cache_key',
            key:key,
            broadcast:true
        });
        else process.send({
            module:'nodee-cache',
            cmd: 'update_cache_key',
            key:key,
            value:value,
            timeout:timeout,
            broadcast:true
        });
    };
}
    
Cache.prototype.cleanUpData = function(value){
    // remove non compatible properties, such as function, etc... (for later redis implementation)
    
    var cleanObject, isArray;
    
    if(typeof value === 'function') return null;
    else if(Array.isArray(value)) {
        cleanObject = [];
        isArray = true;
    }
    else if(Object.prototype.toString.call(value) === '[object Object]') cleanObject = {};
    else return value;
    
    for(var key in value){
        if(value.hasOwnProperty(key) && (typeof value[key] !== 'function' || isArray)){
            cleanObject[key] = this.cleanUpData(value[key]);
        }
    }
    
    return cleanObject;
}
 
    
Cache.prototype.put = function(key, value, timeout, cb){ // cb(err, value)
    if(arguments.length===3){
        cb = arguments[2];
        timeout = null;
    }
    else if(arguments.length !== 2 && arguments.length !== 4) throw new Error('Wrong arguments');
    
    var cache = this;
    
    value = {
        ver: cache.local.get(key) ? cache.local.get(key).ver + 1 : 0,
        value: cache.cleanUpData(value)
    };
    
    if(timeout){
        this.local.put(key, value, timeout, function(){ // sync on timeout
            cache.synchronizeCache(key);
        });
        // TODO: implement versioning
        // if(value.ver > 0) synchronizeCache(key, value, timeout);
    }
    else cache.local.put(key, value);
    
    // put to local cache of all child processes
    cache.synchronizeCache(key, value, timeout);
    
    if(typeof cb === 'function') setImmediate(function(){
        cb(null, value.value);
    });
};


Cache.prototype.del = function(key, cb){ //cb(err)
    this.local.del(key);
    this.synchronizeCache(key);
    if(typeof cb === 'function') {
        setImmediate(cb);
    }
};

Cache.prototype.get = function(key, cb){ // cb(err, value)
    if(arguments.length !== 1 && arguments.length !== 2) throw new Error('Wrong arguments');
    
    var value = this.local.get(key) || {};
    if(typeof cb === 'function') setImmediate(function(){
        cb(null, value.value);
    });
};


/*
 * Single worker cache
 */
var cache = {};
Cache.prototype.local = {
    put: function(key, value, time, timeoutCallback) {
        var localCache = this;
        var oldRecord = cache[key];
        if (oldRecord) {
            clearTimeout(oldRecord.timeout);
        }
        var expire = time + (new Date()).getTime();
        var record = {value: value, expire: expire};
        if (!isNaN(expire)) {
            var timeout = setTimeout(function() {
                localCache.del(key);
                if(typeof timeoutCallback === 'function') {
                    timeoutCallback(key);
                }
            }, time);
            record.timeout = timeout;
        }
        cache[key] = record;
    },
    del: function(key) {
        delete cache[key];
    },
    clear: function() {
        cache = {};
    },
    get: function(key) {
        var data = cache[key];
        if(typeof data !== 'undefined') {
            if(isNaN(data.expire) || data.expire >= (new Date()).getTime()) return data.value;
            else this.del(key);
        }
        return null;
    }
};