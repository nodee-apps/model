'use strict';

var assert = require('assert'),
    cluster = require('cluster'),
    cache = require('../lib/cache.js');


/*
 * run tests
 */
cleanUpData();
localCache();
sharedCachePut();  // single process test

/*
 * testing in cluster
 */
//if(cluster.isMaster) {
//    var w1 = cluster.fork();
//    var w2 = cluster.fork();
//
//    setTimeout(function(){
//        w1.send('test_sharedCacheGet');
//        w2.send('test_sharedCachePut');
//    },100);
//}
//
//if(cluster.isWorker){
//    process.on('message', function(msg) {
//        if(msg === 'test_sharedCacheGet') sharedCacheGet();
//        if(msg === 'test_sharedCachePut') sharedCachePut();
//    });
//}


/*
 * test cache.cleanUpData
 */
function cleanUpData(){
    var T = function(){};
    T.prototype.notOwnProperty = 'not instance property';
    var now = new Date();
    var cleanData = cache.cleanUpData({
        fnc: function(){}, // will be removed
        string: 'text',
        number: 123,
        date: now,
        array:[ function(){}, 123, 'text', now ],
        obj:{
            fnc: function(){}, // will be removed
            string: 'text',
            number: 123,
            date: now
        },
        objProto: new T()
    });
    
    assert.deepEqual(cleanData, {
        string: 'text',
        number: 123,
        date: now,
        array:[ null, 123, 'text', now ],
        obj:{
            string: 'text',
            number: 123,
            date: now
        },
        objProto: {}
    });
}

/*
 * test cache.local put, get, del
 */
function localCache(){
    cache.local.put('localKey', { data:'cached' });
    assert.deepEqual(cache.local.get('localKey'), { data:'cached' });
    
    cache.local.del('localKey');
    assert.ok(!cache.local.get('localKey'));
    
    cache.local.put('localKey', { data:'cached' }, 10, function(){
        assert.ok(!cache.local.get('localKey'));
        console.log('cache local - OK');
    });
    assert.ok(!!cache.local.get('localKey'));
}
/*
 * test cache put, get, del
 */
function sharedCacheGet(){
    setTimeout(function(){
        cache.get('sharedKey', function(err, data){
            if(err) throw err;
            assert.deepEqual(data, { data:'shared' });
        });
        
    }, 50);
    
    setTimeout(function(){
        cache.get('sharedKey', function(err, data){
            if(err) throw err;
            assert.ok(!data);
            console.log('cache shared - OK');
        });
        
    }, 150);
}

function sharedCachePut(){
    cache.put('sharedKey', { data:'shared' }, 1000, function(err){
        if(err) throw err;
        
        cache.get('sharedKey', function(err, data){
            if(err) throw err;
            assert.deepEqual(data, { data:'shared' });
            
            setTimeout(function(){
                cache.get('sharedKey', function(err, data){
                    if(err) throw err;
                    
                    assert.ok(!data);
                    console.log('cache shared - OK');
                });
                
            }, 1500);
        });
    });
}