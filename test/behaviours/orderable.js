'use strict';

var assert = require('assert'),
    async = require('enterprise-utils').async,
    model = require('../../lib/model.js');
    
// load model extensions
require('../../lib/extensions/defaults.js');
require('../../lib/extensions/methods.js');
require('../../lib/extensions/queries.js');
require('../../lib/extensions/hooks.js');
require('../../lib/extensions/validations.js');

// load DataSource
require('../../lib/datasources/DataSource.js');

// load Orderable behaviour
require('../../lib/behaviours/Orderable.js');

// load Memory model
var Memory = require('../../lib/datasources/Memory.js');

/*
 * init test Model
 */
var PersonOrderable = model.define('PersonOrderable', ['MemoryDataSource', 'Orderable'], {
    name:{ isString:true },
    surname:{ isString:true },
    isFetched:{ isBoolean:true }
});
PersonOrderable.extendDefaults({ connection:{ collection:'personsOrderable' } });

/*
 * helper for comparing data, because createDT and modifiedDT will be different
 */
function equalData(exclude, data, equalTo){
    var sortedData = [], sortedEqualTo = [], sortedObj;
    
    data = Array.isArray(data) ? data : [ data ];
    equalTo = Array.isArray(equalTo) ? equalTo : [ equalTo ];
    
    for(var i=0;i<data.length;i++){
        sortedObj = {};
        Object.keys(data[i]).sort().forEach(function(v) {
            if(exclude.indexOf(v)===-1) sortedObj[v] = data[i][v];
        });
        sortedData.push(sortedObj);
        sortedObj = {};
        Object.keys(equalTo[i]).sort().forEach(function(v) {
            if(exclude.indexOf(v)===-1) sortedObj[v] = equalTo[i][v];
        });
        sortedEqualTo.push(sortedObj);
    }
    
    return JSON.stringify(sortedData) === JSON.stringify(sortedEqualTo);
}


/*
 * run tests
 */
testOrderable();

/*
 * test orderable bahaviour
 */
function testOrderable(){
    
    var p;
    var s = new async.Series;
    
    var records = [
        { id:'1', name:'Duri', surname:'Kainsmetke' },
        { id:'2', name:'Jozef', surname:'Kozmeker' },
        { id:'3', name:'Pista', surname:'Horvat' },
        { id:'4', name:'Peter', surname:'Nagy' }
    ];
    
    // create records
    s.add(function(next){
        PersonOrderable.new( records[0] ).create(function(err, record0){
            if(err) throw err;
            
            PersonOrderable.new( records[2] ).create(function(err, record2){
                if(err) throw err;
                
                PersonOrderable.new( records[3] ).create(function(err, record3){
                if(err) throw err;
                    
                    setTimeout(function(){
                        // insert record between 0 and 2
                        records[1].sortOrder = record2.sortOrder;
                        PersonOrderable.new( records[1] ).create(function(err, record1){
                            if(err) throw err;
                            next();
                        });
                    }, 500);
                });
            });
        });
    });
    
    s.add(function(next){
        PersonOrderable.collection().all(function(err, records){
            if(err) throw err;
            
            assert.ok(records[0].id === '1');
            assert.ok(records[1].id === '2');
            assert.ok(records[2].id === '3');
            assert.ok(records[3].id === '4');
            
            next();
        });
    });
    
    
    
    s.execute(function(err){
        assert.ok(!err);
        console.log('Orderable behaviour - OK');
    });
}