'use strict';

var assert = require('assert'),
    async = require('nodee-utils').async,
    model = require('../../lib/model.js');
    
// load model extensions
require('../../lib/extensions/defaults.js');
require('../../lib/extensions/methods.js');
require('../../lib/extensions/queries.js');
require('../../lib/extensions/hooks.js');
require('../../lib/extensions/validations.js');

// load DataSource
require('../../lib/datasources/DataSource.js');

// load Tree behaviour
require('../../lib/behaviours/Tree.js');

// load Memory model
var Memory = require('../../lib/datasources/Memory.js');

/*
 * init test Model
 */
var PersonTree = model.define('PersonTree', ['MemoryDataSource', 'Tree'], {
    name:{ isString:true },
    surname:{ isString:true },
    isFetched:{ isBoolean:true }
});
PersonTree.extendDefaults({
    connection:{ collection:'personsTree' },
    options:{ storeChildren:true, storeChildrenCount:true } // test shildren id sync
});

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
testHooks(testInstanceMethods);

/*
 * test Tree hooks:
 * 1. beforeCreate - validateAncestors, afterCreate - addParentRef
 * 2. beforeUpdate - disableAncestors, (test document.move)
 * 3. beforeRemove - removeChildren, removeParentRef
 */
function testHooks(cb){
    
    var s = new async.Series;
    
    var records = [
        { id:'1', name:'Duri', surname:'Kainsmetke', ancestors:[] }, // root element
        { id:'2', name:'Jozef', surname:'Kozmeker', ancestors:['1'] }, // child of '1'
        { id:'3', name:'Pista', surname:'Horvat', ancestors:['1','2'] }, // child of '2'
        { id:'4', name: 'Pista', surname: 'Lakatos', ancestors: [ '1','2','3' ] }, // child of '3'
    ];
    
    // 1. beforeCreate - validateAncestors, afterCreate - addParentRef
    s.add(function(next){
        PersonTree.new( records[0] ).create(function(err, record1){
            if(err) throw err;
            
            PersonTree.new( records[1] ).create(function(err, record2){
                if(err) throw err;
                
                records[2].ancestors = ['0','2']; // will return error, because ancestor with id '0' not exists
                PersonTree.new( records[2] ).create(function(err, record3){
                    assert.ok( err.message === 'Tree.prototype.validateAncestors: cannot find all ancestors' );
                    
                    records[2].ancestors = ['1','2'];
                    PersonTree.new( records[2] ).create(function(err, record3){
                        if(err) throw err;
                        
                        PersonTree.collection().all(function(err, persons){
                        if(err) throw err;
                            assert.ok(equalData(['modifiedDT', 'createdDT'], persons, [
                                { id:'1', name: 'Duri', surname: 'Kainsmetke', ancestors: [], ancestorsCount:0, children: [ '2' ], childrenCount:1 },
                                { id:'2', name: 'Jozef', surname: 'Kozmeker', ancestors: [ '1' ], ancestorsCount:1, children: [ '3' ], childrenCount:1 },
                                { id:'3', name: 'Pista', surname: 'Horvat', ancestors: [ '1', '2' ], ancestorsCount:2, children: [], childrenCount:0 }
                            ]));
                            next();
                        });
                    });
                });
            });
        });
    });
    
    // 2. beforeUpdate - disableAncestors, (test document.move)
    s.add(function(next){
        PersonTree.collection().findId('3').one(function(err, record3){
            if(err) throw err;
            
            record3.ancestors = ['999']; // trying to change ancestors from ['1','2'] to ['999'], but nothing happens, because updating ancestors is disabled
            record3.update(function(err, record3){
                if(err) throw err;
                assert.deepEqual(record3.ancestors, ['999']);
                record3.ancestors = ['1','2']; // restore ancestors, because testing vith MemoryDataSource
                
                // move record 3 to record id '1'
                record3.move('1', function(err){
                    if(err) throw err;
                    assert.deepEqual(record3.ancestors, ['1']);
                    
                    records[3].ancestors = ['1','2','3']; // test inconsistent ancestors, correctly have to be ['1','3']
                    PersonTree.new( records[3] ).create(function(err, record4){
                        assert.ok(err.message === 'Tree.prototype.validateAncestors: inconsistent path');
                        
                        records[3].ancestors = ['1','3'];
                        PersonTree.new( records[3] ).create(function(err, record4){
                            if(err) throw err;
                            
                            PersonTree.collection().all(function(err, persons){
                                if(err) throw err;
                                assert.ok(equalData(['modifiedDT', 'createdDT'], persons, [
                                    { id:'1', name: 'Duri', surname: 'Kainsmetke', ancestors: [], ancestorsCount:0, children: [ '2','3' ], childrenCount:2 },
                                    { id:'2', name: 'Jozef', surname: 'Kozmeker', ancestors: [ '1' ], ancestorsCount:1, children: [], childrenCount:0 },
                                    { id:'3', name: 'Pista', surname: 'Horvat', ancestors: [ '1' ], ancestorsCount:1, children: [ '4' ], childrenCount:1 },
                                    { id:'4', name: 'Pista', surname: 'Lakatos', ancestors: [ '1','3' ], ancestorsCount:2, children: [], childrenCount:0 }
                                ]));
                                next();
                            });
                        });
                    });
                });
            });
        });
    });
    
    // 2. afterUpdate - updateDescendants - this will update ancestors of all descendants, when parent changes ancestors
    s.add(function(next){
        PersonTree.collection().findId('3').one(function(err, record3){
            if(err) throw err;
            
            // move record 3 inside record2
            record3.move('2', function(err){
                if(err) throw err;
                
                PersonTree.collection().all(function(err, persons){
                    if(err) throw err;
                    
                    assert.ok(equalData(['modifiedDT', 'createdDT'], persons, [
                        { id:'1', name: 'Duri', surname: 'Kainsmetke', ancestors: [], ancestorsCount:0, children: [ '2' ], childrenCount:1 },
                        { id:'2', name: 'Jozef', surname: 'Kozmeker', ancestors: [ '1' ], ancestorsCount:1, children: [ '3' ], childrenCount:1 },
                        { id:'3', name: 'Pista', surname: 'Horvat', ancestors: [ '1','2' ], ancestorsCount:2, children: [ '4' ], childrenCount:1 },
                        { id:'4', name: 'Pista', surname: 'Lakatos', ancestors: [ '1','2','3' ], ancestorsCount:3, children: [], childrenCount:0 }
                    ]));
                    next();
                });
            });
        });
    });
    
    
    // 3. beforeRemove - removeChildren, parent removeRef
    s.add(function(next){
        PersonTree.collection().findId('2').one(function(err, record2){
            if(err) throw err;
            
            // remove record2, this will remove all of children, and also remove reference from parent children
            record2.remove(function(err){
                if(err) throw err;
                
                PersonTree.collection().all(function(err, persons){
                    if(err) throw err;
                    
                    assert.ok(equalData(['modifiedDT', 'createdDT'], persons, [
                        { id:'1', name: 'Duri', surname: 'Kainsmetke', ancestors: [], ancestorsCount:0, children: [], childrenCount:0 }
                    ]));
                    next();
                });
            });
        });
    });
    
    s.execute(function(err){
        assert.ok(!err);
        console.log('Tree behaviour CRUD - OK');
        cb();
    });
}

/*
 * test tree instance methods:
 * addChild, getSiblings(sort, callback), getDescendants(sort, levels, callback)
 */
function testInstanceMethods(){
    var s = new async.Series;
    
    var records = [
        { id:'1', name:'Duri', surname:'Kainsmetke', ancestors:[] }, // root element
        { id:'2', name:'Jozef', surname:'Kozmeker', ancestors:['1'] }, // child of '1'
        { id:'3', name:'Pista', surname:'Horvat' }, // child of '2'
        { id:'4', name: 'Pista', surname: 'Lakatos' }, // child of '2'
    ];
    
    // record 0 is already in datasource, insert 1,2,3
    s.add(function(next){
        PersonTree.new( records[1] ).create(function(err, record1){
            if(err) throw err;
            
            // test addChild
            record1.addChild(records[2], function(err, record2){
                if(err) throw err;
                
                // test addChild
                record1.addChild(records[3], function(err, record3){
                    if(err) throw err;
                    
                    PersonTree.collection().all(function(err, persons){
                        if(err) throw err;
                        
                        assert.ok(equalData(['modifiedDT', 'createdDT'], persons, [
                            { id:'1', name: 'Duri', surname: 'Kainsmetke', ancestors: [], ancestorsCount:0, children: [ '2' ], childrenCount:1 },
                            { id:'2', name: 'Jozef', surname: 'Kozmeker', ancestors: [ '1' ], ancestorsCount:1, children: [ '3','4' ], childrenCount:2 },
                            { id:'3', name: 'Pista', surname: 'Horvat', ancestors: [ '1','2' ], ancestorsCount:2, children: [], childrenCount:0 },
                            { id:'4', name: 'Pista', surname: 'Lakatos', ancestors: [ '1','2' ], ancestorsCount:2, children: [], childrenCount:0 }
                        ]));
                        next();
                    });
                });
            });
        });
    });
    
    // getSiblings(sort, callback)
    s.add(function(next){
        PersonTree.collection().findId('3').one(function(err, record3){
            if(err) throw err;
            
            record3.getSiblings().all(function(err, siblings){
                if(err) throw err;
                
                assert.ok(equalData(['modifiedDT', 'createdDT'], siblings, [
                    { id:'4', name: 'Pista', surname: 'Lakatos', ancestors: [ '1','2' ], ancestorsCount:2, children: [], childrenCount:0 }
                ]));
                next();
            });
        });
    });
    
    // getDescendants(sort, levels, callback)
    s.add(function(next){
        PersonTree.collection().findId('1').one(function(err, record1){
            if(err) throw err;
            
            record1.getDescendants(2).sort({ id:-1 }).all(function(err, descendants){
                if(err) throw err;
                
                assert.ok(equalData(['modifiedDT', 'createdDT'], descendants, [
                    { id:'4', name: 'Pista', surname: 'Lakatos', ancestors: [ '1','2' ], ancestorsCount:2, children: [], childrenCount:0 },
                    { id:'3', name: 'Pista', surname: 'Horvat', ancestors: [ '1','2' ], ancestorsCount:2, children: [], childrenCount:0 }
                ]));
                next();
            });
        });
    });
    
    s.execute(function(err){
        assert.ok(!err);
        console.log('Tree behaviour instance methods - OK');
    });
}