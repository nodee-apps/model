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

// load Memory model
var Memory = require('../../lib/datasources/Memory.js');

/*
 * init test Model
 */
var Person = model.define('Person', ['MemoryDataSource'], {
    name:{ isString:true },
    surname:{ isString:true },
    isFetched:{ isBoolean:true }
});
Person.extendDefaults({ connection:{ collection:'persons' } });

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
testQueryBuilders();
testQueryMethods(testInstanceMethods);

/*
 * test collection.query builders
 * where, whereId, fields, skip, limit/take, sort/order, cache
 */
function testQueryBuilders(){
    
    var query = Person.collection()
        .find({ test:'test' })
        .findId('id')
        .fields({ field1:1, field2:0 })
        .skip(3)
        .limit(10)
        .sort({ field1:1, field2:-1 })
        .cache(1000);
    
    assert.deepEqual(query.getDefaults(), {
        connection:{ collection:'persons' },
        query: {
            test: 'test',
            id: 'id'
        },
        options: {
            sort: { field1: 1, field2: -1 },
            limit: 10,
            skip: 3,
            fields: { field1: 1, field2: 0 },
            softRemove: false,
            optimisticLock: true },
        cache: {
            createKey: query.getDefaults().cache.createKey,
            keyPrefix: 'nodee-model-memory',
            duration: 1000,
            use: true
        },
        singleInstanceOp: undefined
    });
    
    console.log('MemoryDataSource query builders - OK');
}


/*
 * test collection.query methods
 * cleanCache/clearCache, exec, count, one, all, create, update, remove
 * include options: cacheable and fetch
 */
function testQueryMethods(cb){
    // include cacheable, fetch
    
    Person.onFetch(function(data){
        // fetching data
        
        data.isFetched = true;
        return data;
    });

    Person.onFill(function(data, fromDataSource){
        if(fromDataSource) return;
        // filling data
        
        data.isFilled = true;
        return data;
    });

    // test fill hook
    var filledPerson = new Person({ name:'Peter' });
    assert.ok(equalData(['modifiedDT', 'createdDT'], filledPerson, { name: 'Peter', isFilled: true }));
    
    var records = [
        { id:'1', name:'Duri', surname:'Kainsmetke' },
        { id:'2', name:'Jozef', surname:'Kozmeker' },
        { id:'3', name: 'Pista', surname: 'Horvat' }
    ];
    
    var s = new async.Series();
    
    // create
    s.add(function(next){
        Person.collection().create(records, function(err, persons){
            if(err) throw err;
            assert.ok(equalData(['modifiedDT', 'createdDT'], persons, [
                { id:'1', name: 'Duri', surname: 'Kainsmetke', isFetched: true },
                { id:'2', name: 'Jozef', surname: 'Kozmeker', isFetched: true },
                { id:'3', name: 'Pista', surname: 'Horvat', isFetched: true }
            ]));
            
            next();
        });
    });
    
    // one
    s.add(function(next){
        Person.collection().one(function(err, person){
            if(err) throw err;
            assert.ok(equalData(['modifiedDT', 'createdDT'], person,
                                       { id: '1', name: 'Duri', surname: 'Kainsmetke', isFetched: true }));
            next();
        });
    });
    
    // all
    s.add(function(next){
        Person.collection().skip(1).limit(2).all(function(err, persons){
            if(err) throw err;
            assert.ok(equalData(['modifiedDT', 'createdDT'], persons, [
                { id: '2', name: 'Jozef', surname: 'Kozmeker', isFetched: true },
                { id: '3', name: 'Pista', surname: 'Horvat', isFetched: true }
            ]));
            
            next();
        });
    });
    
    // count
    s.add(function(next){
        Person.collection().count(function(err, count){
            if(err) throw err;
            assert.ok(count === 3);
            
            next();
        });
    });
    
    // exec
    s.add(function(next){
        Person.collection().exec('command', 'args', function(err, result){
            if(err) throw err;
            assert.ok(equalData(['modifiedDT', 'createdDT'], result, [
                { id:'1', name: 'Duri', surname: 'Kainsmetke', isFetched: true },
                { id:'2', name: 'Jozef', surname: 'Kozmeker', isFetched: true },
                { id:'3', name: 'Pista', surname: 'Horvat', isFetched: true }
            ]));
            
            next();
        });
    });
    
    // cache - put
    s.add(function(next){
        Person.collection().cache().all(function(err, persons){
            if(err) throw err;
            assert.ok(equalData(['modifiedDT', 'createdDT'], persons, [
                { id:'1', name: 'Duri', surname: 'Kainsmetke', isFetched: true },
                { id:'2', name: 'Jozef', surname: 'Kozmeker', isFetched: true },
                { id:'3', name: 'Pista', surname: 'Horvat', isFetched: true }
            ]));
            
            next();
        });
    });
    
    // update
    s.add(function(next){
        Person.collection().findId(['1','2']).update({ surname:'updated' }, function(err, count){
            if(err) throw err;
            assert.ok(count===2);
            
            Person.collection().all(function(err, persons){
                if(err) throw err;
                assert.ok(equalData(['modifiedDT', 'createdDT'], persons, [
                    { id:'1', name: 'Duri', surname: 'updated', isFetched: true },
                    { id:'2', name: 'Jozef', surname: 'updated', isFetched: true },
                    { id:'3', name: 'Pista', surname: 'Horvat', isFetched: true }
                ]));
                next();
            });
        });
    });
    
    // remove
    s.add(function(next){
        Person.collection().remove(function(err, count){
            if(err) throw err;
            assert.ok(count===3);
            
            Person.collection().all(function(err, persons){
                if(err) throw err;
                assert.ok(persons.length===0);
                next(); 
            });
        });
    });
    
    // resetCache/clearCache
    s.add(function(next){
        Person.collection().cache().all(function(err, persons){
            if(err) throw err;
            assert.ok(equalData(['modifiedDT', 'createdDT'], persons, [
                { id:'1', name: 'Duri', surname: 'Kainsmetke', isFetched: true },
                { id:'2', name: 'Jozef', surname: 'Kozmeker', isFetched: true },
                { id:'3', name: 'Pista', surname: 'Horvat', isFetched: true }
            ]));
            
            Person.collection().resetCache('all', function(err){
                if(err) throw err;
                
                Person.collection().cache().all(function(err, persons){
                    if(err) throw err;
                    assert.ok(persons.length===0);
                    next(); 
                });
            });
        });
    });
    
    
    s.execute(function(err){
        if(err) throw err;
        console.log('MemoryDataSource query methods - OK');
        cb();
    });
}


/*
 * test model instance methods
 * create, update, remove
 */
function testInstanceMethods(cb){
    
    var p;
    var s = new async.Series;
    
    // instance create
    s.add(function(next){
        Person.new().fill({ name:'Duri', surname:'Kainsmetke' }).create(function(err, person){
            if(err) throw err;
            assert.ok(!!person.id);
            assert.ok(equalData(['id', 'modifiedDT', 'createdDT'], person, [
                { name: 'Duri', surname: 'Kainsmetke', isFetched: true }
            ]));
            
            p = person;
            next();
        });
    });
    
    // instance update
    s.add(function(next){
        p.surname = 'updated';
        p.update(function(err, person){
            if(err) throw err;
            assert.ok(!!person.id);
            assert.ok(equalData(['id', 'modifiedDT', 'createdDT'], person, [
                { name: 'Duri', surname: 'updated', isFetched: true }
            ]));
            
            next();
        });
    });
    
    // instance remove
    s.add(function(next){
        setTimeout(function(){
            var fakePerson = Person.new({
                id:p.id,
                modifiedDT: new Date()
            });
            
            // try remove not actual version
            fakePerson.remove(function(err){
                assert.ok(err.message.match(/Person.prototype.remove: NOTFOUND.*/));
                assert.ok(err.code === 'NOTFOUND');
                
                p.remove(function(err){
                    if(err) throw err;
                    
                    // check if record exists
                    Person.collection().all(function(err, persons){
                        if(err) throw err;
                        assert.ok(persons.length===0);
                        next();
                    });
                });
            });
        },10);
    });
    
    s.execute(function(err){
        if(err) throw err;
        console.log('MemoryDataSource instance methods - OK');
    });
}