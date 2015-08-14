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

// load Mongo model
var Mongo = require('../../lib/datasources/Mongo.js');

/*
 * init test Model
 */
var Person = model.define('PersonMDB', ['MongoDataSource'], {
    name:{ isString:true },
    surname:{ isString:true }
});

Person.extendDefaults({
    connection:{
        collection:'persons',
        database:'enterprise_model_test'
    }
});

Person.init();

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
        connection:{
            indexes: {
                id: { 'id':1 },
                createdDT: { 'createdDT':1 }
            },
            collection: 'persons',
            database: 'enterprise_model_test',
            host: 'localhost',
            auto_reconnect: true,
            port: 27017,
            poolSize: 5,
            native_parser: true,
            mongoUrl: 'mongodb://localhost:27017/enterprise_model_test?auto_reconnect=true&poolSize=5&native_parser=true&'
        },
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
            optimisticLock: true,
            shortId:true
        },
        cache: {
            createKey: query.getDefaults().cache.createKey,
            keyPrefix: 'enterprise-model-mongo',
            duration: 1000,
            use: true
        }
    });
    
    console.log('MongoDataSource query builders - OK');
}


/*
 * test collection.query methods
 * cleanCache/clearCache, exec, count, one, all, create, update, remove
 * include options: cacheable and fetch
 */
function testQueryMethods(cb){
    // include cacheable, fetch
    
    var records = [
        { _id:'1', id: '1', name:'Duri', surname:'Kainsmetke' },
        { _id:'2', id: '2', name:'Jozef', surname:'Kozmeker' },
        { _id:'3', id: '3', name: 'Pista', surname: 'Horvat' }
    ];
    
    var s = new async.Series();
    
    // create
    s.add(function(next){
        Person.collection().create(records, function(err, persons){
            if(err) throw err;
            assert.ok(equalData(['modifiedDT', 'createdDT'], persons, [
                { _id:'1', id:'1', name: 'Duri', surname: 'Kainsmetke' },
                { _id:'2', id:'2', name: 'Jozef', surname: 'Kozmeker' },
                { _id:'3', id:'3', name: 'Pista', surname: 'Horvat' }
            ]));
            next();
        });
    });
    
    // one
    s.add(function(next){
        Person.collection().one(function(err, person){
            assert.ok(!err);
            assert.ok(equalData(['modifiedDT', 'createdDT'], person,
                        { _id:'1', id: '1', name: 'Duri', surname: 'Kainsmetke' }));
            next();
        });
    });
    
    // all
    s.add(function(next){
        Person.collection().skip(1).limit(2).all(function(err, persons){
            assert.ok(!err);
            assert.ok(equalData(['modifiedDT', 'createdDT'], persons, [
                { _id:'2', id:'2', name: 'Jozef', surname: 'Kozmeker' },
                { _id:'3', id:'3', name: 'Pista', surname: 'Horvat' }
            ]));
            
            next();
        });
    });
    
    // count
    s.add(function(next){
        Person.collection().count(function(err, count){
            assert.ok(!err);
            assert.ok(count === 3);
            
            next();
        });
    });
    
    // exec
    s.add(function(next){
        Person.collection().exec('findOne', [{},{}], function(err, result){
            if(err) throw err;
            assert.ok(equalData(['modifiedDT', 'createdDT'], result,
                { _id:'1', id: '1', name: 'Duri', surname: 'Kainsmetke' }
            ));
            
            next();
        });
    });
    
    // cache - put
    s.add(function(next){
        Person.collection().cache().all(function(err, persons){
            assert.ok(!err);
            assert.ok(equalData(['modifiedDT', 'createdDT'], persons, [
                { _id:'1', id: '1', name: 'Duri', surname: 'Kainsmetke' },
                { _id:'2', id: '2', name: 'Jozef', surname: 'Kozmeker' },
                { _id:'3', id: '3', name: 'Pista', surname: 'Horvat' }
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
                assert.ok(!err);
                assert.ok(equalData(['modifiedDT', 'createdDT'], persons, [
                    { _id:'1', id: '1', name: 'Duri', surname: 'updated' },
                    { _id:'2', id: '2', name: 'Jozef', surname: 'updated' },
                    { _id:'3', id: '3', name: 'Pista', surname: 'Horvat' }
                ]));
                next();
            });
        });
    });
    
    // remove
    s.add(function(next){
        Person.collection().remove(function(err, count){
            assert.ok(!err);
            assert.ok(count===3);
            
            Person.collection().all(function(err, persons){
                assert.ok(!err);
                assert.ok(persons.length===0);
                next(); 
            });
        });
    });
    
    // resetCache/clearCache
    s.add(function(next){
        Person.collection().cache().all(function(err, persons){
            assert.ok(!err);
            assert.ok(equalData(['modifiedDT', 'createdDT'], persons, [
                { _id:'1', id: '1', name: 'Duri', surname: 'Kainsmetke' },
                { _id:'2', id: '2', name: 'Jozef', surname: 'Kozmeker' },
                { _id:'3', id: '3', name: 'Pista', surname: 'Horvat' }
            ]));
            
            Person.collection().resetCache('all', function(err){
                assert.ok(!err);
                
                Person.collection().cache().all(function(err, persons){
                    assert.ok(!err);
                    assert.ok(persons.length===0);
                    next(); 
                });
            });
        });
    });
    
    
    s.execute(function(err){
        assert.ok(!err);
        console.log('MongoDataSource query methods - OK');
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
            assert.ok(equalData(['id','_id', 'modifiedDT', 'createdDT'], person, [
                { name: 'Duri', surname: 'Kainsmetke' }
            ]));
            
            p = person;
            next();
        });
    });
    
    // instance update
    s.add(function(next){
        p.surname = 'updated';
        p.update(function(err, person){
            assert.ok(!err);
            assert.ok(!!person.id);
            assert.ok(equalData(['id','_id', 'modifiedDT', 'createdDT'], person, [
                { name: 'Duri', surname: 'updated' }
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
                assert.ok(err.message === 'DataSource.prototype.remove: NOTFOUND');
                assert.ok(err.code === 'NOTFOUND');
                
                p.remove(function(err){
                    if(err) throw err;
                    
                    // check if record exists
                    Person.collection().all(function(err, persons){
                        assert.ok(!err);
                        assert.ok(persons.length===0);
                        next();
                    });
                });
            });
        },10);
    });
    
    s.execute(function(err){
        assert.ok(!err);
        console.log('MongoDataSource instance methods - OK');
    });
}