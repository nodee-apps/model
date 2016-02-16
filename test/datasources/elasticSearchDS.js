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

// load Rest and ElasticSearch data sources
require('../../lib/datasources/Rest.js');
require('../../lib/datasources/ElasticSearch.js');


module.exports = function(baseUrl){
    baseUrl = baseUrl || 'http://localhost:9200';

    /*
     * init test Model
     */
    var Person = model.define('PersonES', ['ElasticSearchDataSource'], {
        name:{ isString:true },
        surname:{ isString:true }
    });

    Person.extendDefaults({
        connection:{
            baseUrl: baseUrl,
            index: 'test_index', // elasticsearch index
            type: 'test_type', // elasticsearch type

            // elasticsearch data mappings
            //mapping:{
            //    id:{ type:'string', 'index':'not_analyzed' },
            //    /* example mapping of nested schema:
            //     * variants:{
            //     *    properties:{
            //     *        sku:{ type:'string', 'index':'not_analyzed' },
            //     *        ean:{ type:'string', 'index':'not_analyzed' },
            //     *        internal_code:{ type:'string', 'index':'not_analyzed' }
            //     *    }
            //     * }
            //     */
            //}
        }
    });

    // default timeout to wait for elasticsearch to ensure data will be indexed
    var timeout = 2000;

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
    Person.init(function(){
        testCollectionMethods(testInstanceMethods);
    });

    /*
     * test collection methods
     */
    function testCollectionMethods(cb){
        // include cacheable, fetch

        var records = [
            { id: '1', name:'Duri', surname:'Kainsmetke' },
            { id: '2', name:'Jozef', surname:'Kozmeker' },
            { id: '3', name: 'Pista', surname: 'Horvat' }
        ];

        var s = new async.Series();


        // remove all documents from index to ensure correct results for testing
        s.add(function(next){
            Person.collection().remove(function(err, count){
                if(err) throw err;

                console.log('ElasticSearchDataSource: collection.remove all - OK');
                setTimeout(next, timeout);
            });
        });


        // create
        s.add(function(next){
            Person.collection().create(records, function(err, persons){
                if(err) throw err;

                assert.ok(equalData(['modifiedDT', 'createdDT'], persons, [
                    { id:'1', name: 'Duri', surname: 'Kainsmetke' },
                    { id:'2', name: 'Jozef', surname: 'Kozmeker' },
                    { id:'3', name: 'Pista', surname: 'Horvat' }
                ]));

                console.log('ElasticSearchDataSource: collection.create - OK');
                next();
            });
        });

        // one
        s.add(function(next){
            setTimeout(function(){
                Person.collection().one(function(err, person){
                    if(err) throw err;

                    assert.ok(equalData(['modifiedDT', 'createdDT'], person,
                                { id: '1', name: 'Duri', surname: 'Kainsmetke' }));

                    console.log('ElasticSearchDataSource: collection.one - OK');
                    next();
                });
            }, timeout);
        });

        // all
        s.add(function(next){
            Person.collection().skip(1).limit(2).all(function(err, persons){
                if(err) throw err;

                assert.ok(equalData(['modifiedDT', 'createdDT'], persons, [
                    { id:'2', name: 'Jozef', surname: 'Kozmeker' },
                    { id:'3', name: 'Pista', surname: 'Horvat' }
                ]));

                console.log('ElasticSearchDataSource: collection.all - OK');
                next();
            });
        });

        // count
        s.add(function(next){
            Person.collection().count(function(err, count){
                if(err) throw err;
                assert.ok(count === 3);

                console.log('ElasticSearchDataSource: collection.count - OK');
                next();
            });
        });

        // TODO: test exec method
        // exec
        //s.add(function(next){
        //    Person.collection().exec('findOne', [{},{}], function(err, result){
        //        if(err) throw err;
        //        assert.ok(equalData(['modifiedDT', 'createdDT'], result,
        //            { id: '1', name: 'Duri', surname: 'Kainsmetke' }
        //        ));
        //        
        //        next();
        //    });
        //});

        // cache - put
        s.add(function(next){
            Person.collection().cache().all(function(err, persons){
                if(err) throw err;
                assert.ok(equalData(['modifiedDT', 'createdDT'], persons, [
                    { id: '1', name: 'Duri', surname: 'Kainsmetke' },
                    { id: '2', name: 'Jozef', surname: 'Kozmeker' },
                    { id: '3', name: 'Pista', surname: 'Horvat' }
                ]));

                console.log('ElasticSearchDataSource: collection.cache put - OK');
                next();
            });
        });

        // update
        s.add(function(next){
            Person.collection().findId(['1','2']).update({ surname:'updated' }, function(err, count){
                if(err) throw err;
                assert.ok(count===2);

                setTimeout(function(){
                    Person.collection().all(function(err, persons){
                        if(err) throw err;

                        assert.ok(equalData(['modifiedDT', 'createdDT'], persons, [
                            { id: '1', name: 'Duri', surname: 'updated' },
                            { id: '2', name: 'Jozef', surname: 'updated' },
                            { id: '3', name: 'Pista', surname: 'Horvat' }
                        ]));

                        console.log('ElasticSearchDataSource: collection.update - OK');
                        next();
                    });
                }, timeout);
            });
        });

        // remove
        s.add(function(next){
            Person.collection().remove(function(err, count){
                if(err) throw err;
                assert.ok(count===3);

                setTimeout(function(){
                    Person.collection().all(function(err, persons){
                        assert.ok(!err);
                        assert.ok(persons.length===0);

                        console.log('ElasticSearchDataSource: collection.remove - OK');
                        next(); 
                    });
                }, timeout);
            });
        });

        // resetCache/clearCache
        s.add(function(next){
            Person.collection().cache().all(function(err, persons){
                if(err) throw err;
                assert.ok(equalData(['modifiedDT', 'createdDT'], persons, [
                    { id: '1', name: 'Duri', surname: 'Kainsmetke' },
                    { id: '2', name: 'Jozef', surname: 'Kozmeker' },
                    { id: '3', name: 'Pista', surname: 'Horvat' }
                ]));

                Person.collection().resetCache('all', function(err){
                    if(err) throw err;

                    Person.collection().cache().all(function(err, persons){
                        if(err) throw err;

                        assert.ok(persons.length===0);

                        console.log('ElasticSearchDataSource: collection.resetCache - OK');
                        next(); 
                    });
                });
            });
        });


        s.execute(function(err){
            assert.ok(!err);
            console.log('ElasticSearchDataSource: query methods - OK');
            cb();
        });
    }


    /*
     * test model instance methods
     * create, update, remove
     */
    function testInstanceMethods(){

        var p;
        var s = new async.Series;

        // instance create
        s.add(function(next){
            Person.new().fill({ name:'Duri', surname:'Kainsmetke' }).create(function(err, person){
                if(err) throw err;
                assert.ok(!!person.id);
                assert.ok(equalData(['id', 'modifiedDT', 'createdDT'], person, [
                    { name: 'Duri', surname: 'Kainsmetke' }
                ]));

                p = person;

                console.log('ElasticSearchDataSource: instance.create - OK');
                setTimeout(next, timeout);
            });
        });

        // instance update
        s.add(function(next){
            p.surname = 'updated';

            p.update(function(err, person){
                if(err) throw err;
                assert.ok(!!person.id);
                assert.ok(equalData(['id', 'modifiedDT', 'createdDT'], person, [
                    { name: 'Duri', surname: 'updated' }
                ]));

                p = person;

                console.log('ElasticSearchDataSource: instance.update - OK');
                setTimeout(next, timeout);
            });
        });

        // instance remove
        s.add(function(next){
            var fakePerson = Person.new({
                id:p.id,
                modifiedDT: new Date()
            });

            // try remove not actual version
            fakePerson.remove(function(err){
                assert.ok(err.message === 'PersonES.prototype.remove: NOTFOUND');
                assert.ok(err.code === 'NOTFOUND');

                p.remove(function(err){
                    if(err) throw err;

                    // check if record exists
                    setTimeout(function(){
                        Person.collection().all(function(err, persons){
                            if(err) throw err;
                            assert.ok(persons.length===0);

                            console.log('ElasticSearchDataSource: instance.remove - OK');
                            next();
                        });
                    }, timeout);
                });
            });
        });

        s.execute(function(err){
            if(err) throw err;
            console.log('ElasticSearchDataSource instance methods - OK');
        });
    }
};