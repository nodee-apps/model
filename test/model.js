'use strict';

var assert = require('assert'),
    model = require('../lib/model.js');
    
// load model extensions
require('../lib/extensions/defaults.js');
require('../lib/extensions/methods.js');
require('../lib/extensions/queries.js');
require('../lib/extensions/hooks.js');
require('../lib/extensions/validations.js');
    
/*
 * run tests
 */
testSchema();
testDefaults();
testMethods();
testQueries();
testHooks();
testValidation();

/*
 * Test Model and register methods:
 *
 * 1. model.define
 * 2. Schema inheritance
 *
 */
function testSchema(){
    
    var SuperParent = model.define({
        superProp:{ isString:true }
    });
    
    var Parent = model.define({
        parentProp:{ isString:true }
    });
    
    var Inherited = model.define([SuperParent, Parent], {
        prop:{ isString:true },
        parentProp:{ required:true }
    });
    
    assert.deepEqual(SuperParent.schema, { superProp:{ isString:true } });
    assert.deepEqual(Parent.schema, { parentProp:{ isString:true } });
    assert.deepEqual(Inherited.schema, {
        superProp:{ isString:true },
        parentProp:{ isString:true, required:true },
        prop:{ isString:true }
    });
    
    console.log('model define - OK');
}

/*
 * model defaults extension
 */
function testDefaults(){
    var SuperParent = model.define({}).setDefaults({
        superDefault:{ testing:true, superParent:true }
    });
    
    var Parent = model.define({}).setDefaults({
        parentDefault:{ testing:true }
    });
    
    var Inherited = model.define([SuperParent, Parent], {}).extendDefaults({
        superDefault:{ testing:false, tested:'ok' },
        parentDefault:{},
        defaults:{ someProp:'value' }
    });
    
    assert.deepEqual(SuperParent.getDefaults(), { superDefault:{ testing:true, superParent:true } });
    assert.deepEqual(Parent.getDefaults(), { parentDefault:{ testing:true } });
    assert.deepEqual(Inherited.getDefaults(), {
        superDefault:{ testing:false, tested:'ok', superParent:true },
        parentDefault:{ testing:true },
        defaults:{ someProp:'value' }
    });
    
    console.log('model defaults - OK');
}

/*
 * model defaults extension
 */
function testMethods(){
    var SuperParent = model.define({});
    function superMethod(){}
    SuperParent.addMethod(['m1', 'm2'], superMethod);
    
    var Parent = model.define({});
    function parentMethod(){}
    Parent.addMethod(['m2', 'm3'], parentMethod);
    
    var Inherited = model.define([SuperParent, Parent], {});
    function inheritedMethod(){}
    Inherited.addMethod('m3', inheritedMethod);
    
    assert.ok(SuperParent.m1 === superMethod && SuperParent.m2 === superMethod);
    assert.ok(Parent.m2 === parentMethod && Parent.m3 === parentMethod);
    assert.ok(Inherited.m1 === superMethod && Inherited.m2 === parentMethod && Inherited.m3 === inheritedMethod);
    
    console.log('model methods - OK');
}

/*
 * model queries extension
 */
function testQueries(){
    var SuperParent = model.define({});
    
    // define super parent query methods
    function superQuery(){ this.extendDefaults({ superQuery:true }); return this; }
    function superRead(cb){ return cb(null, 'superRead result'); }
    function superCreate(cb){ return cb(null, 'superCreate result'); }
    
    // attach super parent query methods
    SuperParent.Collection.addMethod(['sq', 'sq_alias'], superQuery);
    SuperParent.Collection.addMethod('all', superRead);
    SuperParent.Collection.addMethod('create', superCreate);
    
    
    var Parent = model.define({});
    
    // define parent query methods
    function parentQuery(){ this.extendDefaults({ parentQuery:true }); return this; }
    function parentRead(cb){ cb(null, 'parentRead result'); }
    function parentUpdate(cb){ cb(null, 'parentUpdate result'); }
    
    // attach parent query methods
    Parent.Collection.addMethod(['sq_alias', 'pq'], parentQuery);
    Parent.Collection.addMethod('all', parentRead);
    Parent.Collection.addMethod('update', parentUpdate);
    
    
    var Inherited = model.define([SuperParent, Parent], {});
    
    // define inherited model query methods
    function inheritedQuery(){ this.extendDefaults({ inheritedQuery:true }); return this; }
    function inheritedUpdate(cb){ cb(null, 'inheritedUpdate result'); }
    
    // replace some inherited query methods
    Inherited.Collection.addMethod(['pq', 'iq'], inheritedQuery);
    Inherited.Collection.addMethod('update', inheritedUpdate);
    
    
    /*
     * test super parent collection methods
     */
    assert.deepEqual(SuperParent.collection().sq().getDefaults(), { superQuery:true });
    assert.deepEqual(SuperParent.collection().sq_alias().getDefaults(), { superQuery:true });
    SuperParent.collection().all(function(err, data){
        assert.ok(!err);
        assert.ok(data === 'superRead result');
    });
    SuperParent.collection().create(function(err, data){
        assert.ok(!err);
        assert.ok(data === 'superCreate result');
    });
    
    /*
     * test parent collection methods
     */
    assert.ok(!Parent.collection().sq);
    assert.ok(!Parent.collection().create);
    assert.deepEqual(Parent.collection().sq_alias().getDefaults(), { parentQuery:true });
    assert.deepEqual(Parent.collection().pq().getDefaults(), { parentQuery:true });
    Parent.collection().all(function(err, data){
        assert.ok(!err);
        assert.ok(data === 'parentRead result');
    });
    Parent.collection().update(function(err, data){
        assert.ok(!err);
        assert.ok(data === 'parentUpdate result');
    });
    
    /*
     * test inherited collection methods
     */
    assert.deepEqual(Inherited.collection().sq().getDefaults(), { superQuery:true });
    assert.deepEqual(Inherited.collection().sq_alias().getDefaults(), { parentQuery:true });
    assert.deepEqual(Inherited.collection().pq().getDefaults(), { inheritedQuery:true });
    assert.deepEqual(Inherited.collection().iq().getDefaults(), { inheritedQuery:true });
    
    Inherited.collection().create(function(err, data){
        assert.ok(!err);
        assert.ok(data === 'superCreate result');
    });
    Inherited.collection().all(function(err, data){
        assert.ok(!err);
        assert.ok(data === 'parentRead result');
    });
    Inherited.collection().update(function(err, data){
        assert.ok(!err);
        assert.ok(data === 'inheritedUpdate result');
    });
    
    console.log('model queries - OK');
}

/*
 * model hooks extension
 */
function testHooks(){
    var hookOrder = [];
    var SuperParent = model.define({});
    SuperParent.prototype.create =
    SuperParent.wrapHooks('create', function(cb){
        hookOrder.push('create by SuperParent');
        cb();
    });
    
    // inherit model from SuperParent, and overwrite his hook method
    var Parent = model.define(SuperParent, {});
    Parent.prototype.create =
    Parent.wrapHooks('create', function(cb){
        hookOrder.push('create by Parent');
        cb();
    });
    
    // run all hooked methods
    SuperParent.on('beforeCreate', function(next){ hookOrder.push('beforeCreate on SuperParent {1}'); next(null); });
    SuperParent.on('beforeCreate', function(next){ hookOrder.push('beforeCreate on SuperParent {2}'); next(); });
    
    Parent.on('beforeCreate', function(next){ hookOrder.push('beforeCreate on Parent {1}'); next(undefined); });
    Parent.on('beforeCreate', function(next){ hookOrder.push('beforeCreate on Parent {2}'); next(); });
    
    // define inherited model
    var Inherited = model.define([SuperParent, Parent], {});
    
    Inherited.on('beforeCreate', function(next){ hookOrder.push('beforeCreate on Inherited {1}'); next(); });
    Inherited.on('beforeCreate', function(next){ hookOrder.push('beforeCreate on Inherited {2}'); next(); });
    Inherited.on('afterCreate', function(args, next){ hookOrder.push('afterCreate on Inherited {1}'); next(); });
    Inherited.on('afterCreate', function(args, next){ hookOrder.push('afterCreate on Inherited {2}'); next(); });
    
    
    // create SuperParent record
    new SuperParent().create(function(err){
        assert.ok(!err);
        assert.deepEqual(hookOrder, [   'beforeCreate on SuperParent {1}',
                                        'beforeCreate on SuperParent {2}',
                                        'create by SuperParent' ]);
        
        createParent();
    });
    
    function createParent(){
        // reset hooks order
        hookOrder = [];
        
        new Parent().create(function(err){
            assert.ok(!err);
            assert.deepEqual(hookOrder, [   'beforeCreate on Parent {1}',
                                            'beforeCreate on Parent {2}',
                                            'create by Parent' ]);
            
            createInherited();
        });
    }
    
    function createInherited(){
        // reset hooks order
        hookOrder = [];
        
        new Inherited().create(function(err){
            assert.ok(!err);
            assert.deepEqual(hookOrder, [   'beforeCreate on SuperParent {1}',
                                            'beforeCreate on SuperParent {2}',
                                            'beforeCreate on Parent {1}',
                                            'beforeCreate on Parent {2}',
                                            'beforeCreate on Inherited {1}',
                                            'beforeCreate on Inherited {2}',
                                            'create by Parent',
                                            'afterCreate on Inherited {1}',
                                            'afterCreate on Inherited {2}'  ]);
            
            console.log('model hooks - OK');
        });
    }    
}

/*
 * model validation extension
 * methods: getData, fill, toJSON, validate
 */
function testValidation(){
    
    var now = new Date();
    var ComplexModel = model.define({
        hidden:{ hidden:true },
        required:{ required:true },
        string:{ isString:true },
        array:{ isArray:true },
        bool:{ isBoolean: true },
        stringLength:{ minLength:2, maxLength:4 },
        arrayLength:{ minLength:2, maxLength:4 },
        date:{ parseDate:'yyyy-MM-dd'  },
        dateString:{ toDateString: 'yyyy-MM-dd' },
        object:{
            keys: function(value){ return value.replace('$',''); },
            values: { isString:true }
        },
        subModel:{
            model: model.define({
                hidden:{ hidden:true },
                name:{ isString:true },
                list:{
                    arrayOf: model.define({
                        name:{ isString:true }    
                    })
                }
            })
        },
        constant:{ setValue: 'constant value' },
        default:{ defaultValue: 'default value' },
        sanitize:{ sanitize: function(value){ return 'sanitized value'; } },
        round:{ round: 3 },
        cleanUrl: { cleanUrl:true }
    });
    
    var validValues = {
        propToSlice: true, // will be sliced, no matter what data s inside
        hidden: 'hidden',
        required: 'required',
        string: 'string',
        array: [ 'item1', 'item2' ],
        bool: true,
        stringLength: '12',
        arrayLength: [ 1,2,3,4 ],
        date: '2014-10-23',
        dateString: now,
        object:{
            '$key1': 'value1',
            '$key2': 'value2'
        },
        subModel:{
            propToSlice: true,
            hidden: 'hidden',
            name: 'name',
            list:[ { name: 'item1' }, { name:'item2' } ]
        },
        constant: 'random value',
        default: '',
        sanitize: 'random value',
        round: 1.123456,
        cleanUrl: '+ľščťžýáíé %^&*()!@#$%="?,.<>'
    };
    
    var filledValues = {
        hidden: 'hidden',
        required: 'required',
        string: 'string',
        array: [ 'item1', 'item2' ],
        bool: true,
        stringLength: '12',
        arrayLength: [ 1,2,3,4 ],
        date: '2014-10-23',
        dateString: now,
        object:{
            '$key1': 'value1',
            '$key2': 'value2'
        },
        subModel:{
            hidden: 'hidden',
            name: 'name',
            list:[ { name: 'item1' }, { name:'item2' } ]
        },
        constant: 'random value',
        default: '',
        sanitize: 'random value',
        round: 1.123456,
        cleanUrl: '+ľščťžýáíé %^&*()!@#$%="?,.<>'
    };
    
    var afterValidation = {
        hidden: 'hidden',
        required: 'required',
        string: 'string',
        array: [ 'item1', 'item2' ],
        bool: true,
        stringLength: '12',
        arrayLength: [ 1, 2, 3, 4 ],
        //date: now,
        //dateString: '2014-10-05',
        object: { 'key1': 'value1', 'key2': 'value2' },
        subModel: { hidden: 'hidden', name: 'name', list: [ { name: 'item1' }, { name:'item2' } ] },
        constant: 'constant value',
        default: 'default value',
        sanitize: 'sanitized value',
        round: 1.123,
        cleanUrl: 'lsctzyaie-.'
    }
    
    var invalidValues = {
        propToSlice: true, // will be sliced, type doesn't matter
        hidden: 'hidden', // can be anything
        required: null, // have to be defined
        string: true,
        array: {},
        bool: 'true',
        stringLength: '1',
        arrayLength: [ 1,2,3,4,5 ],
        date: '20145-10-23',
        dateString: '...',
        object:{ // check non object
            '$key1': true,
            '$key2': false
        },
        subModel:{ // check non object
            propToSlice: true, // will be sliced, type doesn't matter
            hidden: 'hidden', // can be anything
            name: true,
            list:[ { name: true }, undefined ]
        },
        // constant: true, // can be anything
        // default: '', // can be anything
        // sanitize: 'random value',
        round: '1.123456', // have to be number
        // cleanUrl: '+ľščťžýáíé %^&*()!@#$%="?,.<>' // can be anything, always converted to string
    };
    
    /*
     * fill
     */
    var m = ComplexModel.new();
    assert.deepEqual(m.getData(),{}); // model is empty
    
    m.fill(validValues);
    assert.ok(m.subModel.__instanceof === 'Model');
    assert.ok(m.subModel.list[0].__instanceof === 'Model');
    assert.ok(m.subModel.list[1].__instanceof === 'Model');
    
    /*
     * getData
     */
    var data = m.getData();
    assert.deepEqual(data, filledValues);
    m.hide('string'); // hide
    assert.ok(m.getData().string === undefined);
    m.hide('string', false); // unhide
    
    /*
     * validate - valid data
     */
    m.validate();
    assert.ok(m.isValid());
    assert.deepEqual(m.validErrs(), {});
    
    data = m.getData();
    afterValidation.date = data.date; // TODO: date testing
    afterValidation.dateString = data.dateString; // TODO: date testing
    assert.deepEqual(m.getData(), afterValidation);
    
    /*
     * toJSON - hidden:true
     */
    var jsonData = JSON.parse(JSON.stringify(m));
    assert.ok(jsonData.hidden === undefined);
    assert.ok(jsonData.subModel.hidden === undefined);
    
    /*
     * validate - invalid data
     */
    var im = ComplexModel.new();
    im.fill(invalidValues).validate();
    
    assert.ok(im.isValid()===false);
    assert.deepEqual(im.validErrs(), {
        required: [ 'required' ],
        string: [ 'isString' ],
        array: [ 'isArray' ],
        bool: [ 'isBoolean' ],
        stringLength: [ 'minLength' ],
        arrayLength: [ 'maxLength' ],
        date: [ 'parseDate' ],
        dateString: [ 'toDateString' ],
        object:[ { key1: [ 'isString' ], key2: [ 'isString' ] } ],
        subModel: [ { name: [ 'isString' ], list: [ [ { name: [ 'isString' ] } ] ] } ],
        round: [ 'round' ]
    });
    
    console.log('model validations - OK');
}
