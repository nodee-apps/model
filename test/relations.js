'use strict';

var assert = require('assert'),
    Model = require('../lib/model.js'),
    relations = require('../lib/relations.js'),
    Series = require('enterprise-utils').async.Series;
    
// load model extensions
require('../lib/extensions/defaults.js');
require('../lib/extensions/methods.js');
require('../lib/extensions/queries.js');
require('../lib/extensions/hooks.js');
require('../lib/extensions/validations.js');

// load datasources model
require('../lib/datasources/DataSource.js');
require('../lib/datasources/Memory.js');

/*
 * run tests
 */
testParseOptions();
testRelationHlpMethods(function(){
    testIntegrityChild(testIntegrityParent);
});


/*
 * relation.parseOptions
 */
function testParseOptions(){
    
    function parse(str, opts){
        try {
            return relations.parseOptions(str, opts);
        }
        catch(err){
            return err.message;
        }
    }
    
    // wrong type
    assert.ok(parse('CmsImage.docId [document] >- CmsDoc [images]') ===
              'Unrecognized relation type in "CmsImage.docId[document]>-CmsDoc[images]", use "<--","--<","-->",or ">--".');
    
    // missing model
    assert.ok(parse('.docId [document] >-- CmsDoc [images]') ===
              'Model must be defined on both sides of relation: ".docId[document]>--CmsDoc[images]"');
    
    // missing key
    assert.ok(parse('CmsImage. [document] >-- CmsDoc [images]') ===
              'Undefined key on both sides of relation: "CmsImage.[document]>--CmsDoc[images]"');
    
    // missing prop
    assert.ok(parse('CmsImage.docId >-- CmsDoc [images]') ===
              'Unrecognized related property name in brackets: "null" in "CmsImage.docId>--CmsDoc[images]", maybe typo ?');
    
    // rotate id
    assert.ok(parse('CmsImage.docId [document] <-- CmsDoc [images]').id === 'CmsDoc-->CmsImage.docId');
    
    // is same as
    assert.ok(parse('"CmsImage".docId [document] many-to-one CmsDoc [images]').id === 'CmsImage.docId-->CmsDoc');
    assert.ok(parse('CmsImage.docId [document] belongs-to CmsDoc [images]').id === 'CmsImage.docId-->CmsDoc');
    
    // is same as
    assert.ok(parse('"CmsImage".docId [document] --> CmsDoc [images]').id === 'CmsImage.docId-->CmsDoc');
    assert.ok(parse('CmsImage.docId [document] >-- CmsDoc [images]').id === 'CmsImage.docId-->CmsDoc');
    
    // is same as
    assert.ok(parse('CmsImage.docId [document] <-- CmsDoc [images]').id === 'CmsDoc-->CmsImage.docId');
    assert.ok(parse('CmsImage.docId [document] --< CmsDoc [images]').id === 'CmsDoc-->CmsImage.docId');
    
    // is same as
    assert.ok(parse('CmsDoc [images] one-to-many CmsImage.docId [document]').id === 'CmsImage.docId-->CmsDoc');
    assert.ok(parse('CmsDoc [images] has-many CmsImage.docId [document]').id === 'CmsImage.docId-->CmsDoc');
    
    // parse opts
    assert.deepEqual(parse('CmsImage.docId [document] --> CmsDoc [images]', { blabla:'asd' }),
                {
                    id: 'CmsImage.docId-->CmsDoc',
                    parent: { model: 'CmsDoc', key: undefined, as: 'images' },
                    child: { model: 'CmsImage', key: 'docId', as: 'document' },
                    opts: { blabla:'asd' } }
    );
    
    console.log('relation.parseOptions - OK');
}


/*
 * Relation defined properties tests:
 *
 * 1. one-to-one - parent().one(parent)
 * 2. one-to-one - parent.valitade(parent)
 * 
 * 3. one-to-many - children().all([children])
 * 4. one-to-many - children().create(child)
 * 5. one-to-many - children().validate(child)
 * 
 */

function testRelationHlpMethods(cb){

    var Parent = Model.define('P1', ['MemoryDataSource'], {
        content:{}
    });
    Parent.extendDefaults({ connection:{ collection:'parents' } });
    
    var Child = Model.define('CH1', ['MemoryDataSource'], {
        parentId:{ isString:true },
        content:{}
    });
    Child.extendDefaults({ connection:{ collection:'children' } });
    
    relations.create('CH1.parentId [parent] --> P1 [children]',{
        maintainIntegrity: true,
        required: true,
        bulkRemove: false
    });
    
    // same relation cannot be created more than one time
    assert.throws(function(){
        relations.create('CH1.parentId [parent] --> P1 [children]',{
            maintainIntegrity: true,
            require: true,
            bulkRemove: false
        });
    }, 'Same relation already exists: "CH1.parentId-->P1"');
    
    // relation cannot use properties not defined in Model.propSchema
    assert.throws(function(){
        relations.create('CH1.fakeProp [parent1] --> P1 [children1]',{
            maintainIntegrity: true,
            require: true,
            bulkRemove: false
        });
    }, 'Cannot create relation, model "CH1" has not property "fakeProp" defined in property schema');
    
    // relation cannot use already defined properties
    assert.throws(function(){
        relations.create('CH1.content [parent] --> P1 [children]',{
            maintainIntegrity: true,
            require: true,
            bulkRemove: false
        });
    }, 'Cannot create relation, model property name conflict: "P1.children"');
    
    
    var parent = Parent.new().fill({
        id:'p1',
        content:'I am Parent'
    }).validate();
    
    var child = Child.new().fill({
        id:'c1',
        parentId:'p1',
        content:'I am Child of Parent "p1"'
    }).validate();
    
    parent.create(function(err, pDoc){
        if(err) throw err;
        parent = pDoc;
        child.create(function(err, cDoc){
            if(err) throw err;
            
            checkProps();
        });
    });
    
    function checkProps(){
        var s = new Series();
        
        // 1. one-to-one - parent().one(err, parent)
        s.add(function(next){
            child.parent().one(function(err, pDoc){
                assert.ok(pDoc.id === 'p1');
                assert.ok(pDoc.content === 'I am Parent');
                next();
            });
        });
        
        // 2. one-to-one - parent.valitade(parent)
        s.add(function(next){
            var valid = child.parent().validate({ id:999 });
            assert.deepEqual(valid.id, ['isString']);
            next();
        });
        
        // 3. one-to-many - children().all(err, [children])
        s.add(function(next){
            parent.children().all(function(err, children){
                assert.ok(children.length === 1);
                assert.ok(children[0].id === 'c1');
                next();
            });
        });
        
        // 4. one-to-many - children().create(child)
        s.add(function(next){
            parent.children().create({ id:'c2' }, function(err, newChild){
                if(err) throw err;
                assert.ok(newChild.id === 'c2');
                next();
            });
        });
        
        // 5. one-to-many - children().validate(child)
        s.add(function(next){
            var valid = parent.children().validate({ id:999 });
            assert.deepEqual(valid.id, ['isString']);
            next();
        });
        
        // execute
        s.execute(function(err){
            if(err) throw err;
            
            // remove parent, children
            parent.remove(function(err){
                if(err) throw err;
                console.log('relation defined properties - OK');
                cb();
            });
        });
    }
}


/*
 * Integrity-maintainer CRUD tests:
 *
 * (include integrity-maintainer settings ["create","update","remove"], required=true/false, bulkRemove=true/false)
 * 1. Child.on('beforeCreate', check_ref_to_parent(relation)) or require_ref_to_parent, when required=true
 * 2. Child.on('beforeUpdate', check_ref_to_parent(relation))
 * 3. Parent.on('afterRemove', remove_children(relation)
 * 4. Child.on('afterRemove', remove_ref_in_parent(relation))
 * 5. Parent.on('beforeCreate', check_ref_to_children(relation)) or require_ref_to_children, when required=true
 * 6. Parent.on('beforeUpdate', check_ref_to_children(relation))
 * 
 */

/*
 * relations integrity maintain key on Child
 */
function testIntegrityChild(cb){
    testIntegrity({
        pName:'IP1',
        chName:'ICH1',
        p_id:'ip1',
        ch_id:'ich1',
        relation:{
            description: 'ICH1.parentId [parent] --> IP1 [children]',
            opts:{
                maintainIntegrity: true,
                required: true,
                bulkRemove: false
            }
        },
        afterCreate: function(pDoc, cDoc){
            assert.ok(cDoc.getData().parentId === 'ip1');
            assert.ok(cDoc.getData().id === 'ich1');
            assert.ok(pDoc.getData().id === 'ip1');
        },
        afterUpdate: function(err, cDoc){
            assert.ok(err.code === 'INVALID');
            assert.ok(err.message === 'Model integrity: Cannot update or create, parent model "fake_parent_id" does not exists');
        },
        onRemove: function(parent, child, next){
            parent.remove(function(err, pDoc){
                if(err) throw err;
                child.constructor.collection().all(function(err, result){
                    assert.ok(result.length === 0);
                    next();
                });
            });
        }
    },
    function(){
        console.log('relations integrity, key on Child - OK');
        cb();
    });
}

/*
 * relations integrity maintain key on Parent
 */
function testIntegrityParent(){
    testIntegrity({
        pName:'IP2',
        chName:'ICH2',
        p_id:'ip2',
        ch_id:'ich2',
        relation:{
            description: 'ICH2 [parent] --> IP2.childIds [children]',
            opts:{
                maintainIntegrity: true,
                required: true,
                bulkRemove: false
            }
        },
        afterCreate: function(pDoc, cDoc){
            assert.ok('ich2' === cDoc.getData().id);
            assert.deepEqual(['ich2'], pDoc.getData().childIds);
            assert.ok('ip2' === pDoc.getData().id);
        },
        afterUpdate: function(err, cDoc){
            if(err) throw err;
            assert.ok(cDoc.parentId === 'fake_parent_id');
        },
        onRemove: function(parent, child, next){
            child.remove(function(err){
                if(err) throw err;
                
                parent.constructor.collection().findId(parent.id).one(function(err, pDoc){
                    if(err) throw err;
                    
                    assert.ok(pDoc.childIds.length === 0);
                    next();
                });
                
            });
        }
    },
    function(){
        console.log('relations integrity, key on Parent - OK');
    });
}

function testIntegrity(opts, callback){
    
    var pName = opts.pName;
    var chName = opts.chName;
    var p_id = opts.p_id;
    var ch_id = opts.ch_id;
    var relDesc = opts.relation.description;
    var relOpts = opts.relation.opts;
    var afterCreate = opts.afterCreate;
    var afterUpdate = opts.afterUpdate;
    var onRemove = opts.onRemove;

    var reqRelation = defineRelation(pName, chName, relDesc, relOpts);
    
    var Parent = reqRelation.Parent;
    var Child = reqRelation.Child;
    
    var parent = Parent.new({
        id: p_id,
        content:'I am Parent'
    }).validate();
    
    var child = Child.new({
        id: ch_id,
        //parentId:'p1',
        content:'I am Child of Parent "' +p_id+ '"'
    }).validate();
    
    var s = new Series();
    
    // create
    s.add(function(next){
        parent.create(function(err, pDoc){
            if(err) throw err;
            parent = pDoc;
            
            parent.children().create(child, function(err, cDoc){
                if(err) throw err;
                
                child = cDoc;
                cDoc.parent().one(function(err, pDoc){
                    afterCreate(pDoc, cDoc);
                    next();
                });
            });    
        });
    });
    
    // update - check ref to parent
    s.add(function(next){
        child.parentId = 'fake_parent_id';
        child.update(function(err, cDoc){
            afterUpdate(err, cDoc);
            next();
        });
    });
    
    // remove
    s.add(function(next){
        onRemove(parent, child, next);
    });
    
    s.execute(function(err){
        if(err) throw err;
        
        callback();
    });
}

function defineRelation(pName, chName, relDesc, intOpts){
    
    intOpts = intOpts || {
        maintainIntegrity:true,
        required:true,
        bulkRemove:false
    };
    
    var Parent = Model.define(pName, ['MemoryDataSource'], {
        childIds:{ isArray:true },
        content:{}
    });
    Parent.extendDefaults({ connection:{ collection:'parents' } });
    
    var Child = Model.define(chName, ['MemoryDataSource'], {
        parentId:{ isString:true },
        content:{}
    });
    Child.extendDefaults({ connection:{ collection:'children' } });
    
    relations.create(relDesc,{
        maintainIntegrity: intOpts.maintainIntegrity,
        required: intOpts.required,
        bulkRemove: intOpts.bulkRemove
    });
    
    return { Child:Child, Parent:Parent };
}