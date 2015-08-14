'use strict';

var mReg = require('./register.js'),
    integrity = require('./integrity.js');

/*
 * Creating relations example:
 *
 * you can describe relation type by arrows or short cuts, its up to you 
 * "-->" = ">--" = "many-to-one" = "belongs-to"
 * "--<" = "<--" = "one-to-many" = "has-many"
 * 
 * Child has key:
 * 1. CmsDoc has many CmsImages
 * 2. CmsImage have to have property CmsImage.docId to store reference to parent CmsDoc.id
 * 3. Adding helper method CmsDoc.images().all(...) - to get related images
 * 4. Adding helper method CmsImage.document().one(...) - to get related documents
 *
 * Model.relations.create('CmsDoc [images] has-many CmsImage.docId [document]', { maintainIntegrity:true, bulkRemove:true });
 * Model.relations.create('CmsDoc [images] one-to-many CmsImage.docId [document]', { maintainIntegrity:true, bulkRemove:true });
 * same as
 * Model.relations.create('CmsImage.docId [document] belongs-to CmsDoc [images]', { maintainIntegrity:true, bulkRemove:true });
 * Model.relations.create('CmsImage.docId [document] many-to-one CmsDoc [images]', { maintainIntegrity:true, bulkRemove:true });
 * same as
 * Model.relations.create('CmsImage.docId [document] --> CmsDoc [images]', { maintainIntegrity:true, bulkRemove:true });
 * Model.relations.create('CmsImage.docId [document] >-- CmsDoc [images]', { maintainIntegrity:true, bulkRemove:true });
 * same as
 * Model.relations.create('CmsDoc [images] <-- CmsImage.docId [document]', { maintainIntegrity:true, bulkRemove:true });
 * Model.relations.create('CmsDoc [images] --< CmsImage.docId [document]', { maintainIntegrity:true, bulkRemove:true });
 *
 *
 * Parent has key - same as above, but parent owns property where are stored references to children models:
 * 1. CmsDoc has many CmsImages
 * 2. CmsDoc have to have property CmsDoc.imageIds to store references to all child CmsImage.id
 * 3. Adding helper method CmsDoc.images().all(...) - to get related images
 * 4. Adding helper method CmsImage.document().one(...) - to get related documents
 *
 * Model.relations.create('CmsDoc.imageIds [images] has-many CmsImage [document]', { maintainIntegrity:true, bulkRemove:true });
 * same as - combinations above
 * 
 */

module.exports = {
    parseOptions: parseOptions,
    create: create
};


/* 
 * there are more styles to describe same relation, we need unify it:
 * 
 * id = 'CmsImage.docId-->CmsDoc'
 * same as id = 'CmsDoc<--CmsImage.docId' need to turn to 'CmsImage.docId-->CmsDoc'
 * same as id = 'CmsImage.docId>--CmsDoc' need to replace ">--" with "-->"
 * same as id = 'CmsDoc--<CmsImage.docId' need to replace "--<" with "<--"
 *
 * replace "many-to-one", "belongs-to" with "-->"
 * replace "one-to-many", "has-many" with "<--"
 * 
 */
function parseOptions(description, opts){
    // description example = 'CmsImage.docId (images) --> CmsDoc (document)'
    
    // replace "many-to-one", "belongs-to" with "-->"
    description = description.replace('many-to-one','-->').replace('belongs-to','-->');
    
    // replace "one-to-many", "has-many" with "<--"
    description = description.replace('one-to-many','<--').replace('has-many','<--');
    
    // remove white spaces
    description = description.replace(/\s/g,'');
    
    // parse type
    var type = description.match(/.+([\-><]{3}).+/);
    if(!type || type.length!==2)
        throw new Error('Unrecognized relation type in "' +description+ '", use "<--","--<","-->",or ">--".');
    
    type = type[1];
    if(['<--','-->','>--','--<'].indexOf(type)<0)
        throw new Error('Unrecognized relation type: "' +type+ '" in "' +description+ '"');
    
    // parse left and right side
    var left = parseKeys(description.split(type)[0]);
    var right = parseKeys(description.split(type)[1]);
    
    function parseKeys(str){
        var as = str.match(/\[(.+)\].*/);
        if(!as || as.length!==2)
            throw new Error('Unrecognized related property name in brackets: "' +as+ '" in "' +description+ '", maybe typo ?');
        as = as[1];
        
        
        var names = str.replace('['+as+']','');
        names = (names.indexOf('"')!==-1) ? names.split('".') : names.split('.');
        
        return {
            model: (names[0] || '').replace(/"/g,''),
            key: names[1],
            as: as
        };
    }
    
    // replace ">--" with "-->", and "--<" with "<--"
    type = type.replace('>--','-->').replace('--<','<--');
    
    // rotate to unify keys
    if(type==='<--'){
        type='-->';
        var tmp = left;
        left=right;
        right=tmp;
    }
    
    // model must be defined on both sides
    if(!left.model || !right.model)
        throw new Error('Model must be defined on both sides of relation: "' +description+ '"');
    
    // check if left, or right key is defined
    if(!left.key && !right.key)
        throw new Error('Undefined key on both sides of relation: "' +description+ '"');
    
    // only one key must be defined
    if(left.key && right.key)
        throw new Error('Confused by relation definition, the key is defined on both sides: "' +description+ '"');
    
    return {
        id: left.model + (left.key ? '.'+left.key : '') + type + right.model + (right.key ? '.' + right.key : ''),
        parent: right,
        child: left,
        opts: opts
    };
}

function create(description, opts){
    var relation = parseOptions(description, opts);
    
    // check if relation already exists
    if(mReg.getRelation(relation.id))
        throw new Error('Same relation already exists: "' +description+ '"');
    
    // check if models are registered
    var Parent = mReg.get(relation.parent.model);
    if(!Parent)
        throw new Error('Cannot find register reference to model "' +relation.parent.model+ '", please use {Model}.define(name) to register model');
    
    var Child = mReg.get(relation.child.model);
    if(!Child)
        throw new Error('Cannot find register reference to model "' +relation.child.model+ '", please use {Model}.define(name) to register model');
    
    // check Parent prototype propery
    if(Parent.prototype[relation.parent.as])
        throw new Error('Cannot create relation, model property name conflict: "' +relation.parent.model+ '.' +relation.parent.as+ '"');
    
    // check if Parent has defined propname in propSchema - typo check
    if(relation.parent.key && !Parent.schema[relation.parent.key])
        throw new Error('Cannot create relation, model "' +relation.parent.model+ '" has not property "' +relation.parent.key+ '" defined in property schema');
    
    // check Child prototype propery
    if(Child.prototype[relation.child.as])
        throw new Error('Cannot create relation, model property name conflict: "' +relation.child.model+ '.' +relation.child.as+ '"');
    
    // check if Parent has defined propname in propSchema - typo check
    if(relation.child.key && !Child.schema[relation.child.key])
        throw new Error('Cannot create relation, model "' +relation.child.model+ '" has not property "' +relation.child.key+ '" defined in property schema');
    
    
    // register relation
    mReg.setRelation(relation.id, relation);
    
    // define models. prototype property
    addParentProp(Parent, relation);
    addChildProp(Child, relation);
    
    // register integrity mantain functions
    integrity.maintain(relation);
}

function addChildProp(Child, relation){
    
    /*
     * read one - read
     * validate
     */
    
    Child.prototype[relation.child.as] = (function(relation){
        return function(){
            var model = this;
            var key = relation.child.key;
            var foreignKey = relation.parent.key;
            var Parent = mReg.get(relation.parent.model);
            
            var query;
            if(key) query = Parent.collection().findId(model[key]);
            else {
                var q = {};
                q[foreignKey] = model.id;
                query = Parent.collection().find(q);
            }
            
            query.validate = function validateParent(obj){
                return Parent.new(obj).validate().validErrs();
            };
            return query;
        };
    })(relation);
}

function addParentProp(Parent, relation){
    
    /*
     * read all - read,
     * create - create,
     * validate
     */
    
    Parent.prototype[relation.parent.as] = (function(relation){
        return function(){
            var model = this;
            var key = relation.parent.key;
            var childKey = relation.child.key;
            var Child = mReg.get(relation.child.model);
            
            var query;
            if(key) query = Child.collection().findId(model[key]);
            else {
                var q = {};
                q[childKey] = model.id;
                query = Child.collection().find(q);
            }
            
            query.validate = function validateChild(obj){
                return Child.new(obj).validate().validErrs();
            };
            query.create = function(dataObj, callback){
                if(Object.prototype.toString.call(dataObj) !== '[object Object]')
                    throw new Error('First argument must be data object');
                
                createChild.call(model, relation, dataObj, callback);
            };
            
            return query;
        };
    })(relation);
    
    function createChild(relation, obj, callback){
        var model = this;
        var key = relation.parent.key;
        var childKey = relation.child.key;
        var Child = mReg.get(relation.child.model);
        
        if(!model.id) {
            callback(new Error('Parent model doesn\'t have "id"').details({ code:'INVALID' }));
        }
        else {
            if(childKey) obj[childKey] = model.id;
            Child.new(obj).create(function(err, newChild){
                if(err) callback(new Error('Model relations: creating child failed').cause(err));
                // if key is defined - parent owns reference to children, he need update
                else if(key){
                    // after child creation, parent need refresh to ensure no other children was added during creation
                    model.constructor.collection().findId(model.id).one(function(err, model){
                        if(err) callback(new Error('Model relations: reading parent failed').cause(err));
                        else {
                            var refArray = model[key] || [];
                            if(!Array.isArray(refArray)){
                                callback(new Error('Key "' + childKey + '" is not array, cannot register child').details({ code:'INVALID' }));
                            }
                            else {
                                refArray.push(newChild.id);
                                model[key] = refArray;
                                model.update(function(err, pModel){
                                    if(err) callback(new Error('Model relations: updating parent failed').cause(err));
                                    else callback(null, newChild);
                                });
                            }
                        }
                    });
                }
                else callback(null, newChild);
            });
        }
    }
}