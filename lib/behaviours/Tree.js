'use strict';

var Model = require('../model.js'),
    async = require('nodee-utils').async;


var Tree = Model.define('Tree', {
    ancestors:{ isArray: true },
    ancestorsCount:{ isInteger:true },
    children:{ isArray:true },
    childrenCount:{ isInteger:true }
});
    
Tree.extendDefaults({
    db:{ // if repository is mongodb, dont forget to create index for sorting tree
        indexes:{
            ancestors:{ 'ancestors':1 },
            ancestorsCount:{ 'ancestorsCount':1 }
        }
    },
    options:{
        // if you want to store and synchronize children references,
        // sometimes it is useful, but keep in mind that every create, update, remove is querying the datasource more times,
        // so make performance test if you plan to build big tree structures (thousants children, and more)
        storeChildren: false,
        storeChildrenCount: false,
        storeAncestorsCount: true // store ancestors count by default, for easier querying and indexing
    }
});


/*
 * Model hooks
 */

// validate ancestors before creating
Tree.on('beforeCreate', 'validateAncestors', function(next){
    var doc = this;
    doc.ancestors = doc.ancestors || [];
    if(doc.constructor.getDefaults().options.storeAncestorsCount) doc.ancestorsCount = doc.ancestors.length;
    if(doc.constructor.getDefaults().options.storeChildren) doc.children = [];
    if(doc.constructor.getDefaults().options.storeChildrenCount) doc.childrenCount = 0;
    
    doc.validateAncestors(function(err){
        if(err) next(err);
        else next();
    });
});

// after create, update parent.children if defaults.options.storeChildren, or storeChildrenCount is true
Tree.on('afterCreate', 'addParentRef', updateParent('addRef'));

// disable changing ancestors on update
Tree.on('beforeUpdate', 'disableAncestors', function(next){
    var doc = this;
    doc.hide('ancestors', 'data'); // changing ancestors is not allowed on update, use doc.move(parentId)
    doc.hide('ancestorsCount', 'data'); // changing ancestorsCount is not allowed on update
    doc.hide('children', 'data'); // changing children is not allowed on update
    doc.hide('childrenCount', 'data'); // changing childrenCount is not allowed on update
    next();
});

// delete all descendants
Tree.on('beforeRemove', 'removeChildren', function(next){
    var doc = this;
    
    // remove all children
    doc.constructor.collection().find({ ancestors:doc.id }).remove(function(err){
        if(err) next(new Error('Tree on beforeRemove: cannot remove descendants').cause(err));
        else updateParent('removeRef').call(doc, next);
    });
});

// helper for updating parent.children if defaults.options.storeChildren === true
function updateParent(op){
    
    return function(args, next){
        var doc = this;
        if(arguments.length===1) next = arguments[0];
        
        if(!doc.ancestors) next(new Error('Tree updateParent: missing ancestors').details({ code:'INVALID', validErrs:{ ancestors:['required'] } }));
        else if(doc.ancestors.length && (doc.constructor.getDefaults().options.storeChildren ||
                                         doc.constructor.getDefaults().options.storeChildrenCount)) {
            var updateExp = {},
                parentId = doc.ancestors[doc.ancestors.length-1];
            
            if(op==='removeRef') {
                if(doc.constructor.getDefaults().options.storeChildren) updateExp.$pull = { children: doc.id };
                if(doc.constructor.getDefaults().options.storeChildrenCount) updateExp.$inc = { childrenCount: -1 };
            }
            else if(op==='addRef') {
                if(doc.constructor.getDefaults().options.storeChildren) updateExp.$push = { children: doc.id };
                if(doc.constructor.getDefaults().options.storeChildrenCount) updateExp.$inc = { childrenCount: 1 };
            }
            
            doc.constructor.collection().findId(parentId).update(updateExp, function(err){
                if(err) next(new Error('Tree updateParent: cannot update parent').cause(err));
                else next();
            });
        }
        else next();
    };
}


/*
 * Model instance methods
 */
    
Tree.prototype.validateAncestors = function(cb){ // cb(err)
    if(this.ancestors && this.ancestors.length>0){
        var newPath = this.ancestors;
        
        this.constructor.collection().findId(this.ancestors).all(function(err, ancs){
            if(err) cb(new Error('Tree.prototype.validateAncestors: cannot get ancestors').cause(err));
            else if(ancs.length !== newPath.length) cb(new Error('Tree.prototype.validateAncestors: cannot find all ancestors').details({ code:'EXECFAIL' }));
            else {
                var parentPathLength = -1;
                for(var i=0;i<newPath.length;i++) {
                    for(var a=0;a<ancs.length;a++){
                        if(ancs[a].id === newPath[i]) {
                            if(parentPathLength > -1 && parentPathLength + 1 !== ancs[a].ancestors.length) {
                                cb(new Error('Tree.prototype.validateAncestors: inconsistent path'));
                                return;
                            }
                            // TODO: check ancestors order consistency
                            parentPathLength = ancs[a].ancestors.length;
                        }
                    }
                }
                cb();
            }
        });
    }
    else cb();
}

/*
 * move doc in tree, to different parent
 * this method is hookable
 */
Tree.prototype.move = Tree.wrapHooks('move', function(parentId, callback){ // callback(err, doc)
    if(arguments.length!==2 || typeof arguments[1] !== 'function') throw new Error('Wrong arguments');
    var doc = this, oldParentId, oldParent, parent;
    
    // move inside self is not allowed
    if(doc.id===parentId) callback(new Error('Tree.prototype.move: Parent is same as child').details({ code:'INVALID', validErrs:{ ancestors:['invalid'] } }));
    
    // dont trust user data, load old document to ensure data integrity
    else doc.constructor.collection().findId(doc.id).one(function(err, oldDoc){
        if(err) callback(err);
        else if(!oldDoc) callback(new Error('Tree.prototype.move: Document not found').details({ code:'NOTFOUND' }));
        else if(doc.ancestors[ doc.ancestors.length-1 ] === parentId) callback(null, doc); // not moved
        else if(parentId === 'root'){
            // update document ancestors
            doc.ancestors = [];
            var updSet = { ancestors: doc.ancestors };
            if(doc.constructor.getDefaults().options.storeAncestorsCount) updSet.ancestorsCount = doc.ancestors.length;
            
            doc.constructor.collection().findId(doc.id).update(updSet, function(err, updated){
                if(err) callback(err);
                else if(updated!==1) callback(new Error('Tree.prototype.move: Updating failed, document not found').details({ code:'NOTFOUND' }));
                else updateDescendants(doc, oldDoc, callback); // update all descendants
            });
        }
        else {
            // get new parent
            doc.constructor.collection().findId(parentId).one(function(err, parent){
                if(err) callback(err);
                else if(!parent) callback(new Error('Tree.prototype.move: Parent not found').details({ code:'INVALID', validErrs:{ ancestors:['invalid'] } }));
                else {
                    // update document ancestors
                    doc.ancestors = parent.ancestors.slice(0);
                    doc.ancestors.push(parent.id);
                    var updSet = { ancestors: doc.ancestors };
                    if(doc.constructor.getDefaults().options.storeAncestorsCount) updSet.ancestorsCount = doc.ancestors.length;
                    
                    doc.constructor.collection().findId(doc.id).update(updSet, function(err, updated){
                        if(err) callback(err);
                        else if(updated!==1) callback(new Error('Tree.prototype.move: Updating failed, document not found').details({ code:'NOTFOUND' }));
                        else updateDescendants(doc, oldDoc, callback); // update all descendants
                    });
                }
                
            });
        }
    });
    
    // update "ancestors" property of all document descendants
    function updateDescendants(doc, oldDoc, callback){
        doc.constructor.collection().find({ ancestors:doc.id }).fields({ ancestors:1 }).all(function(err, descendants){
            if(err) callback(new Error('Tree.prototype.move: failed to get descendants').cause(err));
            else if(descendants.length > 0) {
                async.Series.each(descendants, function(i, next){
                    
                    // get unchanged ancestors - ancestors lower than parent level in tree
                    var unchangedAncs = descendants[i].ancestors.slice(oldDoc.ancestors.length, descendants[i].ancestors.length);
                    
                    // prepend new parent ancestors
                    descendants[i].ancestors = doc.ancestors.slice(0).concat(unchangedAncs);
                    var updSet = { ancestors: descendants[i].ancestors };
                    if(doc.constructor.getDefaults().options.storeAncestorsCount) updSet.ancestorsCount = descendants[i].ancestors.length;
                    
                    doc.constructor.collection().findId(descendants[i].id).update(updSet, function(err, desc){
                        if(err) next(new Error('Tree.prototype.move: failed to update descendant').cause(err));
                        else next();
                    });
                }, function(err){
                    if(err) callback(err);
                    else updateParent('addRef').call(doc, function(err){
                        if(err) callback(err);
                        else updateParent('removeRef').call(doc.constructor.new({ id: doc.id, ancestors: oldDoc.ancestors }), function(err){
                            if(err) callback(err);
                            else callback(null, doc);
                        });
                    });
                });
            }
            else updateParent('addRef').call(doc, function(err){
                if(err) callback(err);
                else updateParent('removeRef').call(doc.constructor.new({ id: doc.id, ancestors: oldDoc.ancestors }), function(err){
                    if(err) callback(err);
                    else callback(null, doc);
                });
            });
        });
        
    }
});

Tree.prototype.addChild = function(child, callback){ // callback(err, docs)
    if(arguments.length!==2 || typeof arguments[1] !== 'function') throw new Error('Wrong arguments');
    
    child = this.constructor.new(child);
    child.ancestors = this.ancestors.slice(0);       
    child.ancestors.push(this.id);
    child.create(callback);
};

/**
 * query helper
 * @returns {Object}  Query instance
 */
Tree.prototype.getChildren = function(){ // return query
    return this.getDescendants(1);
};

/**
 * helper
 * @returns {Boolean}  has parent
 */
Tree.prototype.hasParent = function(){
    return this.ancestors.length !== 0;
};

/**
 * query helper
 * @returns {Object}  Query instance
 */
Tree.prototype.getParent = function(){
    var parentId = this.ancestors.length === 0 ? 'root' : this.ancestors[this.ancestors.length-1];
    return this.constructor.collection().findId( parentId );
};

/**
 * query helper
 * @returns {Object}  Query instance
 */
Tree.prototype.getParents =
Tree.prototype.getAncestors = function(){
    return this.constructor.collection().findId( this.ancestors );
};

/**
 * query helper
 * @returns {Object}  Query instance
 */
Tree.prototype.getAncestor = function(level){
    return this.constructor.collection().findId( level ? this.ancestors[level] : this.ancestors[0] );
};

/**
 * query helper
 * @param {Boolean} includeSelf optional
 * @returns {Object}  Query instance
 */
Tree.prototype.getSiblings = function(includeSelf){
    var doc = this;
    
    if(doc.ancestors.length===0){
        return doc.constructor.collection().find( includeSelf ? { ancestors:{ $size:0 } } : { id:{ $ne: doc.id }, ancestors:{ $size:0 } });
    }
    else {
        var parent = {
            id: doc.ancestors[ doc.ancestors.length-1 ],
            ancestors: doc.ancestors.slice(0, doc.ancestors.length-1)
        };
        return includeSelf ? doc.getDescendants(1, parent) : doc.getDescendants(1, parent).find({ id:{ $ne: doc.id } });
    }
};

/**
 * query helper
 * @example document.getDescendants(2).cache().sort(...).all(...)
 * @param {Integer} levels optional
 * @returns {Object}  Query instance
 */
Tree.prototype.getDescendants = function(levels, parent){
    parent = parent || this;
    if(!parent.id) throw new Error('Parent has no "id"');
    else if(!Array.isArray(parent.ancestors)) throw new Error('Parent ancestors is not array');
    
    var query = this.constructor.collection();
    
    if(levels){
        if(!Array.isArray(levels)){
            return query.find({
                $and:[ { ancestors:{ $size:parent.ancestors.length + levels } },
                       { ancestors: parent.id }]
            });
        }
        else {
            var $or = [];
            for(var i=0;i<levels.length;i++){
                $or.push({ ancestors:{ $size: parent.ancestors.length + levels[i] }});
            }
            
            return query.find({ $and:[ { $or:$or }, { ancestors: parent.id }] });
        }
    }
    else return query.find({ ancestors: parent.id });
};