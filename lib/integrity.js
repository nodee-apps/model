'use strict';

var mReg = require('./register.js'),
    Series = require('nodee-utils').async.Series;

/*
 * Integrity:
 * main purpose is registering CRUD hooks to maintain data integrity of relations
 * (e.g. removing children after parent removed)
 *
 */

module.exports = {
    maintain: maintain
};


function maintain(relation){
    
    var opts = relation.opts;
    if(opts.maintainIntegrity){
        if(!(opts.maintainIntegrity===true || Array.isArray(opts.maintainIntegrity)))
            throw new Error('Model integrity: Cannot create relation, maintainIntegrity have to be '+
                            'true/false, or array of CRUD operations like ["create","update","remove"]');
    }
    else return;
    
    function check(op){
        if(opts.maintainIntegrity === true) return true;
        else return (opts.maintainIntegrity.indexOf(op) !== -1);
    }
    
    var Child = mReg.get(relation.child.model);
    var childKey = relation.child.key;
    var Parent = mReg.get(relation.parent.model);
    var parentKey = relation.parent.key;
    
    // childKey means child own reference to parent
    if(childKey && check('create')) {
        // reference to parent is required by default, but sometimes it is better turn off (example: in tree structure, you need to create root element with undefined parent)
        if(opts.required !== false) Child.on('beforeCreate', require_ref_to_parent(relation)); // require, child[childKey], check if parent exists
        else Child.on('beforeCreate', check_ref_to_parent(relation)); // check if parent exists, but only if it is defined
    }
    if(childKey && check('update')) Child.on('beforeUpdate', check_ref_to_parent(relation)); // check if parent exists, if is defined
    if(childKey && check('remove')) Parent.on('beforeRemove', remove_children(relation)); // remove all children with reference to parent
    
    // parentKey means parent own reference to children
    if(parentKey && check('remove')) Child.on('beforeRemove', remove_ref_in_parent(relation)); // ensure reference to child, in array parent[parentKey], is removed after child is removed
    if(parentKey && check('create')) {
        // reference to parent is required by default, but sometimes it is better turn off, as in case the key owns child
        if(opts.required !== false) Parent.on('beforeCreate', require_ref_to_children(relation)); // require parent[parentKey], check if all of referenced children exists
        else Parent.on('beforeCreate', check_ref_to_children(relation)); // check if all of referenced children exists, but only when parent[parentKey] is defined
    }
    if(parentKey && check('update')) Parent.on('beforeUpdate', check_ref_to_children(relation)); // check if all of referenced children exists
    

    // child on beforeCreate, only if required:true
    function require_ref_to_parent(relation){
        // console.log('register: require_ref_to_parent');
        
        return function(next){
            // console.log('exec: require_ref_to_parent');
            
            var model = this;
            var key = relation.child.key;
            var propName = relation.child.as;
            
            if(!model[key]) {
                var validErrs = {};
                validErrs[key] = ['required'];
                next(new Error('Model integrity: Cannot create, property "' + key + '" is required').details({ code:'INVALID', validErrs: validErrs }));
            }
            else {
                model[propName]().one(function(err, relatedModel){
                    if(err) next(new Error('Model integrity: reading child failed').cause(err));
                    else if(!relatedModel) {
                        var validErrs = {};
                        validErrs[key] = ['integrity'];
                        next(new Error('Model integrity: Cannot create, parent model "' + model[key] +
                                       '" does not exists').details({ code:'INVALID', validErrs: validErrs }));
                    }
                    else next();
                });
            }
        };
    }
    
    // child on beforeCreate, only if required:false
    function check_ref_to_parent(relation){
        // console.log('register: check_ref_to_parent');
        
        return function(next){
            // console.log('exec: check_ref_to_parent');
            
            var model = this;
            var key = relation.child.key;
            var propName = relation.child.as;
            
            if(model[key]) {
                model[propName]().one(function(err, relatedModel){
                    if(err) next(new Error('Model integrity: reading parent failed').cause(err));
                    else if(!relatedModel) {
                        var validErrs = {};
                        validErrs[key] = ['integrity'];
                        next(new Error('Model integrity: Cannot update or create, parent model "' + model[key] +
                                       '" does not exists').details({ code:'INVALID', validErrs: validErrs }));
                    }
                    else next();
                });
            }
            else next();
        };
    }
        
    // child on beforeRemove, only when reference prop is owned by parent
    function remove_ref_in_parent(relation){
        // console.log('register: remove_ref_in_parent');
        
        return function(next){
            // console.log('exec: remove_ref_in_parent');
            
            var model = this;
            var foreignKey = relation.parent.key;
            var propName = relation.child.as;
            
            model[propName]().one(function(err, parent){
                if(err) next(new Error('Model integrity: reading parent failed').cause(err));
                else if(!parent) next();
                else {
                    var refArray = parent[foreignKey];
                    if(Array.isArray(refArray) && refArray.indexOf(model.id) !== -1){
                        parent[foreignKey] = refArray.splice(1+refArray.indexOf(model.id), 1);
                        parent.update(function(err){
                            if(err) next(new Error('Model integrity: updating parent failed').cause(err));
                            else next();
                        });
                    }
                    else next();
                }
            });
        };
    }
        
    // parent on beforeRemove, will remove children, only if maintainIntegrity=true/['remove']
    function remove_children(relation){
        // console.log('register: remove_children');
        
        return function(next){
            // console.log('exec: remove_children');
            
            var model = this;
            var key = relation.child.key;
            var ChildModel = mReg.get(relation.child.model);
            var bulkRemove = relation.bulkRemove;
            
            var q = {};
            q[key] = model.id;
            
            if(bulkRemove===true){
                ChildModel.collection().find(q).remove(function(err){
                    if(err) next(new Error('Model integrity: bulk removing cheldren failed').cause(err));
                    else next();
                });
            }
            else {
                ChildModel.collection().find(q).all(function(err, children){
                    if(err) next(new Error('Model integrity: reading children failed').cause(err));
                    else Series.each(children, function(i, next){
                        children[i].remove(function(err){
                            if(err) next(new Error('Model integrity: removing children failed').cause(err));
                            else next();
                        });
                    }, next);
                });
            }
            
            // case when Parent owns reference to children (this is not supported),
            // only Child can have reference to parent in strict relation (strict means "all children are removed with Parent")
            //model.refresh(function(err, parent){
            //    if(err) next(err);
            //    else if(model[key] && model[key].length > 0) {
            //        Series.each(model[key], function(i, next){
            //            var childId = model[key][i];
            //            (new ChildModel()).fill({ id:childId }).remove(function(err){
            //                if(err) next(err);
            //                else next();
            //            });
            //        });
            //    }
            //    else if(model[key] && Object.prototype.toString.call(model[key]) !== '[object Array]') next('Cannot remove, property "' + key + '" is reference to children, and is not Array.');
            //    else next();
            //});
        };
    }
        
    // parent on beforeCreate, only if require=true && maintainIntegrity=true/['create']
    function require_ref_to_children(relation){
        // console.log('register: require_ref_to_children');
        
        return function(next){
            // console.log('exec: require_ref_to_children');
            
            var model = this;
            var key = relation.parent.key;
            var propName = relation.parent.as;
            
            model[key] = model[key] || [];
            if(model[key].length > 0) {
                model[propName]().all(function(err, childrens){
                    if(err) next(new Error('Model integrity: reding children failed').cause(err));
                    else if(childrens.length !== model[key].length) {
                        var validErrs = {};
                        validErrs[key] = ['integrity'];
                        next(new Error('Cannot create, not all of cildren exists "' + JSON.stringify(model[key]) +
                                       '"').details({ code:'INVALID', validErrs: validErrs }));
                    }
                    else next();
                });
            }
            else next();
        };
    }
    
    // parent on beforeCreate, only if maintainIntegrity=true/['create']
    function check_ref_to_children(relation){
        // console.log('register: check_ref_to_children');
        
        return function(next){
            // console.log('exec: check_ref_to_children');
            
            var model = this;
            var key = relation.parent.key;
            var propName = relation.parent.as;
            
            if(model[key] && model[key].length > 0) {
                model[propName]().all(function(err, childrens){
                    if(err) next(new Error('Model integrity: reading children failed').cause(err));
                    else if(childrens.length !== model[key].length) {
                        var validErrs = {};
                        validErrs[key] = ['integrity'];
                        next(new Error('Cannot update, not all of cildren exists "' +
                                       JSON.stringify(model[key]) + '"').details({ code:'INVALID', validErrs: validErrs }));
                    }
                    else next();
                });
            }
            else next();
        };
    }
}