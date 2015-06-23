'use_strict';

var mReg = require('./register.js'),
    object = require('enterprise-utils').object;

/**
 * Model can be used to define new model constructor, or to retrieve other,
 * registered model constructors by name e.g. var User = Model('User')
 * @param {String} name registerd Constructor name
 * @returns {Object}  Constructor
 */
var Model = module.exports = function Model(name){
    if(arguments.length>0){
        var M = mReg.get(name);
        if(!M) throw new Error('Cannot find model with name "' +name+ '"');
        return M;
    }
};
    
/** @type {Array}	Register of all Model extensions, such as query-able, fill-able, valid-able or defaults-able,
 { instance:[Function], constructor:[Function] } */
Model.extensions = [];

/**
 * Creates constructor, by inheriting from parent constructor
 * @param {Object} schema model property schema
 * @param {Object} superProto - will be used as created constructor prototype
 * @returns {Function}  new constructor
 */
Model.createConstructor = function(schema, superProto, parentProto){
    var newCnst = function(){};
    
    // clone schema object
    schema = object.clone(schema);
    
    // copy properties from superProto
    if(superProto) for(var sKey in superProto) newCnst.prototype[sKey] = superProto[sKey];
    
    // inherit constructor methods from parent prototype
    for(var key in parentProto) newCnst.prototype[key] = parentProto[key];
    
    // schema getter
    newCnst.prototype.getSchema = function(){ return schema; };
    newCnst.getSchema = function(){ return schema; };
    newCnst.extendSchema = function(obj){ return object.extend(true, obj, schema); };
    
    // helper for creating non enumerable properties    
    newCnst.prototype.addHiddenProperty = function(key, value){
        Object.defineProperty(this, key, {
            configurable:false,
            writable:true,
            enumerable:false,
            value: value
        });
    };
    
    // helper for checking if model is instanceof model,
    // instanceof will not work because after model definition, new constructor is build
    newCnst.prototype.addHiddenProperty('__instanceof', 'Model'); // have to be hidden
    newCnst.__typeof = 'Model';
    
    // extend constructor prototype - add extended methods
    for(var i=0;i<Model.extensions.length;i++){
        if(typeof Model.extensions[i].instance === 'function'){
            object.extend(true,
                          newCnst.prototype,
                          superProto ? Model.extensions[i].instance.call(newCnst.prototype, superProto, schema) : {},
                          Model.extensions[i].instance.call(newCnst.prototype, parentProto, schema));
        }
        else if(Model.extensions[i].instance)
            object.extend(true, newCnst.prototype, Model.extensions[i].instance);
    }
    
    return newCnst;
}
    
/**
 * Creates new Model inheriting from parent
 * @param {Object} ParentCnst
 * @param {Object} extSchema
 * @returns {Object}  new constructor
 */
Model.inherit = function(SuperCnst, ParentCnst, extSchema){
    extSchema = extSchema || {};
    var superModel = SuperCnst ? new SuperCnst() : null;
    var superSchema = (superModel && superModel.getSchema) ? superModel.getSchema() : {};
    
    var parentModel = new ParentCnst();
    var parentSchema = parentModel.getSchema ? parentModel.getSchema() : {};
    var schema = {};
    
    // join parentSchema & superSchema
    object.extend(true, schema, superSchema, parentSchema, extSchema);
    
    // create new, inherited constructor
    var newConstructor = Model.createConstructor(schema, superModel, parentModel);
    
    // safe extend constructor
    for(var i=0;i<Model.extensions.length;i++){
        if(typeof Model.extensions[i].constructor === 'function'){
            object.extend(true,
                          newConstructor,
                          SuperCnst ? Model.extensions[i].constructor.call(newConstructor, SuperCnst, schema) : {},
                          Model.extensions[i].constructor.call(newConstructor, ParentCnst, schema));
            
        }
        else if(Model.extensions[i].constructor)
            object.extend(true, newConstructor, Model.extensions[i].constructor);
    }
    
    
    // constructor has reference to schema
    newConstructor.schema = schema;
    
    return newConstructor;
};


/**
 * Helper for defining and inheriting model
 * @param {String} name Model name
 * @param {Array} Parents parent models, from which will be inherited
 * @param {Object} schema new model schema
 * @returns {Object}  new Model
 */
Model.define = function(name, Parents, schema){
    if(arguments.length > 3 || arguments.length < 1){
        throw new Error('Wrong arguments, to define model use model.define(name, ParentModels, schema), while schema is required');
    }
    else if(arguments.length===1){
        schema = arguments[0];
        name = null;
        Parents = [];
    }
    else if(arguments.length===2){
        
        // if name & Parents as string, schema is missing 
        if(typeof arguments[0]==='string' && typeof arguments[1]==='string'){
            Parents = [ arguments[1] ];
            schema = {};
        }
        
        // if name & schema, Parents is missing
        else if(typeof arguments[0]==='string' && Object.prototype.toString.call(arguments[1])==='[object Object]'){
            schema = arguments[1];
            Parents = [];
        }
        
        // if Parents & schema, name is missing
        else if(typeof arguments[0]!=='string' && Object.prototype.toString.call(arguments[1])==='[object Object]'){
            schema = arguments[1];
            Parents = arguments[0];
            name = null;
        }
    }
    
    if(!Array.isArray(Parents)) Parents = [Parents];
    
    var NewModel, EmptyCnst = function(){};
    if(Parents.length===0) NewModel = Model.inherit(null, EmptyCnst, schema);
    else {
        // inherit new Model from parents
        for(var i=0;i<Parents.length;i++){
            if(typeof Parents[i] === 'string') {
                if(!Model(Parents[i])) throw new Error('Cannot create model, parent model "' +Parents[i]+ '" not registered');
                Parents[i] = Model(Parents[i]);
            }
            NewModel = Model.inherit(NewModel, Parents[i], (i===Parents.length-1 ? schema : {}));
        }
    }
    
    // define model name and register model, 
    // without registering name, model cannot have relations with another model,
    // because there is no reference to his name,
    if(typeof name === 'string'){
        // name is not required, because some times Models are defined temporary (validation, etc...)
        // throw new Error('Model name have to be string "' +name+ '"');
        
        if(mReg.has(name)) throw new Error('There is already registered model with name "' +name+ '", '+
                                           'choose another, or delete registered name first with Model.register.remove(\'modelname\')');
        NewModel._name = name;
        mReg.add(name, NewModel);
    }
    
    return NewModel;
};