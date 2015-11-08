'use_strict';

var Model = require('../model.js'),
    validator = require('enterprise-utils').validator,
    object = require('enterprise-utils').object;


/*
 * Model "validations" extension:
 * adds methods to validate and get/set model instance data,
 * and get/set validation errors
 *
 * @example: modelInstance.fill({ ...data object... }).validate().isValid(); true/flase
 */


/**
 * validate single data property
 * @param {Object} model intance
 * @param {String} modelPropName needed if nested model referencing model[ modelPropName ] is not allways equal to value
 * @param {Object} schemaObj is definition of property validations and sanitizers e.g. name:{ isString:true }
 * @param {Object} value property value
 * @returns {Object}  value and validation errors
 */
function validateProperty(model, modelPropName, schemaObj, value) {
    var output = {
        value: value,
        model: model,
        errs: []
    };
    
    // schema is empty, every value will be valid
    if(isEmpty(schemaObj)) return output;
    
    var validFnc, isValid, args, SubModel;
    
    // loop through schemaObj properties
    for(var propName in schemaObj) {
        validFnc = Model.schemaProps[propName];
        
        // schema property recognized as validator method
        if(typeof validFnc === 'function'){
            args = schemaObj[propName];
            if(!Array.isArray(args)) args = [ args ];
            isValid = validFnc.apply(output, [ output.value ].concat(args));
            
            if(!isValid) {
                output.errs.push(propName);
            }
        }
        
        // schema property recognized as nested model
        else if(Model.schemaProps[propName] === 'nested_model') {
            SubModel = schemaObj[propName];
            if(SubModel.__typeof !== 'Model') {
                throw new Error('Model validations: cannot validate, value by schema property "' +propName+ '" ' +
                                '- argument is not Model constructor');
            }
            else {
                model[ modelPropName ] = SubModel.new(value).validate();
                if(!model[ modelPropName ].isValid()) output.errs.push( model[ modelPropName ].validErrs() );
            }
        }
        
        // schema property recognized as array of nested models
        else if(Model.schemaProps[propName] === 'nested_models_array'){
            SubModel = schemaObj[propName];
            if(!Array.isArray(value)) output.errs.push( propName );
            else if(SubModel.__typeof !== 'Model') {
                throw new Error('Model validations: cannot validate, value by schema property "' +propName+ '" ' +
                                '- argument is not Model constructor');
            }
            else {
                var subErrs = [];
                for(var i=0;i<value.length;i++) {
                    value[i] = SubModel.new(value[i]).validate();
                    if(!value[i].isValid()) subErrs[i] = value[i].validErrs();
                }
                if(subErrs.length>0) output.errs.push( subErrs );
            }
        }
        
        // schema property recognized as nested value validations
        else if(Model.schemaProps[propName] === 'nested_validation'){
            if(Object.prototype.toString.call(value) !== '[object Object]') output.errs.push( propName );
            else {
                var subOut = {}, subErrs = null;
                for(var valueProp in value){
                    subOut = validateProperty(model, modelPropName, schemaObj[propName], value[valueProp]);
                    
                    if(subOut.errs.length > 0) {
                        subErrs = subErrs || {};
                        subErrs[valueProp] = subErrs[valueProp] || [];
                        subErrs[valueProp] = subErrs[valueProp].concat( subOut.errs );
                    }
                }
                if(subErrs) output.errs.push( subErrs );
            }
        }
        
        // schema property not recognized, maybe typo
        else if(!validFnc) {
            throw new Error('Model validation: schema property "' + propName + '" not recognized');
        }
    }

    return output;
}

/**
 * fill submodels in nested schema
 * @param {Object} schemaProp Schema prop
 * @param {Object} objProp Data object
 * @returns {Object}  filled model property
 */
function fillSubModels(schemaProp, objProp){
    var modelProp = objProp;
    for(var subProp in schemaProp) {
        if(Model.schemaProps[subProp] === 'nested_model') {
            modelProp = schemaProp[subProp].new(objProp);
            continue;
        }
        else if(Model.schemaProps[subProp] === 'nested_models_array' && Array.isArray(objProp)) {
            modelProp = [];
            for(var i=0;i<objProp.length;i++)
                modelProp[i] = schemaProp[subProp].new(objProp[i]);
            continue;
        }
        else if(Model.schemaProps[subProp] === 'nested_validation' && Object.prototype.toString.call(objProp) === '[object Object]'){
            modelProp = {};
            for(var objSubProp in objProp)
                modelProp[objSubProp] = fillSubModels(schemaProp[subProp], objProp[objSubProp]);
        }
    }
    return modelProp;
}

/**
 * get submodels data in nested schema
 * @param {Object} schemaProp Schema prop
 * @param {Object} objProp Data object
 * @returns {Object}  filled model property
 */
function getSubModelsData(schemaProp, modelProp, transformFnc){
    var resultProp = modelProp;
    for(var subProp in schemaProp) {
        if(Model.schemaProps[subProp] === 'nested_model') {
            resultProp = modelProp.getData ? modelProp.getData(transformFnc) : modelProp;
            continue;
        }
        else if(Model.schemaProps[subProp] === 'nested_models_array' && Array.isArray(modelProp)) {
            resultProp = [];
            for(var i=0;i<modelProp.length;i++)
                resultProp[i] = modelProp[i].getData ? modelProp[i].getData(transformFnc) : modelProp[i];
            continue;
        }
        else if(Model.schemaProps[subProp] === 'nested_validation' && Object.prototype.toString.call(modelProp) === '[object Object]'){
            resultProp = {};
            for(var modelSubProp in modelProp)
                resultProp[modelSubProp] = getSubModelsData(schemaProp[subProp], modelProp[modelSubProp]);
        }
    }
    return resultProp;
}

/**
 * find nested models schema - usefull to decide when filling property need run method fillSubmodels
 * @param {Object} schemaProp Schema prop
 * @returns {Boolean} contains nested model
 */
function searchNestedModels(schemaProp){
    for(var subProp in schemaProp) {
        if(Model.schemaProps[subProp] === 'nested_model' ||
           Model.schemaProps[subProp] === 'nested_models_array') return true;
        else if(Model.schemaProps[subProp] === 'nested_validation') return searchNestedModels(schemaProp[subProp]);
    }
    return false;
}


Model.extensions.push({
    instance: {
        /**
         * Gets validation errors object
         * @returns {Object}  Validation errors
         */
        getValidErrs: function(){
            return (this._errs || {});
        },
        
        /**
         * Alias for getValidationErrs
         * @returns {Object}  Validation errors
         */
        validErrs: function(){
            return (this._errs || {});
        },
        
        /**
         * Set validation errors - usefull on before/after validate hooks
         * @param {Object} objOrName name of err property, or errors object
         * @param {Object} value error property value, or undefined
         */
        setValidErrs: function(objOrName, value){ // [obj], or [propName, value]
            if(!this.hasOwnProperty('_errs')) this.addHiddenProperty('_errs');
            
            if(arguments.length===1) this._errs = arguments[0];
            else if(arguments.length===2) {
                var errs = this._errs;
                errs[arguments[0]] = arguments[1];
                this._errs = errs;
            }
            this.setValid(false);
        },
        
        /**
         * alias for setValidErrs({})
         */
        clearValidErrs: function(){
            if(!this.hasOwnProperty('_errs')) this.addHiddenProperty('_errs');
            this._errs = {};
        },
        
        /**
         * getter of instance valid state
         * @returns {Boolean}
         */
        isValid: function(){
            return this._valid;
        },
        
        /**
         * Setter of instance valid state
         * @param {Boolean} value undefined is same as false
         */
        setValid: function(value){
            if(!this.hasOwnProperty('_valid')) this.addHiddenProperty('_valid');
            
            if(arguments.length===0 || value===true){
                this._valid = true;   
            }
            else if(typeof value === 'undefined') this._valid = undefined; // initial value
            else this._valid = false;
        },
        
        /**
         * shortcut of setValid(false)
         */
        setInvalid: function(){
            this.setValid(false);
        },
        
        /**
         * validate model instance data, and run all onValidate Functions
         * @param {Object} extendSchema extend model schema
         * @returns {Object}  model
         */
        validate: function(extendSchema){
            var model = this,
                schema = extendSchema ? object.extend(true, {}, this.getSchema(), extendSchema) : this.getSchema(),
                onValidateFncs = model.constructor._instanceListeners['validate'] || [],
                validResult = {},
                isValid = true,
                validErrs = null;
            
            // clear errors before validation
            model.clearValidErrs();
            
            // validate all properties
            for(var propName in schema) {
                if(execValidation(schema[propName]) || model.hasOwnProperty(propName)){
                    validResult = validateProperty(model, propName, schema[propName], model[propName]);
                    if(validResult.errs.length > 0) {
                        isValid = false;
                        validErrs = validErrs || {};
                        validErrs[propName] = validResult.errs;
                    }
                    else model[propName] = validResult.value;
                }
            }
            
            model.setValidErrs(validErrs);
            if(!isValid) model.setValid(false);
            else {
                model.setValid(true);
                
                // run all onValidate functions
                for(var i=0;i<onValidateFncs.length;i++){
                    onValidateFncs[i].call(model, model);
                    if(!model.isValid()) break;
                }
            }
            
            return model;
        },
        
        /**
         * check if single property is valid
         * @param {String} propName
         * @param {Object} value
         * @returns {Object}  property validation errors
         */
        checkProp: function(propName, value){
            if(arguments.length===1) {
                return validateProperty(this, propName, this.getSchema()[propName], this[propName]);
            }
            else {
                return validateProperty(this, propName, this.getSchema()[propName], value);
            }
        },
        
        /**
         * Get instance data - only data defined in schema
         * @param {Object} extendSchema extend model schema
         * @param {Function} transformFnc optional data transformation function(key, value){ return value; }
         * @returns {Object}  instance data
         */
        getData: function(extendSchema, transformFnc){
            if(arguments.length===1 && typeof arguments[0] === 'function'){
                transformFnc = arguments[0];
                extendSchema = null;
            }
            if(transformFnc && typeof transformFnc !== 'function') throw new Error('Wrong arguments');
            
            var model = this,
                result = {},
                schema = extendSchema ? object.extend(true, {}, this.getSchema(), extendSchema) : this.getSchema();
            
            for(var propName in schema) {
                if(this._hiddenProps && (this._hiddenProps[propName]===true || this._hiddenProps[propName]==='getData')) continue;
                else if(model[propName] !== undefined){
                    // avoid repeated nested search in schema
                    if(schema[propName].containsNestedModel === undefined)
                        schema[propName].containsNestedModel = searchNestedModels(schema[propName]);
                        
                    if(schema[propName].containsNestedModel)
                        result[propName] = getSubModelsData(schema[propName], model[propName], transformFnc);
                    else if(transformFnc) {
                        result[propName] = transformFnc(propName, model[propName]);
                    }
                    else result[propName] = model[propName];
                }
            }
            return result;
        },
        
        /**
         * Erase instance data, and reset model state
         */
        clearData: function(){
            for(var propName in this.getSchema()) {
                delete this[propName];
            }
            this.setValid(undefined);
        },
        
        /**
         * fill model instance data, and run all onFill Functions
         * @param {Object} obj value object
         * @param {Object} extendSchema extend model schema
         * @returns {Object}  model
         */
        fill: function(obj, extendSchema){
            obj = obj || {};
            if(Object.prototype.toString.call(obj) !== '[object Object]') {
                throw new Error('model.fill(obj) can fill model only from object.');
            }
            
            var model = this,
                schema = extendSchema ? object.extend(true, {}, this.getSchema(), extendSchema) : this.getSchema(),
                onFillFncs = model.constructor._instanceListeners['fill'] || [];
            
            for(var propName in obj) {
                if(schema[propName]) {
                    // avoid repeated nested search in schema
                    if(schema[propName].containsNestedModel === undefined)
                        schema[propName].containsNestedModel = searchNestedModels(schema[propName]);
                    
                    if(schema[propName].containsNestedModel)
                        model[propName] = fillSubModels(schema[propName], obj[propName]);
                    else
                        model[propName] = obj[propName];
                }
            }
            
            // set model state to initial
            model.clearValidErrs();
            model.setValid(undefined);
            
            // run onFill functions
            for(var i=0;i<onFillFncs.length;i++){
                onFillFncs[i].call(model, model);
            }
            
            return model;
        },
        
        /**
         * runs all onFetch funcs, and model.fill
         * @param {Object} obj value object
         * @returns {Object}  model
         */
        fetch: function(obj){
            var model = this;
            var onFetchFncs = model.constructor._instanceListeners['fetch'] || [];
            
            // run onFetch functions
            for(var i=0;i<onFetchFncs.length;i++){
                onFetchFncs[i].call(model, obj);
            }
            
            return model.fill(obj);
        },
        
        /**
         * replacement of default toJSON method in JSON.stringify,
         * this implements "hidden" attribute defined in model schema,
         * @returns {Object}  instance data
         */
        toJSON: function(){
            var result = {};
            var schema = this.getSchema();
            for(var propName in schema) {
                if(this._hiddenProps && (this._hiddenProps[propName]===true || this._hiddenProps[propName]==='toJSON')) continue;
                else if(schema[propName].hidden!==true) result[propName] = this[propName];
            }
            return result;
        },
        
        /**
         * helper for hiding document properties when stringifying, or getting data,
         * it is usefull in cases like preventing property to be updated in datasource
         * @param {String} propName property to hide
         * @param {String} hideIn optional hide in toJSON, or getData, or both if undefined
         */
        hide: function(propName, hideIn){
            if(hideIn==='data') hideIn = 'getData'; // shortcut
            if(hideIn==='json') hideIn = 'toJSON'; // shortcut
            
            if(!this.hasOwnProperty('_hiddenProps')) this.addHiddenProperty('_hiddenProps');
            this._hiddenProps = this._hiddenProps || {};
            if(hideIn===false) delete this._hiddenProps[ propName ];
            else this._hiddenProps[ propName ] = hideIn ? hideIn : true;
        }
    },
    constructor: function(ParentCnst){
        var newConstructor = this;
        
        // inherit/copy all parent onFill, onFetch, onValidate Functions
        newConstructor._instanceListeners = {};
        for(var eventName in (ParentCnst._instanceListeners || {})){
            newConstructor._instanceListeners[eventName] = (ParentCnst._instanceListeners[eventName] || []).slice(0);
        }
        
        /**
         * add new onFill Function
         * @param {Function} fnc onFill function(model_instance)
         */
        newConstructor.onFill = function(fnc){
            if(typeof fnc!=='function') throw new Error('Wrong arguments');
            else {
                this._instanceListeners['fill'] = this._instanceListeners['fill'] || [];
                this._instanceListeners['fill'].push(fnc);
            }
        };
        
        /**
         * add new onFetch Function
         * @param {Function} fnc onFetch function(model_instance)
         */
        newConstructor.onFetch = function(fnc){
            if(typeof fnc!=='function') throw new Error('Wrong arguments');
            else {
                this._instanceListeners['fetch'] = this._instanceListeners['fetch'] || [];
                this._instanceListeners['fetch'].push(fnc);
            }
        };
        
        /**
         * add new onValidate Function
         * @param {Function} fnc onValidate function(model_instance)
         */
        newConstructor.onValidate = function(fnc){
            if(typeof fnc!=='function') throw new Error('Wrong arguments');
            else {
                this._instanceListeners['validate'] = this._instanceListeners['validate'] || [];
                this._instanceListeners['validate'].push(fnc);
            }
        };
        
        // helper for creating and filling instances
        newConstructor.new = function(data, extendSchema){
            if(data) return (new this()).fill(data, extendSchema);
            else return (new this());
        };
        
        return {};
    }
});

// helper - check if object is empty
function isEmpty(obj){
    for(var propName in obj) {
        return false;
    }
    return true;
}

// list of schema properties which will allways execute validation, even if model property is missing
Model.allwaysValidateSchemaProps = {
    required: true,
    setValue: true,
    valueIfMissing: true,
    valueFrom: true,
    copy: true
};

// helper for checking if validation should execute
function execValidation(propSchema){
    for(var key in propSchema) {
        if(Model.allwaysValidateSchemaProps[ key ]) return true;
    }
    return false;
}

/*
 * set of allowed schema props and types,
 * to ensure there are no typos in model schema definition
 */
Model.schemaProps = {
    /*
     * Reserved, special names
     */
    containsNestedModel: 'reserved', // mark nested schema, this flag prevent repeatable deep searching for nested models in schema
    hidden: 'reserved', // will be excluded in result of model.toJSON method
    
    values: 'nested_validation', // validate object values - nested validation
    model: 'nested_model', // create, fill and validate model instance with data from value
    isModel: 'nested_model', // alias for model
    arrayOf: 'nested_models_array', // create, fill and validate array of model instances with data from value
    isArrayOf: 'nested_models_array', // alias for arrayOf
    
    /*
     * Extended validators
     */
    required: validator.isDefined, // when property have to be defined, and not null --> !(typeof value === 'undefined' || value === null)
    isInteger: validator.isInt, // alias for isInt
    isString: validator.isString, // check if value is string
    isArray: validator.isArray, // check if value is array
    isBoolean: validator.isBoolean, // check if value is boolean
    minLength: validator.minLength, // check if string or array length
    maxLength: validator.maxLength, // check if string or array length
    isIn: validator.isIn, // check if value is in a array of allowed values
    parseDate: validator.parseDate, // (value, formatString) check if value is Date, if not, try to parse date defined by formatString
    toDate: validator.parseDate, // alias for parseDate
    date: validator.parseDate, // alias for parseDate
    toDateString: validator.toDateString, // (value, formatString) check if value is Date, and converts it to formated date string
    round: validator.round, // Math.round value, to specified digits, have to be number
    validate: validator.customValidation, // custom validation function
    
    keyNames: validator.keyNames, // check if value is object, and apply func to change keyNames
    keys: validator.keyNames, // alias for keyNames
    
    /*
     * Extended sanitizers
     */
    setValue: sanitizerWrap(validator.setValue), // always sets value
    defaultValue: sanitizerWrap(validator.defaultValue), // sets value if null or undefined, or empty string
    valueIfMissing: sanitizerWrap(validator.defaultValue), // sets value if null or undefined, or empty string
    valueFrom: sanitizerWrap(validator.valueFrom), // sets value from another model property
    copy: sanitizerWrap(validator.valueFrom), // sets value from another model property
    sanitize: sanitizerWrap(validator.customSanitization), // custom sanitizer function
    cleanUrl: sanitizerWrap(validator.cleanUrl), // replace diacritics and other non "a-z0-9", "-", "_", "." characters
    fullText: sanitizerWrap(validator.fullText), // replace diacritics and generate array of lowercased strings
    
    /*
     * Validators - returns true/false, but it can also modify value
     */
    equals: validator.equals, // (str, comparison) - check if the string matches the comparison.
    contains: validator.contains, // (str, seed) - check if the string contains the seed.
    matches: validator.matches, // (str, pattern [, modifiers]) - check if string matches the pattern. Either matches('foo', /foo/i) or matches('foo', 'foo', 'i').
    isEmail: validatorWrap(validator.isEmail), // (str [, options]) - check if the string is an email.
    isURL: validatorWrap(validator.isUrl), // (str [, options]) - check if the string is an URL. options is an object which defaults to { protocols: ['http','https','ftp'], require_tld: true, require_protocol: false, allow_underscores: false, host_whitelist: false, host_blacklist: false }.
    isFQDN: validatorWrap(validator.isFQDN), // (str [, options]) - check if the string is a fully qualified domain name (e.g. domain.com). options is an object which defaults to { require_tld: true, allow_underscores: false }.
    isIP: validator.isIP, // (str [, version]) - check if the string is an IP (version 4 or 6).
    isAlpha: validator.isAlpha, // (str) - check if the string contains only letters (a-zA-Z).
    isNumeric: validator.isNumeric, // (str) - check if the string contains only numbers.
    isAlphanumeric: validator.isAlphanumeric, // (str) - check if the string contains only letters and numbers.
    isBase64: validator.isBase64, // (str) - check if a string is base64 encoded.
    isHexadecimal: validator.isHexadecimal, // (str) - check if the string is a hexadecimal number.
    isHexColor: validator.isHexColor, // (str) - check if the string is a hexadecimal color.
    isLowercase: validator.isLowercase, // (str) - check if the string is lowercase.
    isUppercase: validator.isUppercase, // (str) - check if the string is uppercase.
    isInt: validator.isInt, // (str) - check if the string is an integer.
    isFloat: validator.isFloat, // (str) - check if the string is a float.
    isDivisibleBy: validator.isDivisibleBy, // (str, number) - check if the string is a number that's divisible by another.
    isNull: validator.isNull, // (str) - check if the string is null.
    isLength: validator.isLength, // (str, min [, max]) - check if the string's length falls in a range. Note: this function takes into account surrogate pairs.
    isByteLength: validator.isByteLength, // (str, min [, max]) - check if the string's length (in bytes) falls in a range.
    isUUID: validator.isUUID, // (str [, version]) - check if the string is a UUID (version 3, 4 or 5).
    isDate: validator.isDate, // (str) - check if the string is a date.
    isAfter: validator.isAfter, // (str [, date]) - check if the string is a date that's after the specified date (defaults to now).
    isBefore: validator.isBefore, // (str [, date]) - check if the string is a date that's before the specified date.
    // isIn: validator.isIn, // (str, values) - check if the string is in a array of allowed values.
    isCreditCard: validator.isCreditCard, // (str) - check if the string is a credit card.
    isISBN: validator.isISBN, // (str [, version]) - check if the string is an ISBN (version 10 or 13).
    isJSON: validator.isJSON, // (str) - check if the string is valid JSON (note: uses JSON.parse).
    isMultibyte: validator.isMultibyte, // (str) - check if the string contains one or more multibyte chars.
    isAscii: validator.isAscii, // (str) - check if the string contains ASCII chars only.
    isFullWidth: validator.isFullWidth, // (str) - check if the string contains any full-width chars.
    isHalfWidth: validator.isHalfWidth, // (str) - check if the string contains any half-width chars.
    isVariableWidth: validator.isVariableWidth, // (str) - check if the string contains a mixture of full and half-width chars.
    isSurrogatePair: validator.isSurrogatePair, // (str) - check if the string contains any surrogate pairs chars.
    
    /*
     * Sanitizers - returns modified value
     */
    toString: sanitizerWrap(validator.toString), // (input) - convert the input to a string.
    toFloat: sanitizerWrap(validator.toFloat), // (input) - convert the input to a float, or NaN if the input is not a float.
    toInt: sanitizerWrap(validator.toInt), // (input [, radix]) - convert the input to an integer, or NaN if the input is not an integer.
    toInteger: sanitizerWrap(validator.toInt), // alias for toInt
    toBoolean: sanitizerWrap(validator.toBoolean), // (input [, strict]) - convert the input to a boolean. Everything except for '0', 'false' and '' returns true. In strict mode only '1' and 'true' return true.
    trim: sanitizerWrap(validator.trim), // (input [, chars]) - trim characters (whitespace by default) from both sides of the input.
    ltrim: sanitizerWrap(validator.ltrim), // (input [, chars]) - trim characters from the left-side of the input.
    rtrim: sanitizerWrap(validator.rtrim), // (input [, chars]) - trim characters from the right-side of the input.
    escape: sanitizerWrap(validator.escape), // (input) - replace <, >, &, ' and " with HTML entities.
    stripLow: sanitizerWrap(validator.stripLow), // (input [, keep_new_lines]) - remove characters with a numerical value < 32 and 127, mostly control characters. If keep_new_lines is true, newline characters are preserved (\n and \r, hex 0xA and 0xD). Unicode-safe in JavaScript.
    whitelist: sanitizerWrap(validator.whitelist), // (input, chars) - remove characters that do not appear in the whitelist.
    blacklist: sanitizerWrap(validator.blacklist), // (input, chars) - remove characters that appear in the blacklist.
    normalizeEmail: sanitizerWrap(validator.normalizeEmail), //(email) - canonicalize a gmail address.
};


// simple helper for wrapping default validator sanitize methods
function sanitizerWrap(fnc){
    return function(value){
        this.value = value = fnc.apply(this, Array.prototype.slice.call(arguments, 0));
        this.valid = true;
        return true;
    };
}

// simple helper for replacing boolean values with empty object, because some validator methods needs objects
function validatorWrap(fnc){
    return function(value, opts){
        if(arguments.length === 2 && arguments[1] === true) return fnc.call(this, value);
        else return fnc.apply(this, Array.prototype.slice.call(arguments, 0));
    };
}