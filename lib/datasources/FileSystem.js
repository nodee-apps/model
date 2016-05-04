'use strict';

var Model = require('../model.js'),
    path = require('path'),
    fs = require('fs'),
    async = require('nodee-utils').async,
    fsExt = require('nodee-utils').fsExt,
    object = require('nodee-utils').object,
    generateId = require('nodee-utils').shortId.generate,
    sift = require('nodee-utils').sift;


/*
 * FileSystem model datasource
 * data are stored as files and folders, if watching is enabled (watching is not implemented yet), whole directory is cached, and walked after change
 * WARNING: searching always execute walk dir recursive,
 * but if filter by ancestors and ancestors size it will walk only this directory,
 * or if filter by id
 * 
 */
var FileSystem = module.exports = Model.define('FileSystemDataSource',['DataSource', 'Tree'], {
    id:{ isString:true },
    // deleted:{ isBoolean:true }, // there is no softRemove in file system
    createdDT:{ date:true },
    modifiedDT: { date:true }, // if optimisticLock
    
    fullPath:{ isString:true, hidden:true }, // hide full path when sending to client
    name:{ isString:true },
    // ancestors: { isArray:true }, - inherited from Tree
    isDir:{ isBoolean:true },
    isFile:{ isBoolean:true },
    ext:{ }, // string or nothing, when this is directory
    size:{ isInteger:true },
    
    content:{ }, // utf8 text
    data:{ buffer:true } // binary or base64 data
});

/*
 * defaults
 */
FileSystem.extendDefaults({
    connection:{
        dirPath: '', // directory path
    },
    query:{
        // deleted:{ $ne:true } // default query when softRemove: true
    },
    options:{
        sort:{ id:1 },
        limit: undefined,
        skip: 0,
        fields: {},
        softRemove: false,
        optimisticLock: true
    },
    cache:{
        keyPrefix:'nodee-model-filesystem',
        // createKey: createCacheKey - using createKey defined in DataSource
        // duration: 3000, // duration (undefined or zero means no expiration)
        // use: false, // by default do ot use cache
    }
});

/*
 * Helpers
 */

// read and require file data, cache results and parse dates
function getFileList(dirPath, findQuery, cb){
    if(arguments.length === 2){
        cb = arguments[1];
        findQuery = {};
    }
    findQuery = findQuery || {};
    var parentDir = walkOnlyChildren(findQuery);
    
    if(typeof findQuery.id === 'string'){
        fsExt.getFileInfo(findQuery.id, dirPath + '/' + findQuery.id, function(err, file){
            if(err) cb(err);
            else cb(null, file ? [file] : []);
        });
    }
    else if(parentDir==='root'){
        fsExt.walkdirRecursive(dirPath,{ levels:1 }, function(err, data){
            if(err) cb(err);
            else {
                var records = [];
                for(var key in data){
                    records.push(data[key]);
                }
                cb(null, records);
            }
        });
    }
    else if(parentDir){ // get children
        // check if parentDir is directory
        fsExt.getFileInfo(parentDir, dirPath + '/' + parentDir, function(err, file){
            if(err) cb(err);
            else if(!file || !file.isDir) cb(null, []);
            else fsExt.walkdirRecursive(dirPath + '/' + parentDir,{ levels:1 }, function(err, data){
                if(err) cb(err);
                else {
                    var records = [];
                    for(var key in data){
                        records.push(data[key]);
                    }
                    cb(null, records);
                }
            }, dirPath);
        });
    }
    else fsExt.walkdirRecursive(dirPath, function(err, data){
        if(err) cb(err);
        else {
            var records = [];
            for(var key in data){
                records.push(data[key]);
            }
            cb(null, records);
        }
    });
}

// decide if walk recursive is needed
function walkOnlyChildren(findQuery){
    //.find({
    //    $and:[ { ancestors:{ $size:parent.ancestors.length + levels } },
    //           { ancestors: parent.id }]
    //})
    
    if(findQuery.ancestors && findQuery.ancestors.$size === 0) return 'root';
    else if(Array.isArray(findQuery.$and)) {
        var parent, level;
        for(var i=0;i<findQuery.$and.length;i++){
            if(typeof findQuery.$and[i].ancestors==='string') parent = findQuery.$and[i].ancestors;
            else if(typeof findQuery.$and[i].ancestors.$size === 'number') level = findQuery.$and[i].ancestors.$size;
        }
        
        if(parent && level!==undefined && level===parent.split('/').length) return parent;
        else if(level===0) return 'root';
        else return false;
    }
    else return false;
}

// helper for sorting records
function sortArray(array, sort){
    var key = Object.keys(sort)[0];
    var asc = true;
    if(sort[key]=== -1 || sort[key]==='desc') asc = false;
    
    if(key) array.sort(function(a,b){
        if(asc) return (a[key] > b[key]) ? 1 : -1;
        else return (a[key] < b[key]) ? 1 : -1;
    });
    
    return array;
}

// helper for reading file content
function getFileData(files, opts, cb){
    if(arguments.length === 2){
        cb = arguments[1];
    }
    
    if(!files || !files.length) return cb(null, files);
    
    var singleFile = !Array.isArray(files);
    if(singleFile) files = [files];
    
    async.Series.each(files, function(i, next){
        if(files[i].isFile) fsExt.readFile(files[i].fullPath, function(err, buffer){
            if(err) return next(new Error(('FileSystemDataSource')+' getFileData: EXECFAIL').details({ code:'EXECFAIL', cause:err }));
            if(!opts || opts.data) files[i].data = buffer.toString('base64');
            if(opts.content) files[i].content = buffer.toString();
            next();
        });
        else next();

    }, function(err){
        cb(err, singleFile ? files[0] : files);
    });
}


/*
 * Constructor methods
 */

/*
 * init method is for index setup, or checking data store connections, etc...
 * init should be run after any new inherited model definition
 */
FileSystem.addMethod('init', function(cb){
    var ModelCnst = this;
    
    this.extendDefaults({
        connection:{
            dirPath: fsExt.resolve(this.getDefaults().connection.dirPath).fullPath
        }
    });
    fsExt.existsOrCreate(this.getDefaults().connection.dirPath, function(err){
        if(err) throw new Error((ModelCnst._name||'FileSystemDataSource')+': init failed').cause(err);
        else if(typeof cb === 'function') cb();
    });
});

/*
 * onFetch - if data modification needed when load from data store (e.g. string to date conversion)
 */
// JsonFile.onFetch(function(data){ });


/*
 * Query builder methods - inherited from DataSource
 */


/*
 * collection().find(...).exec(...) - result is raw data, so do not fetch results
 * data source specific commands (aggregate, upsert, etc...)
 * 
 */
FileSystem.Collection.addMethod('exec', { cacheable:true, fetch:false }, function(command, args, cb){ // cb(err, count)
    var query = this,
        defaults = this._defaults,
        ModelCnst = this.getModelConstructor();
    
    getFileList(defaults.connection.dirPath, defaults.query, function(err, records){
        if(err) return cb(new Error((ModelCnst._name||'FileSystemDataSource')+' exec: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
        
        var matchedRecords = sift(defaults.query, records);
        
        matchedRecords = sortArray(matchedRecords, defaults.options.sort).slice(defaults.options.skip || 0);
        if(defaults.options.limit) matchedRecords = matchedRecords.slice(0, defaults.options.limit);
        
        cb(null, matchedRecords);
    });
});

/*
 * collection().find(...).one(callback) - callback(err, docs) result is single fetched+filled model instance or null
 */
FileSystem.Collection.addMethod('one', { cacheable:true, fetch:true }, function(cb){ // cb(err, docs)
    var query = this,
        defaults = this._defaults,
        ModelCnst = this.getModelConstructor();
    
    getFileList(defaults.connection.dirPath, defaults.query, function(err, records){
        if(err) return cb(new Error((ModelCnst._name||'FileSystemDataSource')+' one: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
        
        var matchedRecords = sift(defaults.query, records);
        
        matchedRecords = sortArray(matchedRecords, defaults.options.sort).slice(defaults.options.skip || 0, (defaults.options.skip || 0)+1);
        
        var getData = (defaults.options.fields||{}).data;
        var getContent = (defaults.options.fields||{}).content;
        if(getData || getContent) getFileData(matchedRecords[0], { data:getData, content:getContent }, cb);
        else cb(null, matchedRecords[0]);
    });
});

/*
 * collection().find(...).all(callback) - callback(err, docs) result is array of fetched+filled model instances,
 * if nothing found returns empty array
 */
FileSystem.Collection.addMethod('all', { cacheable:true, fetch:true }, function(cb){ // cb(err, docs)
    var query = this,
        defaults = this._defaults,
        ModelCnst = this.getModelConstructor();
    
    getFileList(defaults.connection.dirPath, defaults.query, function(err, records){
        if(err) return cb(new Error((ModelCnst._name||'FileSystemDataSource')+' all: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
        
        var matchedRecords = sift(defaults.query, records);
        
        matchedRecords = sortArray(matchedRecords, defaults.options.sort).slice(defaults.options.skip || 0);
        if(defaults.options.limit) matchedRecords = matchedRecords.slice(0, defaults.options.limit);
        
        var getData = (defaults.options.fields||{}).data;
        var getContent = (defaults.options.fields||{}).content;
        if(getData || getContent) getFileData(matchedRecords, { data:getData, content:getContent }, cb);
        else cb(null, matchedRecords);
    });
});

/*
 * collection().find(...).count(callback) - callback(err, count) result is count of documents
 */
FileSystem.Collection.addMethod('count', { cacheable:true, fetch:false }, function(cb){ // cb(err, count)
    var query = this,
        defaults = this._defaults,
        ModelCnst = this.getModelConstructor();
    
    getFileList(defaults.connection.dirPath, defaults.query, function(err, records){
        if(err) return cb(new Error((ModelCnst._name||'FileSystemDataSource')+' count: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
        
        var matchedRecords = sift(defaults.query, records);
        cb(null, matchedRecords.length);
    });
});

/*
 * collection().find(...).create(data, callback) - callback(err, doc/docs) result is array
 * of created documents, if data is array, else single created document
 * WARNING: this method will not run hooks and protection methods such as checking if model is valid, or optimisticLock
 */
// returns array of created documents if data is array, else single created document
FileSystem.Collection.addMethod('create', { cacheable:false, fetch:true }, function(data, cb){ // cb(err, doc/docs)
    var query = this,
        defaults = this._defaults,
        ModelCnst = this.getModelConstructor();
    
    if(!data) return cb();

    var multiple = true;
    if(!Array.isArray(data)) {
        data = [ data ];
        multiple = false;
    }
    else if(data.length===0) return cb(null, []);
    
    // prepare data
    var filename, splitted, ext;
    for(var i=0;i<data.length;i++){
        if(typeof data[i].id !== 'string') return cb(new Error((ModelCnst._name||'FileSystemDataSource')+' create: INVALID').details({ code:'INVALID', validErrs:{ id:['required'] } }));
        else {
            data[i].fullPath = defaults.connection.dirPath + '/' + data[i].id;
            filename = data[i].id.split('/').pop();
            splitted = filename.split('.');
            data[i].ext = splitted.length > 1 ? splitted.pop() : '';
            
            if(data[i].isFile) data[i].isDir = false;
            else if(data[i].isDir) data[i].isFile = false;
            else { // auto detect if it is folder or file
                if(data[i].ext) {
                    data[i].isDir = false;
                    data[i].isFile = true;
                }
                else {
                    data[i].isDir = true;
                    data[i].isFile = false;
                }
            }
        }
    }
    
    async.Series.each(data, function(i, next){
        fsExt.existsOrCreate(data[i].fullPath, { 
            encoding: data[i].encoding, 
            mode: data[i].mode, 
            data: data[i].data || data[i].content,
            isFile: data[i].isFile
        }, function(err, exists){
            // cannot create file if it already exists
            if(err) next(err);
            else if(exists && data[i].isFile) next(new Error((ModelCnst._name||'FileSystemDataSource')+' create: INVALID').details({ code:'INVALID', validErrs:{ id:['unique'] } }));
            else fsExt.getFileInfo(data[i].id, data[i].fullPath, function(err, info){
                if(err) next(err);
                else {
                    data[i] = info;
                    next();
                }
            });
        });
    }, function(err){
        if(err && err.validErrs) cb(err);
        else if(err) cb(new Error((ModelCnst._name||'FileSystemDataSource')+' create: EXECFAIL').details({ code:'EXECFAIL', cause:err }));
        else cb(null, multiple ? data : data[0]);
    });
});

/*
 * collection().find(...).update(data, callback) - callback(err, count) result is count of updated documents
 * WARNING: this method will not run hooks and protection methods such as checking if model is valid, or optimisticLock
 */
FileSystem.Collection.addMethod('update', { cacheable:false, fetch:false }, function(data, cb){ // cb(err, count)
    var query = this,
        defaults = this._defaults,
        ModelCnst = this.getModelConstructor();
    
    getFileList(defaults.connection.dirPath, defaults.query, function(err, records){
        if(err) return cb(new Error((ModelCnst._name||'FileSystemDataSource')+' update: CONNFAIL').details({ code:'CONNFAIL', cause:err }));

        var toUpdate = sift(defaults.query, records);
        var count = 0;

        async.Series.each(toUpdate, function(i, next){
            fsExt.existsOrCreate(toUpdate[i].fullPath, {
                encoding: data.encoding, 
                mode: data.mode, 
                data: data.data || data.content,
                isFile: toUpdate[i].isFile,
                replace: toUpdate[i].isFile
            }, function(err, exists){
                if(err) return next(err);
                delete toUpdate[i].data; // prevent sending data back to client
                delete toUpdate[i].content; // prevent sending data back to client
                count++;
                next();
            });

        }, function(err){
            if(err) cb(new Error((ModelCnst._name||'FileSystemDataSource')+' update: EXECFAIL').details({ code:'EXECFAIL', cause:err }));
            else cb(null, count);
        });
    });
    
});

/*
 * collection().find(...).remove(callback) - callback(err, count) result is count of removed documents
 * WARNING: this method will not run hooks and protection methods such as checking if model is valid, or optimisticLock
 */
FileSystem.Collection.addMethod('remove', { cacheable:false }, function(cb){ // cb(err, count)
    var query = this,
        defaults = this._defaults,
        ModelCnst = this.getModelConstructor();
    
    getFileList(defaults.connection.dirPath, defaults.query, function(err, records){
        if(err) return cb(new Error((ModelCnst._name||'FileSystemDataSource')+' remove: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
        
        var toRemove = sift(defaults.query, records);
        var count = 0;
        var removedFolders = [];
        
        function wasRemoved(removedFolders, ancestors){
            for(var i=0;i<ancestors.length;i++) if(removedFolders.indexOf(ancestors[i])!==-1) return true;
        }
        
        async.Series.each(toRemove, function(i, next){
            
            if(wasRemoved(removedFolders, toRemove[i].ancestors)) { // already removed by rmdirRecursive
                count++;
                next();
            }
            else if(toRemove[i].isDir) fsExt.rmdirRecursive(toRemove[i].fullPath, function(err){
                if(err) next(err);
                else {
                    count++;
                    removedFolders.push(toRemove[i].id);
                    next();
                }
            });
            else fs.unlink(toRemove[i].fullPath, function(err){
                if(err) next(err);
                else {
                    count++;
                    next();
                }
            });
            
        }, function(err){
            if(err) cb(new Error((ModelCnst._name||'FileSystemDataSource')+' remove: EXECFAIL').details({ code:'EXECFAIL', cause:err }));
            else cb(null, count);
        });
    });
});


/*
 * instance.remove(callback) - callback(err)
 * Modified because of recursive removing folder with optimisticLock caused version not found
 */
FileSystem.prototype.remove =
FileSystem.wrapHooks('remove', function(callback){ // callback(err)
    if(typeof callback !== 'function') throw new Error('callback is required');
    var model = this,
        softRemove = (model.constructor.getDefaults().options || {}).softRemove,
        optimisticLock = (model.constructor.getDefaults().options || {}).optimisticLock;
        
    // if removing empty directory turn off optimisticLock
    // TODO: find better solution than turning it off for all directories
    if(model.isDir) optimisticLock = false;
    
    // if model is in initial state, we have to validate it
    if(model.isValid() === undefined) model.validate();
    
    if(!model.isValid()) {
        callback(new Error((model.constructor._name||'FileSystemDataSource')+'.prototype.remove: INVALID').details({
            code:'INVALID',
            validErrs: model.validErrs()
        }));
    }
    else if(!model.id) {
        callback(new Error((model.constructor._name||'FileSystemDataSource')+'.prototype.remove: INVALID - missing "id"').details({
            code:'INVALID', validErrs:{ id:[ 'required' ] } }));
    }
    else if(optimisticLock && !model.modifiedDT){
        callback(new Error((model.constructor._name||'FileSystemDataSource')+'.prototype.remove: INVALID - missing "modifiedDT"').details({
            code:'INVALID', validErrs:{ modifiedDT:[ 'required' ] } }));
    }
    else {
        var query = { id: model.id };
        if(optimisticLock) query.modifiedDT = model.modifiedDT;
        if(softRemove) model.deleted = true;
        
        if(softRemove) model.constructor.collection().find(query).update({ deleted:true }, function(err, count){
            if(err) callback(err);
            else if(count===1) callback(null, model);
            else callback(new Error((model.constructor._name||'FileSystemDataSource')+'.prototype.remove: NOTFOUND "' +JSON.stringify(query)+ '"').details({ code:'NOTFOUND' }));
        });
        else model.constructor.collection().find(query).remove(function(err, count){
            if(err) callback(err);
            else if(count===1) callback(null, model);
            else callback(new Error((model.constructor._name||'FileSystemDataSource')+'.prototype.remove: NOTFOUND "' +JSON.stringify(query)+ '"').details({ code:'NOTFOUND' }));
        });
    }
});

/*
 * Model instance methods - inherited from DataSource
 */

FileSystem.prototype.read =
FileSystem.wrapHooks('read', function(opts, cb){ // cb(err, buffer)
    var file = this;
    
    if(arguments.length === 1){
        cb = arguments[0];
        opts = {};
    }
    if(typeof cb !== 'function') throw new Error('Wrong arguments');
    fsExt.readFile(file.fullPath, opts, function(err, data){
        if(err) cb(new Error((file.constructor._name||'FileSystemDataSource')+'.prototype.read: EXECFAIL').details({ code:'EXECFAIL', cause:err }));
        else cb(null, opts.toString ? data.toString() : data);
    });
});


FileSystem.prototype.readStream = function(opts){
    var file = this;
    return fs.createReadStream(file.fullPath, opts);
};

FileSystem.prototype.write =
FileSystem.wrapHooks('write', function(data, cb){ // cb(err, updatedFile)
    var file = this;
    if(typeof cb !== 'function') throw new Error('Wrong arguments');
    
    if(file.constructor.getDefaults().options.optimisticLock){
        file.constructor.collection().findId(file.id).one(function(err, currentFile){
            if(err) cb(new Error((file.constructor._name||'FileSystemDataSource')+'.prototype.write: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
            else if(!currentFile) cb(new Error((file.constructor._name||'FileSystemDataSource')+'.prototype.write: NOTFOUND').details({ code:'NOTFOUND' }));
            else if(!file.modifiedDT || currentFile.modifiedDT.getTime() - file.modifiedDT.getTime() !== 0) cb(new Error((file.constructor._name||'FileSystemDataSource')+'.prototype.write: version NOTFOUND').details({ code:'NOTFOUND' }));
            else write();
        });
    }
    else write();
    
    function write(){
        fsExt.writeFile(file.fullPath, data, function(err){
            if(err) cb(new Error((file.constructor._name||'FileSystemDataSource')+'.prototype.write: EXECFAIL').details({ code:'EXECFAIL', cause:err }));
            else fsExt.getFileInfo(file.id, file.fullPath, function(err, fileInfo){
                if(err) cb(new Error((file.constructor._name||'FileSystemDataSource')+'.prototype.write: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
                else cb(null, file.constructor.new(fileInfo));
            });
        });
    }
});

FileSystem.prototype.writeStream = function(opts){
    var file = this;
    return fs.createWriteStream(file.fullPath, opts);
};

FileSystem.prototype.rename = function(newName, cb){ // cb(err, fileInfo)
    var file = this;
    if(typeof cb !== 'function' || !newName || typeof newName !== 'string') throw new Error('Wrong arguments');
    
    var oldName = (file.name + (file.ext ? '.'+file.ext : ''));
    var oldPath = file.fullPath;
    var newPath = file.fullPath.replace(new RegExp(oldName.escape()+'$'), newName);
    var newId = file.id.replace(new RegExp(oldName.escape()+'$'), newName);
    
    if(file.constructor.getDefaults().options.optimisticLock){
        file.constructor.collection().findId(file.id).one(function(err, currentFile){
            if(err) cb(new Error((file.constructor._name||'FileSystemDataSource')+'.prototype.rename: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
            else if(!currentFile) cb(new Error((file.constructor._name||'FileSystemDataSource')+'.prototype.rename: NOTFOUND').details({ code:'NOTFOUND' }));
            else if(!file.modifiedDT || currentFile.modifiedDT.getTime() - file.modifiedDT.getTime() !== 0) cb(new Error((file.constructor._name||'FileSystemDataSource')+'.prototype.rename: version NOTFOUND').details({ code:'NOTFOUND' }));
            else rename();
        });
    }
    else rename();
    
    function rename(){
        fs.rename(oldPath, newPath, function(err){
            if(err) cb(new Error((file.constructor._name||'FileSystemDataSource')+'.prototype.rename: EXECFAIL').details({ code:'EXECFAIL', cause:err }));
            else fsExt.getFileInfo(newId, newPath, function(err, fileInfo){
                if(err) cb(new Error((file.constructor._name||'FileSystemDataSource')+'.prototype.rename: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
                else cb(null, file.constructor.new(fileInfo));
            });
        });
    }
};

/*
 * replace default Tree behaviour method
 */
FileSystem.prototype.move = FileSystem.wrapHooks('move', function(parentId, callback){ // callback(err, file)
    if(arguments.length!==2 || typeof arguments[1] !== 'function') throw new Error('Wrong arguments');
    var file = this;
    
    // move inside self is not allowed
    if(file.id === parentId) callback(new Error((file.constructor._name||'FileSystemDataSource')+'.prototype.move: Parent is same as child').details({ code:'INVALID', validErrs:{ ancestors:['invalid'] } }));
    
    // dont trust user data, load old document to ensure data integrity
    else file.constructor.collection().findId(file.id).one(function(err, currentFile){
        if(err) callback(new Error((file.constructor._name||'FileSystemDataSource')+'.prototype.move: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
        else if(!currentFile) callback(new Error((file.constructor._name||'FileSystemDataSource')+'.prototype.move: NOTFOUND').details({ code:'NOTFOUND' }));
        else if(file.ancestors[ file.ancestors.length-1 ] === parentId) callback(null, file); // not moved
        else if(file.constructor.getDefaults().options.optimisticLock && currentFile.modifiedDT.getTime() - file.modifiedDT.getTime() !== 0) {
            callback(new Error((file.constructor._name||'FileSystemDataSource')+'.prototype.move: version NOTFOUND').details({ code:'NOTFOUND' }));
        }
        // parent is "root"
        else if(parentId==='root'){
            var parent = { fullPath: file.constructor.getDefaults().connection.dirPath };
            moveFile(parent, currentFile, true, function(err, movedFileInfo){
                if(err) callback(new Error((file.constructor._name||'FileSystemDataSource')+'.prototype.move: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
                else callback(null, file.constructor.new(movedFileInfo));
            });
        }
        // get new parent
        else file.constructor.collection().findId(parentId).one(function(err, parent){
            if(err) callback(new Error((file.constructor._name||'FileSystemDataSource')+'.prototype.move: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
            else if(!parent || parent.isFile) callback(new Error((file.constructor._name||'FileSystemDataSource')+'.prototype.move: Parent not found, or it is file').details({ code:'INVALID', validErrs:{ ancestors:['invalid'] } }));
            else moveFile(parent, currentFile, true, function(err, movedFileInfo){
                if(err) callback(new Error((file.constructor._name||'FileSystemDataSource')+'.prototype.move: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
                else callback(null, file.constructor.new(movedFileInfo));
            });
        });
    });
});

/*
 * replace default Tree behaviour method
 */
FileSystem.prototype.copy = FileSystem.wrapHooks('copy', function(parentId, callback){ // callback(err, file)
    if(arguments.length!==2 || typeof arguments[1] !== 'function') throw new Error('Wrong arguments');
    var file = this;
    
    // move inside self is not allowed
    if(file.id === parentId) callback(new Error((file.constructor._name||'FileSystemDataSource')+'.prototype.copy: Parent is same as child').details({ code:'INVALID', validErrs:{ ancestors:['invalid'] } }));
    
    // dont trust user data, load old document to ensure data integrity
    else file.constructor.collection().findId(file.id).one(function(err, currentFile){
        if(err) callback(new Error((file.constructor._name||'FileSystemDataSource')+'.prototype.copy: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
        else if(!currentFile) callback(new Error((file.constructor._name||'FileSystemDataSource')+'.prototype.copy: NOTFOUND').details({ code:'NOTFOUND' }));
        else if(file.ancestors[ file.ancestors.length-1 ] === parentId) callback(null, file); // not moved
        else if(file.constructor.getDefaults().options.optimisticLock && currentFile.modifiedDT.getTime() - file.modifiedDT.getTime() !== 0) {
            callback(new Error((file.constructor._name||'FileSystemDataSource')+'.prototype.copy: version NOTFOUND').details({ code:'NOTFOUND' }));
        }
        // parent is "root"
        else if(parentId==='root'){
            var parent = { fullPath: file.constructor.getDefaults().connection.dirPath };
            moveFile(parent, currentFile, false, function(err, movedFileInfo){
                if(err) callback(new Error((file.constructor._name||'FileSystemDataSource')+'.prototype.copy: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
                else callback(null, file.constructor.new(movedFileInfo));
            });
        }
        // get new parent
        else file.constructor.collection().findId(parentId).one(function(err, parent){
            if(err) callback(new Error((file.constructor._name||'FileSystemDataSource')+'.prototype.copy: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
            else if(!parent || parent.isFile) callback(new Error((file.constructor._name||'FileSystemDataSource')+'.prototype.copy: Parent not found, or it is file').details({ code:'INVALID', validErrs:{ ancestors:['invalid'] } }));
            else moveFile(parent, currentFile, false, function(err, movedFileInfo){
                if(err) callback(new Error((file.constructor._name||'FileSystemDataSource')+'.prototype.copy: CONNFAIL').details({ code:'CONNFAIL', cause:err }));
                else callback(null, file.constructor.new(movedFileInfo));
            });
        });
    });
});

// helper for moving files/folders
function moveFile(parent, file, removeAfterCopy, cb){ // cb(err)
    if(file.isFile) {
        var newPath = parent.fullPath +'/'+ file.name +'.'+ file.ext;
        var newId = (parent.id ? parent.id + '/' : '') + file.name;
        fsExt.checkExistsName(newPath, false, function(err, newPath){
            var newName = (newPath||'').split('/').pop();
            newId = newId.replace(new RegExp(file.name.escape()+'$'), newName);
            
            if(err) cb(err);
            else file.read(function(err, data){
                if(err) cb(err);
                else fs.writeFile(newPath, data, function(err){
                    if(err) cb(err);
                    else if(removeAfterCopy) file.remove(function(err){
                        if(err) cb(err);
                        else getFileInfo(newId, newPath, cb);
                    });
                    else getFileInfo(newId, newPath, cb);
                });
            });
        });
    }
    else { // move directory
        var newPath = parent.fullPath +'/'+ file.name;
        var newId = (parent.id ? parent.id + '/' : '') + file.name;
        fsExt.checkExistsName(newPath, true, function(err, newPath){
            var newName = (newPath||'').split('/').pop();
            newId = newId.replace(new RegExp(file.name.escape()+'$'), newName);
            
            fsExt.copydirRecursive(file.fullPath, newPath, function(err){
                if(err) cb(err);
                else if(removeAfterCopy) file.remove(function(err){
                    if(err) cb(err);
                    else getFileInfo(newId, newPath, cb);
                });
                else getFileInfo(newId, newPath, cb); 
            });
        });
    }
    
    function getFileInfo(fileId, filePath, cb){
        fsExt.getFileInfo(fileId, filePath, function(err, fileInfo){
            if(err) cb(err);
            else cb(null, fileInfo);
        });
    }
}