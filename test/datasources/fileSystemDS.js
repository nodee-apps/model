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

// load Tree behaviour
require('../../lib/behaviours/Tree.js');

// load datasource model
var FileSystemDS = require('../../lib/datasources/FileSystem.js');

/*
 * init test Model
 */
var File = model.define('PersonFS', ['FileSystemDataSource'], {});
File.extendDefaults({ connection:{ dirPath:'./test_folder' } });

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
File.init(function(){
    // clear test folder
    File.collection().remove(function(err){
        if(err) throw err;
        testQueryMethods(testInstanceMethods);
    });
});

/*
 * test collection.query methods
 * cleanCache/clearCache, exec, count, one, all, create, update, remove
 * include options: cacheable and fetch
 */
function testQueryMethods(cb){
    // include cacheable, fetch
    
    var s = new async.Series();
    
    var records = [
        { id: 'css' },
        { id: 'css/test.css', content: 'css file test' },
        { id: 'css/test(1).css', content: 'css rename file test' },
        { id: 'css/img' }, // subfolder creation test
    ];
    
    // create
    s.add(function(next){
        File.collection().create(records, function(err, files){
            if(err) throw err;
            assert.ok(equalData(['modifiedDT', 'createdDT', 'fullPath', 'size'], files, [
                { id: 'css', name: 'css', ancestors: [], isDir: true, isFile: false, ext: null },
                { id: 'css/test.css', name: 'test', ancestors: [ 'css' ], isDir: false, isFile: true, ext: 'css' },
                { id: 'css/test(1).css', name: 'test(1)', ancestors: [ 'css' ], isDir: false, isFile: true, ext: 'css' },
                { id: 'css/img', name: 'img', ancestors: [ 'css' ], isDir: true, isFile: false, ext: null }
            ]));
            
            setTimeout(next, 1000);
        });
    });
    
    // one
    s.add(function(next){
        File.collection().one(function(err, file){
            assert.ok(!err);
            assert.ok(equalData(['modifiedDT', 'createdDT', 'fullPath', 'size'], file,
                { id: 'css', name: 'css', ancestors: [], isDir: true, isFile: false, ext: null }));
            setTimeout(next, 1000);
        });
    });
    
    // all
    s.add(function(next){
        File.collection().skip(1).limit(2).all(function(err, files){
            if(err) throw err;
            try {
                assert.ok(equalData(['modifiedDT', 'createdDT', 'fullPath', 'size'], files, [
                    { id: 'css/img', name: 'img', ancestors: [ 'css' ], isDir: true, isFile: false, ext: null },
                    { id: 'css/test(1).css', name: 'test(1)', ancestors: [ 'css' ], isDir: false, isFile: true, ext: 'css' }
                ]));
            }
            catch(err){
                console.warn(files);
                throw err;
            }
            
            setTimeout(next, 1000);
        });
    });
    
    // count
    s.add(function(next){
        File.collection().count(function(err, count){
            if(err) throw err;
            assert.ok(count === 4);
            
            setTimeout(next, 1000);
        });
    });
    
    // exec
    s.add(function(next){
        File.collection().exec('command','args', function(err, result){
            if(err) throw err;
            assert.ok(equalData(['modifiedDT', 'createdDT', 'fullPath', 'size'], result, [
                { id: 'css', name: 'css', ancestors: [], isDir: true, isFile: false, ext: null },
                { id: 'css/img', name: 'img', ancestors: [ 'css' ], isDir: true, isFile: false, ext: null },
                { id: 'css/test(1).css', name: 'test(1)', ancestors: [ 'css' ], isDir: false, isFile: true, ext: 'css' },
                { id: 'css/test.css', name: 'test', ancestors: [ 'css' ], isDir: false, isFile: true, ext: 'css' }
            ]));
            
            setTimeout(next, 1000);
        });
    });
    
    // update - directly update files is disabled
    s.add(function(next){
        File.collection().findId(['1','2']).update({ name:'updated' }, function(err, count){
            if(err) throw err;
            assert.ok(count===0);
            
            setTimeout(next, 1000);
        });
    });
    
    // remove
    s.add(function(next){
        File.collection().remove(function(err, count){
            if(err) throw err;
            assert.ok(count===4);
            
            File.collection().all(function(err, files){
                if(err) throw err;
                assert.ok(files.length===0);
                setTimeout(next, 1000); 
            });
        });
    });
    
    s.execute(function(err){
        assert.ok(!err);
        console.log('FileSystemDataSource: query methods - OK');
        cb();
    });
}


/*
 * test model instance methods
 * create, read, write, rename, remove
 */
function testInstanceMethods(cb){
    
    var f, dir;
    var s = new async.Series;
    
    // instance create file
    s.add(function(next){
        File.new().fill({ id:'willberenamed.txt', content:'test content' }).create(function(err, file){
            if(err) throw err;
            assert.ok(equalData(['modifiedDT', 'createdDT', 'fullPath', 'size'], file,
                { id: 'willberenamed.txt', name: 'willberenamed', ancestors: [], isDir: false, isFile: true, ext: 'txt' }));
            
            f = file;
            setTimeout(next, 1000);
        });
    });
    
    // instance create folder
    s.add(function(next){
        File.new().fill({ id:'subfolder' }).create(function(err, file){
            if(err) throw err;
            assert.ok(equalData(['modifiedDT', 'createdDT', 'fullPath', 'size'], file,
                { id: 'subfolder', name: 'subfolder', ancestors: [], isDir: true, isFile: false, ext: null }));
            
            dir = file;
            setTimeout(next, 1000);
        });
    });
    
    // instance read
    s.add(function(next){
        f.read(function(err, buffer){
            if(err) throw err;
            assert.ok(buffer.toString() === 'test content');
            setTimeout(next, 1000);
        });
    });
    
    // instance write
    s.add(function(next){
        f.write('updated content', function(err, file){
            if(err) throw err;
            
            // try update not last version
            f.write('updated content fail', function(err){
                assert.ok(err.message === 'PersonFS.prototype.write: version NOTFOUND');

                f.read(function(err, buffer){
                    if(err) throw err;
                    assert.ok(buffer.toString() === 'updated content');

                    f = file;
                    setTimeout(next, 1000);
                });
            });
        });
    });
    
    // instance rename - file
    s.add(function(next){
        f.rename('renamed.txt', function(err, renamedFile){
            if(err) throw err;
            assert.ok(renamedFile.id === 'renamed.txt');
            
            f = renamedFile;
            setTimeout(next, 1000);
        });
    });
    
    // instance move - file
    s.add(function(next){
        f.move('subfolder', function(err, movedFile){
            if(err) throw err;
            assert.ok(movedFile.id === 'subfolder/renamed.txt');
            
            f = movedFile;
            setTimeout(next, 1000);
        });
    });
    
    // instance rename - folder
    s.add(function(next){
        dir.rename('renamedfolder', function(err, renamedFile){
            assert.ok(err.message === 'PersonFS.prototype.rename: version NOTFOUND');
            
            File.collection().findId(dir.id).one(function(err, updatedDir){
                if(err) throw err;
                dir = updatedDir;
                
                dir.rename('renamedfolder', function(err, renamedFile){
                    assert.ok(renamedFile.id === 'renamedfolder');
                    
                    dir = renamedFile;
                    setTimeout(next, 1000);
                });
            });
        });
    });
    
    // instance copy - folder
    s.add(function(next){
        dir.copy('root', function(err, newFile){
            if(err) throw err;
            assert.ok(newFile.id === 'renamedfolder(1)');
            
            dir = newFile;
            setTimeout(next, 1000);
        });
    });
    
    // instance move - folder
    s.add(function(next){
        dir.move('renamedfolder', function(err, newFile){
            if(err) throw err;
            assert.ok(newFile.id === 'renamedfolder/renamedfolder(1)');
            
            //dir = newFile;
            setTimeout(next, 1000);
        });
    });
    
    // instance remove - file
    s.add(function(next){
        f.remove(function(err){
            assert.ok(err.code === 'NOTFOUND');
            
            File.collection().findId('renamedfolder/renamed.txt').one(function(err, file){
                if(err) throw err;
                
                file.remove(function(err){
                    if(err) throw err;
                    
                    // check if record exists
                    File.collection().findId('renamedfolder/renamed.txt').all(function(err, files){
                        if(err) throw err;
                        assert.ok(files.length===0);
                        setTimeout(next, 1000);
                    });
                });
            });
        });
    });
    
    // instance remove folder
    s.add(function(next){
        File.collection().findId('renamedfolder').one(function(err, dir){
            if(err) throw err;
            
            dir.remove(function(err){
                if(err) throw err;
                
                // check if record exists
                File.collection().all(function(err, persons){
                    if(err) throw err;
                    assert.ok(persons.length===0);
                    setTimeout(next, 1000);
                });
            });
        });
    });
    
    s.execute(function(err){
        assert.ok(!err);
        console.log('FileSystemDataSource: instance methods - OK');
    });
}