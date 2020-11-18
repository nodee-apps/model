
/*
 * run all tests
 */

require('./cache.js');
require('./model.js');
require('./datasources/memoryDS.js');
require('./relations.js');

// tests require r/w access to jsonData.json file
require('./datasources/jsonFileDS.js');

// behaviours test
require('./behaviours/orderable.js');
require('./behaviours/tree.js');

// tests require filesystem
require('./datasources/fileSystemDS.js');

// test require connection to mongodb, please use your own databse
require('./datasources/mongoDS.js')(null, 'ds061365.mongolab.com', 61365, 'test', 'pass123');

// TODO: update elastic search new version compatibility
// test require connection to elasticSearch, please use your own elasticsearch
// require('./datasources/elasticSearchDS.js')('https://oi6juoqj4w:bhzfvm8a08@nodee-test-9340157225.us-east-1.bonsaisearch.net:443');