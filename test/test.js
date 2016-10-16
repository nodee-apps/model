
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
require('./datasources/mongoDS.js')('ds061365.mongolab.com', 61365, 'test', 'pass123');

// test require connection to elasticSearch, please use your own elasticsearch
require('./datasources/elasticSearchDS.js')('https://gf9b8nkuvr:rjvhm2jhiy@nodee-2480254251.eu-west-1.bonsai.io');