const { MongoClient } = require("mongodb");
const dburl = require("./dbUrl");
const uri = dburl.url;

/**
 * Function for dumping data into the DB
 * @param {Data to be inserted into the DB} data 
 * @param {Callback a function to be called after operation ends} callback 
 */
async function saveToDB(data, callback) {
  const client = new MongoClient(uri, {
      useNewUrlParser: true,
      useUnifiedTopology: true,
  });
  try {
      await client.connect();
      const database = await client.db('mongoDatabase0');
      const user = await database.collection('stackoverflow');
      await user.insertMany(data,async function(err,res){
          if(err) callback(err);
          else {
            const size = await user.countDocuments({});
            callback("Dumped data successfully....");
            console.log("Total number of dumped questions so far : "+size);
          };
      });
      //await client.close();
  } finally {
      // Ensures that the client will close when you finish/error
  }
}

/**
 * Function for retrieving the saved data.
 * @param {Callback which will be called after fetching data from the DB} callback 
 */
async function getDataStackOverFlow(callback) {
  const client = new MongoClient(uri, {
      useNewUrlParser: true,
      useUnifiedTopology: true,
  });
  try {
      await client.connect();
      const database = await client.db('mongoDatabase0');
      const user = await database.collection('stackoverflow');
      const totalQuestions = [];
      await user.find().forEach(function (myDoc) {
        const questionMetadata = {
          questionLink : myDoc.questionLink,
          questionTitle : myDoc.questionTitle,
          questionVotes : myDoc.questionVotes,
          questionViews : myDoc.questionViews,
          questionAnsCount : myDoc.questionAnsCount
        }
        totalQuestions.push(questionMetadata);
      });
      console.log("Retrieved "+totalQuestions.length+" questions from the DB");
      callback(totalQuestions);
  } finally {
      // Ensures that the client will close when you finish/error
      // await client.close();
  }
}

module.exports.saveToDB = saveToDB; 
module.exports.getDataStackOverFlow = getDataStackOverFlow;
