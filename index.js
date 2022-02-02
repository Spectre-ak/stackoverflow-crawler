/**
 * @author Akash Upadhyay
 * @email upadhyayakash2002@gmail.com
 * @repo https://github.com
 */


const fetch = require('node-fetch');
const jsdom = require("jsdom");
const { JSDOM } = jsdom;
const mongodb = require("./dbUtil");
const csvwriter = require('csv-writer');
var createCsvWriter = csvwriter.createObjectCsvWriter;



/**
 * In promise function which will fetch the stackoverflow question page and parse it using JSDOM
 * library and extract the question, link, views, total answers and up votes.
 * @param {Stackoverflow question page url} url 
 * @returns a promise
 */
function processUrlInPromise(url){
  return fetch(url)
        .then(res=>res.text())
        .then(res=>{
          console.log("Page URL: "+url);
          const dom = new JSDOM(res);
          const questions = dom.window.document.querySelectorAll('div.summary > h3 > a');
          const votes = dom.window.document.querySelectorAll('span.vote-count-post');
          const views = dom.window.document.querySelectorAll('div.views');
          const answers = dom.window.document.querySelectorAll('div.status > strong');
          console.log("Total number of questions found "+questions.length);
          const questionsOnPage = [];
          for(let i=0;i<50;i++){
            const questionMetadata = {
              questionLink : 'https://stackoverflow.com'+questions[i].href,
              questionTitle : questions[i].textContent,
              questionVotes : votes[i].textContent,
              questionViews : views[i].textContent,
              questionAnsCount : answers[i].textContent
            }
            console.log(i+1);
            console.log(questionMetadata);
            questionsOnPage.push(questionMetadata);
          }
          console.log("Dumping data to mongodb..."); 
          mongodb.saveToDB(questionsOnPage, (e)=>{
            console.log(e);
          });
        }).catch(err =>{
          console.log("Unable to process this url");
          console.log(url);
          console.log(err);
        });
}


/**
 * Asynchronous Crawler initiator function
 * @param {No of pages to crawl} upperLimitOnPageNumbers 
 */
async function InitiateAsyncCrawler(upperLimitOnPageNumbers) {
  const baseUrl = 'https://stackoverflow.com/questions?tab=frequent&pagesize=50&page=';
  const maxConcurrentRequests = 5;
  console.log("Starting Asynchronous StackOverFlow Questions Crawler........");
  console.log("Base url: "+baseUrl);
  console.log("Maximum Concurrent Requests at once: "+maxConcurrentRequests);
  console.log("No of pages to crawl: "+upperLimitOnPageNumbers);
  for(var i=1; i<=upperLimitOnPageNumbers; i=i+maxConcurrentRequests){
    console.log("Processing parallelly page numbers "+(i)+" to "+(i+maxConcurrentRequests-1));
    // Maintaining a concurrency of max 5 request at a time
    const resultArray = await Promise.all([ 
      processUrlInPromise(baseUrl+i), 
      processUrlInPromise(baseUrl+(i+1)), 
      processUrlInPromise(baseUrl+(i+2)),
      processUrlInPromise(baseUrl+(i+3)),
      processUrlInPromise(baseUrl+(i+4)),
    ]);
    console.log("Processed successfully page numbers "+(i)+" to "+(i+maxConcurrentRequests-1));
    console.log("Moving on to the next batch.....");
    console.log("==============================================")
  }
  console.log("Finished crawling "+upperLimitOnPageNumbers+" pages !!!!");
  console.log("Data will be exported to a CSV file once you terminate the crawler")
  //exitHandler(null, {});
}

InitiateAsyncCrawler(20000);


/**
 * An event listener for exporting data to a CSV file when user kills the script
 * @param {options} options 
 * @param {exitCode} exitCode 
 */
function exitHandler(options, exitCode) {
  console.log("Dumping data to a csv file......hold on a sec.....");
  mongodb.getDataStackOverFlow((data)=>{
    const csvWriter = createCsvWriter({
      path: 'StackOverFlowQuestions.csv',
      header: [
        {id: 'questionLink', title: 'questionLink'},
        {id: 'questionTitle', title: 'questionTitle'},
        {id: 'questionVotes', title: 'questionVotes'},
        {id: 'questionViews', title: 'questionViews'},
        {id: 'questionAnsCount', title: 'questionAnsCount'}
      ]
    });
    csvWriter
      .writeRecords(data)
      .then(()=> {
        console.log('Data dumped successfully to StackOverFlowQuestions.csv');
        process.exit();
      });
  });
}

process.on('SIGINT', exitHandler.bind(null, {exit:true}));
process.on('SIGUSR1', exitHandler.bind(null, {exit:true}));
process.on('SIGUSR2', exitHandler.bind(null, {exit:true}));
process.on('uncaughtException', exitHandler.bind(null, {exit:true}));
