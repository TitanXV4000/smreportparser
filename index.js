/* smreportparser
Listens for changes at config.DOWNLOAD_PATH and parses csv into js objects, then sends to mongo
tsanford
*/
const config = require('./config');
const { MongoClient } = require('mongodb');
const puppeteer = require('puppeteer');
const fs = require('fs');
const chokidar = require('chokidar');
const parse = require('csv-parse/lib/sync');
const tsanfordLogger = require('tsanford-logger');
const nodemailer = require('nodemailer');
const sf = require('./tsanford-sm');
const emitter = require('events').EventEmitter;
const path = require('path');

// TODO integrate with config.js / env variables
const transporter = nodemailer.createTransport({
  service: 'gmail',
  auth: {
    user: config.EMAIL_FROM,
    pass: config.EMAIL_FROM_PASS
  }
});

// Consolidated into a single cases collection
const MONGO_CASES_COLLECTION = 'cases';
const MONGO_UNASSIGNED_COLLECTION = 'unassigned';

var lastUpdateTime;

const logger = tsanfordLogger.newLogger();
logger.info("Starting up.");
var processingCSV = false;

var casesUpdatedEmitter = new emitter();

var initialized = false;
casesUpdatedEmitter
  .on('cases updated', async function(cases) {
    logger.debug("Received 'cases updated' event from casesUpdatedEmitter.");
    // Cross-collection cleanups removed; Mongo upsert handles lifecycle state changes dynamically.
  })
  .on('unassigned updated', async function(mfiSupportCases) {
    logger.debug("Received 'unassigned updated' event from casesUpdatedEmitter.");
    // await updateAssigned(mfiSupportCases);
  })
  .on('csv parsed', async function(filename) {
    logger.debug("Received 'csv parsed' event from casesUpdatedEmitter.");
    processingCSV = false;
    await deleteFile(filename);
  })
  .on('cases parsed', async function(collection, cases) {
    logger.debug("Received 'cases parsed' event from casesUpdatedEmitter.");
    await uploadToMongo(collection, cases);
  });


var watcher = chokidar.watch(config.DOWNLOAD_PATH, 
  {
    ignored: /\.crdownload$/g,
    //ignoreInitial: /\.csv$/g,
    ignoreInitial: true,
    persistent: true,
    depth: 0
  });

watcher
  .on('ready', () => {
    logger.info('Initial scan complete. Ready to process new reports.');
    initialized = true;
  })
  .on('change', function(filePath) { logger.debug('File ' + filePath + ' has been changed.'); })
  .on('unlink', function(filePath) { logger.debug('File ' + filePath + ' has been removed.'); })
  .on('error', function(e) { logger.error(e.toLocaleString()); })
  .on('add', async function(filePath) {
    /* Exit function immediately if not initialized or the previous csv file 
       is still being processed. */
    if (!initialized/* || processingCSV*/) return;

    /* Initialization complete. Time to parse... */
    processingCSV = true;

    /* Get the report tag from the beginning of the basename */
    const reportTag = ((path.basename(filePath)).split('_'))[0];
    logger.debug(`reportTag from filename: ${reportTag}`);
    
    // Direct both open and closed files to the unified 'cases' collection
    let targetCollection = reportTag;
    if (reportTag === "open" || reportTag === "closed") {
      targetCollection = MONGO_CASES_COLLECTION;
    }
    
    logger.info(`Detected new file \'${filePath}\'. Begin parsing...`);
    var latestReport = await readFile(filePath);
    
    // 1. Convert the file buffer to a clean UTF-8 string
    let reportContent = latestReport.toString('utf8');
    
    // 2. Explicitly strip the hidden Byte Order Mark (\ufeff) if it exists
    if (reportContent.startsWith('\uFEFF')) {
        reportContent = reportContent.replace(/^\uFEFF/, '');
    }

    // 3. Pass the clean string data directly to your parser
    var data = await parse(reportContent, {
        columns: true,
        skip_empty_lines: true,
        relax_quotes: true,        // Handles loose or unescaped quotes within fields
        trim: true                 // Trims whitespace around headers and values
    });
    logger.silly("Loaded the following data from file: " + JSON.stringify(data));

    var timestamp = new Date();
    logger.debug("logTime timestamp: " + timestamp.toLocaleString('en-US', { timeZone: config.TIMEZONE}).replace(',', ''));

    var cases = [];
    for (const row of data) {
      values = Object.values(row);
      if (reportTag === "open" || reportTag === "closed") {
        url = `https://us42-smax.saas.microfocus.com/saw/Request/${values[0]}/general?TENANTID=731633586`
        urlPrintView = `https://us42-smax.saas.microfocus.com/saw/Request/${values[0]}/general?TENANTID=731633586`
        const UpdatedOn = convertToSalesforceDate(values[4]);
        const CreatedOn = convertToSalesforceDate(values[9]);
        const ClosedDateTime = convertToSalesforceDate(values[24]);
        // calc ageHours
        //let ageHours = "";
        //if (values[9]) {
        //  ageHours = ((Date.now() - Number(values[9])) / 3600000).toFixed(1);
        //  if (Number(ageHours) < 0) ageHours = "0.0";
        //}

        let ageHours = "";
        if (values[9]) {
            const totalHours = (Date.now() - Number(values[9])) / 3600000;
            ageHours = String(Math.round(totalHours));
            if (Number(ageHours) < 0) ageHours = "0";
        }

        const statusMap = {
            "RequestStatusClosed_c":          "Closed",
            "RequestStatusComplete":          "Solution Suggested",
            "RequestStatusInProgress":        "Pending Support",
            "RequestStatusNeedsAttention_c":  "Pending Support (New Activity)",
            "RequestStatusPending":           "Pending Customer",
            "RequestStatusPendingInternal_c": "Pending Internal",
            "RequestStatusReady":             "New",
            "RequestStatusSuspended":         "Suspended"
        };
        const status = statusMap[values[10]] || values[10];

        // 1. Severity Mapping Table
        const severityMap = {
            "CriticalPriority": "1 - Critical",
            "HighPriority":     "2 - High",
            "MediumPriority":   "3 - Medium",
            "Low Priority":     "4 - Low" // Handled exactly as written in your source data
        };
        const severity = severityMap[values[13]] || values[13];

        cases.unshift({
          "logTime"            : timestamp,
          "_id"                : values[0], // case number - primary identifier in mongo (indexed automatically)
          "caseID"             : values[0],
          "caseOwner"          : (values[2] && values[2].trim()) ? values[2].trim() : "MFI Support",
          "caseOwnerAlias"     : values[3],
          "caseDate"           : UpdatedOn,
          "subject"            : values[1],
          "type"               : values[5],
          "caseOrigin"         : values[6],
          "createdBy"          : values[7],
          "dateTimeOpened"     : CreatedOn,
          "ageHours"           : ageHours,
          "status"             : status,
          "milestoneStatus"    : "Compliant",
          "product"            : values[12],
          "supportProduct"     : values[12],
          "productGroup"       : values[30],
          "severity"           : severity,
          "rdIncident"         : values[38],
          "rdChangeRequest"    : "",
          "contactName"        : values[14],
          "contactEmail"       : values[16],
          "contactPhone"       : values[17],
          //"contactMobile"      : values[18],
          "contactRegion"      : values[19],
          "country"            : values[33],
          "accountName"        : values[21],
          "businessHours"      : "01m1t000000qVUj",
          "description"        : "NOT AVAILABLE",
          "caseComments"       : "NOT AVAILABLE",
          "FTSAccountName"     : values[22],
          "FTSPassword"        : values[23],
          "dateTimeClosed"     : ClosedDateTime,
          "closureSummary"     : "",
          "closeCode"          : "",
          "caseLastModifiedBy" : values[25],
          "accountCountry"     : values[33],
          "url"                : url,
          "urlPrintView"       : urlPrintView,
          "kbArticle"          : "",
          "IdolkbLink"         : "",
        });
      } else if (reportTag === "kb") {
        url = `https://microfocus.lightning.force.com/lightning/r/Knowledge__kav/${values[1]}/view`
        cases.unshift({
          "logTime"               : timestamp,
          "_id"                   : values[0], 
          "kbID"                  : values[1],
          "title"                 : values[2],
          "createdBy"             : values[3],
          "modifiedBy"            : values[4],
          "approver"              : values[5],
          "publicationStatus"     : values[6],
          "createdDate"           : values[7],
          "lastModifiedDate"      : values[8],
          "firstPublishedDate"    : values[9],
          "lastPublishedDate"     : values[10],
          "caseAssocCount"        : values[11],
          "productRelation"       : values[12],
          "sfURL"                 : url,
          "portalURL"             : values[13],
        });
      }
    }
    logger.silly("Created case objects: " + cases);
    casesUpdatedEmitter.emit(`cases parsed`, targetCollection, cases);

    lastUpdateTime = new Date();
    casesUpdatedEmitter.emit('csv parsed', filePath);
  });

/* Accepts list of case objects to upload to mongo */
async function uploadToMongo(collectionName, cases) {
  logger.debug(`Sending cases to mongo collection \'${collectionName}\'.`);
  /* Connect to MongoDB */
  const client = new MongoClient(config.MONGO_URI, {useNewUrlParser: true, useUnifiedTopology: true});
  try {
    logger.debug("Connecting to mongo db \"" + config.MONGO_DB + "\" at url: " + config.MONGO_URI);
    await client.connect();
    const database = client.db(config.MONGO_DB);
    await database.command({ ping: 1 });
    logger.debug("Connected successfully to mongo server.");

    database.createCollection(collectionName, function (e) {
      if (e) logger.debug(e.toLocaleString());
    });
    const collection = database.collection(collectionName);

    await upsertToCollection(collection, cases);

    casesUpdatedEmitter.emit(`${collectionName} updated`, cases);
    logger.debug("Mongo updated successfully.");
  } catch (e) {
    logger.error(e.toLocaleString());
  } finally {
    await client.close();
    logger.debug("Mongo connection closed.");
  }
}

/* Uses bulkWrite to quickly upsert documents to the specified collection */
async function upsertToCollection(collection, docs) {
  logger.debug(`Upserting documents to ${collection.collectionName}.`);
  const bulkOps = [];

  for (const doc of docs) {
    const filter = { _id: doc._id };
    bulkOps.push(
      { replaceOne :
        {
          "filter"      : filter,
          "replacement" : doc,
          "upsert"      : true
        }
      }
    );
  }

  const result = await collection.bulkWrite(bulkOps);
  logger.info(`Updated ${result.nModified} documents to ${collection.collectionName}.`);
  logger.info(`Upserted ${result.nUpserted} documents to ${collection.collectionName}.`);
}

/* Deletes objects from a collection. */
async function deleteDocuments(collection, docs) {
  try {
    logger.debug(`Deleting documents from collection \'${collection.collectionName}\'`);
    const bulkOps = [];

    for (const doc of docs) {
      const filter = { _id: doc._id };
      bulkOps.push(
        { deleteOne :
          {
            "filter" : filter
          }
        }
      );
    }

    const result = await collection.bulkWrite(bulkOps);
    logger.info(`Deleted ${result.nRemoved} documents from ${collection.collectionName}.`);
  } catch (e) {
    logger.error(`Caught error in deleteDocuments: ${e.toLocaleString()}`);
  }
}

/* Query for all open cases with owner 'MFI Support' and add to unassigned collection */
async function updateUnassigned() {
  logger.info("Updating unassigned collection (frontline).");
  const client = new MongoClient(config.MONGO_URI, {useNewUrlParser: true, useUnifiedTopology: true});

  try {
    logger.debug("Connecting to mongo db \"" + config.MONGO_DB + "\" at url: " + config.MONGO_URI);
    await client.connect();
    const database = client.db(config.MONGO_DB);
    await database.command({ ping: 1 });
    logger.debug("Connected successfully to mongo server.");

    database.createCollection(MONGO_UNASSIGNED_COLLECTION, function (e) {
      if (e) logger.debug(e.toLocaleString());
    });
    
    // Updated to pull from unified cases collection filtering by status
    const casesCollection = database.collection(MONGO_CASES_COLLECTION);
    const unassignedCollection = database.collection(MONGO_UNASSIGNED_COLLECTION);

    // Filter by MFI Support and exclude Closed statuses
    const query = { caseOwner: "MFI Support", status: { $ne: "Closed" } };
    const options = { sort: { dateTimeOpened: 1 } };

    const mfiSupportCases = casesCollection.find(query, options);
    logger.debug(`Found ${await mfiSupportCases.count()} cases.`);
    
    await mfiSupportCases.forEach(function(i) {
      logger.silly(`mfiSupportCases: ${i._id}`);
    });

    const mfiSupportCasesArray = await mfiSupportCases.toArray();
    await upsertToCollection(unassignedCollection, mfiSupportCasesArray);

    casesUpdatedEmitter.emit('unassigned updated', mfiSupportCasesArray);
  } catch (e) {
    logger.error(e.toLocaleString());
  } finally {
    client.close();
    logger.debug("Mongo connection closed.");
  }
}

/* Compare new MFI Support cases against previous records and update fields */
async function updateAssigned(mfiSupportCases) {
  logger.info("Updating fields of assigned cases.");
  const client = new MongoClient(config.MONGO_URI, {useNewUrlParser: true, useUnifiedTopology: true});
  try {
    logger.debug("Connecting to mongo db \"" + config.MONGO_DB + "\" at url: " + config.MONGO_URI);
    await client.connect();
    const database = client.db(config.MONGO_DB);
    await database.command({ ping: 1 });
    logger.debug("Connected successfully to mongo server.");

    const casesCollection = database.collection(MONGO_CASES_COLLECTION);
    const unassignedCollection = database.collection(MONGO_UNASSIGNED_COLLECTION);
    
    const moveQueue = [];
    const previousCases = unassignedCollection.find();
    logger.debug("Updating existing cases in mongo that are no longer in the queue...");
    await previousCases.forEach(function(i) {
      let exists = false;

      for (const value of mfiSupportCases) {
        if (value._id === i._id) {
          logger.silly("Case " + i._id + " is still in the queue. Skipping...");
          exists = true;
        }
      }

      if (!exists) {
        logger.debug("Found a case that is no longer in the queue.");
        moveQueue.unshift(i);
      }
    });

    if (moveQueue.length > 0) {
      logger.debug(`Detected ${moveQueue.length} newly-assigned cases. Preparing to update documents.`);

      await updateFields(moveQueue);
      await deleteDocuments(unassignedCollection, moveQueue);
      await upsertToCollection(casesCollection, moveQueue);

      logger.debug("Mongo updated successfully.");
    } else {
      logger.info("No cases need to be updated at this time.");
    }
  } catch (e) {
    logger.error(e.toLocaleString());
  } finally {
    client.close();
    logger.debug("Mongo connection closed.");
  }
}

async function updateFields(moveQueue) {
  logger.debug("In updateFields() function.");
  try {
    var browser = await puppeteer.launch({
      args: ['--no-sandbox'],
    });
    const context = browser.defaultBrowserContext();
    context.overridePermissions(url, ["notifications"]);

    logger.debug("Browser loaded.");

    const page = await sf.login(
      browser, 
      config.SF_LOGIN_URL, 
      config.USER_LOGIN, 
      config.PASS, 
      config.LOGIN_TIMEOUT, 
      logger
    );
    var count = 1;
    for (var _case of moveQueue) {
      logger.debug(`${count++} / ${moveQueue.length}`);
      if (!_case) {
        logger.error("null entry found in moveQueue. Skipping...");
        break;
      }

      logger.silly("_case: " + (JSON.stringify(_case))._id);
      logger.debug("Looping through cases in moveQueue - querying for new owner for case " + _case._id + ".");
      if (!_case.urlPrintView) {
        _case.urlPrintView = urlPrintView = `https://microfocus.my.salesforce.com/${_case.caseID}/p`;
        logger.debug("Added urlPrintView: " + _case.urlPrintView);
      }

      const newFields = await evaluatePage(page, _case.urlPrintView);
      logger.debug("newFields: " + JSON.stringify(newFields));

      if (!newFields.owner) { 
        throw new Error("newFields empty. Skipping mongo update.");
      }

      _case.caseOwner      = newFields.owner;
      _case.caseOwnerAlias = newFields.owner;
      _case.product        = newFields.product;
      _case.subject        = newFields.subject;
    }
    logger.info('Finished updating new fields.');
  } catch (e) {
    logger.error("Throwing error from updateFields().");
    throw (e);
  } finally {
    await browser.close();
    logger.debug("Browser closed.");
  }
}

async function evaluatePage(page, url) {
  logger.debug("Updating fields for [" + url + "].");
  logger.debug("page.goto: " + url);
  await page.goto(url, { waitUntil: 'networkidle2' });
  logger.debug("calling sf.waitForNetworkIdle(2000)...");
  sf.waitForNetworkIdle(page, 5000, 0);
  logger.debug("done");
  
  return await page.evaluate(() => {
    const owner = document.querySelector(
      "#mainTable > div.pbBody > div:nth-child(15) > table > tbody > tr:nth-child(4) > td.dataCol.last.col02"
    ).innerText;

    const product = document.querySelector(
      "#mainTable > div.pbBody > div:nth-child(7) > table > tbody > tr:nth-child(1) > td:nth-child(4)"
    ).innerText;

    const subject = document.querySelector(
      "#mainTable > div.pbBody > div:nth-child(3) > table > tbody > tr:nth-child(5) > td.dataCol.col02"
    ).innerText;

    return { owner, product, subject };
  });
}

/* Sends an error if no new files have been detected in at least 2 minutes */
async function checkLastUpdateTime() {
  let finished = false;
  await sleep(120000); 
  do {
    const currentTime = new Date();
    let diff = currentTime - lastUpdateTime;

    if (diff > 600000) { 
      let errorMessage = "Error: it has been " + Math.floor(diff / 1000 / 60) + " minutes since receiving any updates.\nYou may need to check on the sfexporter.";
      logger.error(errorMessage);

      const mailOptions = {
        from: config.EMAIL_FROM,
        to: config.EMAIL_TO,
        subject: 'Alert - [sfreportparser] error',
        text: errorMessage
      };
      transporter.sendMail(mailOptions, function(error, info){
        if (error) {
          console.log(error);
        } else {
          console.log('Email sent: ' + info.response);
        }
      });
      process.exit(5); 
      
    } else {
      logger.debug("Last update was received " + Math.floor(diff / 1000) + " seconds ago.");
    }
    
    await sleep(60000);
  } while (!finished);
}
checkLastUpdateTime();

async function readFile(path) {
  try {
    return fs.readFileSync(path);
  } catch (e) {
    logger.error("Error caught in readFile: " + e.toLocaleString());
  }
}

async function deleteFile(path) {
  try {
    logger.debug(`Deleting file ${path}`);
    fs.unlinkSync(path);
  } catch (error) { // Changed 'e' to 'error' to completely isolate it from other scopes
    logger.error("Error caught in deleteFile(): " + error.toLocaleString());
  }
}

async function sleep(ms) {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

function convertToSalesforceDate(epochMs) {
  if (!epochMs) return "";
  
  const date = new Date(Number(epochMs));
  
  // Configure formatting options to match standard US format
  const options = {
    year: 'numeric',
    month: 'numeric',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
    second: '2-digit',
    hour12: true,
    timeZone: 'America/Denver' // Keeps the timezone aligned with your local mountain time logs
  };

  // Intl.DateTimeFormat outputs standard formats cleanly
  const formatter = new Intl.DateTimeFormat('en-US', options);
  
  // Replace the narrow/non-breaking space character with a standard space if necessary
  return formatter.format(date).replace(/[\u202f\u2016]/g, ' ');
}
