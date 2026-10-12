/* smreportparser
Listens for changes at config.DOWNLOAD_PATH and parses csv into js objects, then sends to mongo
tsanford
*/
const config = require('./config');
const { MongoClient } = require('mongodb');
const fs = require('fs');
const chokidar = require('chokidar');
const parse = require('csv-parse/lib/sync');
const tsanfordLogger = require('tsanford-logger');
const nodemailer = require('nodemailer');
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
        // Store the raw OTSM values. Salesforce-style formatting for the
        // current qmon client is done by the PHP pages (see otsm_sf.php).
        url = `https://us42-smax.saas.microfocus.com/saw/Request/${values[0]}/general?TENANTID=829453953`

        //Calc caseOwner ("Last, First" -> "First Last"). Blank stays blank (unassigned).
        const finalOwner = lastFirstToFirstLast(values[2]);

        //Calc contactName ("Last, First" -> "First Last")
        const finalContact = lastFirstToFirstLast(values[14]);

        cases.unshift({
          "logTime"            : timestamp,
          "_id"                : values[0], // case number - primary identifier in mongo (indexed automatically)
          "caseID"             : values[0],
          "caseOwner"          : finalOwner,
          "caseOwnerAlias"     : values[3],
          "caseDate"           : toDate(values[4]),
          "subject"            : values[1],
          "type"               : values[5],
          "caseOrigin"         : values[6],
          "createdBy"          : values[7],
          "dateTimeOpened"     : toDate(values[9]),
          "status"             : values[10],
          "product"            : values[12],
          "productModel"       : values[40],
          "productGroup"       : values[30],
          "severity"           : values[13],
          "rdIncident"         : values[38],
          "contactName"        : finalContact,
          "contactEmail"       : values[16],
          "contactPhone"       : values[17],
          //"contactMobile"      : values[18],
          "contactRegion"      : values[19],
          "country"            : values[34],
          "accountName"        : values[21],
          "FTSAccountName"     : values[22],
          "FTSPassword"        : values[23] ? values[23].trimStart() : values[23], // the report adds leading spaces
          "dateTimeClosed"     : toDate(values[24]),
          // by header name, so these columns can be anywhere in the report
          "closeTime"          : toDate(row["Close time"]),
          "dateTimeResolved"   : toDate(row["Resolved DateTime"]),
          "closeCode"          : row["Closure Code.Display label"] || "", // by header name, so the column can be anywhere in the report
          "closeCodeId"        : row["Closure Code.Id"] || "",
          "phaseId"            : values[29],
          "caseLastModifiedBy" : values[25],
          "accountCountry"     : values[34],
          "url"                : url,
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

/* Converts an OTSM epoch-milliseconds value to a Date (stored as a Mongo date). */
function toDate(epochMs) {
  if (!epochMs) return null;
  return new Date(Number(epochMs));
}

/* "Last, First" -> "First Last". Values without a comma are returned trimmed. */
function lastFirstToFirstLast(name) {
  const raw = name ? name.trim() : '';
  if (!raw.includes(',')) return raw;
  const firstCommaIndex = raw.indexOf(',');
  const lastName = raw.substring(0, firstCommaIndex).trim();
  const restOfName = raw.substring(firstCommaIndex + 1).trim();
  return `${restOfName} ${lastName}`.trim();
}
