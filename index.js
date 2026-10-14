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
        // Store the OTSM values under names that follow the report headers.
        // Columns are read by header name, so their order in the report doesn't matter.
        // Status, priority, case type, creation source and phase hold the label
        // OTSM shows users, with the internal value alongside in <field>Id; code
        // should filter on the Id fields, since labels can be renamed in OTSM.
        // Salesforce-style formatting for the current qmon client is done by the
        // PHP pages (see otsm_sf.php).
        const col = (name) => (row[name] === undefined || row[name] === null) ? "" : String(row[name]);
        const caseId = col("Id");
        url = `https://us42-smax.saas.microfocus.com/saw/Request/${caseId}/general?TENANTID=829453953`

        const doc = {
          "logTime"                 : timestamp,
          "_id"                     : caseId, // case number - primary identifier in mongo (indexed automatically)
          "title"                   : col("Title"),
          "ownerName"               : lastFirstToFirstLast(col("Owner.Name")), // "Last, First" -> "First Last"; blank = unassigned
          "updatedOn"               : toDate(col("Updated On")),
          "caseType"                : otsmLabel("caseType", col("Case Type")),
          "caseTypeId"              : col("Case Type"),
          "creationSource"          : otsmLabel("creationSource", col("Creation source")),
          "creationSourceId"        : col("Creation source"),
          "requestedByName"         : lastFirstToFirstLast(col("Requested by.Name")),
          "createdOn"               : toDate(col("Created On")),
          "status"                  : otsmLabel("status", col("Status")),
          "statusId"                : col("Status"),
          "product"                 : col("Product.Display label"),
          "priority"                : otsmLabel("priority", col("Priority")),
          "priorityId"              : col("Priority"),
          "requestedForName"        : lastFirstToFirstLast(col("Requested for.Name")),
          "requestedForEmail"       : col("Requested for.Email"),
          "requestedForOfficePhone" : col("Requested for.Office phone number"),
          "requestedForMobilePhone" : col("Requested for.Mobile phone number"),
          "requestedForTimeZone"    : col("Requested for.Time zone"),
          "companyName"             : col("Requested for company.Display Name"),
          "companyTimeZone"         : col("Requested for company.Time zone"),
          "companyRegion"           : col("Requested for company.Headquarters (Region)"),
          "ftsHostAccount"          : col("FTS Host Account"),
          "ftsHostPassword"         : col("FTS Host Password").trimStart(), // the report adds leading spaces
          "closedDateTime"          : toDate(col("Closed DateTime")),
          "lastUpdatedByName"       : lastFirstToFirstLast(col("Last Updated By.Name")),
          "sltNextTargetTime"       : toDate(col("SLT.Next target time")),
          "phase"                   : otsmLabel("phase", col("Phase Id")),
          "phaseId"                 : col("Phase Id"),
          "assignmentGroupName"     : col("Assignment group.Name"),
          "customerTimeZone"        : col("Customer Time Zone"),
          "country"                 : col("Country of Service.Display label"),
          "productDevId"            : col("Product Dev ID"),
          "productModel"            : col("Product Model.Display label"),
          "closureCode"             : col("Closure Code.Display label"),
          "resolvedDateTime"        : toDate(col("Resolved DateTime")),
          "closeTime"               : toDate(col("Close time")),
          "reportLocation"          : col("Report location.Display label"),  // requester's location, e.g. "NA/MEX"
          "reportLocationName"      : col("Report location.Display Name"),   // e.g. "Mexico"
          "url"                     : url,
        };
        const loc = customerLocation(doc);
        doc.customerRegion  = loc.region;
        doc.customerCountry = loc.country || fallbackCountry(doc);
        cases.unshift(doc);
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
  if (docs.length === 0) {
    // An empty report (e.g. a filter in the OTSM report that matches nothing). bulkWrite would fail.
    logger.warn(`Report had no cases; nothing to write to ${collection.collectionName}.`);
    return;
  }
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

/* Customer region and country, worked out from the requester's location.
   OTSM locations look like "NA/MEX" (region/country code). OTSM has no Latin
   America region, so Latin American countries in NA become LATAM.
   "NA/CAN" is also what OTSM shows when a person's location was never set, so it
   only counts as Canada when something else points to Canada. Otherwise a Latin
   American email address means LATAM, then the company's headquarters region is
   used, then the customer time zone. */
const LATAM_COUNTRIES = new Set([
  'ARG', 'BLZ', 'BOL', 'BRA', 'CHL', 'COL', 'CRI', 'CUB', 'DOM', 'ECU', 'GTM', 'GUY', 'HND', 'HTI',
  'JAM', 'MEX', 'NIC', 'PAN', 'PER', 'PRI', 'PRY', 'SLV', 'SUR', 'TTO', 'URY', 'VEN',
]);
const REGIONS = new Set(['NA', 'LATAM', 'EMEA', 'APAC']);

function customerLocation(doc) {
  const loc = doc.reportLocation.trim();
  const [locRegion, locCode] = loc.split('/');
  const country = cleanCountryName(doc.reportLocationName);

  if (loc && loc !== 'NA/CAN' && locCode) {
    const region = (locRegion === 'NA' && LATAM_COUNTRIES.has(locCode)) ? 'LATAM' : locRegion;
    return { region: region, country: country };
  }
  if (loc === 'NA/CAN' && looksCanadian(doc)) {
    return { region: 'NA', country: country || 'Canada' };
  }
  const latam = latamFromEmail(doc.requestedForEmail);
  if (latam) return latam;
  const hq = doc.companyRegion.trim();
  if (REGIONS.has(hq)) return { region: hq, country: '' };
  const tzRegion = timeZoneRegion(doc.customerTimeZone) || timeZoneRegion(doc.companyTimeZone);
  return { region: tzRegion || 'Unknown', country: '' };
}

/* Latin America from the requester's email address, for when the location
   isn't usable: a Latin American country domain (e.g. .com.br, .mx), or a
   company domain with "latam" in it (e.g. ot-latam.com, country unknown). */
const LATAM_EMAIL_COUNTRIES = {
  ar: 'Argentina', bo: 'Bolivia', br: 'Brazil', cl: 'Chile', co: 'Colombia', cr: 'Costa Rica',
  cu: 'Cuba', do: 'Dominican Republic', ec: 'Ecuador', gt: 'Guatemala', hn: 'Honduras', mx: 'Mexico',
  ni: 'Nicaragua', pa: 'Panama', pe: 'Peru', pr: 'Puerto Rico', py: 'Paraguay', sv: 'El Salvador',
  uy: 'Uruguay', ve: 'Venezuela',
};
function latamFromEmail(email) {
  const domain = (email || '').trim().toLowerCase().split('@')[1] || '';
  const parts = domain.split('.');
  const tld = parts[parts.length - 1];
  // .co on its own is also used by companies outside Colombia; only count .com.co, .gov.co...
  const isCountryDomain = LATAM_EMAIL_COUNTRIES[tld] && (tld !== 'co' || /^(com|gov|edu|org|net|mil)$/.test(parts[parts.length - 2] || ''));
  if (isCountryDomain) return { region: 'LATAM', country: LATAM_EMAIL_COUNTRIES[tld] };
  if (/latam/.test(domain)) return { region: 'LATAM', country: '' };
  return null;
}

/* Does anything besides the location say the requester is in Canada? */
const CANADA_TIME_ZONES = /^(Canada\/|America\/(Toronto|Vancouver|Edmonton|Winnipeg|Halifax|Regina|St_Johns|Moncton|Whitehorse)$)/;
const CANADA_AREA_CODES = new Set(('204 226 236 249 250 263 289 306 343 354 365 367 368 382 403 416 418 428 431 437 438 450 ' +
  '468 474 506 514 519 548 579 581 584 587 604 613 639 647 672 683 705 709 742 753 778 780 782 807 819 825 867 873 879 902 905').split(' '));

function looksCanadian(doc) {
  if (/\.ca$/i.test(doc.requestedForEmail.trim())) return true;
  if (/canada/i.test(doc.companyName)) return true;
  if (CANADA_TIME_ZONES.test(doc.customerTimeZone)) return true;
  // Phone area codes only for North American companies: some Indian mobile numbers
  // start with the same digits as Canadian area codes (902, 905).
  if (doc.companyRegion.trim() === 'NA') {
    for (const phone of [doc.requestedForMobilePhone, doc.requestedForOfficePhone]) {
      let digits = phone.replace(/\D/g, '');
      if (digits.length === 11 && digits[0] === '1') digits = digits.substring(1);
      if (digits.length === 10 && CANADA_AREA_CODES.has(digits.substring(0, 3))) return true;
    }
  }
  return false;
}

/* Region from an IANA time zone name, e.g. "Europe/Berlin" -> "EMEA". */
const LATAM_TIME_ZONES = /^(America\/(Sao_Paulo|Mexico_City|Monterrey|Bogota|Buenos_Aires|Argentina\/.*|Santiago|Lima|Caracas|Montevideo|Asuncion|La_Paz|Guatemala|Costa_Rica|Panama|Havana|Santo_Domingo|Puerto_Rico|Guayaquil|El_Salvador|Tegucigalpa|Managua)|Brazil\/.*|Mexico\/.*|Chile\/.*)$/;
const EMEA_ASIA_TIME_ZONES = /^Asia\/(Dubai|Riyadh|Qatar|Kuwait|Bahrain|Muscat|Tehran|Jerusalem|Tel_Aviv|Beirut|Amman|Baghdad|Baku|Tbilisi|Yerevan|Almaty|Tashkent|Samarkand|Nicosia)$/;

function timeZoneRegion(tz) {
  tz = (tz || '').trim();
  if (!tz) return '';
  if (LATAM_TIME_ZONES.test(tz)) return 'LATAM';
  if (/^(America|Canada|US)\//.test(tz)) return 'NA';
  if (/^(Europe|Africa)\//.test(tz) || /^(Egypt|Turkey|Israel|Iran|GB|Eire)$/.test(tz) || EMEA_ASIA_TIME_ZONES.test(tz)) return 'EMEA';
  if (/^(Asia|Australia|Pacific|Indian)\//.test(tz) || /^(Japan|Singapore|Hongkong|PRC|ROC|ROK|NZ)$/.test(tz)) return 'APAC';
  return '';
}

/* Country when the location gives none, from time zones that point to one country
   reliably (checked 2026-10-14 against cases whose country is known): first the
   requester's own time zone, then the customer time zone (usually the company's).
   Canada/Eastern and America/Montreal are OTSM defaults (about 37% Canada) and other
   zones are mixed (customer Europe/London: 46% UK), so they're left out. Phone country
   codes were tried and dropped: they show where a person is from, not where they work
   (e.g. +91 numbers at North American companies). */
const REQUESTER_TIME_ZONE_COUNTRIES = {     // 94-100% matched
  'Europe/Berlin': 'Germany', 'Asia/Kolkata': 'India', 'Asia/Bangkok': 'Thailand',
  'America/Los_Angeles': 'United States', 'Europe/Helsinki': 'Finland', 'Australia/Sydney': 'Australia',
  'Japan': 'Japan', 'Europe/London': 'United Kingdom', 'Asia/Dubai': 'United Arab Emirates',
};
const CUSTOMER_TIME_ZONE_COUNTRIES = {      // 87-100% matched
  'Asia/Kolkata': 'India', 'Japan': 'Japan', 'Australia/Sydney': 'Australia',
  'Asia/Bangkok': 'Thailand', 'Europe/Berlin': 'Germany', 'Europe/Helsinki': 'Finland',
};
function fallbackCountry(doc) {
  return REQUESTER_TIME_ZONE_COUNTRIES[(doc.requestedForTimeZone || '').trim()] ||
    CUSTOMER_TIME_ZONE_COUNTRIES[(doc.customerTimeZone || '').trim()] || '';
}

/* OTSM country names are the long ISO forms, e.g. "United States of America (the)",
   "Taiwan (Province of China)". Shorten them to the everyday name. */
const COUNTRY_NAMES = {
  'United States of America (the)': 'United States',
  'United Kingdom of Great Britain and Northern Ireland (the)': 'United Kingdom',
  'Korea (the Republic of)': 'South Korea',
  "Korea (the Democratic People's Republic of)": 'North Korea',
  'Russian Federation (the)': 'Russia',
};
function cleanCountryName(name) {
  name = (name || '').trim();
  return COUNTRY_NAMES[name] || name.replace(/ \(.*\)$/, '');
}

/* Converts an OTSM epoch-milliseconds value to a Date (stored as a Mongo date). */
// Labels the OTSM site shows for the internal values in the report
// (qmon/notes/otsm-labels.md, read from OTSM cases on 2026-10-13).
const OTSM_LABELS = {
  status: {
    RequestStatusReady:             "Ready",
    RequestStatusInProgress:        "In Progress",
    RequestStatusPending:           "Pending End User",
    RequestStatusNeedsAttention_c:  "Needs Attention",
    RequestStatusPendingInternal_c: "Pending Internal",
    RequestStatusComplete:          "Resolved",
    RequestStatusSuspended:         "Suspended",
    RequestStatusClosed_c:          "Closed",
  },
  priority: {
    CriticalPriority: "Critical",
    HighPriority:     "High",
    MediumPriority:   "Medium",
    LowPriority:      "Low",
  },
  caseType: {
    TechnicalSupport_c: "Technical Support",
    Licensing_c:        "Licensing",
    GeneralInquiry_c:   "General Inquiry",
    FlexibleCredits_c:  "Flexible Credits",
  },
  creationSource: {
    CreationSourceOther:       "Other",
    CreationSourceEmail:       "Email",
    CreationSourceEss:         "Self service Portal",
    CreationSourceMobile:      "Mobile",
    CreationSourceLiveSupport: "Live support",
    CreationSourceTeams:       "Microsoft Teams",
    CreationSourceVA:          "Virtual Agent",
    CreationSourceExternal:    "External",
  },
  phase: {
    Close:            "Close",
    FirstLineSupport: "First line support",
    Accept:           "Accept",
    Abandon:          "Abandon",
    Classify:         "Classify",
    Escalate:         "Escalate",
    Approve:          "Approve",
    Review:           "Review",
    Fulfill:          "Fulfill",
  },
};

// Values with no known label are stored as they are; each is logged once so
// the table above can be extended.
const unknownLabels = new Set();
function otsmLabel(field, value) {
  if (value === "") return "";
  const label = OTSM_LABELS[field][value];
  if (label !== undefined) return label;
  if (!unknownLabels.has(field + "=" + value)) {
    unknownLabels.add(field + "=" + value);
    logger.warn(`No OTSM label for ${field} value "${value}"; storing it unchanged. Add it to OTSM_LABELS.`);
  }
  return value;
}

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
  // OTSM integration accounts repeat the account name in both parts
  // ("csotsm_fts_intuser_829453953, csotsm_fts_intuser_829453953")
  if (lastName === restOfName) return lastName;
  return `${restOfName} ${lastName}`.trim();
}
