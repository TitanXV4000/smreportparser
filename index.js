const fs = require('fs');
const path = require('path');
const chokidar = require('chokidar');
const { MongoClient } = require('mongodb');
const logger = require('tsanford-logger'); // Adjusted to match your logger package string

// Configuration via environment variables
const MONGO_URI = process.env.MONGO_URI || 'mongodb://localhost:27017';
const DB_NAME = process.env.DB_NAME || 'sm_reports';
const COLLECTION_NAME = 'cases'; // Hardcoded target collection as requested
const WATCH_DIR = process.env.WATCH_DIR || '/app/reports'; // The base directory of your Docker volume

// Import your custom parsing module
// (Update this path if you renamed tsanford-sm.js or jwalker-sf.js locally)
const reportParser = require('./tsanford-sm.js'); 

let dbClient;
let targetCollection;

/**
 * Connects to MongoDB and initializes the target collection
 */
async function initDatabase() {
    try {
        dbClient = new MongoClient(MONGO_URI, { useUnifiedTopology: true });
        await dbClient.connect();
        const db = dbClient.db(DB_NAME);
        targetCollection = db.collection(COLLECTION_NAME);
        logger.info(`Connected successfully to MongoDB. Target collection: [${DB_NAME}.${COLLECTION_NAME}]`);
    } catch (error) {
        logger.error('Failed to connect to MongoDB:', error);
        process.exit(1);
    }
}

/**
 * Processes a newly discovered file, parses it, and inserts it into MongoDB
 * @param {string} filePath 
 */
async function processFile(filePath) {
    const fileName = path.basename(filePath);
    
    // Quick sanity check to skip hidden files or temporary files
    if (fileName.startsWith('.')) return;

    logger.info(`New report detected: ${fileName}. Starting import process...`);

    try {
        // Read file contents
        const fileContent = fs.readFileSync(filePath, 'utf8');

        // Parse records using your custom parser logic
        // Note: You can modify field mapping/massaging inside tsanford-sm.js manually
        const records = await reportParser.parse(fileContent, fileName);

        if (records && records.length > 0) {
            // Add metadata fields to track ingest tracing if desired
            const enrichedRecords = records.map(record => ({
                ...record,
                importedAt: new Date(),
                sourceFile: fileName
            }));

            // Bulk write into the 'cases' collection
            const result = await targetCollection.insertMany(enrichedRecords);
            logger.info(`Successfully imported ${result.insertedCount} records from ${fileName} into 'cases'.`);

            // Optional: Post-processing file cleanup (e.g., delete or archive the file)
            // fs.unlinkSync(filePath); 
            // logger.info(`Deleted processed file: ${fileName}`);
        } else {
            logger.warn(`No valid records found or extracted from ${fileName}.`);
        }

    } catch (error) {
        logger.error(`Error processing file ${fileName}:`, error);
    }
}

/**
 * Initializes the directory watcher on the base volume path
 */
function startWatcher() {
    logger.info(`Initializing file watcher on base directory: ${WATCH_DIR}`);

    // depth: 0 ensures we only look at the root of the volume, ignoring nested directories
    const watcher = chokidar.watch(WATCH_DIR, {
        persistent: true,
        ignoreInitial: false, // Set to true if you don't want to parse files already existing at startup
        depth: 0 
    });

    watcher.on('add', (filePath) => {
        // Debounce or process immediately depending on file lock status
        // Giving it a tiny delay to ensure the file copy stream from smexporter is fully written
        setTimeout(() => processFile(filePath), 500);
    });

    watcher.on('error', (error) => {
        logger.error(`Watcher error occurred:`, error);
    });
}

// Main execution entry point
async function main() {
    if (!fs.existsSync(WATCH_DIR)) {
        logger.error(`Watch directory does not exist: ${WATCH_DIR}. Please check your Docker volume mounts.`);
        process.exit(1);
    }

    await initDatabase();
    startWatcher();
}

main();
