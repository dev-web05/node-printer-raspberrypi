const { init } = require('./db');
const WebSocket = require('ws');
const { exec } = require('child_process');
const fs = require('fs');
const path = require("path");
const axios = require("axios");

require("dotenv").config();

const SHOP_ID = process.env.SHOP_ID || 'PI_001';
const PRINTER_ID = process.env.PRINTER_ID || 'ZINK_hAppy_3233_USB';
const JOBS_COLLECTION = 'jobs';

// Global DB reference
let db;
let ws;
(async () => {
  db = await init();
  startWebSocket();
})();

// WebSocket client to receive print jobs
function startWebSocket() {
  ws = new WebSocket('ws://100.73.198.91:2001');

  ws.on('open', () => {
    ws.send(JSON.stringify({ shopId: SHOP_ID }));
  });

  ws.on('message', async (data) => {
    try {
      const job = JSON.parse(data);
      console.log(`Received print job for ShopID: ${job.shopId}`);
      console.log('job:', job);
      // Check printer status first
      checkPrinterStatus(async (status) => {
        // TODO: remove this line after testing
        if (status === 'online') {
          console.log("Printer is online, printing immediately...");
          // Print immediately, no DB insert
          console.log('job:', job);
          const filePath = await downloadImage(job.imageUrl, path.join(__dirname, 'images'), `oilData-${job.oilDataId}.jpeg`);
          if (filePath) {
            job.filePath = filePath; // add local file path to job
            console.log('Image downloaded successfully:', filePath);

            sendToPrinter(job, PRINTER_ID, true); // true = delete after print
          } else {
            console.error("Failed to download image, not printing.");
          }
        } else {
          console.log(`Printer is ${status}, queueing job...`);
          await insertJobToMongo(job); // status will be 'queued'
        }
      });
    } catch (err) {
      console.error("Error handling WebSocket message:", err);
    }
  });
}

/**
 * Download image from URL and save it to a directory
 */
async function downloadImage(url, dir, filename) {
  try {
    // Make sure directory exists
    if (!fs.existsSync(dir)) {
      fs.mkdirSync(dir, { recursive: true });
    }

    const filePath = path.resolve(dir, filename);

    // check if file already exists
    if (fs.existsSync(filePath)) {
      console.log(`✅ Image already exists: ${filePath}`);
      return filePath;
    }

    // Download image
    const response = await axios({
      url,
      method: "GET",
      responseType: "stream",
    });

    const writer = fs.createWriteStream(filePath);

    response.data.pipe(writer);

    return new Promise((resolve, reject) => {
      writer.on("finish", () => {
        console.log(`✅ Image saved: ${filePath}`);
        resolve(filePath);
      });
      writer.on("error", reject);
    });
  } catch (error) {
    console.error("❌ Error downloading image:", error.message);
    return false;
  }
}

// Insert job into MongoDB with status 'queued'
async function insertJobToMongo(job) {
  if (!db) {
    console.error("MongoDB not connected.");
    return false;
  }
  const oldJob = await db.collection(JOBS_COLLECTION).findOne({
    oilDataId: job.oilDataId,
  });

  if (oldJob) {
    // delete existing queued job for same oilDataId
    await db.collection(JOBS_COLLECTION).deleteMany({ oilDataId: job.oilDataId });
  }
  job.status = 'queued';
  job.createdAt = new Date();
  job.updatedAt = new Date();
  job.printerId = PRINTER_ID;
  await db.collection(JOBS_COLLECTION).insertOne(job);
}

// Enhanced printer status checker using verbose lpstat output
function checkPrinterStatus(callback) {
  exec(`lpstat -p ${PRINTER_ID} -l`, (err, stdout, stderr) => {
    const output = (stdout + stderr).toLowerCase();
    let status = 'unknown';
    if (output.includes('idle')) status = 'online';
    else if (output.includes('printing')) status = 'busy';
    else if (output.includes('disabled')) status = 'offline';
    else if (output.includes('paused')) status = 'paused';
    else if (output.includes('out of paper')) status = 'out_of_paper';
    else if (output.includes('filter failed')) status = 'error_filter_failed';
    else if (output.includes('stopped')) status = 'stopped';

    console.log(`Printer status 1111111111111: ${status}`);
    callback(status, stdout + stderr);
  });
}

// Crop PDF helper
function cropPdfWithPdfcrop(inputPdf, outputPdf, callback) {
  const cropCmd = `pdfcrop "${inputPdf}" "${outputPdf}"`;
  exec(cropCmd, (err, stdout, stderr) => {
    if (err) {
      console.error('PDF crop failed:', stderr);
      if (callback) callback(err);
      return;
    }
    console.log('PDF cropped:', outputPdf);
    if (callback) callback(null, outputPdf);
  });
}

// Clean up temporary files created during print preparation
function cleanupTempFiles(originalImagePath) {
  const outputPdf = originalImagePath.replace(/\.(png|jpg|jpeg)$/i, '.pdf');
  const croppedPdf = outputPdf.replace('.pdf', '-cropped.pdf');
  [outputPdf, croppedPdf, originalImagePath].forEach(file => {
    fs.unlink(file, (err) => {
      if (err) console.warn(`Failed to delete file ${file}:`, err);
      else console.log(`Deleted temporary file: ${file}`);
    });
  });
}

// Send the actual print job to CUPS printer
function sendToPrinter(job, printer, deleteAfterPrint = false) {
  console.log(`Sending job to printer: ${printer} (file: ${job.filePath})`);
  printImageWithZinkPrinter(job.filePath, printer, async (err, stdout) => {
    try {
      if (err) {
        console.error("Print failed:", err);
        if (job._id) {
          // If _id exists (queued job), delete by _id
          await db.collection(JOBS_COLLECTION).deleteOne({ _id: job._id });
        } else {
          // If no _id (immediate print), delete by oilDataId
          await db.collection(JOBS_COLLECTION).deleteOne({ oilDataId: job.oilDataId });
        }
      } else {
        console.log("Image printed successfully:", stdout);
        ws.send(JSON.stringify({
          done: true,
          shopId: job.shopId,
          jobId: job._id,
          message: "Print job completed successfully"
        }));
        if (job._id || deleteAfterPrint) {
          // If _id exists (queued job), or immediate print, delete by _id
          await db.collection(JOBS_COLLECTION).deleteOne({ _id: job._id });
        } else {
          // If no _id (immediate print), delete by oilDataId
          await db.collection(JOBS_COLLECTION).deleteOne({ oilDataId: job.oilDataId });
        }
        // Clean up temp files
        cleanupTempFiles(job.filePath);
      }
    } catch (err) {
      console.error("MongoDB update error:", err);
    }
  });
}

// Convert, crop and print image as PDF with CUPS
function printImageWithZinkPrinter(imagePath, printer, callback) {
  const outputPdf = imagePath.replace(/\.(png|jpg|jpeg)$/i, '.pdf');
  const croppedPdf = outputPdf.replace('.pdf', '-cropped.pdf');
  const size = '144x180pt';

  if (!printer) {
    const error = new Error('Printer is undefined');
    console.error(error);
    if (callback) callback(error);
    return;
  }

  // Step 1: Convert image to PDF with fixed size and no margins
  const convertCmd = `img2pdf "${imagePath}" -o "${outputPdf}" --pagesize ${size} --imgsize ${size} --fit shrink`;
  exec(convertCmd, (err, stdout, stderr) => {
    if (err) {
      console.error('Image to PDF conversion failed:', stderr);
      if (callback) callback(err);
      return;
    }
    console.log('PDF created:', outputPdf);

    // Step 2: Crop PDF to remove whitespace using pdfcrop
    cropPdfWithPdfcrop(outputPdf, croppedPdf, (err, croppedPath) => {
      if (err) {
        console.error('PDF cropping failed:', err);
        if (callback) callback(err);
        return;
      }

      // Step 3: Send cropped PDF to printer with correct options
      const printCmd = `lp -d ${printer} -o ColorModel=RGB -o media=2x2.5 -o print-scaling=none -n 1 "${croppedPath}"`;
      console.log('Executing print command:', printCmd);
      // TODO: remove below line after testing
      // console.log('Print command executed successfully:', stdout);
      // callback(null, stdout);
      exec(printCmd, (err, stdout, stderr) => {
        if (err) {
          console.error('Print command failed:', stderr);
          if (callback) callback(err);
        } else {
          console.log('Print command executed successfully:', stdout);
          if (callback) callback(null, stdout);
        }
      });
    });
  });
}

// Periodically check MongoDB queue and attempt prints if printer is ready
setInterval(async () => {
  if (!db) {
    console.error("MongoDB not connected.");
    return;
  }

  // Find the oldest queued job for this printer/shop
  const job = await db.collection(JOBS_COLLECTION).findOne({
    printerId: PRINTER_ID,
    shopId: SHOP_ID,
    status: 'queued'
  }, { sort: { createdAt: 1 } });
  if (job) {
    checkPrinterStatus(async (status) => {
      if (status === 'online') {
        console.log("Printer online, processing queued job...");
        console.log('job:', job);

        const filePath = await downloadImage(job.imageUrl, path.join(__dirname, 'images'), `oilData-${job.oilDataId}.jpeg`);
        if (filePath) {
          job.filePath = filePath; // add local file path to job
          console.log('Image downloaded successfully:', filePath);

          sendToPrinter(job, PRINTER_ID, true); // true = delete after print
        } else {
          console.error("Failed to download image, not printing.");
        }
        // sendToPrinter(job, PRINTER_ID); // will delete after print
      } else {
        console.log(`Printer not online (${status}), jobId: ${job._id}`);
        // Alert platform if job delayed more than 1 minute
        if (Date.now() - new Date(job.createdAt).getTime() > 1 * 60 * 1000) {
          ws.send(JSON.stringify({
            alert: true,
            shopId: job.shopId,
            jobId: job._id,
            message: `Print job delayed over 1 minute; printer status: ${status}`
          }));
        }
      }
    });
  }
}, 60000); // check every 1 minute (adjust if needed)

console.log("Raspberry Pi client started and connected to WebSocket server.");