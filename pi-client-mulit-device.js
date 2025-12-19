const WebSocket = require('ws');
const { exec } = require('child_process');
const fs = require('fs');
const path = require('path');
const axios = require('axios');
require('dotenv').config();

let PRINTERT_PI_DEVICE_ID = process.env.PRINTERT_PI_DEVICE_ID || 'fe49db0b-c882-4985-a5e6-97f555dcf6ff';
const PRINTER_ID = process.env.PRINTER_ID || 'ZINK_hAppy_3233_USB';
const WS_URL = process.env.WS_URL || 'ws://3.135.208.74:2001';

function loadPrinterDeviceId() {
  const filePath = path.join(__dirname, "config.json");
  try {
    const data = JSON.parse(fs.readFileSync(filePath, "utf8"));
    PRINTERT_PI_DEVICE_ID = data.PRINTERT_PI_DEVICE_ID;
  } catch (error) {
    console.error("something went wrong", error);
  }
}

let ws;
let lastStatus = '';
let statusInterval = null; // guard against multiple intervals

function connect() {
  loadPrinterDeviceId();
  ws = new WebSocket(WS_URL);

  ws.on('open', () => {
    console.log('[PI] WS connected');
  safeSend({ type: 'register', printerId: PRINTER_ID, printerPiDeviceId: PRINTERT_PI_DEVICE_ID });
  startStatusLoop();
  });

  ws.on('message', async (data) => {
    try {
      const msg = JSON.parse(data);
      if (msg.type === 'print_job') {
        await handlePrintJob(msg);
      }
    } catch (e) {
      console.error('[PI] message parse error', e);
    }
  });

  ws.on('close', () => {
    console.log('[PI] WS closed – reconnecting in 5s');
    setTimeout(connect, 5000);
  });

  ws.on('error', err => console.error('[PI] WS error', err.message));
}

async function handlePrintJob(job) {
  console.log('[PI] Received print job', job.jobId);
  try {
    const filePath = await downloadImage(job.imageUrl, path.join(__dirname, 'images'), `job-${job.jobId}.jpg`);
    if (!filePath) throw new Error('Download failed');
    printImage(filePath, (err) => {
      cleanupTempFiles(filePath);
      safeSend({
        type: 'job_result',
        jobId: job.jobId,
        printerId: PRINTER_ID,
        printerPiDeviceId: PRINTERT_PI_DEVICE_ID,
        success: !err,
        error: err ? err.message : null
      });
    });
  } catch (err) {
    safeSend({
      type: 'job_result',
      jobId: job.jobId,
      printerId: PRINTER_ID,
      printerPiDeviceId: PRINTERT_PI_DEVICE_ID,
      success: false,
      error: err.message
    });
  }
}

function startStatusLoop() {
  if (statusInterval) clearInterval(statusInterval);
  statusInterval = setInterval(() => {
    checkPrinterStatus((status) => {
      const changed = status !== lastStatus;
      const open = ws && ws.readyState === 1;
      console.log(`[PI] Printer status: ${status} (changed: ${changed}, ws open: ${open})`);
      if (changed && open) {
        safeSend({ type: 'status', printerId: PRINTER_ID, printerPiDeviceId: PRINTERT_PI_DEVICE_ID, status });
        lastStatus = status;
      }
    });
  }, 15000); // every 15s
}

function checkPrinterStatus(cb) {
  exec(`lpstat -p ${PRINTER_ID} -l`, (err, stdout, stderr) => {
    const out = (stdout + stderr).toLowerCase();
    console.log('out:::::', out);
    let s = 'offline';
    if (out.includes('idle')) s = 'online';
    else if (out.includes('printing')) s = 'busy';
    else if (out.includes('disabled') || out.includes('invalid')) s = 'offline';
    else if (out.includes('paused')) s = 'paused';
    else if (out.includes('out of paper')) s = 'out_of_paper';
    else if (out.includes('stopped')) s = 'stopped';
    cb(s);
  });
}

async function downloadImage(url, dir, filename) {
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  const filePath = path.join(dir, filename);
  const resp = await axios({ url, method: 'GET', responseType: 'stream' });
  return new Promise((res, rej) => {
    const w = fs.createWriteStream(filePath);
    resp.data.pipe(w);
    w.on('finish', () => res(filePath));
    w.on('error', rej);
  });
}

function printImage(imagePath, cb) {
  ensureWorkDirs();
  const outputPdf = imagePath.replace(/\.(png|jpg|jpeg)$/i, '.pdf');
  const croppedPdf = outputPdf.replace('.pdf', '-cropped.pdf');
  const size = '144x180pt';
  const convertCmd = `img2pdf "${imagePath}" -o "${outputPdf}" --pagesize ${size} --imgsize ${size} --fit shrink`;
  console.log('[PI][PRINT] convert:', convertCmd);
  exec(convertCmd, (e) => {
    if (e) return cb(e);
    
    const cropCmd = `pdfcrop "${outputPdf}" "${croppedPdf}"`;
    const execOpts = { cwd: path.dirname(outputPdf), env: { ...process.env, TMPDIR: path.join(__dirname, 'tmp'), HOME: path.join(__dirname, 'tmp') } };
    console.log('[PI][PRINT] crop:', cropCmd);
    
    exec(cropCmd, execOpts, (e2) => {
      let pdfToPrint = croppedPdf;
      if (e2) {
        console.warn('[PI][PRINT] pdfcrop failed, fallback to original:', e2.message);
        pdfToPrint = outputPdf;
      }
      const printCmd = `lp -d ${PRINTER_ID} -o ColorModel=RGB -o media=2x2.5 -o print-scaling=none -n 1 "${pdfToPrint}"`;
      console.log('[PI][PRINT] lp:', printCmd);
      exec(printCmd, (e3) => cb(e3 || null));
    });
  });
}

function cleanupTempFiles(imagePath) {
  const pdf = imagePath.replace(/\.(png|jpg|jpeg)$/i, '.pdf');
  const cropped = pdf.replace('.pdf', '-cropped.pdf');
  [imagePath, pdf, cropped].forEach(f => fs.unlink(f, () => {}));
}

function ensureWorkDirs() {
  [path.join(__dirname, 'images'), path.join(__dirname, 'tmp')].forEach(dir => {
    if (!fs.existsSync(dir)) {
      try { fs.mkdirSync(dir, { recursive: true }); } catch (e) { console.error('[PI] mkdir failed', dir, e.message); }
    }
  });
}

function safeSend(obj) {
  if (!ws || ws.readyState !== 1) return console.warn('[PI] Cannot send, WS not open', obj.type);
  try { ws.send(JSON.stringify(obj)); } catch (e) { console.error('[PI] ws.send error', e.message); }
}

console.log('[PI] Starting client');
connect();
