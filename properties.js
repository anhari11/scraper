const { chromium } = require('playwright');
const { SQSClient, ReceiveMessageCommand, DeleteMessageCommand } = require('@aws-sdk/client-sqs');
const fs = require('fs');
const path = require('path');

const sqsClient = new SQSClient({ region: 'eu-north-1' });
const queueUrl = 'https://sqs.eu-north-1.amazonaws.com/992382591031/quequescraper.fifo';
const outputDir = 'luxury_estate_amenities';
const amenitiesJsonPath = path.join(outputDir, 'amenities.json');
const logPath = path.join(outputDir, 'amenities.log');

// Setup directory
if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir);
if (!fs.existsSync(amenitiesJsonPath)) fs.writeFileSync(amenitiesJsonPath, JSON.stringify({ exteriorAmenities: [], interiorAmenities: [] }, null, 2));

function logToFile(message) {
  fs.appendFileSync(logPath, `${new Date().toISOString()} - ${message}\n`);
}

const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

async function extractAmenities(page, url) {
  try {
    await page.goto(url, { waitUntil: 'domcontentloaded' });
    
    // Extract exterior amenities
    const exteriorAmenities = await page.evaluate(() => {
      const exteriorItem = Array.from(document.querySelectorAll('.feat-item'))
        .find(item => item.textContent.includes('Exterior Amenities'));
      
      if (!exteriorItem) return [];
      
      return Array.from(exteriorItem.querySelectorAll('.multiple-values'))
        .map(el => el.textContent.trim());
    });

    // Extract interior amenities
    const interiorAmenities = await page.evaluate(() => {
      const interiorItem = Array.from(document.querySelectorAll('.feat-item'))
        .find(item => item.textContent.includes('Interior Amenities'));
      
      if (!interiorItem) return [];
      
      return Array.from(interiorItem.querySelectorAll('.multiple-values'))
        .map(el => el.textContent.trim());
    });

    return {
      exterior: exteriorAmenities,
      interior: interiorAmenities
    };
  } catch (err) {
    console.error(`Error extracting amenities from ${url}:`, err);
    logToFile(`Error extracting amenities: ${url} | ${err.message}`);
    return null;
  }
}
async function processUrl(url) {
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext();
  const page = await context.newPage();

  try {
    const amenities = await extractAmenities(page, url);
    if (!amenities) return;

    // Read existing amenities
    const currentData = JSON.parse(fs.readFileSync(amenitiesJsonPath));
    const allAmenities = {
      exteriorAmenities: [...new Set([...currentData.exteriorAmenities, ...amenities.exterior])].sort(),
      interiorAmenities: [...new Set([...currentData.interiorAmenities, ...amenities.interior])].sort()
    };

    // Save updated list
    fs.writeFileSync(amenitiesJsonPath, JSON.stringify(allAmenities, null, 2));
    logToFile(`Processed amenities from: ${url}`);

    return allAmenities;
  } finally {
    await browser.close();
  }
}

async function pollQueue() {
  const allAmenities = {
    exteriorAmenities: new Set(),
    interiorAmenities: new Set()
  };

  while (true) {
    const command = new ReceiveMessageCommand({
      QueueUrl: queueUrl,
      MaxNumberOfMessages: 1,
      WaitTimeSeconds: 10
    });

    try {
      const { Messages } = await sqsClient.send(command);
      if (!Messages || Messages.length === 0) continue;

      for (const message of Messages) {
        const body = JSON.parse(message.Body);
        const url = body.url;

        console.log(`Processing URL: ${url}`);
        logToFile(`Processing: ${url}`);

        const amenities = await processUrl(url);
        if (amenities) {
          amenities.exteriorAmenities.forEach(item => allAmenities.exteriorAmenities.add(item));
          amenities.interiorAmenities.forEach(item => allAmenities.interiorAmenities.add(item));
        }

        const deleteCommand = new DeleteMessageCommand({
          QueueUrl: queueUrl,
          ReceiptHandle: message.ReceiptHandle
        });
        await sqsClient.send(deleteCommand);
        console.log(`Completed processing for ${url}`);
      }
    } catch (error) {
      console.error('Error polling queue:', error);
      logToFile(`Polling error: ${error.message}`);
      await delay(5000);
    }
  }
}

// Cleanup on exit
process.on('SIGINT', () => {
  console.log('\nGracefully shutting down...');
  logToFile('Script stopped by SIGINT');
  process.exit();
});

// Start the process
(async () => {
  try {
    await pollQueue();
  } catch (error) {
    console.error('Fatal error:', error);
    logToFile(`Fatal error: ${error.message}`);
    process.exit(1);
  }
})();