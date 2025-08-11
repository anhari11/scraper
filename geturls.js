// dependencies
const { chromium } = require('playwright');
const { SQSClient, ReceiveMessageCommand, DeleteMessageCommand } = require('@aws-sdk/client-sqs');
const { S3Client, PutObjectCommand } = require('@aws-sdk/client-s3');
const { PrismaClient } = require('@prisma/client');
const fs = require('fs');
const path = require('path');
const https = require('https');


const sqsClient = new SQSClient({ region: 'eu-north-1' });
const s3Client = new S3Client({ region: 'eu-north-1' });
const prisma = new PrismaClient();
const queueUrl = 'https://sqs.eu-north-1.amazonaws.com/992382591031/quequescraper.fifo';
const bucketName = 'iberialuxuryestate2';
const logPath = 'scraper.log';

if (!fs.existsSync(logPath)) fs.writeFileSync(logPath, '');
const logToFile = msg => fs.appendFileSync(logPath, `${new Date().toISOString()} - ${msg}\n`);
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));


async function uploadToS3(url, key) {
  try {
    const response = await new Promise((resolve, reject) => {
      https.get(url, res => {
        const chunks = [];
        res.on('data', chunk => chunks.push(chunk));
        res.on('end', () => resolve(Buffer.concat(chunks)));
      }).on('error', reject);
    });

    const command = new PutObjectCommand({
      Bucket: bucketName,
      Key: key,
      Body: response,
      ContentType: 'image/jpeg'
    });

    await s3Client.send(command);
    return `https://${bucketName}.s3.eu-north-1.amazonaws.com/${key}`;
  } catch (err) {
    console.error(`Error uploading to S3: ${err.message}`);
    return null;
  }
}

// extract data from page
async function extractPropertyData(page, url) {
  try {
    await page.goto(url, { waitUntil: 'domcontentloaded' });
    const html = await page.content();

    const hydrationMatch = html.match(/<script type="application\/json" id="properties-hydration">(.*?)<\/script>/);
    let propertyData = {};
    if (hydrationMatch) {
      try {
        propertyData = JSON.parse(hydrationMatch[1]);
      } catch (_) {
        console.warn("Invalid hydration JSON. Falling back.");
      }
    }

    const features = await page.evaluate(() => {
      const featureMap = {};
      const items = document.querySelectorAll('.feat-item');

      items.forEach(item => {
        const labelEl = item.querySelector('.feat-label');
        const valueEl = item.querySelector('.single-value') || item.querySelector('.multiple-values');
        let label = labelEl?.textContent?.trim().replace(/:$/, '') || labelEl?.textContent?.trim();

        if (label) {
          featureMap[label] = valueEl ? valueEl.textContent.trim() : true;
        }
      });

      return featureMap;
    });

    const images = await page.evaluate(() => {
      return Array.from(document.querySelectorAll('img[src*="properties"]'))
        .map(img => img.src)
        .filter(src => src && !src.includes('placeholder'));
    });

    const agencyName = await page.evaluate(() => {
      const el = document.querySelector('.agency__name-container a');
      return el ? el.textContent.trim() : null;
    });

    const description = await page.evaluate(() => {
      const container = document.querySelector('[data-role="description-text-container"]');
      const content = container?.querySelector('[data-role="description-text-content"]');
      return content?.innerText?.trim() || container?.textContent?.trim() || null;
    });

    if (!propertyData.id) {
      const reference = features["Reference"] || '';
      const fallbackId = reference ? reference.replace(/\s+/g, '-').toUpperCase() : `fallback-${Date.now()}`;
      propertyData = {
        id: fallbackId,
        title: `Property ${reference || 'Unknown'}`,
        price: null,
        location: {},
        currency: 'EUR'
      };
    }

    return {
      id: propertyData.id,
      title: propertyData.title || `Property ${propertyData.id}`,
      price: propertyData.price?.amount || null,
      currency: propertyData.price?.currencyCode || 'EUR',
      location: propertyData.location || {},
      features,
      images,
      agencyName,
      description,
      url
    };
  } catch (err) {
    console.error(`Error extracting data from ${url}:`, err);
    return null;
  }
}

const normalizeAmenityField = value => {
  if (Array.isArray(value)) return value;
  if (typeof value === 'string') return [value];
  return [];
};

const includesAmenity = (list = [], keywords) =>
  keywords.some(k => list.some(a => a.toLowerCase().includes(k.toLowerCase())));

// process a single URL
async function processUrl(url) {
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext();
  const page = await context.newPage();
  const startTime = Date.now();

  try {
    const property = await extractPropertyData(page, url);
    if (!property) return;

    const exists = await prisma.properties.findFirst({
      where: { property_reference: `EXT-${property.id}` }
    });
    if (exists) return logToFile(`Skipped existing: ${url}`);

    const imageUrls = [];
    for (let i = 0; i < property.images.length; i++) {
      const key = `properties/${property.id}_${i}.jpg`;
      const s3Url = await uploadToS3(property.images[i], key);
      if (s3Url) imageUrls.push(s3Url);
    }

    const ext = normalizeAmenityField(property.features['Exterior Amenities']);
    const int = normalizeAmenityField(property.features['Interior Amenities']);

    const dbProperty = await prisma.properties.create({
      data: {
        title: property.title,
        price: property.price ? parseFloat(property.price.toString().replace(/,/g, '')) : 0,
        address: property.location?.address || '',
        city: property.location?.city || '',
        neighborhood: property.location?.neighborhood || '',
        province: property.location?.province || '',
        country: property.location?.country || 'Spain',
        zip_code: property.location?.zipCode || '',
        rooms: parseInt(property.features['Bedrooms'] || property.features['Rooms'] || 0),
        bathrooms: parseInt(property.features['Bathrooms'] || 0),
        area: parseFloat((property.features['Size'] || '').replace(/[^\d.]/g, '') || 0),
        property_type: property.features['Type'] || 'Other',
        currency: property.currency,
        agency_name: property.agencyName,
        listing_url: property.url,
        property_url: property.url,
        property_reference: `EXT-${property.id}`,
        description: property.description || '',
        has_pool: includesAmenity(ext, ['pool']),
        has_garden: includesAmenity(ext, ['garden']),
        has_garage: includesAmenity(ext, ['garage']),
        has_barbeque_area: includesAmenity(ext, ['barbeque area']),
        has_basement: includesAmenity(ext, ['basement']),
        has_courtyard: includesAmenity(ext, ['courtyard']),
        has_disabled_access: includesAmenity(ext, ['disabled access']),
        has_gated_entry: includesAmenity(ext, ['gated entry']),
        has_greenhouse: includesAmenity(ext, ['greenhouse']),
        has_hottub: includesAmenity(ext, ['hottub', 'spa']),
        has_lawn: includesAmenity(ext, ['lawn']),
        has_mother_in_law_unit: includesAmenity(ext, ['mother-in-law', 'mother in law']),
        has_patio: includesAmenity(ext, ['patio']),
        has_pond: includesAmenity(ext, ['pond']),
        has_porch: includesAmenity(ext, ['porch']),
        has_private_patio: includesAmenity(ext, ['private patio']),
        has_sports_court: includesAmenity(ext, ['sports court']),
        has_sprinkler_system: includesAmenity(ext, ['sprinkler system']),
        is_waterfront: includesAmenity(ext, ['waterfront']),
        has_attic: includesAmenity(int, ['attic']),
        has_cable_satellite: includesAmenity(int, ['cable', 'satellite']),
        has_doublepane_windows: includesAmenity(int, ['doublepane windows']),
        has_elevator: includesAmenity(int, ['elevator']),
        has_fireplace: includesAmenity(int, ['fireplace']),
        furnished: includesAmenity(int, ['furnished']),
        has_hand_rails: includesAmenity(int, ['hand rails']),
        has_cinema: includesAmenity(int, ['home theater', 'cinema']),
        has_intercom: includesAmenity(int, ['intercom']),
        has_jacuzzi: includesAmenity(int, ['jacuzzi', 'jetted bath tub']),
        has_sauna: includesAmenity(int, ['sauna']),
        has_security_system: includesAmenity(int, ['security system']),
        has_skylight: includesAmenity(int, ['skylight']),
        has_vaulted_ceiling: includesAmenity(int, ['vaulted ceiling']),
        has_wet_bar: includesAmenity(int, ['wet bar']),
        has_window_coverings: includesAmenity(int, ['window coverings']),
        property_images: {
          create: imageUrls.map((url, i) => ({ image_url: url, is_primary: i === 0 }))
        }
      }
    });

    const time = ((Date.now() - startTime) / 1000).toFixed(2);
    logToFile(`Created property: ${dbProperty.id} (${property.title}) in ${time}s`);
  } catch (err) {
    console.error(`Error processing ${url}:`, err);
    logToFile(`Error processing ${url}: ${err.message}`);
  } finally {
    await browser.close();
  }
}

// polling loop
async function pollQueue() {
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

        console.log(`Processing: ${url}`);
        logToFile(`Processing: ${url}`);

        await processUrl(url);

        const deleteCommand = new DeleteMessageCommand({
          QueueUrl: queueUrl,
          ReceiptHandle: message.ReceiptHandle
        });
        await sqsClient.send(deleteCommand);
        console.log(`Completed: ${url}`);
      }
    } catch (err) {
      console.error('Polling error:', err);
      logToFile(`Polling error: ${err.message}`);
      await delay(5000);
    }
  }
}

// graceful shutdown
process.on('SIGINT', async () => {
  console.log('Shutting down...');
  await prisma.$disconnect();
  process.exit();
});

(async () => {
  try {
    await pollQueue();
  } catch (err) {
    console.error('Fatal error:', err);
    logToFile(`Fatal error: ${err.message}`);
    await prisma.$disconnect();
    process.exit(1);
  }
})();
