const { chromium } = require('playwright');
const AWS = require('aws-sdk');


const sqs = new AWS.SQS({ region: 'eu-west-3' });

(async () => {
  const browser = await chromium.launch({
    headless: true,
    slowMo: 100,
    
  });

  const context = await browser.newContext({
    viewport: { width: 1280, height: 1024 },
    userAgent:
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
  });

  const page = await context.newPage();
  const allPropertyUrls = new Set();
  const MAX_PAGES = 5000;
  const QUEUE_URL = 'https://sqs.eu-west-3.amazonaws.com/992382591031/quequescraper.fifo';

  console.log('Starting scraping process...');

  try {
    let currentPage = 1;
    let hasNextPage = true;

    while (hasNextPage && currentPage <= MAX_PAGES) {
      const url = `https://www.luxuryestate.com/spain?pag=${currentPage}`;
      console.log(`Scraping page ${currentPage}: ${url}`);

      // Retry navigation up to 3 times
      let retries = 3;
      while (retries > 0) {
        try {
          await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 30000 });
          break;
        } catch (error) {
          retries--;
          if (retries === 0) throw error;
          console.log(`Retrying page ${currentPage}... (${retries} retries left)`);
          await page.waitForTimeout(5000);
        }
      }

      const noResults = await page.$('text="No properties found"');
      if (noResults) {
        console.log('Reached end of results');
        hasNextPage = false;
        break;
      }

      await page.waitForSelector('.search-list__item', { timeout: 15000 });

      const pageUrls = await page.$$eval(
        '.search-list__item a[href^="https://www.luxuryestate.com/p"]',
        (links) => links.map((link) => link.href)
      );

      // Send each URL to SQS
      for (const propertyUrl of pageUrls) {
        if (!allPropertyUrls.has(propertyUrl)) {
          allPropertyUrls.add(propertyUrl);

          try {
            await sqs
              .sendMessage({
                QueueUrl: QUEUE_URL,
                MessageBody: propertyUrl,
                MessageGroupId: 'scraper-group', // FIFO group
                MessageDeduplicationId: `${propertyUrl}-${Date.now()}`, // must be unique
              })
              .promise();

            console.log(`Sent to SQS: ${propertyUrl}`);
          } catch (err) {
            console.error(`Failed to send ${propertyUrl} to SQS:`, err);
          }
        }
      }

      // Random delay
      await page.waitForTimeout(2000 + Math.random() * 3000);
      currentPage++;
    }

    console.log(`Scraping complete! Sent ${allPropertyUrls.size} unique URLs to SQS.`);
  } catch (error) {
    console.error('Scraping failed:', error);
  } finally {
    await browser.close();
  }
})();
