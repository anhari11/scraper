const { chromium } = require('playwright');
const fs = require('fs');

(async () => {
  // Launch browser with some delay and realistic viewport
  const browser = await chromium.launch({
    headless: true, // Set false for debugging
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
  const OUTPUT_FILE = 'luxury_estate_properties.json';

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
          await page.goto(url, {
            waitUntil: 'domcontentloaded',
            timeout: 30000,
          });
          break;
        } catch (error) {
          retries--;
          if (retries === 0) throw error;
          console.log(`Retrying page ${currentPage}... (${retries} retries left)`);
          await page.waitForTimeout(5000);
        }
      }

      // Check if no properties found message exists
      const noResults = await page.$('text="No properties found"');
      if (noResults) {
        console.log('Reached end of results (no properties found message)');
        hasNextPage = false;
        break;
      }

      // Wait for property items to load (timeout in 15s)
      await page.waitForSelector('.search-list__item', { timeout: 15000 });

      // Extract property URLs
      const pageUrls = await page.$$eval(
        '.search-list__item a[href^="https://www.luxuryestate.com/p"]',
        (links) => links.map((link) => link.href)
      );

      // Add all URLs to set (to avoid duplicates)
      pageUrls.forEach((url) => allPropertyUrls.add(url));
      console.log(
        `Found ${pageUrls.length} properties on this page (Total so far: ${allPropertyUrls.size})`
      );

      // Save progress after every page
      fs.writeFileSync(OUTPUT_FILE, JSON.stringify([...allPropertyUrls], null, 2));
      console.log(`Progress saved after page ${currentPage}`);

      // Random delay 2-5 seconds
      await page.waitForTimeout(2000 + Math.random() * 3000);
      currentPage++;
    }

    console.log(`Scraping complete! Found ${allPropertyUrls.size} unique properties.`);
    console.log(`Results saved to ${OUTPUT_FILE}`);
  } catch (error) {
    console.error('Scraping failed:', error);
    // Save partial results on error
    fs.writeFileSync(OUTPUT_FILE, JSON.stringify([...allPropertyUrls], null, 2));
    console.log(`Partial results saved to ${OUTPUT_FILE}`);
  } finally {
    await browser.close();
  }
})();
 