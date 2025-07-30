const { chromium } = require('playwright');
const { SQSClient, SendMessageCommand } = require('@aws-sdk/client-sqs');


const sqsClient = new SQSClient({
  region: 'eu-north-1', 
  
});
const queueUrl = 'https://sqs.eu-north-1.amazonaws.com/992382591031/quequescraper.fifo';


const delay = ms => new Promise(resolve => setTimeout(resolve, ms));


const startTimer = () => {
  const start = process.hrtime();
  return {
    getElapsed: () => {
      const diff = process.hrtime(start);
      return (diff[0] * 1000 + diff[1] / 1000000); 
    }
  };
};

async function sendToSQS(url, urlNumber) {
  try {
    const params = {
      QueueUrl: queueUrl,
      MessageBody: JSON.stringify({
        url: url,
        urlNumber: urlNumber,
        timestamp: new Date().toISOString()
      }),
      MessageGroupId: 'property-urls' 
    };

    const command = new SendMessageCommand(params);
    await sqsClient.send(command);
    console.log(`[${urlNumber}] URL sent to SQS: ${url}`);
  } catch (error) {
    console.error(`Error sending URL to SQS: ${error.message}`);
  }
}

(async () => {
  console.log('Starting URL extraction process...');
  const globalTimer = startTimer();
  
  const browser = await chromium.launch({ 
    headless: true,
  });
  
  const context = await browser.newContext({
    userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    viewport: null,
    ignoreHTTPSErrors: true
  });

  try {
    let totalUrls = 0;
    
    for (let pageNum = 1; pageNum <= 5800; pageNum++) {
      const pageTimer = startTimer();
      const page = await context.newPage();
      try {
        const pageUrl = `https://www.luxuryestate.com/spain?pag=${pageNum}`;
        const elapsedTime = globalTimer.getElapsed() / 1000;
        console.log(`\n[${elapsedTime.toFixed(1)}s] Processing page ${pageNum}: ${pageUrl}`);

        await page.goto(pageUrl, {
          waitUntil: 'domcontentloaded',
          timeout: 60000
        });

        await page.waitForSelector('.details', { timeout: 15000 });

        const propertyUrls = await page.$$eval('.details_title a', links => 
          links.map(link => link.href).filter(url => url.includes('/p'))
        );

        console.log(`[${(globalTimer.getElapsed()/1000).toFixed(1)}s] Found ${propertyUrls.length} properties on page ${pageNum}`);
        

        for (const url of propertyUrls) {
          totalUrls++;
          console.log(`[${(globalTimer.getElapsed()/1000).toFixed(1)}s] [${totalUrls}] ${url}`);
          await sendToSQS(url, totalUrls);
        }

        await delay(2000 + Math.random() * 3000);
        
      } catch (error) {
        console.error(`[${(globalTimer.getElapsed()/1000).toFixed(1)}s] Error processing page ${pageNum}:`, error.message);
      } finally {
        const pageTime = pageTimer.getElapsed() / 1000;
        console.log(`[${(globalTimer.getElapsed()/1000).toFixed(1)}s] Page ${pageNum} processed in ${pageTime.toFixed(1)}s`);
        await page.close();
      }
    }

    const totalTime = globalTimer.getElapsed() / 1000;
    console.log(`\n[${totalTime.toFixed(1)}s] Total URLs found and sent to SQS: ${totalUrls}`);
    console.log(`[${totalTime.toFixed(1)}s] Average URLs per second: ${(totalUrls/totalTime).toFixed(2)}`);
    
  } catch (error) {
    console.error(`[${(globalTimer.getElapsed()/1000).toFixed(1)}s] Main error:`, error);
  } finally {
    await browser.close();
    console.log(`\n[${(globalTimer.getElapsed()/1000).toFixed(1)}s] Browser closed. URL extraction completed.`);
  }
})();