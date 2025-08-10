const { chromium } = require('playwright');
const { SQSClient, ReceiveMessageCommand, DeleteMessageCommand } = require('@aws-sdk/client-sqs');
const fs = require('fs');
const path = require('path');

const sqsClient = new SQSClient({ 
  region: 'eu-north-1'
});

const queueUrl = 'https://sqs.eu-north-1.amazonaws.com/992382591031/quequescraper.fifo';
const outputDir = 'luxury_estate_properties';
const imagesDir = path.join(outputDir, 'images');
const propertiesJsonPath = path.join(outputDir, 'properties.json');


if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir);
if (!fs.existsSync(imagesDir)) fs.mkdirSync(imagesDir);


if (!fs.existsSync(propertiesJsonPath)) {
  fs.writeFileSync(propertiesJsonPath, JSON.stringify([], null, 2));
}

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


async function extractAllImages(page) {
  try {

    const galleryButton = await page.$('.gallery__all-media-btn');
    if (!galleryButton) {
      console.log('No gallery button found');
      return [];
    }

    await galleryButton.click();
    await delay(2000); 


    await page.waitForSelector('.gallery__carousel-track', { timeout: 10000 });


    const imageElements = await page.$$('.gallery__carousel-track img.gallery__image');
    const imageUrls = [];


    for (const img of imageElements) {
      const src = await img.getAttribute('src');
      if (src && !src.includes('placeholder')) {
 
        const fullSizeUrl = src.replace('/thumbs/1024x768/', '/');
        imageUrls.push(fullSizeUrl);
      }
    }

    // Close the gallery
    const closeButton = await page.$('.gallery__close');
    if (closeButton) {
      await closeButton.click();
      await delay(500);
    }

    // Remove duplicates
    return [...new Set(imageUrls)];
  } catch (error) {
    console.error('Error extracting images from gallery:', error);
    return [];
  }
}

// Function to extract agency contact info from modal
async function extractAgencyContactInfo(page) {
  try {
    // Click the phone button to open the modal
    const phoneButton = await page.$('button.agency__contact-phone');
    if (!phoneButton) return null;

    await phoneButton.click();
    await delay(1000); // Wait for modal to open

    // Wait for modal to appear
    await page.waitForSelector('.modal__overlay', { timeout: 5000 });

    // Extract agency name from the main page (not modal)
    const agencyName = await page.$eval('.agency__name-container a', el => el.textContent.trim());


    const phoneNumber = await page.$eval('.telephone-number', el => {
      const text = el.textContent.trim();
      return text.replace(/\D/g, ''); 
    });


    const closeButton = await page.$('.modal__close');
    if (closeButton) {
      await closeButton.click();
      await delay(500); // Wait for modal to close
    }

    return {
      name: agencyName,
      phone: phoneNumber
    };
  } catch (error) {
    console.error('Error extracting agency contact info:', error);
    return null;
  }
}

// Function to extract property data from HTML
async function extractPropertyData(page, url) {
  // Get the page HTML content
  const html = await page.content();
  
  // Extract the main property data from the hydration script
  const hydrationMatch = html.match(/<script type="application\/json" id="properties-hydration">(.*?)<\/script>/);
  if (!hydrationMatch) return null;
  
  const propertyData = JSON.parse(hydrationMatch[1]);
  
 
  const galleryMatch = html.match(/<script type="application\/json" id="gallery-hydration">(.*?)<\/script>/);
  const galleryData = galleryMatch ? JSON.parse(galleryMatch[1]) : {};
  
  const featuresMatch = html.match(/<script type="application\/json" id="features-hydration">(.*?)<\/script>/);
  const featuresData = featuresMatch ? JSON.parse(featuresMatch[1]) : {};
  
  const agencyMatch = html.match(/<script type="application\/json" id="agency-hydration">(.*?)<\/script>/);
  const agencyData = agencyMatch ? JSON.parse(agencyMatch[1]) : {};
  

  const descriptionMatch = html.match(/<meta name="description" content="(.*?)"/);
  const description = descriptionMatch ? descriptionMatch[1] : '';
  

  const exteriorFeatures = [];
  const interiorFeatures = [];
  
  if (featuresData.extraFeatures) {
    featuresData.extraFeatures.forEach(feature => {
      if (feature.label === 'exteriorAmenities' && feature.value) {
        try {
          exteriorFeatures.push(...JSON.parse(feature.value));
        } catch (e) {
          console.error('Error parsing exterior features:', e);
        }
      }
      if (feature.label === 'interiorAmenities' && feature.value) {
        try {
          interiorFeatures.push(...JSON.parse(feature.value));
        } catch (e) {
          console.error('Error parsing interior features:', e);
        }
      }
    });
  }
  
  // Extract images from gallery
  const images = await extractAllImages(page);
  
  // Extract agency contact info from modal
  const agencyContactInfo = await extractAgencyContactInfo(page);
  
  // Construct the final property object
  const property = {
    id: propertyData.id,
    title: propertyData.title,
    shortTitle: propertyData.shortTitle || '',
    price: {
      formatted: propertyData.price?.amount + ' ' + propertyData.price?.currency || 'N/A',
      amount: propertyData.price?.raw || 0,
      currency: propertyData.price?.currency || 'EUR'
    },
    surface: propertyData.surface || 0,
    bedrooms: propertyData.bedrooms || 0,
    bathrooms: propertyData.bathrooms || 0,
    type: featuresData.type || 'Villa',
    transaction: featuresData.transaction || 'sale',
    location: {
      city: featuresData.geoInfo?.PPL?.translations?.en_GB || 'Marbella',
      region: featuresData.geoInfo?.ADM1?.translations?.en_GB || 'Andalusia',
      country: featuresData.geoInfo?.PCLI?.translations?.en_GB || 'Spain'
    },
    description: description,
    features: {
      exterior: exteriorFeatures,
      interior: interiorFeatures
    },
    media: {
      images: images,
      floorPlans: galleryData.propertyFloorPlans ? galleryData.propertyFloorPlans.map(plan => plan.src) : [],
      video: galleryData.videoUrl || null,
      virtualTour: galleryData.virtualTourUrl || null
    },
    agency: {
      name: agencyContactInfo?.name || agencyData.agencyName || null,
      logo: agencyData.agencyLogo?.img || null,
      phone: agencyContactInfo?.phone || agencyData.agencyPhoneCrypted || null,
      location: agencyData.agencyLocation || null
    },
    dates: {
      created: featuresData.creationTime || null,
      modified: featuresData.modificationTime || null
    },
    url: url,
    scrapedAt: new Date().toISOString()
  };
  
  return property;
}

async function receiveMessages() {
  const params = {
    QueueUrl: queueUrl,
    MaxNumberOfMessages: 1,
    VisibilityTimeout: 300, 
    WaitTimeSeconds: 10, 
    AttributeNames: ['All'],
    MessageAttributeNames: ['All']
  };

  try {
    const command = new ReceiveMessageCommand(params);
    const data = await sqsClient.send(command);
    return data.Messages || [];
  } catch (error) {
    console.error('Error receiving messages:', error);
    return [];
  }
}

async function deleteMessage(receiptHandle) {
  const params = {
    QueueUrl: queueUrl,
    ReceiptHandle: receiptHandle
  };

  try {
    const command = new DeleteMessageCommand(params);
    await sqsClient.send(command);
    return true;
  } catch (error) {
    console.error('Error deleting message:', error);
    return false;
  }
}

async function savePropertyToJson(property) {
  try {
    // Read existing properties
    const existingData = fs.readFileSync(propertiesJsonPath, 'utf8');
    const properties = JSON.parse(existingData);
    
    // Check if property already exists
    const existingIndex = properties.findIndex(p => p.id === property.id);
    
    if (existingIndex >= 0) {
      // Update existing property
      properties[existingIndex] = property;
    } else {
      // Add new property
      properties.push(property);
    }
    
    // Save back to file
    fs.writeFileSync(propertiesJsonPath, JSON.stringify(properties, null, 2));
    return true;
  } catch (error) {
    console.error('Error saving property to JSON:', error);
    return false;
  }
}


async function downloadImage(page, url, destination) {
  try {
    const response = await page.goto(url, { waitUntil: 'domcontentloaded' });
    const buffer = await response.body();
    fs.writeFileSync(destination, buffer);
    return true;
  } catch (error) {
    console.error(`Error downloading image ${url}:`, error.message);
    return false;
  }
}

(async () => {
  const globalTimer = startTimer();
  let totalPropertiesProcessed = 0;
  let consecutiveEmptyReceives = 0;
  const maxEmptyReceivesBeforeExit = 5;
  
  console.log('Starting property processor...');
  
  const browser = await chromium.launch({ 
    headless: true,
    timeout: 60000
  });
  
  const context = await browser.newContext({
    userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    viewport: null,
    ignoreHTTPSErrors: true
  });

  try {
    while (true) {
      const messages = await receiveMessages();
      
      if (messages.length === 0) {
        consecutiveEmptyReceives++;
        console.log(`[${(globalTimer.getElapsed()/1000).toFixed(1)}s] No messages in queue (attempt ${consecutiveEmptyReceives}/${maxEmptyReceivesBeforeExit})`);
        
        if (consecutiveEmptyReceives >= maxEmptyReceivesBeforeExit) {
          console.log('Maximum empty receives reached. Exiting...');
          break;
        }
        
        await delay(5000);
        continue;
      }

      consecutiveEmptyReceives = 0; 
      
      for (const message of messages) {
        const propertyTimer = startTimer();
        let page;
        
        try {
          const { url, urlNumber } = JSON.parse(message.Body);
          console.log(`\n[${(globalTimer.getElapsed()/1000).toFixed(1)}s] Processing property ${urlNumber}: ${url}`);

          page = await context.newPage();
          await delay(2000 + Math.random() * 3000);
          
          console.log(`[${(globalTimer.getElapsed()/1000).toFixed(1)}s] Navigating to property...`);
          await page.goto(url, { 
            waitUntil: 'domcontentloaded', 
            timeout: 60000 
          });

          // Wait for property content to load
          console.log(`[${(globalTimer.getElapsed()/1000).toFixed(1)}s] Waiting for property content...`);
          await page.waitForSelector('.lx-property__mainContent', { timeout: 15000 });

          // Extract property data using our improved extractor
          console.log(`[${(globalTimer.getElapsed()/1000).toFixed(1)}s] Extracting property data...`);
          const property = await extractPropertyData(page, url);
          
          if (!property) {
            throw new Error('Failed to extract property data');
          }

          // Print agency info
          console.log(`[${(globalTimer.getElapsed()/1000).toFixed(1)}s] Agency: ${property.agency.name}`);
          console.log(`[${(globalTimer.getElapsed()/1000).toFixed(1)}s] Phone: ${property.agency.phone}`);
          console.log(`[${(globalTimer.getElapsed()/1000).toFixed(1)}s] Found ${property.media.images.length} images`);

          // Create directory for this property's data
          const propertyId = property.id || url.split('/').pop() || Date.now();
          const propertyDir = path.join(outputDir, `property_${propertyId}`);
          if (!fs.existsSync(propertyDir)) fs.mkdirSync(propertyDir);

          // Save property data to individual file
          fs.writeFileSync(
            path.join(propertyDir, 'data.json'),
            JSON.stringify(property, null, 2)
          );

          // Save to consolidated properties.json
          const saveSuccess = await savePropertyToJson(property);
          if (!saveSuccess) {
            throw new Error('Failed to save property to properties.json');
          }

          // Download random number of images between 15-21 (or all if less available)
          const minImages = 15;
          const maxImages = 21;
          const imagesToDownload = Math.min(
            property.media.images.length,
            Math.floor(Math.random() * (maxImages - minImages + 1)) + minImages
          );
          
          console.log(`[${(globalTimer.getElapsed()/1000).toFixed(1)}s] Downloading ${imagesToDownload} of ${property.media.images.length} available images`);

          // Shuffle the images array to get random selection
          const shuffledImages = [...property.media.images].sort(() => 0.5 - Math.random());
          
          // Download the selected number of images
          for (let i = 0; i < Math.min(imagesToDownload, shuffledImages.length); i++) {
            const imageUrl = shuffledImages[i];
            const imagePath = path.join(propertyDir, `image_${i + 1}.jpg`);
            
            await downloadImage(page, imageUrl, imagePath);
            console.log(`[${(globalTimer.getElapsed()/1000).toFixed(1)}s] Downloaded image ${i + 1}/${imagesToDownload}`);
          }

          // Delete message from queue after successful processing
          const deleteSuccess = await deleteMessage(message.ReceiptHandle);
          if (!deleteSuccess) {
            throw new Error('Failed to delete message from queue');
          }

          totalPropertiesProcessed++;
          console.log(`[${(globalTimer.getElapsed()/1000).toFixed(1)}s] Successfully processed property ${urlNumber}`);

        } catch (error) {
          console.error(`[${(globalTimer.getElapsed()/1000).toFixed(1)}s] Error processing property:`, error.message);
        } finally {
          if (page) {
            await page.close().catch(e => console.error('Error closing page:', e));
          }
        }
      }
    }
  } catch (error) {
    console.error('Main error:', error);
  } finally {
    await context.close().catch(e => console.error('Error closing context:', e));
    await browser.close().catch(e => console.error('Error closing browser:', e));
    console.log(`\n[${(globalTimer.getElapsed()/1000).toFixed(1)}s] Browser closed. Processed ${totalPropertiesProcessed} properties.`);
  }
})();