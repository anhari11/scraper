const { chromium } = require('playwright');
const fs = require('fs');
const path = require('path');

// Configure directories
const outputDir = 'luxury_estate_properties';
const imagesDir = path.join(outputDir, 'images');

// Create directories if they don't exist
if (!fs.existsSync(outputDir)) fs.mkdirSync(outputDir);
if (!fs.existsSync(imagesDir)) fs.mkdirSync(imagesDir);

// Helper function to delay between requests
const delay = ms => new Promise(resolve => setTimeout(resolve, ms));

// Timer utility functions
const startTimer = () => {
    const start = process.hrtime();
    return {
        end: () => {
            const diff = process.hrtime(start);
            return (diff[0] * 1000 + diff[1] / 1000000).toFixed(2); // ms
        },
        getElapsed: () => {
            const diff = process.hrtime(start);
            return (diff[0] * 1000 + diff[1] / 1000000); // ms as float
        }
    };
};

// Function to format milliseconds into hours, minutes, seconds
const formatTime = (ms) => {
    const seconds = Math.floor(ms / 1000) % 60;
    const minutes = Math.floor(ms / (1000 * 60)) % 60;
    const hours = Math.floor(ms / (1000 * 60 * 60));
    
    return `${hours}h ${minutes}m ${seconds}s`;
};

// Function to calculate directory size
const getDirectorySize = dir => {
    let size = 0;
    const files = fs.readdirSync(dir, { withFileTypes: true });
    
    for (const file of files) {
        if (file.isDirectory()) {
            size += getDirectorySize(path.join(dir, file.name));
        } else {
            const stats = fs.statSync(path.join(dir, file.name));
            size += stats.size;
        }
    }
    return size;
};

(async () => {
    const globalTimer = startTimer();
    let totalPropertiesProcessed = 0;
    let totalImagesDownloaded = 0;
    let totalImageSizeBytes = 0; // Track total image size in bytes
    
    console.log('Starting scraping process in headless mode...');
    
    // Launch browser in headless mode
    const browser = await chromium.launch({ 
        headless: true,  // Run without UI
    });
    
    const context = await browser.newContext({
        userAgent: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        viewport: null,
        ignoreHTTPSErrors: true
    });

    try {
        const properties = [];
        const timingStats = {
            total: 0,
            pages: [],
            properties: [],
            images: []
        };

        // Process pages
        for (let pageNum = 1; pageNum <= 40; pageNum++) {
            const pageTimer = startTimer();
            const pageStats = {
                pageNumber: pageNum,
                time: 0,
                properties: 0,
                images: 0
            };
            
            const page = await context.newPage();
            try {
                const pageUrl = `https://www.luxuryestate.com/spain?pag=${pageNum}`;
                console.log(`\nProcessing page ${pageNum}: ${pageUrl}`);

                await page.goto(pageUrl, {
                    waitUntil: 'domcontentloaded',
                    timeout: 60000
                });

                // Wait for listings to load
                console.log('Waiting for listings to load...');
                await page.waitForSelector('.details', { timeout: 15000 });

                // Extract property URLs
                let propertyUrls = await page.$$eval('.details_title a', links => 
                    links.map(link => link.href).filter(url => url.includes('/p'))
                );

                console.log(`Found ${propertyUrls.length} properties on page ${pageNum}`);

                for (const url of propertyUrls) {
                    const propertyTimer = startTimer();
                    const propertyStats = {
                        url,
                        time: 0,
                        images: 0,
                        sizeMB: 0
                    };
                    
                    let propertyPage;
                    try {
                        const elapsedTime = globalTimer.getElapsed();
                        console.log(`\n[Elapsed: ${formatTime(elapsedTime)}] Processing property ${totalPropertiesProcessed + 1}: ${url}`);

                        console.log("Total size images", totalImageSizeBytes)
                        console.log("Total images downloaded", totalImagesDownloaded)
                        propertyPage = await context.newPage();
                        
                        // Add delay between requests
                        await delay(2000 + Math.random() * 3000);
                        
                        console.log(`[Elapsed: ${formatTime(globalTimer.getElapsed())}] Navigating to property page...`);
                        await propertyPage.goto(url, {
                            waitUntil: 'domcontentloaded',
                            timeout: 60000
                        });

                        // Wait for main content
                        console.log(`[Elapsed: ${formatTime(globalTimer.getElapsed())}] Waiting for property content to load...`);
                        await propertyPage.waitForSelector('.lx-property__mainContent', { timeout: 15000 });

                        // Extract property data
                        console.log(`[Elapsed: ${formatTime(globalTimer.getElapsed())}] [Property ${totalPropertiesProcessed + 1}] Extracting property data...`);
                        const propertyData = await propertyPage.evaluate(() => {
                            const title = document.querySelector('.title-property')?.textContent?.trim() || 'N/A';
                            const price = document.querySelector('.prices .price')?.textContent?.trim() || 'N/A';
                            const description = document.querySelector('.description-container p')?.textContent?.trim() || 'N/A';
                            
                            // Extract specs
                            const specs = {};
                            document.querySelectorAll('.feat-item').forEach(item => {
                                const label = item.querySelector('.feat-label')?.textContent?.replace(':', '').trim().toLowerCase() || 'unknown';
                                const value = item.querySelector('.single-value')?.textContent?.trim() || 
                                             Array.from(item.querySelectorAll('.multiple-values')).map(el => el.textContent).join(', ');
                                specs[label] = value;
                            });

                            // Extract agency info
                            const agency = {
                                name: document.querySelector('.agency__name-container a')?.textContent?.trim() || 'N/A',
                                logo: document.querySelector('.agency__logo img')?.src || 'N/A'
                            };

                            return {
                                title,
                                url: window.location.href,
                                price,
                                description,
                                specs,
                                agency,
                                scrapedAt: new Date().toISOString()
                            };
                        });

                        console.log(`[Elapsed: ${formatTime(globalTimer.getElapsed())}] [Property ${totalPropertiesProcessed + 1}] Processing: ${propertyData.title}`);

                        // Create directory for this property's images with unique identifier
                        const sanitizedTitle = propertyData.title.replace(/[^a-z0-9]/gi, '_').toLowerCase().substring(0, 50);
                        const propertyId = url.split('/').pop() || Date.now(); // Use URL slug or timestamp as unique ID
                        const propertyImageDir = path.join(imagesDir, `${sanitizedTitle}_${propertyId}`);
                        if (!fs.existsSync(propertyImageDir)) fs.mkdirSync(propertyImageDir);

                        // Get all image elements
                        console.log(`[Elapsed: ${formatTime(globalTimer.getElapsed())}] [Property ${totalPropertiesProcessed + 1}] Finding images...`);
                        const imageElements = await propertyPage.$$eval('img[src*="properties"]', imgs => 
                            imgs.map(img => img.src));
                        
                        console.log(`[Elapsed: ${formatTime(globalTimer.getElapsed())}] [Property ${totalPropertiesProcessed + 1}] Found ${imageElements.length} images to download`);

                        // Download images
                        for (let i = 0; i < imageElements.length; i++) {
                            const imageTimer = startTimer();
                            try {
                                const imageUrl = imageElements[i];
                                if (!imageUrl) continue;

                                console.log(`[Elapsed: ${formatTime(globalTimer.getElapsed())}] [Property ${totalPropertiesProcessed + 1}] Downloading image ${i + 1}/${imageElements.length}`);
                                const imagePath = path.join(propertyImageDir, `image_${i + 1}.jpg`);
                                
                                // Use Playwright's evaluate to fetch the image
                                const imageBuffer = await propertyPage.evaluate(async (url) => {
                                    try {
                                        const response = await fetch(url);
                                        if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
                                        const arrayBuffer = await response.arrayBuffer();
                                        return Array.from(new Uint8Array(arrayBuffer));
                                    } catch (error) {
                                        console.error('Fetch error:', error);
                                        return null;
                                    }
                                }, imageUrl);

                                if (imageBuffer) {
                                    // Save the image buffer to file
                                    const buffer = Buffer.from(imageBuffer);
                                    fs.writeFileSync(imagePath, buffer);
                                    
                                    // Update size counters
                                    const imageSizeMB = buffer.length / (1024 * 1024);
                                    totalImageSizeBytes += buffer.length;
                                    propertyStats.sizeMB += imageSizeMB;
                                    
                                    console.log(`[Elapsed: ${formatTime(globalTimer.getElapsed())}] [Property ${totalPropertiesProcessed + 1}] Successfully downloaded image ${i + 1} (${imageSizeMB.toFixed(2)} MB)`);
                                    totalImagesDownloaded++;
                                    propertyStats.images++;
                                } else {
                                    console.log(`[Elapsed: ${formatTime(globalTimer.getElapsed())}] [Property ${totalPropertiesProcessed + 1}] Failed to download image ${i + 1}`);
                                }
                                
                            } catch (error) {
                                console.error(`[Elapsed: ${formatTime(globalTimer.getElapsed())}] [Property ${totalPropertiesProcessed + 1}] Error downloading image ${i + 1}:`, error.message);
                            } finally {
                                const imageTime = imageTimer.end();
                                timingStats.images.push({
                                    url: imageElements[i],
                                    time: imageTime
                                });
                                await delay(1000); // Add delay between image downloads
                            }
                        }

                        properties.push(propertyData);
                        totalPropertiesProcessed++;
                        pageStats.properties++;
                        console.log(`[Elapsed: ${formatTime(globalTimer.getElapsed())}] [Property ${totalPropertiesProcessed}] Completed processing: ${url}`);
                    } catch (error) {
                        console.error(`[Elapsed: ${formatTime(globalTimer.getElapsed())}] [Property ${totalPropertiesProcessed + 1}] Error processing property ${url}:`, error.message);
                    } finally {
                        propertyStats.time = propertyTimer.end();
                        timingStats.properties.push(propertyStats);
                        if (propertyPage) {
                            await propertyPage.close();
                        }
                    }
                }
            } catch (error) {
                console.error(`[Elapsed: ${formatTime(globalTimer.getElapsed())}] Error processing page ${pageNum}:`, error.message);
            } finally {
                pageStats.time = pageTimer.end();
                timingStats.pages.push(pageStats);
                await page.close();
            }
        }

        // Save data
        fs.writeFileSync(path.join(outputDir, 'properties.json'), JSON.stringify(properties, null, 2));
        timingStats.total = globalTimer.end();
        

        fs.writeFileSync(path.join(outputDir, 'timing_stats.json'), JSON.stringify(timingStats, null, 2));
        
        // Calculate total storage used
        const totalImageSizeGB = totalImageSizeBytes / (1024 * 1024 * 1024);
        const storageStats = {
            totalProperties: totalPropertiesProcessed,
            totalImages: totalImagesDownloaded,
            totalSizeBytes: totalImageSizeBytes,
            totalSizeGB: totalImageSizeGB,
            averageImageSizeMB: totalImageSizeBytes / (totalImagesDownloaded * 1024 * 1024) || 0
        };
        fs.writeFileSync(path.join(outputDir, 'storage_stats.json'), JSON.stringify(storageStats, null, 2));
        
        console.log('\nScraping Summary:');
        console.log('===================');
        console.log(`Total execution time: ${formatTime(timingStats.total)}`)
    } catch (error) {
        console.error('Main error:', error);
    } finally {
        await browser.close();
        console.log('\nBrowser closed');
    }
})();