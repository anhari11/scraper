const { chromium } = require('playwright');
const { SQSClient, ReceiveMessageCommand, DeleteMessageCommand } = require('@aws-sdk/client-sqs');
const { PrismaClient } = require('@prisma/client');

const prisma = new PrismaClient();
const sqs = new SQSClient({ region: 'eu-north-1' });
const queueUrl = 'https://sqs.eu-north-1.amazonaws.com/992382591031/quequescraper.fifo';


const extractNumber = (str) => {
  if (!str) return null;
  const match = str.toString().match(/\d+/);
  return match ? parseFloat(match[0]) : null;
};


const normalizeBoolean = (value) => {
  if (value === true || value === 'true' || value === 'yes') return true;
  if (value === false || value === 'false' || value === 'no') return false;
  return null;
};


const parseAmenities = (amenities, property) => {
  if (!amenities) return property;

  
  let amenitiesArray = [];
  if (typeof amenities === 'string') {
    try {
      amenitiesArray = JSON.parse(amenities);
    } catch (e) {
      amenitiesArray = amenities.split(',').map(item => item.trim());
    }
  } else if (Array.isArray(amenities)) {
    amenitiesArray = amenities;
  }

  
  amenitiesArray.forEach(amenity => {
    const normalizedAmenity = amenity.toLowerCase().trim();
    
    switch(normalizedAmenity) {
      case 'pool':
      case 'swimming pool':
        property.has_pool = true;
        break;
      case 'garden':
        property.has_garden = true;
        break;
      case 'garage':
      case 'parking':
        property.has_garage = true;
        break;
      case 'jacuzzi':
        property.has_jacuzzi = true;
        break;
      case 'sauna':
        property.has_sauna = true;
        break;
      case 'gym':
        property.has_gym = true;
        break;
      case 'terrace':
        property.has_terrace = true;
        break;
      case 'elevator':
        property.has_elevator = true;
        break;
      case 'sea view':
        property.has_sea_view = true;
        break;
      case 'barbecue area':
      case 'bbq area':
        property.has_barbeque_area = true;
        break;
      case 'basement':
        property.has_basement = true;
        break;
      case 'courtyard':
        property.has_courtyard = true;
        break;
      case 'disabled access':
        property.has_disabled_access = true;
        break;
      case 'gated entry':
        property.has_gated_entry = true;
        break;
      case 'greenhouse':
        property.has_greenhouse = true;
        break;
      case 'hot tub':
        property.has_hottub = true;
        break;
      case 'lawn':
        property.has_lawn = true;
        break;
      case 'patio':
        property.has_patio = true;
        break;
      case 'pond':
        property.has_pond = true;
        break;
      case 'porch':
        property.has_porch = true;
        break;
      case 'private patio':
        property.has_private_patio = true;
        break;
      case 'sports court':
        property.has_sports_court = true;
        break;
      case 'waterfront':
        property.is_waterfront = true;
        break;
      case 'attic':
        property.has_attic = true;
        break;
      case 'cable/satellite':
        property.has_cable_satellite = true;
        break;
      case 'double pane windows':
        property.has_doublepane_windows = true;
        break;
      case 'security system':
        property.has_security_system = true;
        break;
      case 'skylight':
        property.has_skylight = true;
        break;
      case 'vaulted ceiling':
        property.has_vaulted_ceiling = true;
        break;
      case 'wet bar':
        property.has_wet_bar = true;
        break;
      case 'fireplace':
        property.has_fireplace = true;
        break;
      case 'cinema':
        property.has_cinema = true;
        break;
      case 'tennis court':
        property.has_tennis_court = true;
        break;
      case 'helipad':
        property.has_helipad = true;
        break;
    }
  });

  return property;
};

async function processMessage(message) {
  let propertyUrl;
  try {
    const bodyObj = JSON.parse(message.Body);
    propertyUrl = bodyObj.url; // Extract the URL string
  } catch (err) {
    console.error('Failed to parse message body JSON:', err);
    return; // skip this message because it's malformed
  }
  
  console.log(`Processing property URL: ${propertyUrl}`);

  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();

  try {
    await page.goto(propertyUrl, { waitUntil: 'domcontentloaded' });

    // Hydration JSON (contains structured data)
    const html = await page.content();
    const hydrationMatch = html.match(/<script type="application\/json" id="properties-hydration">(.*?)<\/script>/);
    let propertyData = {};
    if (hydrationMatch) {
      try {
        propertyData = JSON.parse(hydrationMatch[1]);
      } catch (err) {
        console.error("Failed to parse hydration data:", err);
      }
    }


    const description = await page.$eval('[data-role="description-text-content"], [data-role="description-text-container"]', el => el.innerText.trim()).catch(() => null);

    // Features
    const features = await page.evaluate(() => {
      const map = {};
      document.querySelectorAll('.feat-item').forEach(item => {
        const labelEl = item.querySelector('.feat-label');
        if (!labelEl) return;
        
        const label = labelEl.textContent.trim().replace(/:$/, '');
        const valueEls = item.querySelectorAll('.single-value, .multiple-values');
        
        if (valueEls.length > 1) {

          map[label] = Array.from(valueEls).map(el => el.textContent.trim());
        } else if (valueEls.length === 1) {

          map[label] = valueEls[0].textContent.trim();
        } else {

          map[label] = true;
        }
      });
      return map;
    });


    const images = await page.evaluate(() => {
      return Array.from(document.querySelectorAll('img[src*="properties"]'))
        .map(img => img.src)
        .filter(src => src && !src.includes('placeholder'));
    });

    const property = {
      property_url: propertyUrl,
      title: propertyData.title || "Untitled Property", 
      description: description,
      price: propertyData.price?.amount ? parseFloat(propertyData.price.amount) : null,
      currency: propertyData.price?.currencyCode || 'EUR',
      address: propertyData.location?.address || null,
      city: propertyData.location?.city || null,
      neighborhood: features['Neighborhood'] || null,
      province: propertyData.location?.province || null,
      country: propertyData.location?.country || null,
      zip_code: propertyData.location?.postalCode || null,
      rooms: extractNumber(features['Rooms'] || features['Bedrooms']),
      bathrooms: extractNumber(features['Bathrooms']),
      area: extractNumber(features['Size'] || features['Area']),
      lot_area: extractNumber(features['External size']),
      floor: extractNumber(features['Floor']),
      floor_total: extractNumber(features['Floor Count']),
      year_built: extractNumber(features['Year of construction']),
      property_type: propertyData.type || null,
      status: features['Status'] || 'available',
      property_reference: features['Reference'] || null,
      energy_rating: features['Energy Rating'] || null,
      cooling_system: features['Cooling Systems'] || null,
      heating_source: features['Heating Source'] || features['Heating'] || null,
      condition: null,
      agency_name: propertyData.agency?.name || null,
      agency_phone: propertyData.agency?.phone || null,
      listing_url: propertyUrl,
      orientation: null,
      ownership_type: null,
      monthly_community_fee: null,
      exterior_size: extractNumber(features['External size']),
      exterior_type: features['Exterior Type'] || null,
      floor_type: features['Floor Type'] || null,
      garden_type: features['Garden Type'] || null,
      roof_type: features['Roof Type'] || null,
      architectural_style: features['Architectural Style'] || null,
      balcony_count: extractNumber(features['Balcony count']),
      kitchen_count: extractNumber(features['Kitchens']),
      parking_type: features['Parking Type'] || features['Car parking'] || null,
      gas_emission_class: features['Gas emission Class'] || null,
      general_view: Array.isArray(features['View']) ? features['View'].join(', ') : features['View'] || null,

      has_pool: false,
      has_garden: false,
      has_garage: false,
      near_beach: false,
      has_jacuzzi: false,
      has_sauna: false,
      has_gym: false,
      has_terrace: false,
      has_elevator: false,
      has_sea_view: false,
      furnished: false,
      has_barbeque_area: false,
      has_basement: false,
      has_courtyard: false,
      has_disabled_access: false,
      has_gated_entry: false,
      has_greenhouse: false,
      has_hottub: false,
      has_lawn: false,
      has_patio: false,
      has_pond: false,
      has_porch: false,
      has_private_patio: false,
      has_sports_court: false,
      is_waterfront: false,
      has_attic: false,
      has_cable_satellite: false,
      has_doublepane_windows: false,
      has_security_system: false,
      has_skylight: false,
      has_vaulted_ceiling: false,
      has_wet_bar: false,
      has_fireplace: false,
      has_cinema: false,
      has_tennis_court: false,
      has_helipad: false
    };

    // Check general_view for sea terms
    if (property.general_view) {
      const seaTerms = ['sea', 'ocean', 'waterfront', 'beach', 'marina'];
      const hasSeaView = seaTerms.some(term => 
        property.general_view.toLowerCase().includes(term.toLowerCase())
      );
      
      if (hasSeaView) {
        property.has_sea_view = true;
        property.near_beach = true;
      }
    }

    // Existing explicit checks
    property.has_sea_view = normalizeBoolean(features['Sea view']) || property.has_sea_view;
    property.near_beach = normalizeBoolean(features['Beachfront'] || features['Distance to beach']) || property.near_beach;

    property.has_pool = normalizeBoolean(features['Pool'] || features['Swimming pool']) || property.has_pool;
    property.has_garden = normalizeBoolean(features['Garden']) || property.has_garden;
    property.has_garage = normalizeBoolean(features['Garage'] || features['Parking']) || property.has_garage;
    property.near_beach = normalizeBoolean(features['Beachfront'] || features['Distance to beach']) || property.near_beach;
    property.has_jacuzzi = normalizeBoolean(features['Jacuzzi']) || property.has_jacuzzi;
    property.has_sauna = normalizeBoolean(features['Sauna']) || property.has_sauna;
    property.has_gym = normalizeBoolean(features['Gym']) || property.has_gym;
    property.has_terrace = normalizeBoolean(features['Terrace']) || property.has_terrace;
    property.has_elevator = normalizeBoolean(features['Elevator']) || property.has_elevator;
    property.has_sea_view = normalizeBoolean(features['Sea view']) || property.has_sea_view;
    property.furnished = normalizeBoolean(features['Furnished']) || property.furnished;

    parseAmenities(features['Exterior Amenities'], property);
    parseAmenities(features['Interior Amenities'], property);

    const createdProperty = await prisma.properties.create({
      data: property
    });

    // Save images
    if (images.length > 0) {
      await prisma.property_images.createMany({
        data: images.map((imageUrl, index) => ({
          property_id: createdProperty.id,
          image_url: imageUrl,
          is_primary: index === 0
        }))
      });
    }

    console.log(`✅ Successfully saved property ${createdProperty.id} to database`);

  } catch (error) {
    console.error(`Error processing property ${propertyUrl}:`, error);
  } finally {
    await browser.close();
  }
}

async function pollQueue() {
  while (true) {
    try {
      const receiveParams = {
        QueueUrl: queueUrl,
        MaxNumberOfMessages: 1,
        WaitTimeSeconds: 20
      };

      const data = await sqs.send(new ReceiveMessageCommand(receiveParams));
      
      if (data.Messages && data.Messages.length > 0) {
        const message = data.Messages[0];
        await processMessage(message);
        
        const deleteParams = {
          QueueUrl: queueUrl,
          ReceiptHandle: message.ReceiptHandle
        };
        await sqs.send(new DeleteMessageCommand(deleteParams));
      }
    } catch (error) {
      console.error('Error polling SQS queue:', error);
      await new Promise(resolve => setTimeout(resolve, 5000));
    }
  }
}


pollQueue().catch(error => {
  console.error('Fatal error in queue polling:', error);
  process.exit(1);
});