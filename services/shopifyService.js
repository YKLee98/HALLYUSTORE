// src/services/shopifyService.js

// Import the Node adapter FIRST to make platform-specific functions available
require('@shopify/shopify-api/adapters/node');

const {
    shopifyApi,
    ApiVersion,
    GraphqlQueryError,
    BillingInterval,
    LATEST_API_VERSION
} = require('@shopify/shopify-api');

const config = require('../config');
const logger = require('../config/logger');
const { ExternalServiceError, AppError, NotFoundError, ValidationError } = require('../utils/customErrors');

const SERVICE_NAME = 'ShopifySvc';

let shopify;

try {
    let apiVersionEnum;
    const configuredApiVersionString = config.shopify.apiVersion;

    if (!configuredApiVersionString) {
        logger.warn(`[${SERVICE_NAME}] Shopify API version not set in config.shopify.apiVersion. Defaulting to LATEST_API_VERSION.`);
        apiVersionEnum = LATEST_API_VERSION;
    } else if (configuredApiVersionString.toUpperCase() === 'LATEST') {
        apiVersionEnum = LATEST_API_VERSION;
    } else if (ApiVersion[configuredApiVersionString]) {
        apiVersionEnum = ApiVersion[configuredApiVersionString];
    } else {
        let parsedVersionKey = configuredApiVersionString;
        const match = configuredApiVersionString.match(/^(\d{4})-(\d{2})$/);

        if (match) {
            const year = match[1].substring(2);
            const monthIndex = parseInt(match[2], 10) - 1;
            const monthNames = ["January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"];
            
            if (monthIndex >= 0 && monthIndex < 12) {
                parsedVersionKey = monthNames[monthIndex] + year;
            } else {
                logger.warn(`[${SERVICE_NAME}] Invalid month in configured Shopify API version "${configuredApiVersionString}".`);
                parsedVersionKey = configuredApiVersionString;
            }
        }

        if (ApiVersion[parsedVersionKey]) {
            apiVersionEnum = ApiVersion[parsedVersionKey];
        } else {
            const availableVersions = Object.keys(ApiVersion).filter(k => /^[A-Z][a-z]+(2\d|Unstable)$/.test(k)).join(', ');
            logger.warn(`[${SERVICE_NAME}] Configured Shopify API version "${configuredApiVersionString}" (parsed as "${parsedVersionKey}") is not available in the installed @shopify/shopify-api library. Available versions: ${availableVersions}. Defaulting to LATEST_API_VERSION.`);
            apiVersionEnum = LATEST_API_VERSION;
        }
    }
    
    shopify = shopifyApi({
        apiKey: config.shopify.apiKey,
        apiSecretKey: config.shopify.apiSecret,
        scopes: Array.isArray(config.shopify.apiScopes) ? config.shopify.apiScopes : config.shopify.apiScopes.split(','),
        hostName: config.shopify.shopDomain.replace(/^https?:\/\//, '').split('/')[0],
        apiVersion: apiVersionEnum,
        isEmbeddedApp: config.shopify.isEmbeddedApp !== undefined ? config.shopify.isEmbeddedApp : false,
    });

    const actualInitializedApiVersion = shopify.config.apiVersion;
    const apiVersionName = Object.keys(ApiVersion).find(key => ApiVersion[key] === actualInitializedApiVersion) || actualInitializedApiVersion.toString();
    logger.info(`[${SERVICE_NAME}] Shopify API client initialized successfully. Host: ${shopify.config.hostName}, API Version: ${apiVersionName}. (Configured: ${configuredApiVersionString})`);

} catch (error) {
    logger.error(`[${SERVICE_NAME}] CRITICAL: Failed to initialize Shopify API client: ${error.message}`, {
        errorMessage: error.message,
        stack: error.stack,
        details: error.cause
    });
    throw new AppError(`Shopify API library initialization failed: ${error.message}`, 500, 'SHOPIFY_LIB_INIT_FAILURE', { cause: error });
}

function getShopifyAdminGraphQLClient() {
  if (!shopify) {
    logger.error(`[${SERVICE_NAME}] Shopify client instance is not available. Initialization might have failed.`);
    throw new AppError('Failed to create Shopify GraphQL client. Shopify instance not initialized.', 500, 'SHOPIFY_CLIENT_INSTANCE_FAILURE');
  }

  const shopHostname = String(config.shopify.shopDomain || '').replace(/^https?:\/\//, '').split('/')[0];
  const tokenValue = config.shopify.adminAccessToken;
  const tokenPreview = String(tokenValue || '').substring(0, 15);

  logger.debug(
    `[${SERVICE_NAME}] Preparing to create GraphQL client. Shop: '${shopHostname}', ` +
    `Admin Access Token Type: ${typeof tokenValue}, ` +
    `Token starts with: '${tokenPreview}${String(tokenValue || '').length > 15 ? '...' : ''}'`
  );
  
  if (!tokenValue || typeof tokenValue !== 'string' || !tokenValue.startsWith('shpat_')) {
    logger.error(`[${SERVICE_NAME}] CRITICAL: Shopify Admin Access Token is invalid or missing. Type: ${typeof tokenValue}, Value Preview: '${tokenPreview}...'.`);
    throw new AppError('Shopify Admin Access Token is invalid or not set.', 500, 'SHOPIFY_TOKEN_INVALID');
  }

  if (!shopHostname || !shopHostname.includes('.myshopify.com')) {
    logger.error(`[${SERVICE_NAME}] CRITICAL: Shopify shop domain ('${shopHostname}') is invalid.`);
    throw new AppError('Shopify store domain is invalid.', 500, 'SHOPIFY_DOMAIN_INVALID');
  }

  try {
    let session;
    if (shopify.session?.customAppSession) {
        session = shopify.session.customAppSession(shopHostname);
    } else if (shopify.Session?.CustomAppSession) {
        session = shopify.Session.CustomAppSession(shopHostname);
    } else {
        logger.error(`[${SERVICE_NAME}] CRITICAL: shopify.session.customAppSession (or Shopify.Session.CustomAppSession) is not available on the Shopify API instance.`);
        throw new AppError('Cannot create Shopify session object. Library initialization error possible.', 500, 'SHOPIFY_SESSION_ERROR');
    }
    
    session.accessToken = tokenValue;
    session.shop = shopHostname;

    logger.debug(`[${SERVICE_NAME}] Custom app session created for shop: '${session.shop}', accessToken is ${session.accessToken ? 'set on session' : 'NOT set on session'}.`);
    return new shopify.clients.Graphql({ session });

  } catch (clientCreationError) {
    logger.error(`[${SERVICE_NAME}] Error creating Shopify GraphQL client with explicit session: ${clientCreationError.message}`, { stack: clientCreationError.stack });
    throw new AppError(`Error creating Shopify GraphQL client: ${clientCreationError.message}`, 500, 'SHOPIFY_CLIENT_CREATION_EXPLICIT_SESSION_ERROR', { cause: clientCreationError });
  }
}

const MAX_SHOPIFY_RETRIES = parseInt(process.env.SHOPIFY_API_MAX_RETRIES, 10) || 3;
const INITIAL_SHOPIFY_RETRY_DELAY_MS = parseInt(process.env.SHOPIFY_API_INITIAL_RETRY_DELAY_MS, 10) || 2000;
const JITTER_FACTOR = 0.3;

async function shopifyGraphqlRequest(query, variables = {}) {
  const client = getShopifyAdminGraphQLClient();
  const operationName = query.match(/(query|mutation)\s+(\w+)/)?.[2] || 'UnnamedOperation';

  for (let attempt = 0; attempt <= MAX_SHOPIFY_RETRIES; attempt++) {
    try {
      logger.debug(`[${SERVICE_NAME}] GraphQL operation attempt ${attempt + 1}/${MAX_SHOPIFY_RETRIES + 1}: ${operationName}`, { variables: Object.keys(variables) });
      const response = await client.query({ data: { query, variables } });

      if (response.body.errors && response.body.errors.length > 0) {
        const errorDetails = {
            querySummary: query.substring(0, 250) + (query.length > 250 ? '...' : ''),
            variables,
            errors: response.body.errors,
            extensions: response.body.extensions,
            attempt: attempt + 1,
        };
        throw new ExternalServiceError(SERVICE_NAME, null, `Shopify GraphQL API returned user errors for ${operationName}.`, 'SHOPIFY_GQL_USER_ERRORS', errorDetails);
      }
      if (response.body.data === null && query.trim().startsWith('mutation')) {
          logger.warn(`[${SERVICE_NAME}] GraphQL Mutation ${operationName} returned null data without userErrors. Response body:`, response.body);
      }
      return response.body;

    } catch (error) {
      if (error instanceof GraphqlQueryError) {
        const statusCode = error.response?.statusCode;
        const isThrottled = statusCode === 429 || (error.message && error.message.toLowerCase().includes('throttled'));
        const isServerError = statusCode >= 500 && statusCode <= 599;
        const errorLogDetails = {
            message: error.message, operationName, querySummary: query.substring(0, 100) + '...',
            statusCode, isThrottled, isServerError, attempt: attempt + 1,
            responseBody: error.response?.body
        };
        logger.warn(`[${SERVICE_NAME}] GraphqlQueryError for ${operationName}:`, errorLogDetails);

        if (attempt < MAX_SHOPIFY_RETRIES && (isThrottled || isServerError)) {
          let delayMs = INITIAL_SHOPIFY_RETRY_DELAY_MS * Math.pow(2, attempt);
          const jitter = delayMs * JITTER_FACTOR * (Math.random() * 2 - 1);
          delayMs = Math.max(1000, Math.round(delayMs + jitter));
          if (isThrottled && error.response?.headers?.get('retry-after')) {
            const retryAfterSeconds = parseInt(error.response.headers.get('retry-after'), 10);
            if (!isNaN(retryAfterSeconds)) {
                delayMs = Math.max(delayMs, retryAfterSeconds * 1000 + 500);
            }
          }
          logger.info(`[${SERVICE_NAME}] Retrying GraphQL operation ${operationName} after ${Math.round(delayMs / 1000)}s.`);
          await new Promise(resolve => setTimeout(resolve, delayMs));
          continue;
        }
        throw new ExternalServiceError(SERVICE_NAME, error, `Shopify API request failed (Operation: ${operationName}, Status: ${statusCode || 'N/A'})`);
      
      } else if (error instanceof ExternalServiceError || error instanceof AppError) {
        throw error;
      } else {
        logger.error(`[${SERVICE_NAME}] Unexpected system error during Shopify GraphQL operation ${operationName} (Attempt ${attempt + 1}):`, error);
        throw new ExternalServiceError(SERVICE_NAME, error, `Unexpected system error during Shopify API call (Operation: ${operationName})`);
      }
    }
  }
  throw new ExternalServiceError(SERVICE_NAME, null, `Shopify API request failed after all retries (Operation: ${operationName})`);
}

async function createProduct(productInput, collectionGID = null, variantInfo = null) {
  // Remove media field if present (images are added separately)
  const { media, ...baseProductInput } = productInput;
  
  // Ensure product is set to ACTIVE status and published
  baseProductInput.status = 'ACTIVE';
  
  // Set publishedAt to make sure product is visible
  if (!baseProductInput.publishedAt) {
    baseProductInput.publishedAt = new Date().toISOString();
  }
  
  if (collectionGID) {
    baseProductInput.collectionsToJoin = [collectionGID];
  }

  // In API 2025-04, variants are NOT supported in ProductInput
  const mutation = `
    mutation productCreate($input: ProductInput!) {
      productCreate(input: $input) {
        product {
          id
          title
          descriptionHtml
          handle
          status
          publishedAt
          variants(first: 5) {
            edges {
              node {
                id
                sku
                price
                inventoryItem {
                  id
                }
              }
            }
          }
        }
        userErrors {
          field
          message
        }
      }
    }`;
  
  logger.info(`[${SERVICE_NAME}] Attempting to create Shopify product:`, { 
    title: productInput.title, 
    status: baseProductInput.status,
    publishedAt: baseProductInput.publishedAt,
    collectionGID
  });
  
  const response = await shopifyGraphqlRequest(mutation, { input: baseProductInput });
  
  if (response.data.productCreate.userErrors && response.data.productCreate.userErrors.length > 0) {
    const errorMessage = response.data.productCreate.userErrors.map(e => `Field: ${e.field?.join(',') || 'N/A'}, Msg: ${e.message}`).join('; ');
    throw new ExternalServiceError(SERVICE_NAME, null, `Product creation failed: ${errorMessage}`, 'SHOPIFY_PRODUCT_CREATE_ERROR', { userErrors: response.data.productCreate.userErrors });
  }
  
  const createdProduct = response.data.productCreate.product;
  
  if (!createdProduct) {
    throw new ExternalServiceError(SERVICE_NAME, null, 'Product creation returned null product', 'SHOPIFY_PRODUCT_CREATE_NULL');
  }
  
  logger.info(`[${SERVICE_NAME}] Successfully created Shopify product:`, { 
    id: createdProduct.id, 
    title: createdProduct.title,
    handle: createdProduct.handle,
    variantCount: createdProduct.variants?.edges?.length || 0
  });
  
  // Now update the default variant with price and inventory information
  if (variantInfo && createdProduct.variants?.edges?.length > 0) {
    const defaultVariant = createdProduct.variants.edges[0].node;
    const variantId = defaultVariant.id;
    const inventoryItemId = defaultVariant.inventoryItem?.id;
    
    try {
      // Update variant price and inventory policy
      await updateProductVariant({
        id: variantId,
        price: variantInfo.price,
        inventoryPolicy: variantInfo.inventoryPolicy || 'DENY'
      });
      
      logger.info(`[${SERVICE_NAME}] Updated variant price for product ${createdProduct.id}`);
      
      // Update SKU using productVariantUpdate mutation (if needed)
      if (variantInfo.sku && variantInfo.sku !== defaultVariant.sku) {
        await updateVariantSku(variantId, variantInfo.sku);
        logger.info(`[${SERVICE_NAME}] Updated SKU to ${variantInfo.sku} for product ${createdProduct.id}`);
      }
      
      // Update inventory
      if (inventoryItemId && variantInfo.locationId && typeof variantInfo.quantity === 'number') {
        await updateInventoryLevel(inventoryItemId, variantInfo.locationId, variantInfo.quantity);
        logger.info(`[${SERVICE_NAME}] Updated inventory for product ${createdProduct.id}`);
      }
    } catch (variantError) {
      logger.error(`[${SERVICE_NAME}] Failed to update variant details after product creation: ${variantError.message}`);
      // Don't fail the entire operation - product is already created
    }
  }
  
  // Publish product to all available sales channels
  try {
    logger.info(`[${SERVICE_NAME}] Publishing product ${createdProduct.id} to sales channels...`);
    
    // Get all available publications (sales channels)
    const pubQuery = `
      query {
        publications(first: 20) {
          edges {
            node {
              id
              name
              supportsFuturePublishing
            }
          }
        }
      }`;
    
    const pubResponse = await shopifyGraphqlRequest(pubQuery, {});
    const publications = pubResponse.data?.publications?.edges || [];
    
    logger.info(`[${SERVICE_NAME}] Found ${publications.length} sales channels`);
    
    // Find online store and any other active channels
    const channelsToPublish = publications.filter(pub => {
      const name = pub.node.name.toLowerCase();
      // Include online store and potentially other channels
      return name.includes('online store') || 
             name === 'online store' ||
             name.includes('온라인 스토어') ||
             name.includes('shop');
    });
    
    if (channelsToPublish.length > 0) {
      logger.info(`[${SERVICE_NAME}] Publishing to ${channelsToPublish.length} channels: ${channelsToPublish.map(ch => ch.node.name).join(', ')}`);
      
      // Use publishablePublish to add product to sales channels
      const publishMutation = `
        mutation publishProduct($id: ID!, $input: [PublicationInput!]!) {
          publishablePublish(id: $id, input: $input) {
            publishable {
              availablePublicationCount
              publicationCount
            }
            userErrors {
              field
              message
            }
          }
        }`;
      
      const publicationInputs = channelsToPublish.map(ch => ({
        publicationId: ch.node.id
      }));
      
      const publishResult = await shopifyGraphqlRequest(publishMutation, {
        id: createdProduct.id,
        input: publicationInputs
      });
      
      if (publishResult.data?.publishablePublish?.userErrors?.length > 0) {
        logger.error(`[${SERVICE_NAME}] Errors publishing product:`, publishResult.data.publishablePublish.userErrors);
      } else {
        const pubCount = publishResult.data?.publishablePublish?.publishable?.publicationCount || 0;
        logger.info(`[${SERVICE_NAME}] Product is now published to ${pubCount} sales channels`);
      }
    } else {
      logger.warn(`[${SERVICE_NAME}] No online store publication found. Product may not be visible.`);
    }
    
  } catch (publishError) {
    logger.error(`[${SERVICE_NAME}] Failed to publish product to sales channels: ${publishError.message}`);
    // Don't fail the entire operation
  }
  
  return createdProduct;
}

async function publishProductToOnlineStore(productId) {
  try {
    // Get all publications
    const pubQuery = `
      query {
        publications(first: 10) {
          edges {
            node {
              id
              name
            }
          }
        }
      }`;
    
    const pubResponse = await shopifyGraphqlRequest(pubQuery, {});
    const publications = pubResponse.data?.publications?.edges || [];
    const onlineStorePub = publications.find(pub => 
      pub.node.name.toLowerCase().includes('online store') || 
      pub.node.name.toLowerCase() === 'online store'
    );
    
    if (!onlineStorePub) {
      logger.error(`[${SERVICE_NAME}] Online Store publication not found`);
      return null;
    }
    
    const publicationId = onlineStorePub.node.id;
    logger.info(`[${SERVICE_NAME}] Publishing product ${productId} to ${onlineStorePub.node.name} (${publicationId})`);
    
    // Use productResourcePublicationsUpdate mutation (API 2025-04 compatible)
    const publishMutation = `
      mutation publishProduct($id: ID!, $input: [ResourcePublicationInput!]!) {
        productResourcePublicationsUpdate(id: $id, input: $input) {
          product {
            id
            publishedAt
            resourcePublicationsV2(first: 5) {
              edges {
                node {
                  publication {
                    id
                    name
                  }
                  isPublished
                  publishDate
                }
              }
            }
          }
          userErrors {
            field
            message
          }
        }
      }`;
    
    const publishResponse = await shopifyGraphqlRequest(publishMutation, {
      id: productId,
      input: [{
        publicationId: publicationId,
        publishDate: new Date().toISOString()
      }]
    });
    
    if (publishResponse.data?.productResourcePublicationsUpdate?.userErrors?.length > 0) {
      const errors = publishResponse.data.productResourcePublicationsUpdate.userErrors;
      logger.error(`[${SERVICE_NAME}] Failed to publish product:`, errors);
      throw new Error(`Failed to publish: ${errors.map(e => e.message).join(', ')}`);
    }
    
    const product = publishResponse.data?.productResourcePublicationsUpdate?.product;
    if (product) {
      const publishedChannels = product.resourcePublicationsV2?.edges || [];
      const isPublished = publishedChannels.some(ch => 
        ch.node.isPublished && 
        ch.node.publication.name.toLowerCase().includes('online store')
      );
      
      if (isPublished) {
        logger.info(`[${SERVICE_NAME}] Product ${productId} successfully published to Online Store`);
      } else {
        logger.warn(`[${SERVICE_NAME}] Product ${productId} may not be published to Online Store yet`);
      }
    }
    
    return publishResponse.data?.productResourcePublicationsUpdate;
    
  } catch (error) {
    logger.error(`[${SERVICE_NAME}] Error in publishProductToOnlineStore:`, error);
    throw error;
  }
}

async function updateVariantSku(variantId, sku) {
  // Special function to update SKU using the productVariantUpdate mutation
  // This is necessary because SKU cannot be updated via productVariantsBulkUpdate
  const mutation = `
    mutation productVariantUpdate($input: ProductVariantInput!) {
      productVariantUpdate(input: $input) {
        productVariant {
          id
          sku
        }
        userErrors {
          field
          message
        }
      }
    }`;
  
  const response = await shopifyGraphqlRequest(mutation, { 
    input: {
      id: variantId,
      sku: sku
    }
  });
  
  if (response.data?.productVariantUpdate?.userErrors && response.data.productVariantUpdate.userErrors.length > 0) {
    const errorMessage = response.data.productVariantUpdate.userErrors.map(e => `Field: ${e.field?.join(',') || 'N/A'}, Msg: ${e.message}`).join('; ');
    logger.error(`[${SERVICE_NAME}] Failed to update SKU: ${errorMessage}`);
    throw new ExternalServiceError(SERVICE_NAME, null, `SKU update failed: ${errorMessage}`, 'SHOPIFY_SKU_UPDATE_ERROR');
  }
  
  return response.data?.productVariantUpdate?.productVariant;
}

async function updateProductVariant(variantInput) {
  // Get the product ID for this variant
  const getProductQuery = `
    query getProductFromVariant($id: ID!) {
      productVariant(id: $id) {
        product {
          id
        }
      }
    }`;
  
  const productResponse = await shopifyGraphqlRequest(getProductQuery, { id: variantInput.id });
  const productId = productResponse.data?.productVariant?.product?.id;
  
  if (!productId) {
    throw new Error('Could not find product ID for variant');
  }
  
  // Use productVariantsBulkUpdate for price and inventory policy
  const mutation = `
    mutation productVariantsBulkUpdate($productId: ID!, $variants: [ProductVariantsBulkInput!]!) {
      productVariantsBulkUpdate(productId: $productId, variants: $variants) {
        productVariants {
          id
          price
          inventoryPolicy
        }
        userErrors {
          field
          message
        }
      }
    }`;
  
  const bulkVariantInput = {
    id: variantInput.id,
    price: variantInput.price,
    inventoryPolicy: variantInput.inventoryPolicy
  };
  
  logger.debug(`[${SERVICE_NAME}] Updating variant with bulk mutation:`, { productId, variant: bulkVariantInput });
  
  const response = await shopifyGraphqlRequest(mutation, { 
    productId: productId,
    variants: [bulkVariantInput]
  });
  
  if (response.data?.productVariantsBulkUpdate?.userErrors && response.data.productVariantsBulkUpdate.userErrors.length > 0) {
    const errorMessage = response.data.productVariantsBulkUpdate.userErrors.map(e => `Field: ${e.field?.join(',') || 'N/A'}, Msg: ${e.message}`).join('; ');
    throw new ExternalServiceError(SERVICE_NAME, null, `Variant update failed: ${errorMessage}`, 'SHOPIFY_VARIANT_UPDATE_ERROR');
  }
  
  if (variantInput.sku) {
    logger.warn(`[${SERVICE_NAME}] SKU update requested but not supported in productVariantsBulkUpdate. SKU: ${variantInput.sku} was not updated.`);
  }
  
  return response.data?.productVariantsBulkUpdate?.productVariants?.[0];
}

async function updateProduct(productUpdateInput, collectionGIDToJoin = null, collectionGIDToLeave = null) {
  if (!productUpdateInput.id) {
    throw new ValidationError('Shopify product GID (id) is required for update.', [{ field: 'id', message: 'Product GID is required.'}]);
  }
  
  // Remove media field if present
  const { media, ...finalProductUpdateInput } = productUpdateInput;
  
  if (collectionGIDToJoin) {
    finalProductUpdateInput.collectionsToJoin = [collectionGIDToJoin];
  }
  if (collectionGIDToLeave) {
    finalProductUpdateInput.collectionsToLeave = [collectionGIDToLeave];
  }

  const mutation = `
    mutation productUpdate($input: ProductInput!) {
      productUpdate(input: $input) {
        product {
          id
          title
          handle
          status
          updatedAt
          variants(first: 5) {
            edges {
              node {
                id
                sku
                price
                inventoryItem {
                  id
                }
              }
            }
          }
          collections(first: 5) {
            edges {
              node {
                id
                title
              }
            }
          }
        }
        userErrors {
          field
          message
        }
      }
    }`;
    
  logger.info(`[${SERVICE_NAME}] Attempting to update Shopify product:`, { 
    id: productUpdateInput.id, 
    title: productUpdateInput.title, 
    collectionGIDToJoin, 
    collectionGIDToLeave
  });
  
  const response = await shopifyGraphqlRequest(mutation, { input: finalProductUpdateInput });
  
  if (response.data.productUpdate.userErrors && response.data.productUpdate.userErrors.length > 0) {
    const errorMessage = response.data.productUpdate.userErrors.map(e => `Field: ${e.field?.join(',') || 'N/A'}, Msg: ${e.message}`).join('; ');
    throw new ExternalServiceError(SERVICE_NAME, null, `Product update failed: ${errorMessage}`, 'SHOPIFY_PRODUCT_UPDATE_ERROR', { userErrors: response.data.productUpdate.userErrors });
  }
  
  const updatedProduct = response.data.productUpdate.product;
  
  if (!updatedProduct) {
    throw new ExternalServiceError(SERVICE_NAME, null, 'Product update returned null product', 'SHOPIFY_PRODUCT_UPDATE_NULL');
  }
  
  logger.info(`[${SERVICE_NAME}] Successfully updated Shopify product:`, { 
    id: updatedProduct.id, 
    title: updatedProduct.title,
    variantCount: updatedProduct.variants?.edges?.length || 0
  });
  
  // Ensure product is published to online store after update
  try {
    // Get online store publication ID
    const pubQuery = `
      query getProductPublications($id: ID!) {
        product(id: $id) {
          id
          resourcePublicationsV2(first: 10) {
            edges {
              node {
                publication {
                  id
                  name
                }
                isPublished
              }
            }
          }
        }
      }`;
    
    const pubResponse = await shopifyGraphqlRequest(pubQuery, { id: updatedProduct.id });
    const publications = pubResponse.data?.product?.resourcePublicationsV2?.edges || [];
    const onlineStore = publications.find(pub => 
      pub.node.publication.name.toLowerCase().includes('online store') || 
      pub.node.publication.name.toLowerCase() === 'online store'
    );
    
    if (onlineStore && !onlineStore.node.isPublished) {
      // Publish to online store
      const publishMutation = `
        mutation publishProduct($id: ID!, $input: [ResourcePublicationInput!]!) {
          productResourcePublicationsUpdate(id: $id, input: $input) {
            product {
              id
              publishedAt
            }
            userErrors {
              field
              message
            }
          }
        }`;
      
      const publishResult = await shopifyGraphqlRequest(publishMutation, {
        id: updatedProduct.id,
        input: [{
          publicationId: onlineStore.node.publication.id,
          publishDate: new Date().toISOString()
        }]
      });
      
      if (publishResult.data?.productResourcePublicationsUpdate?.userErrors?.length === 0) {
        logger.info(`[${SERVICE_NAME}] Product re-published to online store after update`);
      }
    }
  } catch (publishError) {
    logger.error(`[${SERVICE_NAME}] Failed to publish updated product: ${publishError.message}`);
  }
  
  return updatedProduct;
}

async function updateInventoryLevel(inventoryItemId, locationId, availableQuantity) {
  if (!inventoryItemId || !locationId || typeof availableQuantity !== 'number') {
    logger.error(`[${SERVICE_NAME}] Invalid parameters for inventory update:`, {
      inventoryItemId,
      locationId,
      availableQuantity
    });
    return null;
  }

  const mutation = `
    mutation inventorySetOnHandQuantities($input: InventorySetOnHandQuantitiesInput!) {
      inventorySetOnHandQuantities(input: $input) {
        inventoryAdjustmentGroup {
          createdAt
          reason
        }
        userErrors {
          field
          message
          code
        }
      }
    }`;
  
  const input = {
    reason: "correction",
    setQuantities: [{
      inventoryItemId: inventoryItemId,
      locationId: locationId,
      quantity: availableQuantity
    }]
  };
  
  logger.debug(`[${SERVICE_NAME}] Updating inventory level:`, JSON.stringify(input, null, 2));
  
  const response = await shopifyGraphqlRequest(mutation, { input });
  
  if (response.data?.inventorySetOnHandQuantities?.userErrors && response.data.inventorySetOnHandQuantities.userErrors.length > 0) {
    const userErrors = response.data.inventorySetOnHandQuantities.userErrors;
    const errorMessage = userErrors.map(e => `Field: ${e.field?.join(',') || 'N/A'}, Code: ${e.code || 'N/A'}, Msg: ${e.message}`).join('; ');
    throw new ExternalServiceError(SERVICE_NAME, null, `Inventory update failed: ${errorMessage}`, 'SHOPIFY_INVENTORY_UPDATE_ERROR', { userErrors });
  }
  
  logger.info(`[${SERVICE_NAME}] Successfully updated inventory level for item ${inventoryItemId} at location ${locationId} to ${availableQuantity}`);
  return response.data?.inventorySetOnHandQuantities;
}

async function appendMediaToProduct(productId, mediaInputs) {
  if (!productId) {
    throw new ValidationError('Product ID is required to append media.', []);
  }
  if (!mediaInputs || !Array.isArray(mediaInputs) || mediaInputs.length === 0) {
    logger.info(`[${SERVICE_NAME}] No media inputs provided to append for product ${productId}. Skipping.`);
    return { mediaUserErrors: [], media: [] };
  }

  // Process and validate URLs
  const processedMediaInputs = mediaInputs.map(media => {
    let url = media.originalSource;
    
    // Convert to HTTPS
    if (url && url.startsWith('http://')) {
      url = url.replace('http://', 'https://');
    }
    
    // Handle {res} placeholder for Bunjang URLs
    if (url && url.includes('{res}')) {
      // Replace {res} with a standard resolution
      url = url.replace('{res}', '856');
      logger.debug(`[${SERVICE_NAME}] Replaced {res} placeholder with 856 in URL: ${url}`);
    }
    
    // Log warning for Bunjang URLs
    if (url && (url.includes('media.bunjang.co.kr') || url.includes('img.bunjang.co.kr'))) {
      logger.warn(`[${SERVICE_NAME}] Bunjang image URL detected: ${url}. These URLs often fail Shopify validation due to regional restrictions.`);
    }
    
    return {
      ...media,
      originalSource: url,
      mediaContentType: media.mediaContentType || 'IMAGE'
    };
  });

  const mutation = `
    mutation productCreateMedia($productId: ID!, $media: [CreateMediaInput!]!) {
      productCreateMedia(productId: $productId, media: $media) {
        media {
          id
          status
          alt
          mediaContentType
          preview {
            image {
              url
              width
              height
            }
          }
        }
        mediaUserErrors {
          field
          message
          code
        }
        product {
          id
        }
      }
    }
  `;
  
  logger.info(`[${SERVICE_NAME}] Attempting to append ${processedMediaInputs.length} media items to product ${productId}`);
  
  try {
    const response = await shopifyGraphqlRequest(mutation, { productId, media: processedMediaInputs });
    
    if (response.data.productCreateMedia.mediaUserErrors && response.data.productCreateMedia.mediaUserErrors.length > 0) {
      logger.error(`[${SERVICE_NAME}] User errors while appending media to product ${productId}:`, response.data.productCreateMedia.mediaUserErrors);
      
      const failedCount = response.data.productCreateMedia.mediaUserErrors.length;
      const successCount = processedMediaInputs.length - failedCount;
      
      logger.warn(`[${SERVICE_NAME}] ${failedCount} images failed validation (likely due to server access restrictions), ${successCount} may have succeeded.`);
      
      return {
        mediaUserErrors: response.data.productCreateMedia.mediaUserErrors,
        media: response.data.productCreateMedia.media || [],
        warning: `${failedCount} images failed validation. This is common with Bunjang image URLs due to regional server restrictions.`
      };
    }
    
    logger.info(`[${SERVICE_NAME}] Successfully processed media attachment for product ${productId}. Media items processed: ${response.data.productCreateMedia.media?.length || 0}`);
    return response.data.productCreateMedia;
    
  } catch (error) {
    logger.error(`[${SERVICE_NAME}] Exception while appending media to product ${productId}:`, error);
    if (error instanceof ExternalServiceError) throw error;
    throw new ExternalServiceError(SERVICE_NAME, error, `Failed to append media to product ${productId}`);
  }
}

async function findProductByBunjangPidTag(bunjangPid) {
  const searchQuery = `tag:'bunjang_pid:${String(bunjangPid).trim()}'`;
  const query = `
    query productsByTag($query: String!) {
      products(first: 1, query: $query) {
        edges {
          node {
            id
            title
            handle
            metafield(namespace: "bunjang", key: "pid") { id value }
          }
        }
      }
    }`;
    
  logger.info(`[${SERVICE_NAME}] Searching Shopify product by Bunjang PID tag: ${searchQuery}`);
  
  try {
    const response = await shopifyGraphqlRequest(query, { query: searchQuery });
    
    if (response.data.products.edges.length > 0) {
      const productNode = response.data.products.edges[0].node;
      logger.info(`[${SERVICE_NAME}] Found Shopify product by Bunjang PID ${bunjangPid} (tag match): ${productNode.id}`);
      return productNode;
    }
    
    logger.info(`[${SERVICE_NAME}] No Shopify product found matching Bunjang PID tag: ${bunjangPid}.`);
    return null;
    
  } catch (error) {
    logger.error(`[${SERVICE_NAME}] Error searching product by Bunjang PID tag ${bunjangPid}: ${error.message}`);
    throw error;
  }
}

async function updateOrder(orderUpdateInput) {
  if (!orderUpdateInput.id) {
    throw new ValidationError('Shopify Order GID (id) is required for update.', [{ field: 'id', message: 'Order GID is required.'}]);
  }
  
  const mutation = `
    mutation orderUpdate($input: OrderInput!) {
      orderUpdate(input: $input) {
        order {
          id
          updatedAt
          tags
          metafields(first: 10) {
            edges {
              node {
                id
                namespace
                key
                value
                type
              }
            }
          }
        }
        userErrors {
          field
          message
        }
      }
    }`;
    
  logger.info(`[${SERVICE_NAME}] Attempting to update Shopify order:`, { id: orderUpdateInput.id, keys: Object.keys(orderUpdateInput).filter(k => k !== 'id') });
  const response = await shopifyGraphqlRequest(mutation, { input: orderUpdateInput });
  
  if (response.data.orderUpdate.userErrors && response.data.orderUpdate.userErrors.length > 0) {
    const errorMessage = response.data.orderUpdate.userErrors.map(e => `Field: ${e.field?.join(',') || 'N/A'}, Msg: ${e.message}`).join('; ');
    throw new ExternalServiceError(SERVICE_NAME, null, `Order update failed: ${errorMessage}`, 'SHOPIFY_ORDER_UPDATE_ERROR', { userErrors: response.data.orderUpdate.userErrors });
  }
  
  logger.info(`[${SERVICE_NAME}] Shopify order updated successfully:`, { id: response.data.orderUpdate.order.id });
  return response.data.orderUpdate.order;
}

async function addProductsToCollection(collectionGID, productGIDs) {
  if (!collectionGID || !productGIDs || !Array.isArray(productGIDs) || productGIDs.length === 0) {
    throw new ValidationError('Valid Collection GID and at least one Product GID array are required.', []);
  }
  
  const mutation = `
    mutation collectionAddProducts($id: ID!, $productIds: [ID!]!) {
      collectionAddProducts(id: $id, productIds: $productIds) {
        collection {
          id
          title
          productsCount
        }
        userErrors {
          field
          message
        }
      }
    }`;
    
  logger.info(`[${SERVICE_NAME}] Attempting to add products to collection:`, { collectionGID, productCount: productGIDs.length });
  const response = await shopifyGraphqlRequest(mutation, { id: collectionGID, productIds: productGIDs });
  
  if (response.data.collectionAddProducts.userErrors && response.data.collectionAddProducts.userErrors.length > 0) {
    const errorMessage = response.data.collectionAddProducts.userErrors.map(e => `Field: ${e.field?.join(',') || 'N/A'}, Msg: ${e.message}`).join('; ');
    throw new ExternalServiceError(SERVICE_NAME, null, `Failed to add products to collection: ${errorMessage}`, 'SHOPIFY_COLLECTION_ADD_ERROR', { userErrors: response.data.collectionAddProducts.userErrors });
  }
  
  logger.info(`[${SERVICE_NAME}] Products added to collection successfully:`, { collectionGID, productsAdded: productGIDs.length, currentProductCount: response.data.collectionAddProducts.collection?.productsCount });
  return response.data.collectionAddProducts.collection;
}

module.exports = {
  shopifyGraphqlRequest,
  createProduct,
  updateProduct,
  updateProductVariant,
  appendMediaToProduct,
  findProductByBunjangPidTag,
  updateOrder,
  addProductsToCollection,
  updateInventoryLevel,
  publishProductToOnlineStore,
};