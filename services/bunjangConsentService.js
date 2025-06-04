// services/bunjangConsentService.js
// 기존 shopifyService.js와 함께 사용하기 위한 통합 서비스

const { shopifyGraphqlRequest } = require('./shopifyService');
const logger = require('../config/logger');

const SERVICE_NAME = 'BunjangConsentSvc';

class BunjangConsentService {
  constructor() {
    this.consentKeys = {
      priceAdjustment: 'bunjang_consent_price_adjustment',
      noCancellation: 'bunjang_consent_no_cancellation',
      timestamp: 'bunjang_consent_timestamp'
    };
  }

  /**
   * 주문에 BUNJANG 상품이 포함되어 있는지 확인
   */
  async checkOrderHasBunjangItems(orderId) {
    const query = `
      query getOrder($id: ID!) {
        order(id: $id) {
          id
          lineItems(first: 100) {
            edges {
              node {
                id
                vendor
                product {
                  id
                  tags
                  collections(first: 10) {
                    edges {
                      node {
                        id
                        handle
                        title
                      }
                    }
                  }
                }
                customAttributes {
                  key
                  value
                }
              }
            }
          }
        }
      }
    `;

    try {
      const response = await shopifyGraphqlRequest(query, { id: orderId });
      const lineItems = response.data?.order?.lineItems?.edges || [];
      
      return lineItems.some(({ node: item }) => {
        // 여러 방법으로 BUNJANG 상품 확인
        const isBunjangVendor = item.vendor === 'Bunjang';
        const hasBunjangTag = item.product?.tags?.includes('bungjang');
        const inBunjangCollection = item.product?.collections?.edges?.some(
          ({ node: collection }) => collection.handle === 'bunjang'
        );
        const hasBunjangAttribute = item.customAttributes?.some(
          attr => attr.key === 'supplier' && attr.value === 'bungjang'
        );
        
        return isBunjangVendor || hasBunjangTag || inBunjangCollection || hasBunjangAttribute;
      });
    } catch (error) {
      logger.error(`[${SERVICE_NAME}] Error checking BUNJANG items in order:`, error);
      throw error;
    }
  }

  /**
   * 주문의 동의 상태 확인
   */
  async validateOrderConsent(orderId) {
    const query = `
      query getOrderAttributes($id: ID!) {
        order(id: $id) {
          id
          customAttributes {
            key
            value
          }
          createdAt
        }
      }
    `;

    try {
      const response = await shopifyGraphqlRequest(query, { id: orderId });
      const order = response.data?.order;
      
      if (!order) {
        throw new Error('Order not found');
      }

      const attributes = order.customAttributes || [];
      
      // 필수 동의 확인
      const priceConsent = attributes.find(
        attr => attr.key === this.consentKeys.priceAdjustment && attr.value === 'true'
      );
      const cancelConsent = attributes.find(
        attr => attr.key === this.consentKeys.noCancellation && attr.value === 'true'
      );
      const consentTimestamp = attributes.find(
        attr => attr.key === this.consentKeys.timestamp
      );

      // 동의 시간 유효성 검증 (1시간 이내)
      let isTimestampValid = true;
      if (consentTimestamp?.value) {
        const consentTime = new Date(consentTimestamp.value);
        const orderTime = new Date(order.createdAt);
        const timeDiff = orderTime - consentTime;
        isTimestampValid = timeDiff <= 3600000; // 1시간
      }

      return {
        hasRequiredConsents: !!(priceConsent && cancelConsent),
        isTimestampValid,
        details: {
          priceAdjustmentConsent: !!priceConsent,
          noCancellationConsent: !!cancelConsent,
          consentTimestamp: consentTimestamp?.value,
          orderCreatedAt: order.createdAt
        }
      };
    } catch (error) {
      logger.error(`[${SERVICE_NAME}] Error validating order consent:`, error);
      throw error;
    }
  }

  /**
   * 주문에 동의 관련 태그 추가
   */
  async tagOrderWithConsentStatus(orderId, consentValid) {
    const tags = consentValid 
      ? ['bunjang-consent-verified', 'bunjang-order']
      : ['bunjang-consent-missing', 'requires-review'];

    const mutation = `
      mutation addOrderTags($input: OrderInput!) {
        orderUpdate(input: $input) {
          order {
            id
            tags
          }
          userErrors {
            field
            message
          }
        }
      }
    `;

    try {
      const currentTags = await this.getOrderTags(orderId);
      const newTags = [...new Set([...currentTags, ...tags])];

      const response = await shopifyGraphqlRequest(mutation, {
        input: {
          id: orderId,
          tags: newTags
        }
      });

      if (response.data?.orderUpdate?.userErrors?.length > 0) {
        logger.error(`[${SERVICE_NAME}] Error adding tags to order:`, response.data.orderUpdate.userErrors);
      }

      return response.data?.orderUpdate?.order;
    } catch (error) {
      logger.error(`[${SERVICE_NAME}] Error tagging order:`, error);
      throw error;
    }
  }

  /**
   * 주문의 현재 태그 가져오기
   */
  async getOrderTags(orderId) {
    const query = `
      query getOrderTags($id: ID!) {
        order(id: $id) {
          id
          tags
        }
      }
    `;

    try {
      const response = await shopifyGraphqlRequest(query, { id: orderId });
      return response.data?.order?.tags || [];
    } catch (error) {
      logger.error(`[${SERVICE_NAME}] Error getting order tags:`, error);
      return [];
    }
  }

  /**
   * 주문에 메모 추가 (관리자용)
   */
  async addConsentNote(orderId, consentStatus) {
    const mutation = `
      mutation updateOrderNote($input: OrderInput!) {
        orderUpdate(input: $input) {
          order {
            id
            note
          }
          userErrors {
            field
            message
          }
        }
      }
    `;

    const noteContent = consentStatus.hasRequiredConsents
      ? `BUNJANG Consent Status: ✅ Verified
Price Adjustment Consent: ${consentStatus.details.priceAdjustmentConsent ? '✅' : '❌'}
No Cancellation Consent: ${consentStatus.details.noCancellationConsent ? '✅' : '❌'}
Consent Timestamp: ${consentStatus.details.consentTimestamp || 'N/A'}
Timestamp Valid: ${consentStatus.isTimestampValid ? '✅' : '❌'}`
      : `⚠️ BUNJANG Consent Missing - Manual verification required`;

    try {
      const response = await shopifyGraphqlRequest(mutation, {
        input: {
          id: orderId,
          note: noteContent
        }
      });

      return response.data?.orderUpdate?.order;
    } catch (error) {
      logger.error(`[${SERVICE_NAME}] Error adding consent note:`, error);
      throw error;
    }
  }

  /**
   * BUNJANG 컬렉션의 모든 상품에 필수 태그 추가
   */
  async ensureBunjangProductTags() {
    const query = `
      query getBunjangProducts($first: Int!, $after: String) {
        collection(handle: "bunjang") {
          products(first: $first, after: $after) {
            edges {
              node {
                id
                tags
              }
              cursor
            }
            pageInfo {
              hasNextPage
            }
          }
        }
      }
    `;

    const mutation = `
      mutation updateProductTags($input: ProductInput!) {
        productUpdate(input: $input) {
          product {
            id
            tags
          }
          userErrors {
            field
            message
          }
        }
      }
    `;

    let hasNextPage = true;
    let cursor = null;
    let updatedCount = 0;

    try {
      while (hasNextPage) {
        const response = await shopifyGraphqlRequest(query, {
          first: 50,
          after: cursor
        });

        const products = response.data?.collection?.products?.edges || [];
        hasNextPage = response.data?.collection?.products?.pageInfo?.hasNextPage || false;

        for (const { node: product, cursor: productCursor } of products) {
          cursor = productCursor;
          
          // bungjang 태그가 없으면 추가
          if (!product.tags.includes('bungjang')) {
            const updatedTags = [...product.tags, 'bungjang', 'requires-consent'];
            
            await shopifyGraphqlRequest(mutation, {
              input: {
                id: product.id,
                tags: updatedTags
              }
            });
            
            updatedCount++;
            logger.info(`[${SERVICE_NAME}] Added bungjang tags to product ${product.id}`);
          }
        }
      }

      logger.info(`[${SERVICE_NAME}] Updated ${updatedCount} products with bungjang tags`);
      return { updatedCount };
    } catch (error) {
      logger.error(`[${SERVICE_NAME}] Error ensuring BUNJANG product tags:`, error);
      throw error;
    }
  }

  /**
   * 주문 처리 전 동의 검증 (Webhook 핸들러용)
   */
  async processOrderWithConsentCheck(order) {
    try {
      // 1. BUNJANG 상품 포함 여부 확인
      const hasBunjangItems = await this.checkOrderHasBunjangItems(order.admin_graphql_api_id);
      
      if (!hasBunjangItems) {
        logger.info(`[${SERVICE_NAME}] Order ${order.id} has no BUNJANG items, skipping consent check`);
        return { requiresConsent: false, consentValid: true };
      }

      // 2. 동의 상태 검증
      const consentStatus = await this.validateOrderConsent(order.admin_graphql_api_id);
      
      // 3. 주문에 태그 추가
      await this.tagOrderWithConsentStatus(order.admin_graphql_api_id, consentStatus.hasRequiredConsents);
      
      // 4. 관리자 메모 추가
      await this.addConsentNote(order.admin_graphql_api_id, consentStatus);

      // 5. 동의가 없는 경우 처리
      if (!consentStatus.hasRequiredConsents) {
        logger.warn(`[${SERVICE_NAME}] Order ${order.id} missing BUNJANG consent`, consentStatus);
        
        // 이메일 알림 또는 추가 처리
        // await this.notifyAdminAboutMissingConsent(order);
      }

      return {
        requiresConsent: true,
        consentValid: consentStatus.hasRequiredConsents,
        consentDetails: consentStatus.details
      };
    } catch (error) {
      logger.error(`[${SERVICE_NAME}] Error processing order consent check:`, error);
      throw error;
    }
  }
}

// Webhook 핸들러 예제
const bunjangConsentService = new BunjangConsentService();

async function handleOrderCreatedWebhook(req, res) {
  const order = req.body;
  
  try {
    // BUNJANG 동의 검증
    const consentResult = await bunjangConsentService.processOrderWithConsentCheck(order);
    
    if (consentResult.requiresConsent && !consentResult.consentValid) {
      // 동의가 필요하지만 없는 경우
      logger.warn(`Order ${order.id} requires BUNJANG consent but none found`);
      
      // 옵션 1: 주문 플래그 지정 (권장)
      // 주문은 유지하되 수동 검토 필요로 표시
      
      // 옵션 2: 주문 취소 (신중히 사용)
      // await cancelOrder(order.id);
      
      // 옵션 3: 고객에게 이메일 발송
      // await sendConsentReminderEmail(order);
    }
    
    res.status(200).json({ 
      processed: true,
      consentStatus: consentResult
    });
  } catch (error) {
    logger.error('Order webhook processing error:', error);
    res.status(500).json({ error: error.message });
  }
}

module.exports = {
  BunjangConsentService,
  bunjangConsentService,
  handleOrderCreatedWebhook
};