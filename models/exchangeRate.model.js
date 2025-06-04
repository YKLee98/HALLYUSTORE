// src/models/exchangeRate.model.js
const mongoose = require('mongoose');

const exchangeRateSchema = new mongoose.Schema({
  base: { // 기준 통화 (예: "USD")
    type: String,
    required: true,
    trim: true,
    uppercase: true,
    index: true, // 기본 통화로 조회
  },
  rates: { // 다른 통화 대비 환율 맵 (예: { "KRW": 1350.25, "JPY": 150.5 })
    type: Map,
    of: Number,
    required: true,
  },
  // 편의를 위해 자주 사용될 환율은 별도 필드로 저장 가능
  krwToUsdRate: { // 1 KRW가 몇 USD인지 (계산된 값)
    type: Number,
    // required: true, // rates.KRW가 있으면 계산 가능하므로 필수는 아닐 수 있음
  },
  lastUpdatedByApp: { // 이 미들웨어 시스템에서 DB 문서를 마지막으로 업데이트한 시간
    type: Date,
    default: Date.now,
    required: true,
  },
  sourceApiTimestamp: { // 환율 정보를 제공한 외부 API의 데이터 기준 시각 (UNIX 타임스탬프 또는 Date)
    type: Date,
    required: true,
  },
  sourceName: { // 환율 정보 출처 (예: "openexchangerates.org")
    type: String,
    trim: true,
  }
}, {
  timestamps: true, // createdAt, updatedAt (Mongoose 문서 자체의 생성/수정 시간) 자동 생성
  versionKey: false, // __v 필드 사용 안 함
});

// 단일 문서만 관리할 것이므로 (예: base: 'USD'), unique 인덱스는 upsert와 함께 사용 시 주의.
// findOneAndUpdate의 upsert 옵션으로 고유성을 보장하는 것이 일반적.
// exchangeRateSchema.index({ base: 1 }, { unique: true });

// krwToUsdRate 자동 계산 (rates.KRW 변경 시) - 선택적 스키마 로직
exchangeRateSchema.pre('save', function(next) {
  if (this.isModified('rates') && this.rates.has('KRW') && this.base === 'USD') {
    const usdToKrw = this.rates.get('KRW');
    if (usdToKrw && usdToKrw > 0) {
      this.krwToUsdRate = 1 / usdToKrw;
    } else {
      this.krwToUsdRate = undefined; // 유효하지 않으면 제거
    }
  }
  next();
});


const ExchangeRate = mongoose.model('ExchangeRate', exchangeRateSchema);

module.exports = ExchangeRate;
