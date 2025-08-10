// scripts/fetch-naver.js
// 네이버 서치애드 API 데이터 수집 및 Supabase 저장 (StatReport 기반)

import axios from 'axios';
import { createClient } from '@supabase/supabase-js';
import dotenv from 'dotenv';
import crypto from 'crypto';

// ========================================================================================
// 설정 및 상수
// ========================================================================================

dotenv.config();

/** @typedef {Object} NaverConfig */
const CONFIG = {
  // 네이버 API 설정
  NAVER: {
    API_KEY: process.env.DOK_NAVER_API_KEY,
    SECRET_KEY: process.env.DOK_NAVER_SECRET_KEY,
    CUSTOMER_ID: process.env.DOK_NAVER_CUSTOMER_ID,
    BASE_URL: 'https://api.searchad.naver.com',
    API_VERSION: '2'
  },
  
  // Supabase 설정
  SUPABASE: {
    URL: process.env.SUPABASE_URL,
    KEY: process.env.SUPABASE_KEY,
    TABLE: 'dok_naver_insights'
  },
  
  // 리포트 설정
  REPORT: {
    MAX_ATTEMPTS: 30,
    POLLING_INTERVAL: 10000, // 10초
    API_DELAY: 1000 // API 호출 간 대기 시간
  },
  
  // 광고 설정
  AD: {
    BRAND_SEARCH_DAILY_SPEND: 19486,
    VAT_RATE: 1.1, // 10% VAT
    KST_OFFSET: 9 * 60 * 60 * 1000 // UTC+9
  },
  
  // 광고 타입 매핑
  CAMPAIGN_TYPE_MAPPING: {
    'BRAND_SEARCH': 'BRAND_SEARCH_AD',
    'SHOPPING': 'SHOPPING_PRODUCT_AD',
    'WEB_SITE': 'TEXT_45'
  },
  
  // CSV 필드 매핑 (StatReport 문서 기준)
  AD_REPORT_FIELDS: {
    DATE: 0, CUSTOMER_ID: 1, CAMPAIGN_ID: 2, ADGROUP_ID: 3, KEYWORD_ID: 4,
    AD_ID: 5, BUSINESS_CHANNEL_ID: 6, MEDIA_CODE: 7, PC_MOBILE_TYPE: 8,
    IMPRESSIONS: 9, CLICKS: 10, COST: 11, SUM_AD_RANK: 12, VIEW_COUNT: 13
  },
  
  CONVERSION_REPORT_FIELDS: {
    DATE: 0, CUSTOMER_ID: 1, CAMPAIGN_ID: 2, ADGROUP_ID: 3, KEYWORD_ID: 4,
    AD_ID: 5, BUSINESS_CHANNEL_ID: 6, MEDIA_CODE: 7, PC_MOBILE_TYPE: 8,
    CONVERSION_METHOD: 9, CONVERSION_TYPE: 10, CONVERSION_COUNT: 11, CONVERSION_VALUE: 12
  }
};

// ========================================================================================
// 유틸리티 함수들
// ========================================================================================

/**
 * 대기 함수
 * @param {number} ms - 대기할 밀리초
 * @returns {Promise<void>}
 */
const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

/**
 * KST 어제 날짜 계산
 * @returns {string} YYYY-MM-DD 형식의 어제 날짜
 */
const getKSTYesterday = () => {
  const now = new Date();
  const kstNow = new Date(now.getTime() + CONFIG.AD.KST_OFFSET);
  const kstYesterday = new Date(kstNow.getTime() - 24 * 60 * 60 * 1000);
  return kstYesterday.toISOString().slice(0, 10);
};

/**
 * CSV 파싱 함수
 * @param {string} csvText - CSV 텍스트
 * @param {string} delimiter - 구분자 (기본값: 탭)
 * @returns {string[][]} 파싱된 CSV 데이터
 */
const parseCSV = (csvText, delimiter = '\t') => {
  const lines = csvText.trim().split('\n');
  if (lines.length < 1) return [];

  console.log('🔍 CSV 파싱 시작:', lines.length, '라인');
  
  const rows = lines
    .map(line => line.split(delimiter).map(v => v.trim()))
    .filter(values => values.length > 0 && values[0]);
  
  console.log('📊 파싱된 데이터 행 수:', rows.length);
  return rows;
};

/**
 * 메트릭 계산 함수들
 */
const calculateMetrics = {
  ctr: (clicks, impressions) => impressions > 0 ? clicks / impressions : 0,
  cpc: (spend, clicks) => clicks > 0 ? spend / clicks : 0,
  cvr: (conversions, clicks) => clicks > 0 ? conversions / clicks : 0,
  cpm: (spend, impressions) => impressions > 0 ? spend / (impressions / 1000) : 0,
  cpa: (spend, conversions) => conversions > 0 ? spend / conversions : 0,
  aov: (conversionValue, conversions) => conversions > 0 ? conversionValue / conversions : 0,
  roas: (conversionValue, spend) => spend > 0 ? conversionValue / spend : 0,
  avgRank: (sumAdRank, impressions) => impressions > 0 ? sumAdRank / impressions : 0
};

// ========================================================================================
// 네이버 API 클라이언트 클래스
// ========================================================================================

class NaverAPIClient {
  constructor() {
    this.baseURL = CONFIG.NAVER.BASE_URL;
    this.apiKey = CONFIG.NAVER.API_KEY;
    this.secretKey = CONFIG.NAVER.SECRET_KEY;
    this.customerId = CONFIG.NAVER.CUSTOMER_ID;
    
    // 환경변수 검증
    if (!this.apiKey || !this.secretKey || !this.customerId) {
      throw new Error('네이버 API 환경변수가 설정되지 않았습니다.');
    }
  }

  /**
   * 네이버 API 인증 서명 생성
   * @param {string} method - HTTP 메서드
   * @param {string} uri - API 엔드포인트
   * @param {string} timestamp - 타임스탬프
   * @returns {string} 서명
   */
  generateSignature(method, uri, timestamp) {
    const message = `${timestamp}.${method}.${uri}`;
    return crypto
      .createHmac('sha256', this.secretKey)
      .update(message)
      .digest('base64');
  }

  /**
   * API 요청 헤더 생성
   * @param {string} method - HTTP 메서드
   * @param {string} uri - API 엔드포인트
   * @returns {Object} 헤더 객체
   */
  createHeaders(method, uri) {
    const timestamp = Date.now().toString();
    const signature = this.generateSignature(method, uri, timestamp);

    return {
      'X-Timestamp': timestamp,
      'X-API-KEY': this.apiKey,
      'X-Customer': this.customerId,
      'X-Signature': signature,
      'X-API-Version': CONFIG.NAVER.API_VERSION,
      'Content-Type': 'application/json;charset=UTF-8'
    };
  }

  /**
   * 캠페인 목록 조회
   * @returns {Promise<Map<string, string>>} 캠페인 ID -> 광고 타입 매핑
   */
  async fetchCampaignTypes() {
    console.log('📋 캠페인 타입 정보 수집...');
    
    const campaignTypeMap = new Map();
    
    try {
      const uri = '/ncc/campaigns';
      const headers = this.createHeaders('GET', uri);
      
      const response = await axios.get(`${this.baseURL}${uri}`, { headers });

      if (response.data && Array.isArray(response.data)) {
        for (const campaign of response.data) {
          const campaignId = campaign.nccCampaignId;
          const campaignType = campaign.campaignTp || 'WEB_SITE';
          
          const adType = CONFIG.CAMPAIGN_TYPE_MAPPING[campaignType] || 'TEXT_45';
          campaignTypeMap.set(campaignId, adType);
        }
      }
      
      console.log(`✅ 캠페인 타입 매핑 완료: ${campaignTypeMap.size}개`);
      
    } catch (error) {
      console.error('⚠️ 캠페인 타입 수집 실패:', error.message);
    }
    
    return campaignTypeMap;
  }

  /**
   * StatReport 생성 요청
   * @param {string} reportType - 리포트 타입
   * @param {string} date - 날짜 (YYYY-MM-DD)
   * @param {string[]|null} fields - 필드 목록
   * @returns {Promise<Object>} 리포트 응답
   */
  async createStatReport(reportType, date, fields = null) {
    console.log(`📊 ${reportType} 리포트 생성 요청...`);
    
    const uri = '/stat-reports';
    const headers = this.createHeaders('POST', uri);

    const reportRequest = {
      reportTp: reportType,
      statDt: date.replace(/-/g, '')
    };

    if (fields) {
      reportRequest.fields = fields;
    }

    console.log('📝 리포트 요청:', JSON.stringify(reportRequest, null, 2));

    try {
      const response = await axios.post(`${this.baseURL}${uri}`, reportRequest, { headers });
      console.log(`✅ ${reportType} 리포트 생성 응답:`, response.data);
      return response.data;
    } catch (error) {
      console.error(`❌ ${reportType} 리포트 생성 실패:`, error.message);
      console.error('📄 에러 상세:', error.response?.data || error.response?.status);
      throw error;
    }
  }

  /**
   * StatReport 처리 상태 확인 및 CSV 다운로드
   * @param {string} reportJobId - 리포트 작업 ID
   * @param {string} reportType - 리포트 타입
   * @returns {Promise<Object>} 리포트 데이터
   */
  async processStatReport(reportJobId, reportType) {
    console.log(`🔄 ${reportType} 리포트 처리 시작: ${reportJobId}`);
    
    // 리포트 완료 대기
    const reportData = await this.waitForReportCompletion(reportJobId, reportType);
    
    // CSV 다운로드
    const csvData = await this.downloadCSV(reportData.downloadUrl, reportType);
    
    return parseCSV(csvData);
  }

  /**
   * 리포트 완료 대기
   * @param {string} reportJobId - 리포트 작업 ID
   * @param {string} reportType - 리포트 타입
   * @returns {Promise<Object>} 리포트 데이터
   */
  async waitForReportCompletion(reportJobId, reportType) {
    let attempts = 0;
    
    while (attempts < CONFIG.REPORT.MAX_ATTEMPTS) {
      await sleep(CONFIG.REPORT.POLLING_INTERVAL);
      attempts++;

      try {
        const uri = `/stat-reports/${reportJobId}`;
        const headers = this.createHeaders('GET', uri);
        
        const statusResponse = await axios.get(`${this.baseURL}${uri}`, { headers });
        const status = statusResponse.data?.status;
        
        console.log(`📋 ${reportType} 리포트 시도 ${attempts}: ${status}`);

        if (status === 'COMPLETE' || status === 'BUILT') {
          console.log(`✅ ${reportType} 리포트 처리 완료!`);
          return statusResponse.data;
        } 
        
        if (status === 'FAILED') {
          throw new Error(`${reportType} 리포트 생성 실패: ${JSON.stringify(statusResponse.data)}`);
        }
      } catch (statusError) {
        console.log(`⚠️ ${reportType} 리포트 상태 확인 시도 ${attempts} 실패:`, statusError.message);
      }
    }
    
    throw new Error(`${reportType} 리포트 처리 시간 초과`);
  }

  /**
   * CSV 파일 다운로드
   * @param {string} downloadUrl - 다운로드 URL
   * @param {string} reportType - 리포트 타입
   * @returns {Promise<string>} CSV 텍스트
   */
  async downloadCSV(downloadUrl, reportType) {
    if (!downloadUrl) {
      throw new Error(`${reportType} 리포트 다운로드 URL 없음`);
    }

    console.log(`📥 ${reportType} CSV 다운로드...`);
    
    const uri = new URL(downloadUrl).pathname;
    const headers = this.createHeaders('GET', uri);
    headers['Accept'] = 'text/csv;charset=UTF-8';
    
    const csvResponse = await axios.get(downloadUrl, {
      headers,
      responseType: 'text'
    });

    console.log(`🔍 ${reportType} CSV 샘플:`, csvResponse.data.substring(0, 200) + '...');
    return csvResponse.data;
  }
}

// ========================================================================================
// 데이터 변환 클래스
// ========================================================================================

class NaverDataTransformer {
  /**
   * AD 리포트 데이터 변환
   * @param {string[][]} csvData - CSV 데이터
   * @returns {Object[]} 변환된 AD 데이터
   */
  static transformAdData(csvData) {
    const { AD_REPORT_FIELDS: fields } = CONFIG;
    
    return csvData
      .filter(row => row.length >= 14)
      .map(row => ({
        date: row[fields.DATE],
        customerId: row[fields.CUSTOMER_ID],
        campaignId: row[fields.CAMPAIGN_ID],
        adgroupId: row[fields.ADGROUP_ID],
        keywordId: row[fields.KEYWORD_ID],
        adId: row[fields.AD_ID],
        businessChannelId: row[fields.BUSINESS_CHANNEL_ID],
        mediaCode: row[fields.MEDIA_CODE],
        pcMobileType: row[fields.PC_MOBILE_TYPE],
        impressions: parseInt(row[fields.IMPRESSIONS]) || 0,
        clicks: parseInt(row[fields.CLICKS]) || 0,
        cost: parseFloat(row[fields.COST]) || 0,
        sumAdRank: parseInt(row[fields.SUM_AD_RANK]) || 0,
        viewCount: parseInt(row[fields.VIEW_COUNT]) || 0
      }));
  }

  /**
   * 전환 리포트 데이터 변환
   * @param {string[][]} csvData - CSV 데이터
   * @returns {Object[]} 변환된 전환 데이터
   */
  static transformConversionData(csvData) {
    const { CONVERSION_REPORT_FIELDS: fields } = CONFIG;
    
    return csvData
      .filter(row => row.length >= 13)
      .map(row => ({
        date: row[fields.DATE],
        customerId: row[fields.CUSTOMER_ID],
        campaignId: row[fields.CAMPAIGN_ID],
        adgroupId: row[fields.ADGROUP_ID],
        keywordId: row[fields.KEYWORD_ID],
        adId: row[fields.AD_ID],
        businessChannelId: row[fields.BUSINESS_CHANNEL_ID],
        mediaCode: row[fields.MEDIA_CODE],
        pcMobileType: row[fields.PC_MOBILE_TYPE],
        conversionMethod: row[fields.CONVERSION_METHOD],
        conversionType: row[fields.CONVERSION_TYPE],
        conversionCount: parseInt(row[fields.CONVERSION_COUNT]) || 0,
        conversionValue: parseFloat(row[fields.CONVERSION_VALUE]) || 0
      }));
  }

  /**
   * Supabase 저장용 데이터 생성
   * @param {Object} aggregatedData - 집계된 데이터
   * @param {string} date - 날짜
   * @returns {Object[]} Supabase 저장용 데이터
   */
  static createSupabaseData({ powerlink, brand }, date) {
    const rows = [];

    // 파워링크 데이터
    if (powerlink.spend > 0 || powerlink.impressions > 0 || powerlink.clicks > 0) {
      const metrics = NaverDataTransformer.calculateAllMetrics(powerlink);
      
      rows.push({
        date,
        campaign: 'Naver SA',
        spend: Number(powerlink.spend.toFixed(2)),
        impressions: powerlink.impressions,
        clicks: powerlink.clicks,
        conversion: powerlink.conversion,
        conversion_value: Number(powerlink.conversionValue.toFixed(2)),
        quality_index: 0,
        ...metrics
      });
    }

    // 브랜드검색 데이터
    if (brand.spend > 0 || brand.impressions > 0 || brand.clicks > 0) {
      const metrics = NaverDataTransformer.calculateAllMetrics(brand);
      
      rows.push({
        date,
        campaign: 'Naver BS',
        spend: Number(brand.spend.toFixed(2)),
        impressions: brand.impressions,
        clicks: brand.clicks,
        conversion: brand.conversion,
        conversion_value: Number(brand.conversionValue.toFixed(2)),
        quality_index: 0,
        ...metrics
      });
    }

    return rows;
  }

  /**
   * 모든 메트릭 계산
   * @param {Object} data - 집계 데이터
   * @returns {Object} 계산된 메트릭들
   */
  static calculateAllMetrics(data) {
    const { spend, impressions, clicks, conversion, conversionValue, sumAdRank } = data;
    
    return {
      ctr: Number(calculateMetrics.ctr(clicks, impressions).toFixed(4)),
      cpc: Number(calculateMetrics.cpc(spend, clicks).toFixed(2)),
      cvr: Number(calculateMetrics.cvr(conversion, clicks).toFixed(4)),
      cpm: Number(calculateMetrics.cpm(spend, impressions).toFixed(2)),
      cpa: Number(calculateMetrics.cpa(spend, conversion).toFixed(2)),
      aov: Number(calculateMetrics.aov(conversionValue, conversion).toFixed(2)),
      roas: Number(calculateMetrics.roas(conversionValue, spend).toFixed(4)),
      rank_avg: Number(calculateMetrics.avgRank(sumAdRank, impressions).toFixed(2))
    };
  }
}

// ========================================================================================
// 데이터 집계 클래스
// ========================================================================================

class NaverDataAggregator {
  /**
   * 리포트 데이터 집계
   * @param {Object[]} adData - AD 리포트 데이터
   * @param {Object[]} conversionData - 전환 리포트 데이터
   * @param {Map<string, string>} campaignTypeMap - 캠페인 타입 매핑
   * @returns {Object} 집계된 데이터
   */
  static aggregateReports(adData, conversionData, campaignTypeMap) {
    console.log('🔄 데이터 병합 및 집계 시작...');
    
    // 캠페인별 성과 데이터 집계
    const campaignStats = NaverDataAggregator.aggregateAdData(adData);
    
    // 전환 데이터 병합
    NaverDataAggregator.mergeConversionData(campaignStats, conversionData);
    
    // 광고 타입별 집계
    const result = NaverDataAggregator.aggregateByAdType(campaignStats, campaignTypeMap);
    
    console.log('📊 집계 완료:');
    console.log('🔸 파워링크:', result.powerlink);
    console.log('🔸 브랜드검색:', result.brand);
    
    return result;
  }

  /**
   * AD 데이터 캠페인별 집계
   * @param {Object[]} adData - AD 리포트 데이터
   * @returns {Map<string, Object>} 캠페인별 집계 데이터
   */
  static aggregateAdData(adData) {
    const campaignStats = new Map();
    
    for (const ad of adData) {
      const { campaignId, impressions, clicks, cost, sumAdRank } = ad;
      
      if (!campaignStats.has(campaignId)) {
        campaignStats.set(campaignId, {
          impressions: 0, clicks: 0, cost: 0, sumAdRank: 0,
          conversions: 0, conversionValue: 0
        });
      }
      
      const stats = campaignStats.get(campaignId);
      stats.impressions += impressions;
      stats.clicks += clicks;
      stats.cost += cost;
      stats.sumAdRank += sumAdRank;
    }
    
    return campaignStats;
  }

  /**
   * 전환 데이터 병합
   * @param {Map<string, Object>} campaignStats - 캠페인별 집계 데이터
   * @param {Object[]} conversionData - 전환 리포트 데이터
   */
  static mergeConversionData(campaignStats, conversionData) {
    const campaignConversions = new Map();
    
    // 캠페인별 전환 데이터 집계 (중복 제거)
    for (const conv of conversionData) {
      const { campaignId, conversionCount, conversionValue } = conv;
      
      if (!campaignConversions.has(campaignId)) {
        campaignConversions.set(campaignId, { count: 0, value: 0 });
      }
      
      const convStats = campaignConversions.get(campaignId);
      convStats.count += conversionCount;
      convStats.value += conversionValue;
    }
    
    // 전환 데이터를 캠페인 통계에 병합
    for (const [campaignId, convData] of campaignConversions) {
      if (campaignStats.has(campaignId)) {
        const stats = campaignStats.get(campaignId);
        stats.conversions = convData.count;
        stats.conversionValue = convData.value;
      }
    }
  }

  /**
   * 광고 타입별 집계
   * @param {Map<string, Object>} campaignStats - 캠페인별 집계 데이터
   * @param {Map<string, string>} campaignTypeMap - 캠페인 타입 매핑
   * @returns {Object} 광고 타입별 집계 결과
   */
  static aggregateByAdType(campaignStats, campaignTypeMap) {
    const powerlink = {
      spend: 0, impressions: 0, clicks: 0, conversion: 0, conversionValue: 0, 
      sumAdRank: 0, campaignCount: 0
    };
    
    const brand = {
      spend: CONFIG.AD.BRAND_SEARCH_DAILY_SPEND,
      impressions: 0, clicks: 0, conversion: 0, conversionValue: 0, 
      sumAdRank: 0, campaignCount: 0
    };
    
    for (const [campaignId, stats] of campaignStats) {
      const adType = campaignTypeMap.get(campaignId) || 'TEXT_45';
      
      if (adType === 'BRAND_SEARCH_AD') {
        brand.impressions += stats.impressions;
        brand.clicks += stats.clicks;
        brand.conversion += stats.conversions;
        brand.conversionValue += stats.conversionValue;
        brand.sumAdRank += stats.sumAdRank;
        brand.campaignCount++;
      } else {
        powerlink.spend += stats.cost * CONFIG.AD.VAT_RATE;
        powerlink.impressions += stats.impressions;
        powerlink.clicks += stats.clicks;
        powerlink.conversion += stats.conversions;
        powerlink.conversionValue += stats.conversionValue;
        powerlink.sumAdRank += stats.sumAdRank;
        powerlink.campaignCount++;
      }
    }
    
    return { powerlink, brand };
  }
}

// ========================================================================================
// 메인 실행 함수
// ========================================================================================

/**
 * 네이버 광고 데이터 수집 메인 함수
 * @returns {Promise<void>}
 */
async function fetchNaverData() {
  const yesterday = getKSTYesterday();
  console.log(`\n📅 네이버 광고 데이터 수집 시작 (${yesterday})...`);

  try {
    // API 클라이언트 초기화
    const apiClient = new NaverAPIClient();
    
    // Supabase 클라이언트 초기화
    const supa = createClient(CONFIG.SUPABASE.URL, CONFIG.SUPABASE.KEY);

    // 1. 캠페인 타입 정보 수집
    const campaignTypeMap = await apiClient.fetchCampaignTypes();
    await sleep(CONFIG.REPORT.API_DELAY);
    
    // 2. AD 성과 리포트 수집
    const adReportData = await apiClient.createStatReport('AD', yesterday);
    const adJobId = adReportData?.reportJobId || adReportData?.id;
    
    if (!adJobId) {
      throw new Error('AD 리포트 작업 ID를 받지 못했습니다');
    }

    const adCsvData = await apiClient.processStatReport(adJobId, 'AD');
    const adData = NaverDataTransformer.transformAdData(adCsvData);
    
    console.log(`✅ AD 성과 데이터 ${adData.length}개 수집 완료`);
    if (adData.length > 0) {
      console.log('📊 AD 데이터 샘플:', adData[0]);
    }

    await sleep(CONFIG.REPORT.API_DELAY);
    
    // 3. 전환 리포트 수집
    const convReportData = await apiClient.createStatReport('AD_CONVERSION', yesterday);
    const convJobId = convReportData?.reportJobId || convReportData?.id;
    
    if (!convJobId) {
      throw new Error('AD_CONVERSION 리포트 작업 ID를 받지 못했습니다');
    }

    const convCsvData = await apiClient.processStatReport(convJobId, 'AD_CONVERSION');
    const conversionData = NaverDataTransformer.transformConversionData(convCsvData);
    
    console.log(`✅ 전환 데이터 ${conversionData.length}개 수집 완료`);
    if (conversionData.length > 0) {
      console.log('📊 전환 데이터 샘플:', conversionData[0]);
    }
    
    // 4. 데이터 집계
    const aggregatedData = NaverDataAggregator.aggregateReports(
      adData, conversionData, campaignTypeMap
    );
    
    // 5. Supabase 저장용 데이터 생성
    const rows = NaverDataTransformer.createSupabaseData(aggregatedData, yesterday);
    
    console.log(`📝 처리된 네이버 데이터 (${rows.length}건):`, rows);
    
    // 6. Supabase 저장
    if (rows.length > 0) {
      const now = new Date().toISOString();
      rows.forEach(row => {
        row.updated_at = now;
      });
      
      console.log('💾 Supabase에 네이버 데이터 저장 중...');
      const { data, error } = await supa
        .from(CONFIG.SUPABASE.TABLE)
        .upsert(rows, { onConflict: ['date', 'campaign'] });

      if (error) {
        console.error('❌ Supabase 에러:', error);
        throw error;
      }

      console.log(`✅ ${yesterday} 네이버 데이터 ${rows.length}건 저장 완료`);
    } else {
      console.log('⚠️ 저장할 네이버 데이터가 없습니다.');
    }

  } catch (error) {
    console.error('💥 네이버 데이터 수집 실패:', error.message);
    throw error;
  }
}

// ========================================================================================
// 스크립트 실행
// ========================================================================================

// 스크립트 직접 실행 시
if (import.meta.url === `file://${process.argv[1]}`) {
  fetchNaverData().catch(err => {
    console.error(err);
    process.exit(1);
  });
}

export { fetchNaverData }; 